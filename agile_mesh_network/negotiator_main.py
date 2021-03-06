#!/usr/bin/env python3

import asyncio
import functools
import logging
import signal
import sys
from logging import getLogger
from typing import Awaitable, Callable, Dict, Optional, Sequence

import click
from async_exit_stack import AsyncExitStack

from agile_mesh_network.common.models import LayersDescriptionRPCModel, TunnelModel
from agile_mesh_network.common.rpc import RPCBroadcast, RPCCommand, RPCUnixServer
from agile_mesh_network.common.types import MACAddress
from agile_mesh_network.negotiator.layers import openvpn_config
from agile_mesh_network.negotiator.tunnel import (
    BaseTunnel, PendingTunnel, TunnelIntention
)

logger = getLogger("negotiator")
logging.basicConfig(level=logging.INFO)


class TunnelsState:
    """Local state (tunnels + mac addresses)."""

    def __init__(self, *, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        self.tunnel_created_callback: Optional[
            Callable[[TunnelModel], Awaitable[None]]
        ] = None
        self.tunnel_destroyed_callback: Optional[
            Callable[[TunnelModel], Awaitable[None]]
        ] = None
        self.tunnels: Dict[BaseTunnel, BaseTunnel] = {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        logger.info(f"Closing all tunnels...")
        await self.close_tunnels_wait()

    async def _create_pending_tunnel(
        self, pending_tunnel: PendingTunnel
    ) -> TunnelModel:
        tunnel_intention = pending_tunnel.tunnel_intention

        old_tunnel = self.tunnels.get(tunnel_intention)
        if old_tunnel:
            if old_tunnel.is_dead:
                self.tunnels.pop(old_tunnel)
            else:
                raise ValueError(f"tunnel {old_tunnel} is already created")
        # Prevent concurrent tunnel creations.
        self.tunnels[tunnel_intention] = tunnel_intention
        tunnel = await pending_tunnel.create_tunnel()
        assert isinstance(self.tunnels[tunnel], TunnelIntention)
        self.tunnels[tunnel] = tunnel

        tunnel.process_manager.add_dead_callback(
            functools.partial(self._dead_tunnel_handler, tunnel)
        )

        tunnel_model = tunnel.model()
        if self.tunnel_created_callback:
            await self.tunnel_created_callback(tunnel_model)
        return tunnel_model

    def _dead_tunnel_handler(self, tunnel: BaseTunnel) -> None:
        self.tunnels.pop(tunnel, None)

        async def task(tunnel_model: TunnelModel):
            try:
                assert self.tunnel_destroyed_callback
                await self.tunnel_destroyed_callback(tunnel_model)
            except:
                logging.error("Error while destroying a tunnel", exc_info=True)

        if self.tunnel_destroyed_callback:
            tunnel_model = tunnel.model()
            asyncio.ensure_future(task(tunnel_model), loop=self._loop)

    async def create_tunnel(
        self,
        src_mac: MACAddress,
        dst_mac: MACAddress,
        timeout: float,
        layers: LayersDescriptionRPCModel,
    ) -> TunnelModel:
        pending_tunnel = PendingTunnel.tunnel_intention_for_initiator(
            src_mac, dst_mac, layers, timeout
        )
        tunnel_model = await self._create_pending_tunnel(pending_tunnel)
        return tunnel_model

    def active_tunnels(self) -> Sequence[TunnelModel]:
        return [lt.model() for lt in self.tunnels.values()]

    def register_tunnel_created_callback(self, callback):
        if callback:
            assert not self.tunnel_created_callback
        self.tunnel_created_callback = callback

    def register_tunnel_destroyed_callback(self, callback):
        if callback:
            assert not self.tunnel_destroyed_callback
        self.tunnel_destroyed_callback = callback

    async def close_tunnels_wait(self):
        for tunnel in self.tunnels:
            tunnel.close()
        # TODO ?? wait until closed?

    def create_tunnel_from_protocol(self) -> asyncio.Protocol:
        protocol, aw_pending_tunnel = PendingTunnel.tunnel_intention_for_responder()

        async def task():
            try:
                with protocol.pipe_context:
                    pending_tunnel: PendingTunnel = await aw_pending_tunnel
                    await self._create_pending_tunnel(pending_tunnel)
            except:
                logging.error("Error while creating a responder tunnel", exc_info=True)

        asyncio.ensure_future(task(), loop=self._loop)
        return protocol


class RPCResponder:
    """Provides RPC to the ryu app."""

    socket_path = "/var/run/amn_negotiator.sock"

    def __init__(self, tunnels_state: TunnelsState, socket_path: str = None) -> None:
        if socket_path:
            self.socket_path = socket_path
        self.rpc_server = RPCUnixServer(self.socket_path, self._handle_command)
        self._tunnels_state = tunnels_state

    def __str__(self):
        return f"RPCResponder server at {self.socket_path}"

    async def __aenter__(self):
        logger.info(f"Starting {self}")
        await self.start_server()
        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        logger.info(f"Closing {self}...")
        await self.close_wait()

    async def start_server(self):
        self._tunnels_state.register_tunnel_created_callback(self.notify_tunnel_created)
        self._tunnels_state.register_tunnel_destroyed_callback(
            self.notify_tunnel_destroyed
        )
        await self.rpc_server.start()

    async def close_wait(self):
        await self.rpc_server.close_wait()
        self._tunnels_state.register_tunnel_created_callback(None)
        self._tunnels_state.register_tunnel_destroyed_callback(None)

    async def _handle_command(self, session, msg):
        assert not isinstance(msg, RPCBroadcast)
        assert isinstance(msg, RPCCommand)

        cmd: Callable[..., Awaitable[Dict]] = getattr(self, f"_command_{msg.name}")
        try:
            payload: Dict = await cmd(**msg.kwargs)
        except Exception as e:
            await msg.respond(e)
            raise
        else:
            await msg.respond(payload)

    async def _command_dump_tunnels_state(self):
        tunnels: Sequence[Dict] = [
            m.asdict() for m in self._tunnels_state.active_tunnels()
        ]
        return {"tunnels": tunnels}

    async def _command_create_tunnel(self, src_mac, dst_mac, timeout, layers):
        layers = LayersDescriptionRPCModel(**layers)
        tunnel: TunnelModel = await self._tunnels_state.create_tunnel(
            src_mac, dst_mac, timeout, layers
        )
        tunnels: Sequence[Dict] = [
            m.asdict() for m in self._tunnels_state.active_tunnels()
        ]
        # TODO deal with the duplicated response? here and in the notify_tunnel_created.
        return {"tunnel": tunnel.asdict(), "tunnels": tunnels}

    # TODO stop_tunnel RPC command

    async def notify_tunnel_created(self, tunnel: TunnelModel):
        await self._broadcast_tunnels("tunnel_created", tunnel)

    async def notify_tunnel_destroyed(self, tunnel: TunnelModel):
        await self._broadcast_tunnels("tunnel_destroyed", tunnel)

    async def _broadcast_tunnels(self, topic_name: str, tunnel: TunnelModel) -> None:
        tunnels = self._tunnels_state.active_tunnels()
        tunnels = [m.asdict() for m in tunnels]
        for s in list(self.rpc_server.sessions):
            if s.is_connected:
                await s.issue_broadcast(
                    topic_name, {"tunnel": tunnel.asdict(), "tunnels": tunnels}
                )


class TCPExteriorServerProtocol(asyncio.Protocol):
    def __init__(self, protocol_factory):
        try:
            self.protocol: asyncio.Protocol = protocol_factory()
        except:
            logger.error("%s: __init__", type(self).__name__, exc_info=True)
            raise

    def connection_made(self, transport):
        self.protocol.connection_made(transport)

    def data_received(self, data):
        self.protocol.data_received(data)

    def connection_lost(self, exc):
        self.protocol.connection_lost(exc)


class TCPExteriorServer:
    def __init__(
        self,
        tunnels_state: TunnelsState,
        *,
        tcp_port: int = None,
        tcp_host: str = "0.0.0.0",
    ) -> None:
        self.tcp_host = tcp_host
        self.tcp_port = tcp_port
        self.server: Optional[asyncio.AbstractServer] = None
        self._tunnels_state = tunnels_state

    def __str__(self):
        return f"TCPExteriorServer server at {self.tcp_host}:{self.tcp_port}"

    async def __aenter__(self):
        logger.info(f"Starting {self}")
        await self.start_server()
        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        logger.info(f"Closing {self}...")
        await self.close_wait()

    async def start_server(self):
        loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
        self.server = await loop.create_server(
            lambda: TCPExteriorServerProtocol(
                self._tunnels_state.create_tunnel_from_protocol
            ),
            self.tcp_host,
            self.tcp_port,
        )
        assert 1 == len(self.server.sockets)
        _, self.tcp_port = self.server.sockets[0].getsockname()

    async def close_wait(self):
        self.server.close()
        await self.server.wait_closed()


async def main_async_exit_stack(*, tcp_port, socket_path):
    loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
    stack = AsyncExitStack()
    tunnels_state = await stack.enter_async_context(TunnelsState(loop=loop))
    await stack.enter_async_context(
        RPCResponder(tunnels_state, socket_path=socket_path)
    )
    await stack.enter_async_context(TCPExteriorServer(tunnels_state, tcp_port=tcp_port))
    return stack


def stop_loop(loop):
    loop.stop()


@click.command()
@click.option(
    "--openvpn-tcp-port",
    default=11194,
    show_default=True,
    help="Port to bind for accepting openvpn connections via another negotiator.",
)
@click.option(
    "--openvpn-bin-path",
    default=openvpn_config.exe_path,
    show_default=True,
    help="Path to openvpn executable.",
)
@click.option(
    "--openvpn-client-config-path",
    default=openvpn_config.client_config_path,
    show_default=True,
    help="Path to openvpn client (initiator) config.",
)
@click.option(
    "--openvpn-server-config-path",
    default=openvpn_config.server_config_path,
    show_default=True,
    help="Path to openvpn server (responder) config.",
)
@click.option(
    "--rpc-unix-sock",
    default=RPCResponder.socket_path,
    show_default=True,
    help="Path of Unix socket to bind for accepting RPC requests.",
)
def negotiator(
    openvpn_tcp_port,
    openvpn_bin_path,
    openvpn_client_config_path,
    openvpn_server_config_path,
    rpc_unix_sock,
):
    """Negotiator daemon. Initializes tunnels by commands from RPC.
    """

    logger.info("Starting...")
    loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

    openvpn_config.exe_path = openvpn_bin_path
    openvpn_config.client_config_path = openvpn_client_config_path
    openvpn_config.server_config_path = openvpn_server_config_path
    try:
        openvpn_config.validate()
    except Exception as e:
        click.echo(f"Openvpn configuration validation failure: {e}")
        sys.exit(1)

    stack = loop.run_until_complete(
        main_async_exit_stack(tcp_port=openvpn_tcp_port, socket_path=rpc_unix_sock)
    )

    for signame in ("SIGINT", "SIGTERM"):
        loop.add_signal_handler(
            getattr(signal, signame), functools.partial(stop_loop, loop)
        )
    # TODO add SIGHUP handler, printing out the process_manager stdout
    try:
        logger.info("Running forever...")
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        loop.run_until_complete(stack.aclose())
        loop.run_until_complete(loop.shutdown_asyncgens())
        pending = asyncio.Task.all_tasks()
        loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        loop.close()


if __name__ == "__main__":
    negotiator()
