#!/usr/bin/env python3

import asyncio
import logging
from collections import namedtuple
from logging import getLogger

from agile_mesh_network.common.models import TunnelModel, LayersDescriptionModel
from agile_mesh_network.common.rpc import (
    RpcBroadcast, RpcCommand, RpcSession, RpcUnixServer
)
from agile_mesh_network.negotiator.tunnel import PendingTunnel

logger = getLogger('negotiator')
logging.basicConfig(level=logging.INFO)


class TunnelsState:
    """Local state (tunnels + mac addresses)."""

    def __init__(self, *, loop):
        self.tunnel_created_callback = None
        self.tunnels = {}
        self.loop = loop

    async def create_tunnel(self, src_mac, dst_mac, timeout,
                            layers: LayersDescriptionModel):
        pending_tunnel = PendingTunnel.tunnel_intention_for_initiator(
            src_mac, dst_mac, layers, timeout)
        tunnel_intention = pending_tunnel.tunnel_intention

        old_tunnel = self.tunnels.get(tunnel_intention)
        if old_tunnel:
            if old_tunnel.is_dead:
                self.tunnels.pop(old_tunnel)
            else:
                raise ValueError(f"tunnel {src_mac}<->{dst_mac} is already "
                                 "created")
        # Prevent concurrent tunnel creations.
        self.tunnels[tunnel_intention] = tunnel_intention
        tunnel = await pending_tunnel.create_tunnel(loop=self.loop)
        assert isinstance(self.tunnels[tunnel], TunnelIntention)
        self.tunnels[tunnel] = tunnel

        tunnel_model = tunnel.model()
        if self.tunnel_created_callback:
            await self.tunnel_created_callback(tunnel_model)
        return tunnel_model

    def active_tunnels(self):
        return [lt.model() for lt in self.tunnels.values()]

    def register_tunnel_created_callback(self, callback):
        assert not self.tunnel_created_callback
        self.tunnel_created_callback = callback

    # TODO notify via RPC when a tunnel is destroyed

    async def close_tunnels_wait(self):
        pass  # TODO

    def create_tunnel_from_transport(self, transport) -> asyncio.Protocol:
        protocol, aw_pending_tunnel = \
            PendingTunnel.tunnel_intention_for_responder(transport)
        fut = asyncio.ensure_future(aw_pending_tunnel, loop=self.loop)
        # TODO fut callback - close on exc and verify on succ.
        return protocol


class RpcResponder:
    """Provides RPC to the ryu app."""

    # socket_path = '/Users/kostya/amn_negotiator.sock'
    socket_path = '/var/run/amn_negotiator.sock'

    def __init__(self, tunnels_state, *, loop):
        self.rpc_server = RpcUnixServer(self.socket_path,
                                        self._handle_command, loop=loop)
        self._tunnels_state = tunnels_state
        tunnels_state.register_tunnel_created_callback(self.notify_tunnel_created)

    def __str__(self):
        return f"RpcResponder server at {self.socket_path}"

    async def start_server(self):
        await self.rpc_server.start()

    async def close_wait(self):
        await self.rpc_server.close_wait()

    async def _handle_command(self, session, msg):
        assert not isinstance(msg, RpcBroadcast)
        assert isinstance(msg, RpcCommand)

        cmd = getattr(self, f'_command_{msg.name}')
        try:
            payload = await cmd(**msg.kwargs)
        except Exception as e:
            await msg.respond(e)
            raise
        else:
            await msg.respond(payload)

    async def _command_dump_tunnels_state(self):
        tunnels = self._tunnels_state.active_tunnels()
        tunnels = [m.asdict() for m in tunnels]
        return {"tunnels": tunnels}

    async def _command_create_tunnel(self, src_mac, dst_mac, timeout, layers):
        layers = LayersDescriptionModel(**layers)
        tunnel = await self._tunnels_state.create_tunnel(
            src_mac, dst_mac, timeout, layers)
        tunnels = self._tunnels_state.active_tunnels()
        tunnels = [m.asdict() for m in tunnels]
        # TODO deal with the duplicated response? here and in the notify_tunnel_created.
        return {"tunnel": tunnel.asdict(), "tunnels": tunnels}

    # TODO stop_tunnel RPC command

    async def notify_tunnel_created(self, tunnel: TunnelModel):
        tunnels = self._tunnels_state.active_tunnels()
        tunnels = [m.asdict() for m in tunnels]
        for s in list(self.rpc_server.sessions):
            if not s.is_closed:
                await s.issue_broadcast(
                    'tunnel_created',
                    {"tunnel": tunnel.asdict(), "tunnels": tunnels})


class TcpExteriorServerProtocol(asyncio.Protocol):
    def __init__(self, protocol_factory):
        self.protocol_factory = protocol_factory
        self.protocol = None

    def connection_made(self, transport):
        self.protocol = self.protocol_factory(transport)
        self.protocol.connection_made(transport)

    def data_received(self, data):
        self.protocol.data_received(data)

    def connection_lost(self, exc):
        self.protocol.connection_lost(exc)


class TcpExteriorServer:
    def __init__(self, tunnels_state, *, loop, tcp_port=None, tcp_host='0.0.0.0'):
        self.tcp_host = tcp_host
        self.tcp_port = tcp_port
        self.server = None
        self._tunnels_state = tunnels_state
        self.loop = loop

    def __str__(self):
        return f"TcpExteriorServer server at {self.tcp_host}:{self.tcp_port}"

    async def start_server(self):
        self.server = await self.loop.create_server(
            lambda: TcpExteriorServerProtocol(
                self._tunnels_state.create_tunnel_from_transport),
            self.tcp_host, self.tcp_port)
        assert 1 == len(self.server.sockets)
        _, self.tcp_port = self.server.sockets[0].getsockname()

    async def close_wait(self):
        self.server.close()
        await self.server.wait_closed()


def main():
    logger.info('Starting...')
    loop = asyncio.get_event_loop()

    tunnels_state = TunnelsState(loop=loop)

    rpc_responder = RpcResponder(tunnels_state, loop=loop)
    logger.info(f'Starting {rpc_responder}')
    loop.run_until_complete(rpc_responder.start_server())

    tcp_server = TcpExteriorServer(tunnels_state, tcp_port=1194, loop=loop)
    logger.info(f'Starting {tcp_server}')
    loop.run_until_complete(tcp_server.start_server())

    try:
        logger.info('Running forever...')
        loop.run_forever()
    except KeyboardInterrupt:
        # TODO graceful shutdown
        pass

    logger.info(f'Closing {tcp_server}...')
    loop.run_until_complete(tcp_server.close_wait())
    logger.info(f'Closing all tunnels...')
    loop.run_until_complete(tunnels_state.close_tunnels_wait())
    logger.info(f'Closing {rpc_responder}...')
    loop.run_until_complete(rpc_responder.close_wait())
    loop.close()


if __name__ == '__main__':
    main()
