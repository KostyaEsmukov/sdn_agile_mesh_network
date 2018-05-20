#!/usr/bin/env python3

import asyncio
import logging
from collections import namedtuple
from logging import getLogger

from agile_mesh_network.lib.rpc import RpcBroadcast, RpcCommand, RpcSession, RpcUnixServer

logger = getLogger('layers_manager')
logging.basicConfig(level=logging.INFO)


class LocalTunnel:
    def __init__(self, src_mac, dst_mac, process_manager):
        self.src_mac = src_mac
        self.dst_mac = dst_mac
        self.process_manager = process_manager

    @property
    def is_dead(self):
        """A final state. Tunnel won't be alive anymore.
        It needs to be stopped.
        """
        return self.process_manager.is_dead

    @property
    def is_tunnel_active(self):
        """Tunnel is alive."""
        return self.process_manager.is_tunnel_active

    def __hash__(self):
        return hash((self.src_mac, self.dst_mac))

    def __eq__(self, other):
        if not other or not isinstance(other, type(self)):
            return False
        return self.src_mac == other.src_mac and self.dst_mac == other.dst_mac

    def __ne__(self, other):
        return not (self == other)

    def serialize(self):
        return dict(src_mac=src_mac, dst_mac=dst_mac,
                    layers=process_manager.layers)


class OpenvpnProcessManager:
    """Manages openvpn processes."""
    layers = ('openvpn',)

    def __init__(self, dst_mac, protocol, remote):
        pass

    async def start(self, timeout):
        logger.warning('Pretending to start openvpn!')

    @property
    def is_tunnel_active(self):
        pass

    @property
    def is_dead(self):
        pass
    # TODO stop??


class TunnelsState:
    """Local state (tunnels + mac addresses)."""

    def __init__(self):
        self.tunnel_created_callback = None
        self.tunnels = set()

    async def create_tunnel(self, src_mac, dst_mac, timeout, layers):
        pm = OpenvpnProcessManager(dst_mac, **layers['openvpn'])
        lt = LocalTunnel(src_mac, dst_mac, pm)
        if lt in self.tunnels:
            lt_old = self.tunnels[lt]
            if lt_old.is_dead:
                self.tunnels.pop(lt_old)
            else:
                raise ValueError("tunnel is already created")
        self.tunnels.add(lt)
        await pm.start(timeout)
        tunnel = lt.serialize()
        if self.tunnel_created_callback:
            await self.tunnel_created_callback(tunnel)
        return tunnel

    async def active_tunnels(self):
        return [lt.serialize() for lt in self.tunnels]

    def register_tunnel_created_callback(self, callback):
        assert not self.tunnel_created_callback
        self.tunnel_created_callback = callback

    async def close_tunnels_wait(self):
        pass  # TODO


class LocalControlChannel:
    """Provides RPC to the ryu app."""

    # socket_path = '/Users/kostya/layers.sock'
    socket_path = '/var/run/layers_manager.sock'

    def __init__(self, tunnels_state, *, loop):
        self.rpc_server = RpcUnixServer(self.socket_path,
                                        self._handle_command, loop=loop)
        self._tunnels_state = tunnels_state
        tunnels_state.register_tunnel_created_callback(self.notify_tunnel_created)

    def __str__(self):
        return f"LocalControlChannel server at {self.socket_path}"

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
        tunnels = await self._tunnels_state.active_tunnels()
        return {"tunnels": tunnels}

    async def _command_create_tunnel(self, src_mac, dst_mac, timeout, layers):
        tunnel = await self._tunnels_state.create_tunnel(
            src_mac, dst_mac, timeout, layers)
        tunnels = await self._tunnels_state.active_tunnels()
        # TODO deal with the duplicated response? here and in the notify_tunnel_created.
        return {"tunnel": tunnel, "tunnels": tunnels}

    # TODO stop_tunnel

    async def notify_tunnel_created(self, tunnel):
        tunnels = await self._tunnels_state.active_tunnels()
        for s in list(self.rpc_server.sessions):
            if not s.is_closed:
                await s.issue_broadcast('tunnel_created',
                                        {"tunnel": tunnel, "tunnels": tunnels})


class TcpBalancer:
    def __init__(self, tcp_port, tcp_host='0.0.0.0'):
        self.tcp_host = tcp_host
        self.tcp_port = tcp_port
        self.server = None

    def __str__(self):
        return f"TcpBalancer server at {self.tcp_host}:{self.tcp_port}"

    async def start_server(self, loop):
        self.server = await asyncio.start_server(self._handle_accept,
                                                 self.tcp_host, self.tcp_port,
                                                 loop=loop)

    async def close_wait(self):
        self.server.close()
        await self.server.wait_closed()

    async def _handle_accept(self, reader, writer):
        print('hi')
        pass


def main():
    logger.info('Starting...')
    loop = asyncio.get_event_loop()

    tunnels_state = TunnelsState()

    local_control = LocalControlChannel(tunnels_state, loop=loop)
    logger.info(f'Starting {local_control}')
    loop.run_until_complete(local_control.start_server())

    openvpn_balancer = TcpBalancer(1194)
    logger.info(f'Starting {openvpn_balancer}')
    loop.run_until_complete(openvpn_balancer.start_server(loop=loop))

    try:
        logger.info('Running forever...')
        loop.run_forever()
    except KeyboardInterrupt:
        # TODO graceful shutdown
        pass

    logger.info(f'Closing {openvpn_balancer}...')
    loop.run_until_complete(openvpn_balancer.close_wait())
    logger.info(f'Closing all tunnels...')
    loop.run_until_complete(tunnels_state.close_tunnels_wait())
    logger.info(f'Closing {local_control}...')
    loop.run_until_complete(local_control.close_wait())
    loop.close()


if __name__ == '__main__':
    main()
