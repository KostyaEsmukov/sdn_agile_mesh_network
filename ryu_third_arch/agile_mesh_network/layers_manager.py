#!/usr/bin/env python3

import asyncio
import logging
from collections import namedtuple
from logging import getLogger

logger = getLogger('layers_manager')
logging.basicConfig(level=logging.INFO)

LocalTunnel = namedtuple('LocalTunnel', [
    'mac_local',
    'mac_remote',
    'openvpn_pid',
])


class LocalControlChannel:
    socket_path = '/Users/kostya/layers.sock'
    # socket_path = '/var/run/layers_manager.sock'

    def __init__(self):
        self.server = None

    def __str__(self):
        return f"LocalControlChannel server at {self.socket_path}"

    async def start_server(self, loop):
        self.server = await asyncio.start_unix_server(
            self._handle_accept, self.socket_path, loop=loop)

    async def close_wait(self):
        self.server.close()
        await self.server.wait_closed()

    async def _handle_accept(self, reader, writer):
        # TODO !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        # dump state
        # create tunnel
        # (back) - new tunnel created

        # keep open connections (notify from outside)
        # mark command end
        print('hi')
        pass


class OpenvpnManager:
    # start/stop
    pass


class TunnelsState:
    # local state (tunnels + mac addresses)
    pass


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

    local_control = LocalControlChannel()
    logger.info(f'Starting {local_control}')
    loop.run_until_complete(local_control.start_server(loop=loop))

    openvpn_balancer = TcpBalancer(1194)
    logger.info(f'Starting {openvpn_balancer}')
    loop.run_until_complete(openvpn_balancer.start_server(loop=loop))

    try:
        logger.info('Running forever...')
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    logger.info(f'Closing {openvpn_balancer}...')
    loop.run_until_complete(openvpn_balancer.close_wait())
    logger.info(f'Closing {local_control}...')
    loop.run_until_complete(local_control.close_wait())
    loop.close()


if __name__ == '__main__':
    main()

