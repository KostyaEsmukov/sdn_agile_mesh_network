import asyncio
from abc import ABCMeta, abstractmethod
from typing import Tuple

from agile_mesh_network.common.models import LayersDescriptionModel
from agile_mesh_network.negotiator.tunnel_protocols import PipeContext


class ProcessManager(metaclass=ABCMeta):
    @staticmethod
    def from_layers_responder(dst_mac, layers: LayersDescriptionModel,
                              pipe_context: PipeContext
                              ) -> 'ProcessManager':
        # TODO other layers
        assert ('openvpn',) == tuple(layers.keys()), \
            'Only openvpn is implemented yet'

        return OpenvpnResponderProcessManager(dst_mac, layers['openvpn'], pipe_context)

    @staticmethod
    def from_layers_initiator(dst_mac, layers: LayersDescriptionModel,
                              pipe_context: PipeContext
                              ) -> 'ProcessManager':
        # TODO other layers
        assert ('openvpn',) == tuple(layers.keys()), \
            'Only openvpn is implemented yet'

        return OpenvpnInitiatorProcessManager(dst_mac, layers['openvpn'], pipe_context)

    @abstractmethod
    async def start(self, timeout=None):
        pass

    @property
    @abstractmethod
    def is_tunnel_active(self):
        """Tunnel is alive."""
        pass

    @property
    @abstractmethod
    def is_dead(self):
        """A final state. Tunnel won't be alive anymore.
        It needs to be stopped.
        """
        pass


class OpenvpnProcessManager(ProcessManager):
    """Manages openvpn processes."""

    def __init__(self, dst_mac, protocol, remote: Tuple[str, int]):
        # TODO setup configs, certs
        pass

    async def start(self, timeout=None):
        logger.warning('Pretending to start openvpn!')
        # TODO impl!

    # @property
    # def is_tunnel_active(self):
    #     pass

    # @property
    # def is_dead(self):
    #     pass
    # TODO stop??


async def create_local_tcp_client(pipe_context, local_dest_tcp_port, *,
                                  loop=None):
    loop = loop or asyncio.get_event_loop()
    transport, _ = await loop.create_connection(
        single_connection_factory(InteriorProtocol(pipe_context)),
        '127.0.0.1', local_dest_tcp_port)
    return transport


async def create_local_tcp_server(pipe_context, *, loop=None):
    loop = loop or asyncio.get_event_loop()
    protocol = InteriorProtocol(pipe_context)
    server = await loop.create_server(
        single_connection_factory(protocol),
        '127.0.0.1')
    assert 1 == len(server.sockets)
    _, port = server.sockets[0].getsockname()
    # TODO ?? server - wait until connected?
    return server, port


def single_connection_factory(protocol):
    is_called = False

    def f():
        nonlocal is_called
        if is_called:
            raise ValueError('Connection has already been accepted')
        is_called = True
        return protocol
    return f


class InteriorProtocol(asyncio.Protocol):

    def __init__(self, pipe_context: PipeContext):
        self.transport = None
        self.pipe_context = pipe_context

    def connection_made(self, transport):
        self.transport = transport
        self.pipe_context.contribute_interior_transport(transport)

    def data_received(self, data):
        self.pipe_context.write_to_exterior(data)

    def connection_lost(self, exc):
        self.pipe_context.close()
