import asyncio
import subprocess
import socket
from abc import ABCMeta, abstractmethod
from contextlib import closing
from typing import Tuple

from agile_mesh_network.common.models import LayersDescriptionModel
from agile_mesh_network.negotiator.tunnel_protocols import PipeContext
from agile_mesh_network.common.async_utils import (
    future_set_exception_silent, future_set_result_silent,
)


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


class BaseOpenvpnProcessManager(ProcessManager, metaclass=ABCMeta):
    """Manages openvpn processes."""

    _exec_path = 'openvpn'  # TODO

    def __init__(self, dst_mac, openvpn_options,
                 pipe_context: PipeContext):
        self._process_transport = None
        self.tun_dev_name = f'tap{dst_mac.replace(":", "")}'
        self._pipe_context = pipe_context
        # TODO options
        # TODO setup configs, certs

    async def _start_openvpn_process(self, args):
        loop = asyncio.get_event_loop()
        self._process_transport, _ = await loop.subprocess_exec(
            lambda: OpenvpnProcessProtocol(self._pipe_context),
            self._exec_path, *args,
            stdin=subprocess.DEVNULL, stderr=subprocess.STDOUT)

    @property
    def is_tunnel_active(self):
        if self._pipe_context.is_closed:
            return False
        if self._process_transport is None:
            return False
        # TODO make this more granular (respect negotiation phase?)
        return True

    @property
    def is_dead(self):
        return self._pipe_context.is_closed

    # TODO stop??


class OpenvpnResponderProcessManager(BaseOpenvpnProcessManager):

    async def start(self, timeout=None):
        self._local_port = get_free_local_tcp_port()
        await self._start_openvpn_process(self._build_process_args())
        self.interior_protocol = await create_local_tcp_client(
            self._pipe_context, self._local_port)

    def _build_process_args(self):
        # Server with self._local_port
        # TODO
        return tuple()


class OpenvpnInitiatorProcessManager(BaseOpenvpnProcessManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._local_port = None

    async def start(self, timeout=None):
        protocol, self._local_port = await create_local_tcp_server(
            self._pipe_context)
        await self._start_openvpn_process(self._build_process_args())
        await protocol.fut_connected

    def _build_process_args(self):
        # Client to self._local_port
        # TODO
        return tuple()


def get_free_local_tcp_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('localhost', 0))
        return s.getsockname()[1]


async def create_local_tcp_client(pipe_context, local_dest_tcp_port, *,
                                  loop=None):
    loop = loop or asyncio.get_event_loop()
    protocol = InteriorProtocol(pipe_context)
    await loop.create_connection(
        single_connection_factory(protocol),
        '127.0.0.1', local_dest_tcp_port)
    return protocol


async def create_local_tcp_server(pipe_context, *, loop=None):
    loop = loop or asyncio.get_event_loop()
    protocol = InteriorProtocol(pipe_context)
    server = await loop.create_server(
        single_connection_factory(protocol),
        '127.0.0.1')
    assert 1 == len(server.sockets)
    _, port = server.sockets[0].getsockname()
    return protocol, port


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
        self.fut_connected = asyncio.Future()
        pipe_context.add_close_callback(
            lambda: future_set_exception_silent(
                self.fut_connected, OSError('connection closed')))

    def connection_made(self, transport):
        self.transport = transport
        self.pipe_context.contribute_interior_transport(transport)
        future_set_result_silent(self.fut_connected, None)

    def data_received(self, data):
        self.pipe_context.write_to_exterior(data)

    def connection_lost(self, exc):
        self.pipe_context.close()


class OpenvpnProcessProtocol(asyncio.SubprocessProtocol):
    def __init__(self, pipe_context: PipeContext):
        self.transport = None
        self.pipe_context = pipe_context
        pipe_context.add_closing
        self.fut_exit = asyncio.Future()

    def connection_made(self, transport):
        self.transport = transport
        self.pipe_context.add_closing(transport)

    def process_exited(self):
        self.fut_exit.set_result(None)
        self.pipe_context.close()
