import asyncio
import socket
from abc import ABCMeta, abstractmethod
from contextlib import closing
from logging import getLogger
from typing import Any, Awaitable, Mapping, Set, Tuple, Type

import psutil

from agile_mesh_network.common.async_utils import (
    future_set_exception_silent, future_set_result_silent
)
from agile_mesh_network.common.models import LayersDescriptionModel
from agile_mesh_network.negotiator.tunnel_protocols import PipeContext

logger = getLogger(__name__)

_registry = {}


def add_layers_managers(
    layers_set: Set[str],
    responder: Type["ProcessManager"],
    initiator: Type["ProcessManager"],
):
    assert issubclass(responder, ProcessManager)
    assert issubclass(initiator, ProcessManager)
    layers = tuple(sorted(layers_set))
    _registry[layers] = responder, initiator


def get_layers_managers(
    layers: Mapping[str, Any]
) -> Tuple[Type["ProcessManager"], Type["ProcessManager"]]:
    key = tuple(sorted(layers.keys()))
    try:
        responder, initiator = _registry[key]
    except KeyError:
        raise KeyError("Unknown layers set: %s" % key)
    return responder, initiator


class ProcessManager(metaclass=ABCMeta):

    @staticmethod
    def from_layers_responder(
        dst_mac, layers: LayersDescriptionModel, pipe_context: PipeContext
    ) -> "ProcessManager":
        responder, _ = get_layers_managers(layers.layers)
        return responder(dst_mac, layers.layers, pipe_context)

    @staticmethod
    def from_layers_initiator(
        dst_mac, layers: LayersDescriptionModel, pipe_context: PipeContext
    ) -> "ProcessManager":
        _, initiator = get_layers_managers(layers.layers)
        return initiator(dst_mac, layers.layers, pipe_context)

    def __init__(
        self, dst_mac: str, layers_options: Mapping[str, Any], pipe_context: PipeContext
    ) -> None:
        self._dst_mac = dst_mac
        self._layers_options = layers_options
        self._pipe_context = pipe_context

    @abstractmethod
    async def start(self, timeout=None):
        pass

    @abstractmethod
    async def tunnel_started(self, timeout=None):
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

    @abstractmethod
    def add_dead_callback(self, callback):
        """A handler which will be called when the tunnel dies."""
        pass


def get_free_local_tcp_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("localhost", 0))
        return s.getsockname()[1]


async def wait_localport_is_bound(pid, port_number, proto="tcp", pool_interval=0.3):
    # throws psutil.NoSuchProcess
    assert proto in ("tcp",)
    socket_type = socket.SOCK_STREAM  # tcp
    proc = psutil.Process(pid)
    while True:
        bound = [
            pconn
            for pconn in proc.connections()
            if pconn.status == "LISTEN"
            and pconn.type == socket_type
            and pconn.laddr.port == port_number
        ]
        if bound:
            break
        await asyncio.sleep(pool_interval)


async def create_local_tcp_client(pipe_context, local_dest_tcp_port, *, loop=None):
    loop = loop or asyncio.get_event_loop()
    protocol = InteriorProtocol(pipe_context)
    await loop.create_connection(
        single_connection_factory(protocol), "127.0.0.1", local_dest_tcp_port
    )
    return protocol


async def create_local_tcp_server(pipe_context, *, loop=None):
    loop = loop or asyncio.get_event_loop()
    protocol = InteriorProtocol(pipe_context)
    server = await loop.create_server(single_connection_factory(protocol), "127.0.0.1")
    pipe_context.add_closing(server)  # Closes the listening server.
    assert 1 == len(server.sockets)
    _, port = server.sockets[0].getsockname()
    return protocol, port


def single_connection_factory(protocol):
    is_called = False

    def f():
        nonlocal is_called
        if is_called:
            raise ValueError("Connection has already been accepted")
        is_called = True
        return protocol

    return f


class InteriorProtocol(asyncio.Protocol):

    def __init__(self, pipe_context: PipeContext) -> None:
        self.transport = None
        self.pipe_context = pipe_context
        self.fut_connected: Awaitable[None] = asyncio.Future()
        pipe_context.add_close_callback(
            lambda: future_set_exception_silent(
                self.fut_connected, OSError("connection closed")
            )
        )

    def connection_made(self, transport):
        self.transport = transport
        self.pipe_context.contribute_interior_transport(transport)
        future_set_result_silent(self.fut_connected, None)

    def data_received(self, data):
        self.pipe_context.write_to_exterior(data)

    def connection_lost(self, exc):
        self.pipe_context.close()


class BaseProcessProtocol(asyncio.SubprocessProtocol, metaclass=ABCMeta):

    def __init__(self, pipe_context: PipeContext) -> None:
        self.transport = None
        self.pipe_context = pipe_context
        self.fut_exit: Awaitable[None] = asyncio.Future()
        self.stdout_data = b""  # stderr is piped to stdout
        self.fut_tunnel_ready: Awaitable[None] = asyncio.Future()

    def connection_made(self, transport):
        self.transport = transport
        self.pipe_context.add_closing(transport)
        self.pipe_context.add_close_callback(self.log_stopped)
        logger.info("Process [%s] has been started.", transport.get_pid())

    def pipe_data_received(self, fd, data):
        self.stdout_data += data
        try:
            if not self.fut_tunnel_ready.done():
                if self.is_tunnel_ready(self.stdout_data):
                    future_set_result_silent(self.fut_tunnel_ready, None)
        except Exception as e:
            future_set_exception_silent(self.fut_tunnel_ready, e)

    @abstractmethod
    def is_tunnel_ready(self, data):
        pass

    def process_exited(self):
        self.fut_exit.set_result(None)
        future_set_exception_silent(self.fut_tunnel_ready, Exception('Process exited'))
        self.pipe_context.close()

    def log_stopped(self):
        exit_code = self.transport.get_returncode()
        if exit_code is not None and exit_code != 0:
            logger.error(
                "Process [%s] failed [exit code %s]. Output: %s",
                self.transport.get_pid(),
                self.transport.get_returncode(),
                self.stdout_data.decode(),
            )
        else:
            logger.error("Process [%s] stopped [exit code 0].")
