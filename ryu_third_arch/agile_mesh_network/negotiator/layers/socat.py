import asyncio
import subprocess
from abc import ABCMeta
from logging import getLogger

from agile_mesh_network.common.tun_mapper import mac_to_tun_name

from .base import (
    BaseProcessProtocol, ProcessManager, create_local_tcp_client, create_local_tcp_server,
    get_free_local_tcp_port, wait_localport_is_bound
)

logger = getLogger(__name__)


class SocatConfig:
    exe_path = "socat"
    # TODO ?? slip, pi

    def validate(self):
        errors = []
        try:
            proc = subprocess.run(
                [self.exe_path, "-V"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT
            )
        except Exception as e:
            errors.append(f"Unable to start socat process ({self.exe_path}): {e}")
        else:
            if b"socat" not in proc.stdout:
                errors.append(
                    f"Unable to check socat process ({self.exe_path}) version: \n"
                    f"{proc.stdout.decode()}"
                )
        if errors:
            raise ValueError("\n".join(errors))


socat_config = SocatConfig()


class BaseSocatProcessManager(ProcessManager, metaclass=ABCMeta):
    """Manages socat processes."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._process_transport = None
        self.tun_dev_name = mac_to_tun_name(self._dst_mac)

    @property
    def _exec_path(self):
        return socat_config.exe_path

    async def _start_socat_process(self, args):
        logger.info("Starting socat process: %s %r", self._exec_path, args)
        loop = asyncio.get_event_loop()
        self._process_transport, self._process_protocol = await loop.subprocess_exec(
            lambda: SocatProcessProtocol(self._pipe_context),
            self._exec_path,
            *args,
            stdin=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )

    async def tunnel_started(self, timeout=None):
        # TODO timeout? deal with the hardcode.
        await asyncio.wait_for(
            self._process_protocol.fut_tunnel_ready, timeout=(timeout or 10)
        )

    @property
    def is_tunnel_active(self):
        if self._pipe_context.is_closed:
            return False
        if self._process_transport is None:
            return False
        if not self._process_protocol.fut_tunnel_ready.done():
            return False
        return True

    @property
    def is_dead(self):
        return self._pipe_context.is_closed

    def add_dead_callback(self, callback):
        self._pipe_context.add_close_callback(callback)

    def close(self):
        self._pipe_context.close()


class SocatResponderProcessManager(BaseSocatProcessManager):

    async def start(self, timeout=None):
        self._local_port = get_free_local_tcp_port()
        await self._start_socat_process(self._build_process_args())
        await wait_localport_is_bound(
            self._process_transport.get_pid(), self._local_port, proto="tcp"
        )
        self.interior_protocol = await create_local_tcp_client(
            self._pipe_context, self._local_port
        )

    def _build_process_args(self):
        return (
            "-d",
            "-d",
            f"TUN,tun-name={self.tun_dev_name},tun-type=tap,iff-no-pi,up",
            f"TCP-L:{self._local_port}",
        )


class SocatInitiatorProcessManager(BaseSocatProcessManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._local_port = None

    async def start(self, timeout=None):
        protocol, self._local_port = await create_local_tcp_server(self._pipe_context)
        await self._start_socat_process(self._build_process_args())
        await protocol.fut_connected

    def _build_process_args(self):
        return (
            "-d",
            "-d",
            f"TUN,tun-name={self.tun_dev_name},tun-type=tap,iff-no-pi,up",
            f"TCP:127.0.0.1:{self._local_port}",
        )


class SocatProcessProtocol(BaseProcessProtocol):

    def is_tunnel_ready(self, data):
        return b"starting data transfer loop" in data
