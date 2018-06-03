import asyncio
import os
import subprocess
from abc import ABCMeta
from logging import getLogger

from agile_mesh_network.common.tun_mapper import mac_to_tun_name

from .base import (
    BaseProcessProtocol, ProcessManager, create_local_tcp_client, create_local_tcp_server,
    get_free_local_tcp_port, wait_localport_is_bound
)

logger = getLogger(__name__)


class OpenvpnConfig:
    exe_path = "openvpn"
    client_config_path = "/etc/openvpn/client.conf"
    server_config_path = "/etc/openvpn/server.conf"

    def validate(self):
        errors = []
        if not os.path.isfile(self.client_config_path):
            errors.append(
                f"Client config {self.client_config_path} does not exist "
                "or is not a file."
            )
        if not os.path.isfile(self.client_config_path):
            errors.append(
                f"Server config {self.server_config_path} does not exist "
                "or is not a file."
            )
        try:
            proc = subprocess.run(
                [self.exe_path, "--version"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
        except Exception as e:
            errors.append(f"Unable to start openvpn process ({self.exe_path}): {e}")
        else:
            # `openvpn --version` errorcode is 1.
            if b"openvpn" not in proc.stdout:
                errors.append(
                    f"Unable to check openvpn process ({self.exe_path}) version: \n"
                    f"{proc.stdout.decode()}"
                )
        # TODO check client and server don't contain remote/port
        if errors:
            raise ValueError("\n".join(errors))


openvpn_config = OpenvpnConfig()


class BaseOpenvpnProcessManager(ProcessManager, metaclass=ABCMeta):
    """Manages openvpn processes."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._process_transport = None
        self.tun_dev_name = mac_to_tun_name(self._dst_mac)
        # TODO ?? setup configs, certs

    @property
    def _exec_path(self):
        return openvpn_config.exe_path

    async def _start_openvpn_process(self, args):
        logger.info("Starting openvpn process: %s %r", self._exec_path, args)
        loop = asyncio.get_event_loop()
        self._process_transport, _ = await loop.subprocess_exec(
            lambda: OpenvpnProcessProtocol(self._pipe_context),
            self._exec_path,
            *args,
            stdin=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )

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

    def add_dead_callback(self, callback):
        self._pipe_context.add_close_callback(callback)

    def close(self):
        self._pipe_context.close()


class OpenvpnResponderProcessManager(BaseOpenvpnProcessManager):

    async def start(self, timeout=None):
        self._local_port = get_free_local_tcp_port()
        await self._start_openvpn_process(self._build_process_args())
        await wait_localport_is_bound(
            self._process_transport.get_pid(), self._local_port, proto="tcp"
        )
        self.interior_protocol = await create_local_tcp_client(
            self._pipe_context, self._local_port
        )

    def _build_process_args(self):
        cd = os.path.dirname(openvpn_config.server_config_path)
        return (
            "--mode",
            "server",
            "--proto",
            "tcp-server",
            "--port",
            str(self._local_port),
            "--config",
            openvpn_config.server_config_path,
            "--cd",
            cd,
            "--dev-type",
            "tap",
            "--dev",
            self.tun_dev_name,
        )


class OpenvpnInitiatorProcessManager(BaseOpenvpnProcessManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._local_port = None

    async def start(self, timeout=None):
        protocol, self._local_port = await create_local_tcp_server(self._pipe_context)
        await self._start_openvpn_process(self._build_process_args())
        await protocol.fut_connected

    def _build_process_args(self):
        cd = os.path.dirname(openvpn_config.client_config_path)
        return (
            "--proto",
            "tcp-client",
            "--remote",
            "127.0.0.1",
            str(self._local_port),
            "--config",
            openvpn_config.client_config_path,
            "--cd",
            cd,
            "--dev-type",
            "tap",
            "--dev",
            self.tun_dev_name,
        )


class OpenvpnProcessProtocol(BaseProcessProtocol):
    pass