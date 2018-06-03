from logging import getLogger
from typing import Sequence

from async_exit_stack import AsyncExitStack

from agile_mesh_network.common.models import LayersDescriptionRpcModel, TunnelModel
from agile_mesh_network.common.rpc import RpcBroadcast, RpcUnixClient

logger = getLogger(__name__)


class NegotiatorRpc:
    # Execution context: run entirely in the asyncio event loop,
    # no thread safety is required.

    def __init__(self, unix_sock_path):
        self.unix_sock_path = unix_sock_path
        self._tunnels_changed_callbacks = []
        self._stack = AsyncExitStack()
        self._rpc = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._stack.aclose()

    def add_tunnels_changed_callback(self, callback):
        self._tunnels_changed_callbacks.append(callback)

    async def _get_session(self):
        if not self._rpc:
            self._rpc = await self._stack.enter_async_context(
                RpcUnixClient(self.unix_sock_path, self._rpc_command_handler)
            )
        return self._rpc.session

    async def start_tunnel(
        self, src_mac, dst_mac, timeout, layers: LayersDescriptionRpcModel
    ) -> None:
        session = await self._get_session()
        msg = await session.issue_command(
            "create_tunnel",
            {
                "src_mac": src_mac,
                "dst_mac": dst_mac,
                "timeout": timeout,
                "layers": layers.asdict(),
            },
        )
        assert msg.keys() == {"tunnel", "tunnels"}
        self._call_callbacks("create_tunnel", msg)

    async def list_tunnels(self) -> Sequence[TunnelModel]:
        session = await self._get_session()
        msg = await session.issue_command("dump_tunnels_state")
        assert msg.keys() == {"tunnels"}
        self._call_callbacks("dump_tunnels_state", msg)
        return [TunnelModel.from_dict(d) for d in msg["tunnels"]]

    async def _rpc_command_handler(self, session, cmd: RpcBroadcast):
        if cmd.name not in ("tunnel_created", "tunnel_destroyed"):
            logger.error(
                "Ignoring RPC broadcast with an unknown topic_name: %s", cmd.name
            )
            return
        msg = cmd.kwargs
        assert msg.keys() == {"tunnel", "tunnels"}
        self._call_callbacks(topic=cmd.name, msg=msg)

    def _call_callbacks(self, topic, msg):
        tunnel = TunnelModel.from_dict(msg["tunnel"]) if msg.get("tunnel") else None
        tunnels = (
            [TunnelModel.from_dict(d) for d in msg["tunnels"]]
            if "tunnels" in msg
            else None
        )
        for callback in self._tunnels_changed_callbacks:
            callback(topic=topic, tunnel=tunnel, tunnels=tunnels)
