import asyncio
import os
import tempfile
from collections import defaultdict
from contextlib import ExitStack
from logging import getLogger
from unittest import TestCase
from unittest.mock import patch

from async_exit_stack import AsyncExitStack

from agile_mesh_network.common.models import LayersDescriptionRpcModel
from agile_mesh_network.common.rpc import RpcBroadcast, RpcUnixClient
from agile_mesh_network.negotiator.process_managers import (
    BaseOpenvpnProcessManager, OpenvpnInitiatorProcessManager, OpenvpnProcessProtocol,
    OpenvpnResponderProcessManager
)
from agile_mesh_network.negotiator_main import (
    RpcResponder, TcpExteriorServer, TunnelsState
)

logger = getLogger(__name__)
RUN_TCP_PATH = os.path.join(os.path.dirname(__file__), "bin", "run_tcp.py")

RUN_TCP_CLIENT_DATA = "hi please don't change my request"
RUN_TCP_SERVER_DATA = "hi please don't change my response"


class IntegrationTestCase(TestCase):
    maxDiff = None  # unittest: show full diff on assertion failure

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        self.openvpn_stdout = openvpn_stdout = defaultdict(lambda: b"")

        def openvpn_pipe_data_received(self, fd, data):
            openvpn_stdout[self.transport] += data
            # TODO call the parent data_received too?

        self._stack = ExitStack()
        self.temp_dir = self._stack.enter_context(tempfile.TemporaryDirectory())
        self._stack.enter_context(
            patch.object(
                OpenvpnProcessProtocol, "pipe_data_received", openvpn_pipe_data_received
            )
        )

    def tearDown(self):
        if self.openvpn_stdout:
            logger.info("Captured openvpn outputs: %s", self.openvpn_stdout.values())

        self._stack.close()

        self.loop.run_until_complete(self.loop.shutdown_asyncgens())
        self.loop.close()

    def test_basic_rpc(self):
        loop = self.loop
        rpc_sock_path = os.path.join(self.temp_dir, "l.sock")

        async def command_cb(session, msg):
            assert False

        async def f():
            async with AsyncExitStack() as stack:
                tunnels_state = await stack.enter_async_context(TunnelsState())
                await stack.enter_async_context(
                    RpcResponder(tunnels_state, rpc_sock_path)
                )
                rpc_c = await stack.enter_async_context(
                    RpcUnixClient(rpc_sock_path, command_cb)
                )

                rpc = rpc_c.session
                resp = await asyncio.wait_for(
                    rpc.issue_command("dump_tunnels_state"), timeout=3
                )
                self.assertDictEqual(resp, {"tunnels": []})

        loop.run_until_complete(f())

    @patch.object(BaseOpenvpnProcessManager, "_exec_path", RUN_TCP_PATH)
    @patch.object(
        OpenvpnResponderProcessManager,
        "_build_process_args",
        lambda self: (
            "--mode",
            "server",
            "--port",
            f"{self._local_port}",
            "--data",
            RUN_TCP_SERVER_DATA,
        ),
    )
    @patch.object(
        OpenvpnInitiatorProcessManager,
        "_build_process_args",
        lambda self: (
            "--mode",
            "client",
            "--port",
            f"{self._local_port}",
            "--data",
            RUN_TCP_CLIENT_DATA,
        ),
    )
    def test_negotiation(self):
        loop = self.loop
        mac_a = "00:11:22:33:44:00"
        mac_b = "00:11:22:33:44:01"

        rpc_commands_a = []
        rpc_commands_b = []

        def command_cb_factory(commands_container):

            async def command_cb(session, msg):
                commands_container.append(msg)

            return command_cb

        async def f():
            async with AsyncExitStack() as stack:
                tunnels_state_a = await stack.enter_async_context(TunnelsState())
                tunnels_state_b = await stack.enter_async_context(TunnelsState())
                await stack.enter_async_context(
                    RpcResponder(tunnels_state_a, os.path.join(self.temp_dir, "a.sock"))
                )
                await stack.enter_async_context(
                    RpcResponder(tunnels_state_b, os.path.join(self.temp_dir, "b.sock"))
                )
                tcp_server_b = await stack.enter_async_context(
                    TcpExteriorServer(tunnels_state_b)
                )
                rpc_a_c = await stack.enter_async_context(
                    RpcUnixClient(
                        os.path.join(self.temp_dir, "a.sock"),
                        command_cb_factory(rpc_commands_a),
                    )
                )
                rpc_b_c = await stack.enter_async_context(
                    RpcUnixClient(
                        os.path.join(self.temp_dir, "b.sock"),
                        command_cb_factory(rpc_commands_b),
                    )
                )

                self.assertIsNotNone(tcp_server_b.tcp_port)

                # Setup RPC
                rpc_a = rpc_a_c.session
                rpc_b = rpc_b_c.session

                # Issue an RPC command for a tunnel creation
                create_tunnel_future = asyncio.ensure_future(
                    rpc_a.issue_command(
                        "create_tunnel",
                        {
                            "src_mac": mac_a,
                            "dst_mac": mac_b,
                            "timeout": 5,
                            "layers": LayersDescriptionRpcModel(
                                protocol="tcp",
                                dest=("127.0.0.1", tcp_server_b.tcp_port),
                                layers={"openvpn": {"mock": True}},
                            ).asdict(),
                        },
                    )
                )

                tunnel_data_a = {
                    "src_mac": mac_a,
                    "dst_mac": mac_b,
                    "is_dead": False,
                    "is_tunnel_active": False,
                    "layers": ["openvpn"],
                }
                tunnel_data_b = {
                    "src_mac": mac_b,
                    "dst_mac": mac_a,
                    "is_dead": False,
                    "is_tunnel_active": False,
                    "layers": ["openvpn"],
                }

                # Issue another command while the tunnel creation is in progress
                tunnels = await asyncio.wait_for(
                    rpc_a.issue_command("dump_tunnels_state"), timeout=0.5
                )
                self.assertDictEqual(tunnels, {"tunnels": [tunnel_data_a]})

                # Wait until tunnel is created
                resp = await asyncio.wait_for(create_tunnel_future, timeout=3)
                tunnel_data_a["is_tunnel_active"] = True
                tunnel_data_b["is_tunnel_active"] = True
                self.assertDictEqual(
                    resp, {"tunnel": tunnel_data_a, "tunnels": [tunnel_data_a]}
                )

                # Ensure that responder has the same view
                tunnels = await asyncio.wait_for(
                    rpc_b.issue_command("dump_tunnels_state"), timeout=0.5
                )
                self.assertDictEqual(tunnels, {"tunnels": [tunnel_data_b]})

                # Check RPC broadcasts
                self.assertListEqual(
                    rpc_commands_a,
                    [
                        RpcBroadcast(
                            name="tunnel_created",
                            kwargs={
                                "tunnel": tunnel_data_a,
                                "tunnels": [tunnel_data_a],
                            },
                        )
                    ],
                )
                self.assertListEqual(
                    rpc_commands_b,
                    [
                        RpcBroadcast(
                            name="tunnel_created",
                            kwargs={
                                "tunnel": tunnel_data_b,
                                "tunnels": [tunnel_data_b],
                            },
                        )
                    ],
                )

                # Verify that the data was correctly piped
                self.assertSetEqual(
                    set(self.openvpn_stdout.values()),
                    {RUN_TCP_CLIENT_DATA.encode(), RUN_TCP_SERVER_DATA.encode()},
                )

        loop.run_until_complete(f())
