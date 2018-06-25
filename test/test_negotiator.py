import asyncio
import os
import tempfile
from collections import defaultdict
from contextlib import ExitStack
from logging import getLogger
from unittest import TestCase
from unittest.mock import patch

from async_exit_stack import AsyncExitStack

from agile_mesh_network.common import timeout_backoff
from agile_mesh_network.common.models import LayersDescriptionRPCModel
from agile_mesh_network.common.rpc import RPCBroadcast, RPCUnixClient
from agile_mesh_network.negotiator.layers import (
    OpenvpnInitiatorProcessManager, OpenvpnResponderProcessManager
)
from agile_mesh_network.negotiator.layers.openvpn import (
    BaseOpenvpnProcessManager, OpenvpnProcessProtocol
)
from agile_mesh_network.negotiator_main import (
    RPCResponder, TCPExteriorServer, TunnelsState
)

logger = getLogger(__name__)
RUN_TCP_PATH = os.path.join(os.path.dirname(__file__), "bin", "run_tcp.py")

RUN_TCP_DONE_DATA = "please"
RUN_TCP_CLIENT_DATA = "hi please don't change my request."
RUN_TCP_SERVER_DATA = "hi please don't change my response."


class IntegrationTestCase(TestCase):
    maxDiff = None  # unittest: show full diff on assertion failure

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        self.openvpn_stdout = openvpn_stdout = defaultdict(lambda: b"")

        original_pipe_data_received = OpenvpnProcessProtocol.pipe_data_received

        def openvpn_pipe_data_received(self, fd, data):
            openvpn_stdout[self.transport] += data
            original_pipe_data_received(self, fd, data)

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
                tunnels_state = await stack.enter_async_context(TunnelsState(loop=loop))
                await stack.enter_async_context(
                    RPCResponder(tunnels_state, rpc_sock_path)
                )
                rpc_c = await stack.enter_async_context(
                    RPCUnixClient(rpc_sock_path, command_cb)
                )

                rpc = rpc_c.session
                resp = await rpc.issue_command("dump_tunnels_state")
                self.assertDictEqual(resp, {"tunnels": []})

        loop.run_until_complete(asyncio.wait_for(f(), timeout=3))

    def test_rpc_client_reconnect(self):
        loop = self.loop
        rpc_sock_path = os.path.join(self.temp_dir, "l.sock")

        async def command_cb(session, msg):
            assert False

        async def f():
            backoff_semaphore = asyncio.Semaphore(0)

            def _mock_sleep(time):
                return backoff_semaphore.acquire()

            async with AsyncExitStack() as stack:
                tunnels_state = await stack.enter_async_context(TunnelsState(loop=loop))
                stack.enter_context(
                    patch.object(timeout_backoff, "_sleep", _mock_sleep)
                )

                async with RPCResponder(tunnels_state, rpc_sock_path):
                    rpc_c = RPCUnixClient(rpc_sock_path, command_cb)
                    await rpc_c.start()

                    rpc = rpc_c.session
                    resp = await rpc.issue_command("dump_tunnels_state")
                    self.assertDictEqual(resp, {"tunnels": []})
                    self.assertTrue(rpc.is_connected)

                # Make the session aware that the remote end is gone
                with self.assertRaises((OSError, ValueError)):
                    await rpc.issue_command("dump_tunnels_state")

                self.assertFalse(rpc.is_connected)

                backoff_semaphore.release()  # Trigger reconnect
                # Yield control to event loop to start reconnection.
                await asyncio.sleep(0.001)

                # Bring back the server, ensure that the session is working.
                async with RPCResponder(tunnels_state, rpc_sock_path):
                    backoff_semaphore.release()  # Trigger reconnect
                    await rpc_c._reconnect_task
                    resp = await rpc.issue_command("dump_tunnels_state")
                    self.assertDictEqual(resp, {"tunnels": []})
                    self.assertTrue(rpc.is_connected)

                await rpc_c.close_wait()

        loop.run_until_complete(asyncio.wait_for(f(), timeout=3))

    @patch.object(BaseOpenvpnProcessManager, "_exec_path", RUN_TCP_PATH)
    @patch.object(
        OpenvpnProcessProtocol,
        "is_tunnel_ready",
        lambda self, data: RUN_TCP_DONE_DATA.encode() in data,
    )
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
                tunnels_state_a = await stack.enter_async_context(
                    TunnelsState(loop=loop)
                )
                tunnels_state_b = await stack.enter_async_context(
                    TunnelsState(loop=loop)
                )
                await stack.enter_async_context(
                    RPCResponder(tunnels_state_a, os.path.join(self.temp_dir, "a.sock"))
                )
                await stack.enter_async_context(
                    RPCResponder(tunnels_state_b, os.path.join(self.temp_dir, "b.sock"))
                )
                tcp_server_b = await stack.enter_async_context(
                    TCPExteriorServer(tunnels_state_b)
                )
                rpc_a_c = await stack.enter_async_context(
                    RPCUnixClient(
                        os.path.join(self.temp_dir, "a.sock"),
                        command_cb_factory(rpc_commands_a),
                    )
                )
                rpc_b_c = await stack.enter_async_context(
                    RPCUnixClient(
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
                            "layers": LayersDescriptionRPCModel(
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

                # Give the event loop opportunity to process
                # the tunnel creation command.
                await asyncio.sleep(0)

                # Issue another command while the tunnel creation is in progress
                tunnels = await rpc_a.issue_command("dump_tunnels_state")
                self.assertDictEqual(tunnels, {"tunnels": [tunnel_data_a]})

                # Wait until tunnel is created
                resp = await create_tunnel_future
                tunnel_data_a["is_tunnel_active"] = True
                tunnel_data_b["is_tunnel_active"] = True
                self.assertDictEqual(
                    resp, {"tunnel": tunnel_data_a, "tunnels": [tunnel_data_a]}
                )

                # Ensure that responder has the same view
                tunnels = await rpc_b.issue_command("dump_tunnels_state")
                self.assertDictEqual(tunnels, {"tunnels": [tunnel_data_b]})

                # Check RPC broadcasts
                self.assertListEqual(
                    rpc_commands_a,
                    [
                        RPCBroadcast(
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
                        RPCBroadcast(
                            name="tunnel_created",
                            kwargs={
                                "tunnel": tunnel_data_b,
                                "tunnels": [tunnel_data_b],
                            },
                        )
                    ],
                )
                rpc_commands_a.clear()
                rpc_commands_b.clear()

                # Give some time for the data to be completely transferred
                await asyncio.sleep(0.1)

                # Verify that the data was correctly piped
                self.assertSetEqual(
                    set(self.openvpn_stdout.values()),
                    {RUN_TCP_CLIENT_DATA.encode(), RUN_TCP_SERVER_DATA.encode()},
                )

                # Break the tunnel by terminating a process.
                pm = next(iter(tunnels_state_a.tunnels.values())).process_manager
                pm._process_transport.close()

                # Let these poor fellows process what has happened.
                await pm._pipe_context.closed_event.wait()
                await asyncio.sleep(0.1)

                # Check RPC broadcasts
                tunnel_data_a["is_tunnel_active"] = False
                tunnel_data_b["is_tunnel_active"] = False
                tunnel_data_a["is_dead"] = True
                tunnel_data_b["is_dead"] = True
                self.assertListEqual(
                    rpc_commands_a,
                    [
                        RPCBroadcast(
                            name="tunnel_destroyed",
                            kwargs={"tunnel": tunnel_data_a, "tunnels": []},
                        )
                    ],
                )
                self.assertListEqual(
                    rpc_commands_b,
                    [
                        RPCBroadcast(
                            name="tunnel_destroyed",
                            kwargs={"tunnel": tunnel_data_b, "tunnels": []},
                        )
                    ],
                )

                # Ensure that the tunnels state is now empty
                tunnels = await rpc_a.issue_command("dump_tunnels_state")
                self.assertDictEqual(tunnels, {"tunnels": []})

                tunnels = await rpc_b.issue_command("dump_tunnels_state")
                self.assertDictEqual(tunnels, {"tunnels": []})

        loop.run_until_complete(asyncio.wait_for(f(), timeout=3))

    # TODO socat??
