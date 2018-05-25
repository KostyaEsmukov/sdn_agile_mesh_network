import asyncio
import tempfile
import os
from logging import getLogger
from unittest import TestCase
from unittest.mock import patch

from agile_mesh_network.negotiator_main import (
    RpcResponder, TunnelsState, TcpExteriorServer
)
from agile_mesh_network.common.rpc import RpcSession, RpcUnixClient
from agile_mesh_network.common.models import LayersDescriptionRpcModel
from agile_mesh_network.negotiator.process_managers import (
    BaseOpenvpnProcessManager, OpenvpnResponderProcessManager,
    OpenvpnInitiatorProcessManager
)

logger = getLogger(__name__)
RUN_TCP_PATH = os.path.join(os.path.dirname(__file__), 'bin', 'run_tcp.py')


class IntegrationTestCase(TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def tearDown(self):
        self.loop.close()

    def test_basic_rpc(self):
        loop = self.loop
        with tempfile.TemporaryDirectory() as td, \
                patch.object(RpcResponder, 'socket_path',
                             os.path.join(td, 'l.sock')):

            tunnels_state = TunnelsState()
            rpc_responder = RpcResponder(tunnels_state)
            loop.run_until_complete(rpc_responder.start_server())

            try:
                async def command_cb(session, msg):
                    assert False

                async def f():
                    rpc_c = RpcUnixClient(os.path.join(td, 'l.sock'), command_cb)
                    await rpc_c.start()
                    rpc = rpc_c.session
                    resp = await asyncio.wait_for(
                        rpc.issue_command("dump_tunnels_state"), timeout=3)
                    self.assertDictEqual(resp, {"tunnels": []})

                loop.run_until_complete(f())
            finally:
                loop.run_until_complete(tunnels_state.close_tunnels_wait())
                loop.run_until_complete(rpc_responder.close_wait())

    @patch.object(BaseOpenvpnProcessManager, '_exec_path', RUN_TCP_PATH)
    @patch.object(OpenvpnResponderProcessManager, '_build_process_args',
                  lambda self: ('--mode', 'server', '--port', f'{self._local_port}'))
    @patch.object(OpenvpnInitiatorProcessManager, '_build_process_args',
                  lambda self: ('--mode', 'client', '--port', f'{self._local_port}',
                                '--data', "hi please don't change me"))
    def test_negotiation(self):
        loop = self.loop
        mac_a = "00:11:22:33:44:00"
        mac_b = "00:11:22:33:44:01"

        with tempfile.TemporaryDirectory() as td:

            tunnels_state_a = TunnelsState()
            tunnels_state_b = TunnelsState()

            with patch.object(RpcResponder, 'socket_path',
                              os.path.join(td, 'a.sock')):
                rpc_responder_a = RpcResponder(tunnels_state_a)
            with patch.object(RpcResponder, 'socket_path',
                              os.path.join(td, 'b.sock')):
                rpc_responder_b = RpcResponder(tunnels_state_b)

            tcp_server_b = TcpExteriorServer(tunnels_state_b)
            loop.run_until_complete(rpc_responder_a.start_server())
            loop.run_until_complete(rpc_responder_b.start_server())
            loop.run_until_complete(tcp_server_b.start_server())

            try:
                self.assertIsNotNone(tcp_server_b.tcp_port)

                async def command_cb(session, msg):
                    assert False

                async def f():
                    rpc_a = RpcUnixClient(os.path.join(td, 'a.sock'), command_cb)
                    await rpc_a.start()
                    rpc = rpc_a.session
                    resp = await asyncio.wait_for(
                        rpc.issue_command("create_tunnel", {
                            "src_mac": mac_a, "dst_mac": mac_b,
                            "timeout": 5, "layers": LayersDescriptionRpcModel(
                                protocol="tcp",
                                dest=('127.0.0.1', tcp_server_b.tcp_port),
                                layers={'openvpn': {'mock': True}},
                            ).asdict()
                        }), timeout=3)
                    # TODO
                    self.assertDictEqual(resp, {"tunnels": []})

                loop.run_until_complete(f())
            finally:
                loop.run_until_complete(tcp_server_b.close_wait())
                loop.run_until_complete(tunnels_state_a.close_tunnels_wait())
                loop.run_until_complete(tunnels_state_b.close_tunnels_wait())
                loop.run_until_complete(rpc_responder_a.close_wait())
                loop.run_until_complete(rpc_responder_b.close_wait())


