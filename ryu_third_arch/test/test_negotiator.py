import asyncio
import tempfile
import os
from unittest import TestCase
from unittest.mock import patch

from agile_mesh_network.negotiator_main import RpcResponder, TunnelsState
from agile_mesh_network.common.rpc import RpcSession, RpcUnixClient


class CreateOpenvpnTunnelUseCase(TestCase):

    def test_create_openvpn_tunnel_use_case(self):
        loop = asyncio.get_event_loop()
        with tempfile.TemporaryDirectory() as td, \
                patch.object(RpcResponder, 'socket_path',
                             os.path.join(td, 'l.sock')):

            tunnels_state = TunnelsState(loop=loop)
            rpc_responder = RpcResponder(tunnels_state, loop=loop)
            loop.run_until_complete(rpc_responder.start_server())

            try:
                async def command_cb(session, msg):
                    assert False

                async def f():
                    rpc_c = RpcUnixClient(os.path.join(td, 'l.sock'), command_cb,
                                          loop=loop)
                    await rpc_c.start()
                    rpc = rpc_c.session
                    resp = await asyncio.wait_for(
                        rpc.issue_command("dump_tunnels_state"), timeout=3)
                    self.assertDictEqual(resp, {"tunnels": []})

                loop.run_until_complete(f())
            finally:
                loop.run_until_complete(tunnels_state.close_tunnels_wait())
                loop.run_until_complete(rpc_responder.close_wait())
                loop.close()
