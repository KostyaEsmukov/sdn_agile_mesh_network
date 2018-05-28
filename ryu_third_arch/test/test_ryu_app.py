import asyncio
import os
import tempfile
import unittest
from contextlib import ExitStack
from time import sleep
from unittest.mock import patch

from async_exit_stack import AsyncExitStack
from mockupdb import MockupDB, go

from agile_mesh_network import settings
from agile_mesh_network.ryu_app import AgileMeshNetworkManager
from agile_mesh_network.common.rpc import RpcUnixServer


class ManagerTestCase(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        self.server = MockupDB()
        self.server.run()

        self._astack = AsyncExitStack()
        self._stack = ExitStack()

        td = self._stack.enter_context(tempfile.TemporaryDirectory())
        self.rpc_unix_sock = os.path.join(td, "l.sock")

        self._stack.enter_context(
            patch.object(settings, "REMOTE_DATABASE_MONGO_URI", self.server.uri)
        )
        self._stack.enter_context(
            patch.object(settings, "NEGOTIATOR_RPC_UNIX_SOCK_PATH", self.rpc_unix_sock)
        )

        async def command_cb(session, msg):
            assert False

        self.rpc_server = self.loop.run_until_complete(
            self._astack.enter_async_context(
                RpcUnixServer(self.rpc_unix_sock, command_cb)))

    def tearDown(self):
        self.loop.run_until_complete(self._astack.aclose())
        self._stack.close()

        self.loop.run_until_complete(self.loop.shutdown_asyncgens())
        self.loop.close()

    def test_topology_database_sync(self):
        with AgileMeshNetworkManager() as manager:
            sleep(2)
