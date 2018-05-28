import asyncio
import os
import tempfile
import unittest
from contextlib import ExitStack
from time import sleep
from unittest.mock import patch

from async_exit_stack import AsyncExitStack
from mockupdb import Command, MockupDB

from agile_mesh_network import settings
from agile_mesh_network.common.models import SwitchEntity
from agile_mesh_network.common.rpc import RpcUnixServer
from agile_mesh_network.ryu_app import AgileMeshNetworkManager

SWITCH_ENTITY_RELAY_DATA = {
    "hostname": "relay1",
    "is_relay": True,
    "mac": "00:11:22:33:44:00",
    "layers_config": {},
}
SWITCH_ENTITY_BOARD_DATA = {
    "hostname": "board1",
    "is_relay": False,
    "mac": "00:11:22:33:44:01",
    "layers_config": {},
}
TOPOLOGY_DATABASE_DATA = [SWITCH_ENTITY_RELAY_DATA, SWITCH_ENTITY_BOARD_DATA]


class ManagerTestCase(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        self.server = MockupDB(auto_ismaster={"maxWireVersion": 6})
        self.server.run()
        self.server.autoresponds(
            Command("find", "switch_collection", namespace="topology_database"),
            {
                "cursor": {
                    "id": 0,
                    "firstBatch": [
                        {**d, "_id": i} for i, d in enumerate(TOPOLOGY_DATABASE_DATA)
                    ],
                }
            },
        )

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
                RpcUnixServer(self.rpc_unix_sock, command_cb)
            )
        )

    def tearDown(self):
        self.loop.run_until_complete(self._astack.aclose())
        self._stack.close()

        self.loop.run_until_complete(self.loop.shutdown_asyncgens())
        self.loop.close()

    def test_topology_database_sync(self):
        with AgileMeshNetworkManager() as manager:
            local_database = manager.topology_database.local
            self.assertTrue(local_database.is_filled_event.wait(timeout=2))
            self.assertTrue(local_database.is_filled)

            self.assertListEqual(
                manager.topology_database.find_random_relay_switches(),
                [SwitchEntity(**SWITCH_ENTITY_RELAY_DATA)],
            )

            with self.assertRaises(KeyError):
                manager.topology_database.find_switch_by_mac("99:99:99:88:88:88")

            self.assertEqual(
                manager.topology_database.find_switch_by_mac(
                    SWITCH_ENTITY_BOARD_DATA["mac"]
                ),
                SwitchEntity(**SWITCH_ENTITY_BOARD_DATA),
            )
