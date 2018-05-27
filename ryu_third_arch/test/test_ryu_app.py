import unittest
from contextlib import ExitStack
from unittest.mock import patch

from mockupdb import MockupDB, go

from agile_mesh_network import settings
from agile_mesh_network.ryu_app import AgileMeshNetworkManager


class ManagerTestCase(unittest.TestCase):

    def setUp(self):
        self.server = MockupDB()
        self.server.run()
        self._stack = ExitStack()
        self._stack.enter_context(
            patch.object(settings, "REMOTE_DATABASE_MONGO_URI", self.server.uri)
        )

    def tearDown(self):
        self._stack.close()

    def test_topology_database_sync(self):
        with AgileMeshNetworkManager() as manager:
            pass
