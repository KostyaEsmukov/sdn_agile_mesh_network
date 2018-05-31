import asyncio
import os
import tempfile
import unittest
from contextlib import ExitStack
from itertools import zip_longest
from unittest.mock import patch

from async_exit_stack import AsyncExitStack
from mockupdb import Command, MockupDB

from agile_mesh_network import settings
from agile_mesh_network.common.models import SwitchEntity, TunnelModel
from agile_mesh_network.common.rpc import RpcCommand, RpcUnixServer
from agile_mesh_network.ryu import events_scheduler
from agile_mesh_network.ryu_app import AgileMeshNetworkManager, SwitchApp

LOCAL_MAC = "00:11:22:33:00:00"
SWITCH_ENTITY_RELAY_DATA = {
    "hostname": "relay1",
    "is_relay": True,
    "mac": "00:11:22:33:44:00",
    "layers_config": {
        "dest": ["192.0.2.0", 65000],
        "protocol": "tcp",
        "layers": {"openvpn": {}},
    },
}
SWITCH_ENTITY_BOARD_DATA = {
    "hostname": "board1",
    "is_relay": False,
    "mac": "00:11:22:33:44:01",
    "layers_config": {
        "dest": ["192.0.2.1", 65000],
        "protocol": "tcp",
        "layers": {"openvpn": {}},
    },
}
TOPOLOGY_DATABASE_DATA = [SWITCH_ENTITY_RELAY_DATA, SWITCH_ENTITY_BOARD_DATA]

SAMPLE_TUNNEL_DATA = {
    "src_mac": LOCAL_MAC,
    "dst_mac": "00:11:22:33:44:01",
    "is_dead": False,
    "is_tunnel_active": True,
    "layers": ["openvpn"],
}

SAMPLE_RELAY_TUNNEL_DATA = {
    **SAMPLE_TUNNEL_DATA,
    "dst_mac": SWITCH_ENTITY_RELAY_DATA["mac"],
}


def zip_equal(*iterables):
    # https://stackoverflow.com/a/32954700
    sentinel = object()
    for combo in zip_longest(*iterables, fillvalue=sentinel):
        if sentinel in combo:
            raise ValueError("Iterables have different lengths")
        yield combo


class ManagerTestCase(unittest.TestCase):
    maxDiff = None  # unittest: show full diff on assertion failure

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

        self._stack = AsyncExitStack()

        td = self._stack.enter_context(tempfile.TemporaryDirectory())
        self.rpc_unix_sock = os.path.join(td, "l.sock")

        self._stack.enter_context(
            patch.object(settings, "REMOTE_DATABASE_MONGO_URI", self.server.uri)
        )
        self._stack.enter_context(
            patch.object(settings, "NEGOTIATOR_RPC_UNIX_SOCK_PATH", self.rpc_unix_sock)
        )
        self._stack.enter_context(
            patch("agile_mesh_network.ryu_app.OVSManager", DummyOVSManager)
        )
        self._stack.enter_context(
            # To avoid automatic connection to a relay.
            patch.object(settings, "IS_RELAY", True)
        )

        self._stack.enter_context(
            patch.object(events_scheduler, "RyuAppEventLoopScheduler")
        )
        self.ryu_ev_loop_scheduler = events_scheduler.RyuAppEventLoopScheduler()
        self._stack.enter_context(self.ryu_ev_loop_scheduler)

        async def command_cb(session, msg):
            assert isinstance(msg, RpcCommand)
            await self._rpc_command_cb(msg)

        self.rpc_server = self.loop.run_until_complete(
            self._stack.enter_async_context(
                RpcUnixServer(self.rpc_unix_sock, command_cb)
            )
        )

    async def _rpc_command_cb(self, msg: RpcCommand):
        self.assertEqual(msg.name, "dump_tunnels_state")
        await msg.respond({"tunnels": []})

    def tearDown(self):
        self.loop.run_until_complete(self._stack.aclose())

        self.loop.run_until_complete(self.loop.shutdown_asyncgens())
        self.loop.close()

        self.server.stop()

    def test_topology_database_sync(self):
        unk_mac = "99:99:99:88:88:88"

        async def f():
            async with AgileMeshNetworkManager(
                ryu_ev_loop_scheduler=self.ryu_ev_loop_scheduler
            ) as manager:
                topology_database = manager.topology_database
                local_database = topology_database.local
                await local_database.is_filled_event.wait()
                self.assertTrue(local_database.is_filled)

                self.assertListEqual(
                    topology_database.find_random_relay_switches(),
                    [SwitchEntity(**SWITCH_ENTITY_RELAY_DATA)],
                )

                with self.assertRaises(KeyError):
                    topology_database.find_switch_by_mac(unk_mac)

                self.assertEqual(
                    topology_database.find_switch_by_mac(
                        SWITCH_ENTITY_BOARD_DATA["mac"]
                    ),
                    SwitchEntity(**SWITCH_ENTITY_BOARD_DATA),
                )

                self.assertListEqual(
                    topology_database.find_switches_by_mac_list([]), []
                )
                self.assertListEqual(
                    topology_database.find_switches_by_mac_list([unk_mac]), []
                )
                self.assertListEqual(
                    topology_database.find_switches_by_mac_list(
                        [unk_mac, SWITCH_ENTITY_BOARD_DATA["mac"]]
                    ),
                    [SwitchEntity(**SWITCH_ENTITY_BOARD_DATA)],
                )

                # TODO after resync extra tunnels/flows are destroyed

        self.loop.run_until_complete(asyncio.wait_for(f(), timeout=3))

    def test_rpc(self):

        async def f():
            rpc_responses = iter(
                [
                    ("dump_tunnels_state", {"tunnels": [SAMPLE_TUNNEL_DATA]}),
                    (
                        "create_tunnel",
                        {
                            "tunnel": SAMPLE_RELAY_TUNNEL_DATA,
                            "tunnels": [SAMPLE_TUNNEL_DATA, SAMPLE_RELAY_TUNNEL_DATA],
                        },
                    ),
                ]
            )

            async def _rpc_command_cb(msg: RpcCommand):
                name, resp = next(rpc_responses)
                self.assertEqual(msg.name, name)
                await msg.respond(resp)

            with ExitStack() as stack:
                stack.enter_context(
                    patch.object(self, "_rpc_command_cb", _rpc_command_cb)
                )
                stack.enter_context(patch.object(settings, "IS_RELAY", False))

                async with AgileMeshNetworkManager(
                    ryu_ev_loop_scheduler=self.ryu_ev_loop_scheduler
                ) as manager:
                    neg = manager.negotiator_rpc
                    await manager._initialization_task

                    # TODO incoming tunnel events are respected in NV
                    # TODO relay tunnel connection is automatically sent

                    # TODO unknown tunnels after resync are dropped via RPC

            expected_event_calls = [
                [TunnelModel.from_dict(SAMPLE_TUNNEL_DATA)],
                [
                    TunnelModel.from_dict(SAMPLE_TUNNEL_DATA),
                    TunnelModel.from_dict(SAMPLE_RELAY_TUNNEL_DATA),
                ],
            ]
            for (args, kwargs), ev_expected in zip_equal(
                self.ryu_ev_loop_scheduler.send_event_to_observers.call_args_list,
                expected_event_calls,
            ):
                ev = args[0]
                self.assertListEqual(sorted(ev.tunnels), sorted(ev_expected))

        self.loop.run_until_complete(asyncio.wait_for(f(), timeout=3))

    def test_flows(self):

        async def f():
            async with AgileMeshNetworkManager(
                ryu_ev_loop_scheduler=self.ryu_ev_loop_scheduler
            ) as manager:
                # TODO missing flows from RPC sync are added
                # TODO after packet in a tunnel creation request is sent
                # TODO after tunnel creation a flow is set up
                pass

        self.loop.run_until_complete(asyncio.wait_for(f(), timeout=3))


# class RyuAppTestCase(unittest.TestCase):

#     def setUp(self):
#         self.app = SwitchApp()
#         self.app.start()

#     def tearDown(self):
#         self.app.stop()

#     def test(self):
#         pass


class DummyOVSManager:

    def __init__(self, *args, **kwargs):
        self.bridge_mac = LOCAL_MAC

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
