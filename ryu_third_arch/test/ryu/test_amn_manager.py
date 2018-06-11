import asyncio
import os
import tempfile
import unittest
from contextlib import ExitStack
from itertools import zip_longest
from unittest.mock import patch

from async_exit_stack import AsyncExitStack
from mockupdb import Command, MockupDB
from ryu.base import app_manager
from ryu.controller.controller import Datapath

from agile_mesh_network import settings
from agile_mesh_network.common.models import SwitchEntity, TunnelModel
from agile_mesh_network.common.rpc import RpcCommand, RpcUnixServer
from agile_mesh_network.ryu import events_scheduler
from agile_mesh_network.ryu.amn_manager import AgileMeshNetworkManager
from test.data import (
    LOCAL_MAC, SECOND_MAC, SWITCH_ENTITY_BOARD_DATA, SWITCH_ENTITY_RELAY_DATA,
    TOPOLOGY_DATABASE_DATA, TUNNEL_MODEL_BOARD_DATA, TUNNEL_MODEL_RELAY_DATA, UNK_MAC
)


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
            patch("agile_mesh_network.ryu.amn_manager.OVSManager", DummyOVSManager)
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
        async def f():
            async with AgileMeshNetworkManager(
                ryu_ev_loop_scheduler=self.ryu_ev_loop_scheduler
            ) as manager:
                manager.start_initialization()

                topology_database = manager.topology_database
                local_database = topology_database.local
                await local_database.is_filled_event.wait()
                self.assertTrue(local_database.is_filled)

                self.assertListEqual(
                    topology_database.find_random_relay_switches(),
                    [SwitchEntity.from_dict(SWITCH_ENTITY_RELAY_DATA)],
                )

                with self.assertRaises(KeyError):
                    topology_database.find_switch_by_mac(UNK_MAC)

                self.assertEqual(
                    topology_database.find_switch_by_mac(
                        SWITCH_ENTITY_BOARD_DATA["mac"]
                    ),
                    SwitchEntity.from_dict(SWITCH_ENTITY_BOARD_DATA),
                )

                self.assertListEqual(
                    topology_database.find_switches_by_mac_list([]), []
                )
                self.assertListEqual(
                    topology_database.find_switches_by_mac_list([UNK_MAC]), []
                )
                self.assertListEqual(
                    topology_database.find_switches_by_mac_list(
                        [UNK_MAC, SWITCH_ENTITY_BOARD_DATA["mac"]]
                    ),
                    [SwitchEntity.from_dict(SWITCH_ENTITY_BOARD_DATA)],
                )

                # TODO after resync extra tunnels/flows are destroyed

        self.loop.run_until_complete(asyncio.wait_for(f(), timeout=3))

    def test_rpc(self):
        async def f():
            rpc_responses = iter(
                [
                    ("dump_tunnels_state", {"tunnels": [TUNNEL_MODEL_BOARD_DATA]}),
                    (
                        "create_tunnel",
                        {
                            "tunnel": TUNNEL_MODEL_RELAY_DATA,
                            "tunnels": [
                                TUNNEL_MODEL_BOARD_DATA,
                                TUNNEL_MODEL_RELAY_DATA,
                            ],
                        },
                    ),
                    (
                        "create_tunnel",
                        {
                            "tunnel": TUNNEL_MODEL_BOARD_DATA,
                            "tunnels": [TUNNEL_MODEL_BOARD_DATA],
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
                    manager.start_initialization()
                    await manager._initialization_task

                    self.assertDictEqual({}, manager._tunnel_creation_tasks)

                    # Don't attempt to connect to unknown macs.
                    manager.ask_for_tunnel(UNK_MAC)
                    self.assertDictEqual({}, manager._tunnel_creation_tasks)

                    # Connect to a switch, ensure that the task is cleaned up.
                    manager.ask_for_tunnel(SECOND_MAC)
                    await next(iter(manager._tunnel_creation_tasks.values()))
                    self.assertDictEqual({}, manager._tunnel_creation_tasks)

                    # Send a broadcast
                    await next(iter(self.rpc_server.sessions)).issue_broadcast(
                        "tunnel_created",
                        {
                            "tunnel": TUNNEL_MODEL_RELAY_DATA,
                            "tunnels": [TUNNEL_MODEL_RELAY_DATA],
                        },
                    )
                    await asyncio.sleep(0.001)

                    # TODO unknown tunnels after resync are dropped via RPC

            expected_event_calls = [
                # Initialization list:
                [TunnelModel.from_dict(TUNNEL_MODEL_BOARD_DATA)],
                # Initialization relay tunnel:
                [
                    TunnelModel.from_dict(TUNNEL_MODEL_BOARD_DATA),
                    TunnelModel.from_dict(TUNNEL_MODEL_RELAY_DATA),
                ],
                # ask_for_tunnel:
                [TunnelModel.from_dict(TUNNEL_MODEL_BOARD_DATA)],
                # Broadcast:
                [TunnelModel.from_dict(TUNNEL_MODEL_RELAY_DATA)],
            ]
            for (args, kwargs), ev_expected in zip_equal(
                self.ryu_ev_loop_scheduler.send_event_to_observers.call_args_list,
                expected_event_calls,
            ):
                ev = args[0]
                self.assertListEqual(
                    sorted(t for t, _ in ev.mac_to_tunswitch.values()),
                    sorted(ev_expected),
                )

        self.loop.run_until_complete(asyncio.wait_for(f(), timeout=3))

    def test_flows(self):
        async def f():
            async with AgileMeshNetworkManager(
                ryu_ev_loop_scheduler=self.ryu_ev_loop_scheduler
            ) as manager:
                manager.start_initialization()
                # TODO missing flows from RPC sync are added
                # TODO after packet in a tunnel creation request is sent
                # TODO after tunnel creation a flow is set up
                pass

        self.loop.run_until_complete(asyncio.wait_for(f(), timeout=3))


class DummyOVSManager:
    def __init__(self, *args, **kwargs):
        self.bridge_mac = LOCAL_MAC

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
