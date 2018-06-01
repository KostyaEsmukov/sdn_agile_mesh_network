import asyncio
import functools
import os
import tempfile
import unittest
from contextlib import ExitStack
from itertools import zip_longest
from unittest.mock import MagicMock, patch

from async_exit_stack import AsyncExitStack
from mockupdb import Command, MockupDB
from ryu.base import app_manager
from ryu.controller.controller import Datapath
from ryu.lib.packet import ether_types, ethernet, packet
from ryu.ofproto import ofproto_v1_4 as ryu_ofproto
from ryu.ofproto import ofproto_v1_4_parser as ryu_ofproto_parser

from agile_mesh_network import settings
from agile_mesh_network.common.models import SwitchEntity, TunnelModel
from agile_mesh_network.common.rpc import RpcCommand, RpcUnixServer
from agile_mesh_network.common.tun_mapper import mac_to_tun_name
from agile_mesh_network.ryu import events_scheduler
from agile_mesh_network.ryu_app import (
    AgileMeshNetworkManager, FlowsLogic, OFPPacketIn, OVSManager, SwitchApp
)

LOCAL_MAC = "00:11:22:33:00:00"
SECOND_MAC = "00:11:22:33:44:01"
THIRD_MAC = "00:11:22:33:44:02"
SWITCH_ENTITY_RELAY_DATA = {
    "hostname": "relay1",
    "is_relay": True,
    "mac": LOCAL_MAC,
    "layers_config": {
        "dest": ["192.0.2.0", 65000],
        "protocol": "tcp",
        "layers": {"openvpn": {}},
    },
}
SWITCH_ENTITY_BOARD_DATA = {
    "hostname": "board1",
    "is_relay": False,
    "mac": SECOND_MAC,
    "layers_config": {
        "dest": ["192.0.2.1", 65000],
        "protocol": "tcp",
        "layers": {"openvpn": {}},
    },
}
TOPOLOGY_DATABASE_DATA = [SWITCH_ENTITY_RELAY_DATA, SWITCH_ENTITY_BOARD_DATA]

TUNNEL_MODEL_RELAY_DATA = {
    "src_mac": LOCAL_MAC,
    "dst_mac": SWITCH_ENTITY_RELAY_DATA["mac"],
    "is_dead": False,
    "is_tunnel_active": True,
    "layers": ["openvpn"],
}

TUNNEL_MODEL_BOARD_DATA = {
    **TUNNEL_MODEL_RELAY_DATA,
    "dst_mac": SWITCH_ENTITY_BOARD_DATA["mac"],
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
                manager.start_initialization()

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
                    neg = manager.negotiator_rpc
                    await manager._initialization_task

                    # TODO incoming tunnel events are respected in NV
                    # TODO relay tunnel connection is automatically sent

                    # TODO unknown tunnels after resync are dropped via RPC

            expected_event_calls = [
                [TunnelModel.from_dict(TUNNEL_MODEL_BOARD_DATA)],
                [
                    TunnelModel.from_dict(TUNNEL_MODEL_BOARD_DATA),
                    TunnelModel.from_dict(TUNNEL_MODEL_RELAY_DATA),
                ],
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


class FlowsLogicTestCase(unittest.TestCase):

    def setUp(self):
        self._stack = ExitStack()
        MockedOVSManager = self._stack.enter_context(
            patch("agile_mesh_network.ryu_app.OVSManager")
        )
        get_ofport_ex = OVSManager.get_ofport_ex
        self.ovs_manager = MockedOVSManager()
        self.ovs_manager.bridge_mac = LOCAL_MAC
        self.ovs_manager.get_ofport_ex.side_effect = functools.partial(
            get_ofport_ex, self.ovs_manager
        )

    def tearDown(self):
        self._stack.close()

    def assertOFPMatchEquals(self, m1, m2):
        self.assertEqual(m1.items(), m2.items())

    def _build_ofp_packet_in(
        self, dst_mac, src_mac=LOCAL_MAC, in_port=123
    ) -> OFPPacketIn:
        msg = MagicMock(spec=OFPPacketIn)
        msg.match = dict(in_port=in_port)
        msg.buffer_id = 47
        msg.datapath = datapath = MagicMock(spec=Datapath)
        datapath.id = 123456
        datapath.ofproto = ryu_ofproto
        datapath.ofproto_parser = ryu_ofproto_parser

        payload = bytearray(b"\xca\xfe\xba\xbe")
        msg.data = ethernet.ethernet(dst=dst_mac, src=src_mac).serialize(payload, None)
        return msg

    def test_tunnel_add(self):
        ovs_manager = self.ovs_manager

        fl = FlowsLogic(is_relay=False, ovs_manager=ovs_manager)
        self.assertIsNone(fl.relay_mac)

        # Update with one switch (w/o a relay)
        mac_to_tunswitch = {
            SWITCH_ENTITY_BOARD_DATA["mac"]: (
                TunnelModel.from_dict(TUNNEL_MODEL_BOARD_DATA),
                SwitchEntity.from_dict(SWITCH_ENTITY_BOARD_DATA),
            )
        }
        fl.sync_ovs_from_tunnels(mac_to_tunswitch)
        self.assertIsNone(fl.relay_mac)
        ovs_manager.add_port_to_bridge.assert_called_with(
            mac_to_tun_name(SWITCH_ENTITY_BOARD_DATA["mac"])
        )
        ovs_manager.add_port_to_bridge.reset_mock()

        # Update with 2 switches (one is a relay)
        mac_to_tunswitch = {
            SWITCH_ENTITY_BOARD_DATA["mac"]: (
                TunnelModel.from_dict(TUNNEL_MODEL_BOARD_DATA),
                SwitchEntity.from_dict(SWITCH_ENTITY_BOARD_DATA),
            ),
            SWITCH_ENTITY_RELAY_DATA["mac"]: (
                TunnelModel.from_dict(TUNNEL_MODEL_RELAY_DATA),
                SwitchEntity.from_dict(SWITCH_ENTITY_RELAY_DATA),
            ),
        }
        fl.sync_ovs_from_tunnels(mac_to_tunswitch)
        self.assertEqual(fl.relay_mac, SWITCH_ENTITY_RELAY_DATA["mac"])
        self.assertListEqual(
            [tun for (tun,), _ in ovs_manager.add_port_to_bridge.call_args_list],
            [
                mac_to_tun_name(SWITCH_ENTITY_BOARD_DATA["mac"]),
                mac_to_tun_name(SWITCH_ENTITY_RELAY_DATA["mac"]),
            ],
        )

        # TODO ensure that ports are removed on tunnels removal.

    def test_packet_in_on_relay(self):
        ovs_manager = self.ovs_manager
        OFPORT_BOARD = 41

        fl = FlowsLogic(is_relay=True, ovs_manager=ovs_manager)

        # Incoming packet from a switch
        msg = self._build_ofp_packet_in(dst_mac=LOCAL_MAC, src_mac=SECOND_MAC)
        fl.packet_in(msg)
        ovs_manager.get_ofport.assert_not_called()
        flow_add, packet_out = msg.datapath.send_msg.call_args_list
        (flow_add_msg,), _ = flow_add
        (packet_out_msg,), _ = packet_out
        # PACKET_OUT should be sent
        self.assertEqual(ryu_ofproto.OFPP_LOCAL, packet_out_msg.actions[0].port)
        # Flow should be installed
        self.assertEqual(
            ryu_ofproto.OFPP_LOCAL, flow_add_msg.instructions[0].actions[0].port
        )
        self.assertOFPMatchEquals(
            ryu_ofproto_parser.OFPMatch(eth_dst=LOCAL_MAC), flow_add_msg.match
        )

        # Outgoing to a connected switch
        ovs_manager.get_ofport.side_effect = lambda tun: {
            mac_to_tun_name(SECOND_MAC): OFPORT_BOARD
        }[tun]
        msg = self._build_ofp_packet_in(dst_mac=SECOND_MAC)
        fl.packet_in(msg)
        ovs_manager.get_ofport.assert_called_once()
        ovs_manager.get_ofport.reset_mock()
        flow_add, packet_out = msg.datapath.send_msg.call_args_list
        (flow_add_msg,), _ = flow_add
        (packet_out_msg,), _ = packet_out
        # PACKET_OUT should be sent
        self.assertEqual(OFPORT_BOARD, packet_out_msg.actions[0].port)
        # Flow should be installed
        self.assertEqual(OFPORT_BOARD, flow_add_msg.instructions[0].actions[0].port)
        self.assertOFPMatchEquals(
            ryu_ofproto_parser.OFPMatch(eth_dst=SECOND_MAC), flow_add_msg.match
        )

        # Outgoing to a not connected switch
        ovs_manager.get_ofport.side_effect = Exception
        msg = self._build_ofp_packet_in(dst_mac=SECOND_MAC)
        fl.packet_in(msg)
        ovs_manager.get_ofport.assert_called_once()
        ovs_manager.get_ofport.reset_mock()
        msg.datapath.send_msg.assert_not_called()

        # Incoming broadcast packet
        msg = self._build_ofp_packet_in(
            dst_mac="ff:ff:ff:ff:ff:ff", src_mac=SECOND_MAC, in_port=12399
        )
        fl.packet_in(msg)
        ovs_manager.get_ofport.assert_not_called()
        packet_out, = msg.datapath.send_msg.call_args_list
        (packet_out_msg,), _ = packet_out
        # PACKET_OUT should be sent
        self.assertEqual(ryu_ofproto.OFPP_FLOOD, packet_out_msg.actions[0].port)
        self.assertEqual(12399, packet_out_msg.in_port)

        # Outgoing broadcast packet
        msg = self._build_ofp_packet_in(
            dst_mac="ff:ff:ff:ff:ff:ff",
            src_mac=LOCAL_MAC,
            in_port=ryu_ofproto.OFPP_LOCAL,
        )
        fl.packet_in(msg)
        ovs_manager.get_ofport.assert_not_called()
        packet_out, = msg.datapath.send_msg.call_args_list
        (packet_out_msg,), _ = packet_out
        # PACKET_OUT should be sent
        self.assertEqual(ryu_ofproto.OFPP_FLOOD, packet_out_msg.actions[0].port)
        self.assertEqual(ryu_ofproto.OFPP_LOCAL, packet_out_msg.in_port)

        # TODO !! incoming multicast packet

    def test_packet_in_on_board(self):
        ovs_manager = self.ovs_manager
        OFPORT_RELAYER = 41

        fl = FlowsLogic(is_relay=False, ovs_manager=ovs_manager)

        # Incoming packet from a switch
        msg = self._build_ofp_packet_in(dst_mac=LOCAL_MAC, src_mac=SECOND_MAC)
        fl.packet_in(msg)
        ovs_manager.get_ofport.assert_not_called()
        flow_add, packet_out = msg.datapath.send_msg.call_args_list
        (flow_add_msg,), _ = flow_add
        (packet_out_msg,), _ = packet_out
        # PACKET_OUT should be sent
        self.assertEqual(ryu_ofproto.OFPP_LOCAL, packet_out_msg.actions[0].port)
        # Flow should be installed
        self.assertEqual(
            ryu_ofproto.OFPP_LOCAL, flow_add_msg.instructions[0].actions[0].port
        )
        self.assertOFPMatchEquals(
            ryu_ofproto_parser.OFPMatch(eth_dst=LOCAL_MAC), flow_add_msg.match
        )

        # Outgoing to a connected switch
        ovs_manager.get_ofport.side_effect = lambda tun: {
            mac_to_tun_name(SECOND_MAC): OFPORT_RELAYER
        }[tun]
        msg = self._build_ofp_packet_in(dst_mac=SECOND_MAC)
        fl.packet_in(msg)
        ovs_manager.get_ofport.assert_called_once()
        ovs_manager.get_ofport.reset_mock()
        flow_add, packet_out = msg.datapath.send_msg.call_args_list
        (flow_add_msg,), _ = flow_add
        (packet_out_msg,), _ = packet_out
        # PACKET_OUT should be sent
        self.assertEqual(OFPORT_RELAYER, packet_out_msg.actions[0].port)
        # Flow should be installed
        self.assertEqual(OFPORT_RELAYER, flow_add_msg.instructions[0].actions[0].port)
        self.assertOFPMatchEquals(
            ryu_ofproto_parser.OFPMatch(eth_dst=SECOND_MAC), flow_add_msg.match
        )

        # Incoming broadcast packet
        msg = self._build_ofp_packet_in(
            dst_mac="ff:ff:ff:ff:ff:ff", src_mac=SECOND_MAC, in_port=12399
        )
        fl.packet_in(msg)
        ovs_manager.get_ofport.assert_not_called()
        packet_out, = msg.datapath.send_msg.call_args_list
        (packet_out_msg,), _ = packet_out
        # PACKET_OUT should be sent
        self.assertEqual(ryu_ofproto.OFPP_LOCAL, packet_out_msg.actions[0].port)
        self.assertEqual(12399, packet_out_msg.in_port)

        # Without a connected relay - do nothing.
        ovs_manager.get_ofport.side_effect = Exception
        fl.relay_mac = None

        # Without a connected relay: Outgoing to a not connected switch
        msg = self._build_ofp_packet_in(dst_mac=SECOND_MAC)
        fl.packet_in(msg)
        ovs_manager.get_ofport.assert_called_once_with(mac_to_tun_name(SECOND_MAC))
        ovs_manager.get_ofport.reset_mock()
        msg.datapath.send_msg.assert_not_called()

        # Without a connected relay: Outgoing broadcast packet
        msg = self._build_ofp_packet_in(
            dst_mac="ff:ff:ff:ff:ff:ff",
            src_mac=LOCAL_MAC,
            in_port=ryu_ofproto.OFPP_LOCAL,
        )
        fl.packet_in(msg)
        ovs_manager.get_ofport.assert_not_called()
        msg.datapath.send_msg.assert_not_called()

        # With a connected relay
        fl.relay_mac = SECOND_MAC
        ovs_manager.get_ofport.side_effect = lambda tun: {
            mac_to_tun_name(SECOND_MAC): OFPORT_RELAYER
        }[tun]

        # With a connected relay: Outgoing to a not connected switch
        msg = self._build_ofp_packet_in(dst_mac=THIRD_MAC)
        fl.packet_in(msg)

        ofport_third, ofport_relayer = ovs_manager.get_ofport.call_args_list
        (ofport_third_tun,), _ = ofport_third
        (ofport_relayer_tun,), _ = ofport_relayer
        self.assertEqual(ofport_third_tun, mac_to_tun_name(THIRD_MAC))
        self.assertEqual(ofport_relayer_tun, mac_to_tun_name(SECOND_MAC))
        ovs_manager.get_ofport.reset_mock()

        flow_add, packet_out = msg.datapath.send_msg.call_args_list
        (flow_add_msg,), _ = flow_add
        (packet_out_msg,), _ = packet_out
        # PACKET_OUT should be sent
        self.assertEqual(OFPORT_RELAYER, packet_out_msg.actions[0].port)
        # Flow should be installed
        self.assertEqual(OFPORT_RELAYER, flow_add_msg.instructions[0].actions[0].port)
        self.assertOFPMatchEquals(
            ryu_ofproto_parser.OFPMatch(eth_dst=THIRD_MAC), flow_add_msg.match
        )

        # With a connected relay: Outgoing broadcast packet
        msg = self._build_ofp_packet_in(
            dst_mac="ff:ff:ff:ff:ff:ff",
            src_mac=LOCAL_MAC,
            in_port=ryu_ofproto.OFPP_LOCAL,
        )
        fl.packet_in(msg)
        ovs_manager.get_ofport.assert_called_once()
        ovs_manager.get_ofport.reset_mock()
        packet_out, = msg.datapath.send_msg.call_args_list
        (packet_out_msg,), _ = packet_out
        # PACKET_OUT should be sent
        self.assertEqual(OFPORT_RELAYER, packet_out_msg.actions[0].port)
        self.assertEqual(ryu_ofproto.OFPP_LOCAL, packet_out_msg.in_port)


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
