import functools
import unittest
from contextlib import ExitStack
from unittest.mock import MagicMock, patch

from ryu.base import app_manager
from ryu.controller.controller import Datapath
from ryu.lib.packet import ethernet
from ryu.ofproto import ofproto_v1_4 as ryu_ofproto
from ryu.ofproto import ofproto_v1_4_parser as ryu_ofproto_parser

from agile_mesh_network.common.models import SwitchEntity, TunnelModel
from agile_mesh_network.common.tun_mapper import mac_to_tun_name
from agile_mesh_network.ryu.flows_logic import (
    FlowsLogic, OFPPacketIn, TunnelIntentionsProvider, is_group_mac
)
from agile_mesh_network.ryu.ovs_manager import OVSManager
from agile_mesh_network.ryu.topology_database import TopologyDatabase
from test.data import (
    LOCAL_MAC, SECOND_MAC, SWITCH_ENTITY_BOARD_DATA, SWITCH_ENTITY_RELAY_DATA, THIRD_MAC,
    TUNNEL_MODEL_BOARD_DATA, TUNNEL_MODEL_RELAY_DATA, UNK_MAC
)


class FlowsLogicTestCase(unittest.TestCase):

    def setUp(self):
        self._stack = ExitStack()
        get_ofport_ex = OVSManager.get_ofport_ex
        MockedOVSManager = self._stack.enter_context(
            patch("agile_mesh_network.ryu.ovs_manager.OVSManager")
        )
        self.ovs_manager = MockedOVSManager()
        self.ovs_manager.bridge_mac = LOCAL_MAC
        self.ovs_manager.get_ofport_ex.side_effect = functools.partial(
            get_ofport_ex, self.ovs_manager
        )
        self.tunnel_intentions_provider = MagicMock(spec=TunnelIntentionsProvider)()
        self.topology_database = MagicMock(spec=TopologyDatabase)()
        self.topology_database.find_switch_by_mac.side_effect = (
            self._mocked_find_switch_by_mac
        )

    def tearDown(self):
        self._stack.close()

    def assertOFPMatchEquals(self, m1, m2):
        self.assertEqual(m1.items(), m2.items())

    def test_group_mac(self):
        # https://tools.ietf.org/html/rfc7042#section-2.1
        self.assertTrue(is_group_mac("ff:ff:ff:ff:ff:ff"))
        self.assertTrue(is_group_mac("01:00:5E:00:00:00"))
        self.assertTrue(is_group_mac("33:33:00:00:11:22"))

        self.assertFalse(is_group_mac("02:11:22:33:33:01"))
        self.assertFalse(is_group_mac(LOCAL_MAC))

    def _mocked_find_switch_by_mac(self, mac):
        return {
            LOCAL_MAC: SwitchEntity.from_dict(SWITCH_ENTITY_RELAY_DATA),
            SECOND_MAC: SwitchEntity.from_dict(SWITCH_ENTITY_BOARD_DATA),
            THIRD_MAC: SwitchEntity.from_dict(
                {**SWITCH_ENTITY_BOARD_DATA, "mac": THIRD_MAC}
            ),
        }[mac]

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

        fl = FlowsLogic(
            is_relay=False,
            ovs_manager=ovs_manager,
            tunnel_intentions_provider=self.tunnel_intentions_provider,
            topology_database=self.topology_database,
        )
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

        fl = FlowsLogic(
            is_relay=True,
            ovs_manager=ovs_manager,
            tunnel_intentions_provider=self.tunnel_intentions_provider,
            topology_database=self.topology_database,
        )

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

        # Outgoing to a foreign mac address
        ovs_manager.get_ofport.side_effect = Exception
        msg = self._build_ofp_packet_in(dst_mac=UNK_MAC)
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

        self.tunnel_intentions_provider.ask_for_tunnel.assert_not_called()

    def test_packet_in_on_board(self):
        ovs_manager = self.ovs_manager
        OFPORT_RELAYER = 41

        fl = FlowsLogic(
            is_relay=False,
            ovs_manager=ovs_manager,
            tunnel_intentions_provider=self.tunnel_intentions_provider,
            topology_database=self.topology_database,
        )

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
        self.tunnel_intentions_provider.ask_for_tunnel.assert_called_once_with(
            SECOND_MAC
        )
        self.tunnel_intentions_provider.ask_for_tunnel.reset_mock()

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

        self.tunnel_intentions_provider.ask_for_tunnel.assert_called_once_with(
            THIRD_MAC
        )
        self.tunnel_intentions_provider.ask_for_tunnel.reset_mock()

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

        # Outgoing to a foreign mac address
        msg = self._build_ofp_packet_in(dst_mac=UNK_MAC)
        fl.packet_in(msg)
        ovs_manager.get_ofport.assert_called_once_with(mac_to_tun_name(UNK_MAC))
        ovs_manager.get_ofport.reset_mock()
        msg.datapath.send_msg.assert_not_called()
        self.tunnel_intentions_provider.ask_for_tunnel.assert_not_called()
