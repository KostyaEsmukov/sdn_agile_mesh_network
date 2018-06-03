from abc import ABCMeta, abstractmethod
from logging import getLogger
from typing import Mapping, Optional, Sequence, Tuple

from ryu.controller.controller import Datapath
from ryu.lib.packet import ether_types, ethernet, packet
from ryu.ofproto.ofproto_v1_4_parser import OFPAction, OFPMatch, OFPPacketIn

from agile_mesh_network.common.models import SwitchEntity, TunnelModel
from agile_mesh_network.common.tun_mapper import mac_to_tun_name
from agile_mesh_network.ryu.ovs_manager import OVSManager

logger = getLogger(__name__)


def is_group_mac(mac):
    # Multicast or Broadcast.
    # https://tools.ietf.org/html/rfc7042#section-2.1
    first_octet = int(mac[:2], 16)
    return bool(first_octet & 1)


class NoSuitableOutPortError(Exception):
    pass


class TunnelIntentionsProvider(metaclass=ABCMeta):

    @abstractmethod
    def ask_for_tunnel(self, dst_mac):
        pass


class FlowsLogic:
    # Execution context: run entirely in the Ryu thread (eventlet's
    # green thread, actually), no thread safety is required. Async
    # operations can be performed using ryu's `hub` module.

    def __init__(
        self,
        is_relay: bool,
        ovs_manager: OVSManager,
        tunnel_intentions_provider: TunnelIntentionsProvider,
    ) -> None:
        self.is_relay: bool = is_relay
        self.relay_mac: Optional[str] = None
        self.ovs_manager: OVSManager = ovs_manager
        self.tunnel_intentions_provider = tunnel_intentions_provider
        self.datapath = None
        # self.mac_to_port = {}

    def set_datapath(self, datapath: Datapath) -> None:
        # TODO And that's it? just an assignment? Shouldn't we queue unsent messages?
        self.datapath = datapath

    def sync_ovs_from_tunnels(
        self, mac_to_tunswitch: Mapping[str, Tuple[TunnelModel, SwitchEntity]]
    ) -> None:

        logger.warning(
            "negotiator tunnels are:\n%s\n%s\n%s",
            "-" * 40,
            "\n\n".join(
                f"- {mac} {mac_to_tun_name(mac)} {swi.hostname}:\n  |--{tun}\n  |--{swi}"
                for mac, (tun, swi) in sorted(mac_to_tunswitch.items())
            ),
            "-" * 40,
        )

        expected_tuns_in_bridge = [
            mac_to_tun_name(tunnel.dst_mac) for tunnel, _ in mac_to_tunswitch.values()
        ]

        for tun in expected_tuns_in_bridge:
            self.ovs_manager.add_port_to_bridge(tun)

        current_tuns_set = set(self.ovs_manager.get_ports_in_bridge())
        expected_tuns_set = set(expected_tuns_in_bridge)
        extraneous_tuns_in_bridge = current_tuns_set - expected_tuns_set

        if extraneous_tuns_in_bridge:
            logger.debug(
                "Removing extraneous ports from bridge: %s", extraneous_tuns_in_bridge
            )

        for tun in extraneous_tuns_in_bridge:
            self.ovs_manager.del_port_from_bridge(tun)
        # TODO remove flows via relay

        relays = [
            tunnel for tunnel, switch in mac_to_tunswitch.values() if switch.is_relay
        ]
        assert len(relays) <= 1
        if relays:
            self.relay_mac = relays[0].dst_mac

    def packet_in(self, msg: OFPPacketIn) -> None:
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match["in_port"]

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocols(ethernet.ethernet)[0]

        if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            # ignore lldp packet
            return

        dst = eth.dst
        src = eth.src
        dpid = datapath.id
        # self.mac_to_port.setdefault(dpid, {})

        logger.info(
            "packet in [%s %s %s %s] [dpid src dst in_port]",
            dpid,
            src,
            dst,
            self._ofport_to_string(in_port, ofproto),
        )

        # self.mac_to_port[dpid][src] = in_port

        is_broadcast = is_group_mac(dst)
        out_port = -1
        if dst == self.ovs_manager.bridge_mac:
            # This packet is specifically for us - capture it!
            out_port = ofproto.OFPP_LOCAL
        elif not is_broadcast:
            # This packet is for someone else - send it right away
            # if receiver is locally connected.
            out_port = self.ovs_manager.get_ofport_ex(mac_to_tun_name(dst))

        # Either receiver is not directly reachable,
        # or it's a broadcast/multicast packet.
        if out_port < 0:
            try:
                if self.is_relay:
                    out_port = self._packet_in_relay(dst, ofproto)
                else:
                    out_port = self._packet_in_board(dst, src, in_port, ofproto)
            except NoSuitableOutPortError as e:
                logger.info("PACKET_IN: dropping packet: %s", str(e))
                # TODO ICMP unreachable?
                return
        assert out_port >= 0
        logger.info(
            "PACKET_IN: decision: out_port %s",
            self._ofport_to_string(out_port, ofproto),
        )

        # if dst in self.mac_to_port[dpid]:
        #     out_port = self.mac_to_port[dpid][dst]

        actions = [parser.OFPActionOutput(out_port)]

        if not is_broadcast:
            assert out_port != ofproto.OFPP_FLOOD
            # install a flow to avoid packet_in next time
            match = parser.OFPMatch(eth_dst=dst)
            self.add_flow(datapath, 1, match, actions)

        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data

        out = parser.OFPPacketOut(
            datapath=datapath,
            buffer_id=msg.buffer_id,
            in_port=in_port,
            actions=actions,
            data=data,
        )
        datapath.send_msg(out)

    def _ofport_to_string(self, ofport, ofproto):
        for s in ("OFPP_LOCAL", "OFPP_FLOOD"):
            if ofport == getattr(ofproto, s):
                return s

        try:
            port_name = self.ovs_manager.get_port_name_by_ofport(ofport)
        except KeyError:
            port_name = "<unknown port>"
        return f"[ofport={ofport}, port_name={port_name}]"

    def _packet_in_board(self, dst, src, in_port, ofproto):
        is_broadcast = is_group_mac(dst)
        if is_broadcast and in_port != ofproto.OFPP_LOCAL:
            # Broadcast/multicast, originating from somewhere else.
            # Assuming that we don't relay broadcasts, we should forward
            # it to the bridge.
            return ofproto.OFPP_LOCAL

        # Assuming that each board is connected to a relayer, and
        # relayers are in the same L2 domain, outgoing broadcast/multicast
        # packets should be sent to a relayer only.

        if not is_broadcast:
            self.tunnel_intentions_provider.ask_for_tunnel(dst)

        if not self.relay_mac:
            raise NoSuitableOutPortError("no relay connected")

        out_port = self.ovs_manager.get_ofport_ex(mac_to_tun_name(self.relay_mac))
        if out_port < 0:
            raise NoSuitableOutPortError("relay out_port is faulty")

        return out_port

    def _packet_in_relay(self, dst, ofproto):
        if is_group_mac(dst):
            return ofproto.OFPP_FLOOD

        # This packet is for some switch which is not connected yet.

        # TODO try to reach them via other switches?
        # like using self.mac_to_port

        # TODO maybe try to start negotiation? (then we should also
        # buffer this packet and send them after the tunnel is created).
        # Although it's unlikely that they're reachable/will respond.
        raise NoSuitableOutPortError("destination is not connected")

    def add_flow(
        self,
        datapath: Datapath,
        priority,
        match: Sequence[OFPMatch],
        actions: Sequence[OFPAction],
    ) -> None:
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]

        mod = parser.OFPFlowMod(
            datapath=datapath,
            priority=priority,
            match=match,
            instructions=inst,
            # idle_timeout=3600,  # TODO
            # hard_timeout=3600,  # TODO
        )
        datapath.send_msg(mod)
