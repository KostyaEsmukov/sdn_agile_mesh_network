from abc import ABCMeta, abstractmethod
from enum import Enum
from logging import getLogger
from timeit import default_timer
from typing import Dict, Iterable, Mapping, Optional, Sequence, Tuple

from ryu.controller.controller import Datapath
from ryu.lib import hub
from ryu.lib.packet import ether_types, ethernet, packet
from ryu.ofproto.ofproto_v1_4_parser import (
    OFPAction, OFPMatch, OFPPacketIn, OFPPort, OFPPortStatus
)

from agile_mesh_network import settings
from agile_mesh_network.common.models import SwitchEntity, TunnelModel
from agile_mesh_network.common.tun_mapper import mac_to_tun_name
from agile_mesh_network.common.types import MACAddress, TUNPortName
from agile_mesh_network.ryu.ovs_manager import OVSManager
from agile_mesh_network.ryu.topology_database import TopologyDatabase
from agile_mesh_network.ryu.types import OFPort

logger = getLogger(__name__)


class OFPriority(Enum):
    RELAY = 1
    DIRECT = 10


MACToTunnelAndSwitch = Mapping[MACAddress, Tuple[TunnelModel, SwitchEntity]]
TUNToTunnelAndSwitch = Mapping[TUNPortName, Tuple[TunnelModel, SwitchEntity]]


def is_group_mac(mac: MACAddress) -> bool:
    # Multicast or Broadcast.
    # https://tools.ietf.org/html/rfc7042#section-2.1
    first_octet = int(mac[:2], 16)
    return bool(first_octet & 1)


class FlowHysteresis:
    """This class protects against installing flows before
    `hysteresis_seconds` pass since adding a port to the bridge.

    This allows to solve 2 problems:

    1. Right after adding a TUN port to the bridge, its ofport might be
    not initialized yet (in my observations this doesn't last more
    than 2.0 seconds).

    2. If we install flows immediately, the other side might not even
    added the port to their bridge yet, so the sent date frames would be
    lost. The hysteresis should be reasonable enough for both sides
    to have a chance to add the TUNs to their bridges before actual
    traffic starts flowing.
    """

    def __init__(
        self, is_relay: bool, ovs_manager: OVSManager, hysteresis_seconds: float = 4
    ) -> None:
        self.is_relay = is_relay  # On relay hysteresis is disabled.
        self.ovs_manager = ovs_manager
        self.hysteresis_seconds = hysteresis_seconds
        # Special clock value for relays, because flows to them
        # must be installed as soon as possible.
        self._relay_clock = self.clock() - hysteresis_seconds * 2
        # Mapping from TUNPortName to `self.clock()`, when the corresponding
        # tunnel was reported to be `active`.
        self._tun_to_active_clock: Dict[TUNPortName, float] = {}

    def clock(self) -> float:  # pragma: no cover
        return default_timer()

    def update(self, tun_to_tunswitch: TUNToTunnelAndSwitch) -> None:
        known_tuns = set(self._tun_to_active_clock.keys())
        new_tuns = set(tun_to_tunswitch.keys())
        extraneous = known_tuns - new_tuns
        for tun in extraneous:
            self._tun_to_active_clock.pop(tun, None)

        now = self.clock()

        for tun, (tunnel, switch) in tun_to_tunswitch.items():
            if tunnel.is_tunnel_active:
                def_clock = self._relay_clock if switch.is_relay else now
                # Keep already set, set current clock otherwise.
                self._tun_to_active_clock.setdefault(tun, def_clock)
            else:
                # Remove, if not active.
                self._tun_to_active_clock.pop(tun, None)

    def get_ofport_if_tun_is_ready(self, tun: TUNPortName) -> OFPort:
        clock_active = self._tun_to_active_clock.get(tun)
        if clock_active is None:
            raise PortNotReadyError("Unknown TUNPortName")
        if self.is_relay:
            return self._ofport_or_raise(tun)
        if self.clock() - clock_active < self.hysteresis_seconds:
            raise PortNotReadyError("Hysteresis delay is not over yet.")
        return self._ofport_or_raise(tun)

    def _ofport_or_raise(self, tun: TUNPortName) -> OFPort:
        ofport = self.ovs_manager.get_ofport_ex(tun)
        if ofport < 0:
            raise PortNotReadyError("Returned ofport is negative.")
        return ofport


class NoSuitableOutPortError(Exception):
    pass


class PortNotReadyError(Exception):
    pass


class TunnelIntentionsProvider(metaclass=ABCMeta):

    @abstractmethod
    def ask_for_tunnel(self, dst_mac: MACAddress) -> None:
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
        topology_database: TopologyDatabase,
    ) -> None:
        self.is_relay: bool = is_relay
        self.relay_tun: Optional[TUNPortName] = None
        self.ovs_manager: OVSManager = ovs_manager
        self.tunnel_intentions_provider = tunnel_intentions_provider
        self.topology_database = topology_database
        self.datapath: Optional[Datapath] = None
        # self.mac_to_port = {}

        self.flow_hysteresis = FlowHysteresis(is_relay, ovs_manager)

        # Cached mapping from TUNPortName to MACAddress (for direct tunnels).
        self._tun_to_dest_mac: Dict[TUNPortName, MACAddress] = {}

    def set_datapath(self, datapath: Datapath) -> None:
        # TODO And that's it? just an assignment? Shouldn't we queue unsent messages?
        self.datapath = datapath

    def sync_ovs_from_tunnels(self, mac_to_tunswitch: MACToTunnelAndSwitch) -> None:

        logger.warning(
            "negotiator tunnels are:\n%s\n%s\n%s",
            "-" * 40,
            "\n\n".join(
                f"- {mac} {mac_to_tun_name(mac)} {swi.hostname}:\n  |--{tun}\n  |--{swi}"
                for mac, (tun, swi) in sorted(mac_to_tunswitch.items())
            ),
            "-" * 40,
        )

        tun_to_tunswitch: TUNToTunnelAndSwitch = {
            mac_to_tun_name(mac): (tunnel, switch)
            for mac, (tunnel, switch) in mac_to_tunswitch.items()
        }

        # TODO clean the dict? Eventually it might eat up a lot of memory.
        self._tun_to_dest_mac.update(
            (tun, tunnel.dst_mac) for tun, (tunnel, _) in tun_to_tunswitch.items()
        )

        self._update_relay_mac_in_ovs_sync(mac_to_tunswitch)
        self._add_ports_to_bridge(tun_to_tunswitch)
        self.flow_hysteresis.update(tun_to_tunswitch)
        self._remove_extraneous_ports_in_ovs_sync(tun_to_tunswitch)

        # Create a new eventlet's green thread (akin to asyncio.ensure_future).
        hub.spawn_after(
            self.flow_hysteresis.hysteresis_seconds + 0.1,
            self._update_port_flows,
            list(tun_to_tunswitch.keys()),
        )

    def _update_relay_mac_in_ovs_sync(
        self, mac_to_tunswitch: MACToTunnelAndSwitch
    ) -> None:
        relays = [
            tunnel for tunnel, switch in mac_to_tunswitch.values() if switch.is_relay
        ]
        assert len(relays) <= 1
        # TODO maybe drop all except one instead of failing?
        self.relay_tun = None
        if relays:
            self.relay_tun = mac_to_tun_name(relays[0].dst_mac)

    def _add_ports_to_bridge(self, tun_to_tunswitch: TUNToTunnelAndSwitch) -> None:
        for tun in tun_to_tunswitch.keys():
            self.ovs_manager.add_port_to_bridge(tun)

    def _remove_extraneous_ports_in_ovs_sync(
        self, tun_to_tunswitch: TUNToTunnelAndSwitch
    ) -> None:
        current_tuns_set = set(self.ovs_manager.get_ports_in_bridge())
        expected_tuns_set = set(tun_to_tunswitch.keys())
        extraneous_tuns = current_tuns_set - expected_tuns_set

        if not extraneous_tuns:
            return

        logger.debug("Removing extraneous ports from bridge: %s", extraneous_tuns)

        for tun in extraneous_tuns:
            # Flows will be removed in the `ofp_event.EventOFPPortStatus`
            # event handler (See self.cleanup_flows_for_deleted_port).
            self.ovs_manager.del_port_from_bridge(tun)

    def _update_port_flows(self, tuns: Iterable[TUNPortName]) -> None:
        datapath = self.datapath
        assert datapath, "Datapath is undefined. Is controller connected?"
        parser = datapath.ofproto_parser

        for tun in tuns:
            self.add_flows_for_new_port(datapath, parser, tun)

    def packet_in(self, msg: OFPPacketIn) -> None:
        datapath: Datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port: OFPort = OFPort(msg.match["in_port"])

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocols(ethernet.ethernet)[0]

        if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            # ignore lldp packet
            return

        dst: MACAddress = eth.dst
        src: MACAddress = eth.src
        dpid: int = datapath.id  # Bridge identifier in OVS, usually derived from MAC.
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
        out_port: OFPort = OFPort(-1)
        priority: Optional[OFPriority] = OFPriority.DIRECT
        if dst == self.ovs_manager.bridge_mac:
            # This packet is specifically for us - capture it!
            out_port = ofproto.OFPP_LOCAL
        elif not is_broadcast:
            # This packet is for someone else - send it right away
            # if receiver is locally connected.
            tun = mac_to_tun_name(dst)
            try:
                out_port = self.flow_hysteresis.get_ofport_if_tun_is_ready(tun)
            except PortNotReadyError:
                pass
        # Either receiver is not directly reachable,
        # or it's a broadcast/multicast packet.
        if out_port < 0:
            try:
                if self.is_relay:
                    out_port, priority = self._packet_in_relay(dst, ofproto)
                else:
                    out_port, priority = self._packet_in_board(
                        dst, src, in_port, ofproto
                    )
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
            assert priority in (OFPriority.DIRECT, OFPriority.RELAY)
            is_direct = priority == OFPriority.DIRECT
            # install a flow to avoid packet_in next time
            match = parser.OFPMatch(eth_dst=dst)
            self.add_flow(datapath, priority, match, actions, is_direct_flow=is_direct)
            logger.info(
                "Added %s flow to %s via %s",
                "direct" if is_direct else "indirect",
                dst,
                self._ofport_to_string(out_port, ofproto),
            )

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

    def _ofport_to_string(self, ofport: OFPort, ofproto) -> str:
        for s in ("OFPP_LOCAL", "OFPP_FLOOD"):
            if ofport == getattr(ofproto, s):
                return s

        try:
            port_name: str = self.ovs_manager.get_port_name_by_ofport(ofport)
        except KeyError:
            port_name = "<unknown port>"
        return f"[ofport={ofport}, port_name={port_name}]"

    def _packet_in_board(
        self, dst: MACAddress, src: MACAddress, in_port: OFPort, ofproto
    ) -> Tuple[OFPort, OFPriority]:
        is_broadcast = is_group_mac(dst)
        if is_broadcast and in_port != ofproto.OFPP_LOCAL:
            # Broadcast/multicast, originating from somewhere else.
            # Assuming that we don't relay broadcasts, we should forward
            # it to the bridge.
            return ofproto.OFPP_LOCAL, OFPriority.DIRECT

        # Assuming that each board is connected to a relayer, and
        # relayers are in the same L2 domain, outgoing broadcast/multicast
        # packets should be sent to a relayer only.

        if not is_broadcast:
            if not self._allow_unicast_packet(dst):
                raise NoSuitableOutPortError(
                    f"This mac address {dst} doesn't belong to any known switch"
                )
            self.tunnel_intentions_provider.ask_for_tunnel(dst)

        if not self.relay_tun:
            raise NoSuitableOutPortError("no relay connected")

        try:
            out_port = self.flow_hysteresis.get_ofport_if_tun_is_ready(self.relay_tun)
        except PortNotReadyError:
            raise NoSuitableOutPortError("relay out_port is faulty or not ready yet")

        return out_port, OFPriority.RELAY

    def _packet_in_relay(
        self, dst: MACAddress, ofproto
    ) -> Tuple[OFPort, Optional[OFPriority]]:
        if is_group_mac(dst):
            return ofproto.OFPP_FLOOD, None

        if not self._allow_unicast_packet(dst):
            raise NoSuitableOutPortError(
                f"This mac address {dst} doesn't belong to any known switch"
            )

        # This packet is for some switch which is not connected yet.

        # TODO try to reach them via other switches?
        # like using self.mac_to_port

        # We could start trying to negotiate with that switch here,
        # but this is probably a bad idea:
        # 1. Relays might be in the same L2 domain (in a cloud,
        # for example), in this case that frame should be forwarded
        # to other relays, which might have that switch connected.
        # 2. Switch initiates connection to a relay anyway.
        # Assuming that relays are deployed in a cloud, which are
        # always reachable (i.e. no NATs/Proxies in front of them),
        # if switch is not connected, then it's 99.9% probability
        # that it's unreachable.
        raise NoSuitableOutPortError("destination is not connected")

    def _allow_unicast_packet(self, dst_mac: MACAddress) -> bool:
        assert not is_group_mac(dst_mac)
        try:
            self.topology_database.find_switch_by_mac(dst_mac)
        except KeyError:
            return False
        else:
            return True

    def add_flows_for_new_port(
        self, datapath: Datapath, parser, tun: TUNPortName
    ) -> None:
        # This method should be triggered twice for the same port:
        # 1. ofport has been initialized (i.e. on ofp_event.EventOFPPortStatus event).
        # 2. Hysteresis delay is over.
        # Both conditions must be true for adding a flow. First trigger would fail,
        # but the second will actually add the flow.

        mac: Optional[MACAddress] = self._tun_to_dest_mac.get(tun)
        if not mac:
            logger.error(
                f"Attempted to add flows for {tun}, but it's mac address is unknown."
            )
            return

        try:
            ofport = self.flow_hysteresis.get_ofport_if_tun_is_ready(tun)
        except PortNotReadyError:
            logger.debug(
                f"Expected tun [{tun}] to be ready for a flow, but it's not.",
                exc_info=True,
            )
            return

        actions = [parser.OFPActionOutput(ofport)]
        match = parser.OFPMatch(eth_dst=mac)
        # TODO it is okay to add the exact same flow multiple times?
        # OVS seems to ignore that, but AFAIK according to OpenFlow
        # spec behavior is undefined in this case.
        self.add_flow(datapath, OFPriority.DIRECT, match, actions, is_direct_flow=True)
        logger.info(f"Added direct flow to {mac} via [tun={tun}, ofport={ofport}]")

    def cleanup_flows_for_deleted_port(self, msg: OFPPortStatus) -> None:
        datapath = msg.datapath
        ofp_port: OFPPort = msg.desc
        port_no: OFPort = ofp_port.port_no
        assert isinstance(ofp_port.name, bytes)
        tun = ofp_port.name.decode()
        self.del_flows_to_ofport(datapath, port_no)
        logger.info(f"Removed flows via [tun={tun}, ofport={port_no}]")

    def add_flow(
        self,
        datapath: Datapath,
        priority,
        match: Sequence[OFPMatch],
        actions: Sequence[OFPAction],
        is_direct_flow: bool = True,
    ) -> None:
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]

        # If flow via relayer was infinite, then we would never receive a
        # PACKET_IN event for that destination and would never know that we
        # should attempt to connect directly. By using a finite timeout for
        # such flows we have a chance for getting a PACKET_IN event at some
        # point (after the indirect flow is expired).
        hard_timeout = (
            0 if is_direct_flow else settings.FLOW_INDIRECT_MAX_LIFETIME_SECONDS
        )
        mod = parser.OFPFlowMod(
            datapath=datapath,
            priority=priority,
            match=match,
            instructions=inst,
            # idle_timeout=3600,
            hard_timeout=hard_timeout,
        )
        datapath.send_msg(mod)

    def del_flows_to_ofport(self, datapath: Datapath, out_port: OFPort) -> None:
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        mod = parser.OFPFlowMod(
            datapath=datapath,
            command=ofproto.OFPFC_DELETE,
            buffer_id=ofproto.OFPCML_NO_BUFFER,
            out_port=out_port,
            out_group=ofproto.OFPG_ANY,
            match=parser.OFPMatch(),
            instructions=[],
        )
        datapath.send_msg(mod)
