import asyncio
import threading
from abc import ABCMeta, abstractmethod
from concurrent.futures import CancelledError
from contextlib import ExitStack
from logging import getLogger
from typing import Mapping, Optional, Sequence, Tuple

from async_exit_stack import AsyncExitStack
from ryu import cfg
from ryu.app.ofctl import api as ofctl_api
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.controller import Datapath, ofp_event
from ryu.controller.event import EventBase
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, set_ev_cls
from ryu.lib import mac as lib_mac
from ryu.lib.ovs import bridge as ovs_bridge
from ryu.lib.ovs import vsctl as ovs_vsctl
from ryu.lib.packet import ether_types, ethernet, packet
from ryu.ofproto import ofproto_v1_4
from ryu.ofproto.ofproto_v1_4_parser import OFPAction, OFPMatch, OFPPacketIn

from agile_mesh_network import settings
from agile_mesh_network.common.models import (
    LayersDescriptionRpcModel, SwitchEntity, TunnelModel
)
from agile_mesh_network.common.rpc import RpcBroadcast, RpcUnixClient
from agile_mesh_network.common.tun_mapper import mac_to_tun_name
from agile_mesh_network.ryu.events_scheduler import RyuAppEventLoopScheduler
from agile_mesh_network.ryu.topology_database import TopologyDatabase

logger = getLogger("amn_ryu_app")
# CONF = cfg.CONF['amn-app']
# TODO https://github.com/osrg/ryu/blob/master/ryu/services/protocols/bgp/application.py

OVSDB_PORT = 6640  # The IANA registered port for OVSDB [RFC7047]


class NegotiatorRpc:

    def __init__(self, unix_sock_path):
        self.unix_sock_path = unix_sock_path
        self._tunnels_changed_callbacks = []
        self._stack = AsyncExitStack()
        self._rpc = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._stack.aclose()

    def add_tunnels_changed_callback(self, callback):
        self._tunnels_changed_callbacks.append(callback)

    async def _get_session(self):
        if not self._rpc:
            self._rpc = await self._stack.enter_async_context(
                RpcUnixClient(self.unix_sock_path, self._rpc_command_handler)
            )
            # TODO recover on connection loss?
        return self._rpc.session

    async def start_tunnel(
        self, src_mac, dst_mac, timeout, layers: LayersDescriptionRpcModel
    ) -> None:
        session = await self._get_session()
        msg = await session.issue_command(
            "create_tunnel",
            {
                "src_mac": src_mac,
                "dst_mac": dst_mac,
                "timeout": timeout,
                "layers": layers.asdict(),
            },
        )
        assert msg.keys() == {"tunnel", "tunnels"}
        self._call_callbacks("create_tunnel", msg)
        # TODO return??

    async def list_tunnels(self) -> Sequence[TunnelModel]:
        session = await self._get_session()
        msg = await session.issue_command("dump_tunnels_state")
        assert msg.keys() == {"tunnels"}
        self._call_callbacks("dump_tunnels_state", msg)
        return [TunnelModel.from_dict(d) for d in msg["tunnels"]]

    async def _rpc_command_handler(self, session, cmd: RpcBroadcast):
        assert cmd.name == "tunnel_created"
        msg = cmd.kwargs
        assert msg.keys() == {"tunnel", "tunnels"}
        self._call_callbacks(topic=cmd.name, msg=msg)

    def _call_callbacks(self, topic, msg):
        tunnel = TunnelModel.from_dict(msg["tunnel"]) if msg.get("tunnel") else None
        tunnels = (
            [TunnelModel.from_dict(d) for d in msg["tunnels"]]
            if "tunnels" in msg
            else None
        )
        for callback in self._tunnels_changed_callbacks:
            callback(topic=topic, tunnel=tunnel, tunnels=tunnels)


class AgileMeshNetworkManager:
    # TODO deal with L2 loops

    def __init__(self, *, ryu_ev_loop_scheduler):
        self.ryu_ev_loop_scheduler = ryu_ev_loop_scheduler
        self.topology_database = TopologyDatabase()
        self.negotiator_rpc = NegotiatorRpc(settings.NEGOTIATOR_RPC_UNIX_SOCK_PATH)
        self.ovs_manager = OVSManager(
            datapath_id=settings.OVS_DATAPATH_ID,
            # TODO ryu_app.CONF?
        )

        self._stack = AsyncExitStack()
        self._initialization_task = None

        self.topology_database.add_local_db_synced_callback(self._event_db_synced)

        self.negotiator_rpc.add_tunnels_changed_callback(
            self._event_negotiator_tunnels_update
        )

    async def __aenter__(self):
        try:
            self._stack.enter_context(self.ovs_manager)
            await self._stack.enter_async_context(self.topology_database)
            await self._stack.enter_async_context(self.negotiator_rpc)
        except:
            await self._stack.aclose()
            raise
        return self

    def start_initialization(self):
        assert self._initialization_task is None
        self._initialization_task = asyncio.ensure_future(self._initialization())

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        if self._initialization_task:
            self._initialization_task.cancel()
        await self._stack.aclose()

    async def _initialization(self):
        logger.info("Initial sync: waiting for Local DB to initialize...")
        await self.topology_database.local.is_filled_event.wait()
        logger.info("Initial sync: waiting for Local DB to initialize: done.")
        logger.info("Initial sync: retrieving tunnels from negotiator...")
        tunnels = None
        while True:
            try:
                tunnels = await self.negotiator_rpc.list_tunnels()
                logger.info("Initial sync: retrieving tunnels from negotiator: done.")
                break
            except CancelledError:
                return
            except:
                logger.error(
                    "Initial sync: failed to retrieve tunnels from Negotiator.",
                    exc_info=True,
                )
                await asyncio.sleep(5)
        while True:
            relay_switch = None
            try:
                if self._is_relay_connected(tunnels):
                    logger.info("Initial sync: no need to connect to a relay.")
                else:
                    # TODO support multiple switches?
                    relay_switch, = self.topology_database.find_random_relay_switches(1)
                    logger.info("Initial sync: connecting to %s...", relay_switch)
                    await self.connect_switch(relay_switch)
                    logger.info("Initial sync: connecting to %s: done.", relay_switch)
                break
            except CancelledError:
                return
            except:
                logger.error(
                    "Initial sync: failed to connect to relay switch %s.",
                    relay_switch,
                    exc_info=True,
                )
                await asyncio.sleep(5)
        logger.info("Initial sync: complete!")

    # These methods must be fast and not throw any exceptions.
    def _event_db_synced(self):
        pass  # TODO

    def _event_negotiator_tunnels_update(
        self, topic: str, tunnel: TunnelModel, tunnels: Sequence[TunnelModel]
    ) -> None:
        # Note that for the tunnel_created response this would be
        # called twice in a row.
        if not self.topology_database.local.is_filled:
            logger.error(
                "Skipping negotiator event, because Local DB is not initialized yet"
            )
            return

        logger.debug("Processing a list of tunnels from Negotiator: %s", tunnels)

        valid, invalid, mac_to_tunnel, mac_to_switch = self._filter_tunnels(tunnels)

        valid_tunnels = [mac_to_tunnel[mac] for mac in valid]
        invalid_tunnels = [mac_to_tunnel[mac] for mac in invalid]
        logger.debug(
            "Negotiator tunnels processed. Valid: %s. Invalid: %s",
            valid_tunnels,
            invalid_tunnels,
        )

        # TODO for invalid_tunnels - send tunnel_stop command to negotiator

        # TODO ?? maybe don't always make a full sync, but use more granular
        # events (like a tunnel has been added/removed)?
        self.ryu_ev_loop_scheduler.send_event_to_observers(
            EventActiveTunnelsList(
                {mac: (mac_to_tunnel[mac], mac_to_switch[mac]) for mac in valid}
            )
        )

    def _event_flow_packet_in(self):
        pass  # TODO

    def _filter_tunnels(self, tunnels):
        for t in tunnels:
            assert t.src_mac == self.ovs_manager.bridge_mac, (
                f"Negotiator accepted a tunnel which src MAC {t.src_mac} doesn't "
                f"match OVS bridge's {self.ovs_manager.bridge_mac} one."
            )

        existing_switches = self.topology_database.find_switches_by_mac_list(
            [t.dst_mac for t in tunnels]
        )

        mac_to_tunnel = {t.dst_mac: t for t in tunnels}
        mac_to_switch = {s.mac: s for s in existing_switches}

        valid_tunnels = list(mac_to_tunnel.keys() & mac_to_switch.keys())
        invalid_tunnels = list(mac_to_tunnel.keys() - mac_to_switch.keys())
        return valid_tunnels, invalid_tunnels, mac_to_tunnel, mac_to_switch

    def _is_relay_connected(self, tunnels):
        if settings.IS_RELAY:
            return True
        valid, _, _, mac_to_switch = self._filter_tunnels(tunnels)
        relays = [mac_to_switch[m] for m in valid if mac_to_switch[m].is_relay]
        assert len(relays) <= 1  # TODO drop extra
        if relays:
            logger.info("Relay is connected: %s", relays[0])
        return bool(relays)

    async def connect_switch(self, switch: SwitchEntity):
        # TODO layers? udp? negotiation?
        layers = LayersDescriptionRpcModel.from_dict(switch.layers_config)
        await self.negotiator_rpc.start_tunnel(
            src_mac=self.ovs_manager.bridge_mac,
            dst_mac=switch.mac,
            timeout=20,
            layers=layers,
        )  # TODO track result? timeout?


class AgileMeshNetworkManagerThread:

    def __init__(self, **amn_kwargs):
        self._manager = None
        self._thread = None
        self._thread_started_event = threading.Event()
        self._start_initialization_event = threading.Event()
        self._loop = None
        self._amn_kwargs = amn_kwargs

    @property
    def ovs_manager(self):
        return self._manager.ovs_manager

    def start_initialization(self):
        self._start_initialization_event.set()

    def __enter__(self):
        assert self._thread is None
        self._thread = threading.Thread(target=self._run, name=type(self).__name__)
        self._thread.start()
        self._thread_started_event.wait()
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        if self._thread is not None and self._thread.is_alive():
            assert self._loop
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join()
        self._manager = None
        self._thread = None
        self._thread_started_event.clear()
        self._loop = None

    def _run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)  # Related to the current thread only.
        self._loop = loop

        async def enter_stack(stack):
            manager = await stack.enter_async_context(
                AgileMeshNetworkManager(**self._amn_kwargs)
            )
            return manager

        stack = AsyncExitStack()
        try:
            self._manager = loop.run_until_complete(enter_stack(stack))
            self._thread_started_event.set()
            self._start_initialization_event.wait()
            self._manager.start_initialization()
            loop.run_forever()
        except Exception as e:
            logger.error("Uncaught exception in the AMN event loop", exc_info=True)
            # TODO shutdown the app? It's harmful to keep it alive at this point.
            # Obviously, everything is horribly broken: there's no manager
            # controlling the app.
        finally:
            logger.info("Shutting down AMN event loop")
            loop.run_until_complete(stack.aclose())
            loop.run_until_complete(loop.shutdown_asyncgens())
            pending = asyncio.Task.all_tasks()
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.close()


class RyuConfMock:
    """Can be used instead of ryu_app.CONF."""
    ovsdb_timeout = 2


class OVSManager:

    def __init__(self, datapath_id, CONF=RyuConfMock):
        ovsdb_addr = "tcp:%s:%d" % ("127.0.0.1", OVSDB_PORT)
        self.datapath_id = datapath_id
        self.ovs = ovs_bridge.OVSBridge(
            CONF=CONF, datapath_id=datapath_id, ovsdb_addr=ovsdb_addr
        )
        self._lock = threading.Lock()
        self._bridge_mac = None

    def __enter__(self):
        self.ovs.init()
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        pass

    @property
    def bridge_mac(self):
        if not self._bridge_mac:
            with self._lock:
                mac_in_use = self.ovs.db_get_val(
                    "Interface", self.ovs.br_name, "mac_in_use"
                )
            assert len(mac_in_use) == 1
            self._bridge_mac = mac_in_use[0]
        return self._bridge_mac

    def is_port_up(self, port_name):
        with self._lock:
            ports = set(self.ovs.get_port_name_list())
            if port_name not in ports:
                return False
            return self._is_port_up(port_name)

    def add_port_to_bridge(self, port_name):
        with self._lock:
            command = ovs_vsctl.VSCtlCommand(
                "add-port", (self.ovs.br_name, port_name), "--may-exist"
            )
            self.ovs.run_command([command])

    def del_port_from_bridge(self, port_name):
        with self._lock:
            self._del_port(port_name)

    def del_all_down_ports(self):
        with self._lock:
            ports = set(self.ovs.get_port_name_list())
            down_ports = [port for port in ports if not self._is_port_up(port)]
            for port in down_ports:
                self._del_port(port)
            return down_ports

    def get_ofport(self, port_name):
        return self.ovs.get_ofport(port_name)

    def get_ofport_ex(self, port_name):
        try:
            return self.get_ofport(port_name)
        except:
            # Exception: no row "tapanaaaaamzt16" in table Interface
            return -1

    def _is_port_up(self, port_name):
        if self.ovs.get_ofport_ex(port_name) < 0:
            # Interface is on the OVS DB, but is missing in the OS.
            return False
        link_state = self.ovs.db_get_val("Interface", port_name, "link_state")
        if link_state != ["up"]:
            # []  -- interface doesn't exist
            # ['down']  -- interface is down
            # ['up']  -- interface is up
            return False
        # Also there's `admin_state`.
        return True

    def _del_port(self, port_name):
        command = ovs_vsctl.VSCtlCommand("del-port", (self.ovs.br_name, port_name))
        self.ovs.run_command([command])


class FlowsLogic:

    def __init__(self, is_relay: bool, ovs_manager: OVSManager) -> None:
        self.is_relay: bool = is_relay
        self.relay_mac: Optional[str] = None
        self.ovs_manager: OVSManager = ovs_manager
        self.datapath = None
        # self.mac_to_port = {}

    def set_datapath(self, datapath: Datapath) -> None:
        # TODO And that's it? just an assignment? Shouldn't we queue unsent messages?
        self.datapath = datapath

    def sync_ovs_from_tunnels(
        self, mac_to_tunswitch: Mapping[str, Tuple[TunnelModel, SwitchEntity]]
    ) -> None:

        logger.warning("FlowsLogic: negotiator tunnels are: %s", mac_to_tunswitch)
        for tunnel, _ in mac_to_tunswitch.values():
            self.ovs_manager.add_port_to_bridge(mac_to_tun_name(tunnel.dst_mac))
        # ovs_manager.del_all_down_ports()
        # TODO remove from ovs all extraneous tuns (not just down ones).
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
            "FlowsLogic: packet in [%s %s %s %s] [dpid src dst in_port]",
            dpid,
            src,
            dst,
            self._ofport_to_string(in_port, ofproto),
        )

        # self.mac_to_port[dpid][src] = in_port

        is_broadcast = self._is_broadcast_mac(dst)
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
            "PACKET_IN: decision: out_port %s", self._ofport_to_string(out_port, ofproto)
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
        return str(ofport)

    def _is_broadcast_mac(self, mac):
        # TODO !!! multicast
        return mac == lib_mac.BROADCAST_STR

    def _packet_in_board(self, dst, src, in_port, ofproto):
        is_broadcast = self._is_broadcast_mac(dst)
        if is_broadcast and in_port != ofproto.OFPP_LOCAL:
            # Broadcast/multicast, originating from somewhere else.
            # Assuming that we don't relay broadcasts, we should forward
            # it to the bridge.
            return ofproto.OFPP_LOCAL

        if not self.relay_mac:
            raise NoSuitableOutPortError("no relay connected")

        out_port = self.ovs_manager.get_ofport_ex(mac_to_tun_name(self.relay_mac))
        if out_port < 0:
            raise NoSuitableOutPortError("relay out_port is faulty")

        # Assuming that each board is connected to a relayer, and
        # relayers are in the same L2 domain, outgoing broadcast/multicast
        # packets should be sent to a relayer only.

        if not is_broadcast:
            # TODO ask for direct connectivity?
            # self.manager.ask_for_tunnel(src, dst)
            pass
        return out_port

    def _packet_in_relay(self, dst, ofproto):
        if self._is_broadcast_mac(dst):
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


class NoSuitableOutPortError(Exception):
    pass


class EventActiveTunnelsList(EventBase):

    def __init__(
        self, mac_to_tunswitch: Mapping[str, Tuple[TunnelModel, SwitchEntity]]
    ) -> None:
        self.mac_to_tunswitch = mac_to_tunswitch


class SwitchApp(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_4.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mac_to_port = {}
        self.register_observer(EventActiveTunnelsList, self.name)
        self._ryu_ev_loop_scheduler = RyuAppEventLoopScheduler(self)
        self.manager = AgileMeshNetworkManagerThread(
            ryu_ev_loop_scheduler=self._ryu_ev_loop_scheduler
        )
        self._stack = ExitStack()
        self.flows_logic: FlowsLogic = None

    def start(self):
        try:
            self._stack.enter_context(self._ryu_ev_loop_scheduler)
            self._stack.enter_context(self.manager)
            self.flows_logic = FlowsLogic(
                is_relay=settings.IS_RELAY, ovs_manager=self.manager.ovs_manager
            )
        except:
            self._stack.close()
            raise
        super().start()

    def stop(self):
        self._stack.close()
        super().stop()

    # def _get_datapath(self) -> Datapath:
    #     return ofctl_api.get_datapath(self, settings.OVS_DATAPATH_ID)

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        # install table-miss flow entry
        #
        # We specify NO BUFFER to max_len of the output action due to
        # OVS bug. At this moment, if we specify a lesser number, e.g.,
        # 128, OVS will send Packet-In with invalid buffer_id and
        # truncated packet data. In that case, we cannot output packets
        # correctly.  The bug has been fixed in OVS v2.1.0.
        match = parser.OFPMatch()
        actions = [
            parser.OFPActionOutput(ofproto.OFPP_CONTROLLER, ofproto.OFPCML_NO_BUFFER)
        ]
        self.flows_logic.set_datapath(datapath)
        self.flows_logic.add_flow(datapath, 0, match, actions)
        self.manager.start_initialization()

    @set_ev_cls(EventActiveTunnelsList)
    def active_tunnels_list_handler(self, ev):
        self.flows_logic.sync_ovs_from_tunnels(ev.mac_to_tunswitch)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def packet_in_handler(self, ev):
        self.flows_logic.packet_in(ev.msg)
