import asyncio
import threading
from contextlib import ExitStack
from logging import getLogger
from time import sleep

from async_exit_stack import AsyncExitStack
from ryu import cfg
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, set_ev_cls
from ryu.lib.packet import ether_types, ethernet, packet
from ryu.ofproto import ofproto_v1_4

from agile_mesh_network import settings
from agile_mesh_network.common.models import LayersDescriptionRpcModel
from agile_mesh_network.common.rpc import RpcUnixClient
from agile_mesh_network.ryu.topology_database import SwitchEntity, TopologyDatabase

logger = getLogger("amn_ryu_app")
# CONF = cfg.CONF['amn-app']
# TODO https://github.com/osrg/ryu/blob/master/ryu/services/protocols/bgp/application.py


class NetworkView:
    """Syncs the state between Negotiator, Topology Database and OVS.

    Must be thread-safe.
    """

    # TODO deal with L2 loops
    def __init__(
        self, topology_database: TopologyDatabase, negotiator_rpc: "NegotiatorRpc"
    ) -> None:
        self.topology_database = topology_database
        topology_database.add_local_db_synced_callback(self._event_db_synced)

        self.negotiator_rpc = negotiator_rpc
        negotiator_rpc.add_tunnels_changed_callback(
            self._event_negotiator_tunnels_update
        )

        # TODO state: ovsdb tunnels list
        # TODO state: negotiator tunnels list

    # These methods are called from other threads, so they must be
    # fast, thread-safe and don't throw any exceptions.
    def _event_db_synced(self):
        pass  # TODO

    def _event_negotiator_tunnels_update(self, topic, tunnel, tunnels):
        print(tunnels)
        pass  # TODO

    def _event_flow_packet_in(self):
        pass  # TODO


class NegotiatorRpc:

    def __init__(self, unix_sock_path):
        self.unix_sock_path = unix_sock_path
        self._tunnels_changed_callbacks = []
        self._stack = AsyncExitStack()
        self._rpc = None
        self._initial_sync_task = None

    def add_tunnels_changed_callback(self, callback):
        self._tunnels_changed_callbacks.append(callback)

    async def _get_session(self):
        if not self._rpc:
            self._rpc = await self._stack.enter_async_context(
                RpcUnixClient(self.unix_sock_path, self._rpc_command_handler)
            )
        return self._rpc.session

    async def __aenter__(self):
        assert self._initial_sync_task is None
        self._initial_sync_task = asyncio.ensure_future(self._initial_sync())
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._initial_sync_task.cancel()
        await self._stack.aclose()

    async def _initial_sync(self):
        try:
            await self.list_tunnels()
        except:
            logger.error("Negotiator initial sync failed", exc_info=True)

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
        for callback in self._tunnels_changed_callbacks:
            callback(
                topic="create_tunnel", tunnel=msg["tunnels"], tunnels=msg["tunnels"]
            )
        # TODO return??

    async def list_tunnels(self):
        session = await self._get_session()
        msg = await session.issue_command("dump_tunnels_state")
        assert msg.keys() == {"tunnels"}
        for callback in self._tunnels_changed_callbacks:
            callback(
                topic="dump_tunnels_state",
                tunnel=None,
                tunnels=msg["tunnels"],
            )
        # TODO return??

    async def _rpc_command_handler(self, session, msg):
        assert msg.keys() == {"tunnels"}
        for callback in self._tunnels_changed_callbacks:
            callback(topic=None, tunnel=None, tunnels=msg["tunnels"])


class AgileMeshNetworkManager:

    def __init__(self):
        self.topology_database = TopologyDatabase()
        self.negotiator_rpc = NegotiatorRpc(settings.NEGOTIATOR_RPC_UNIX_SOCK_PATH)
        self.network_view = NetworkView(self.topology_database, self.negotiator_rpc)
        self._stack = AsyncExitStack()

    async def __aenter__(self):
        await self._stack.enter_async_context(self.topology_database)
        await self._stack.enter_async_context(self.negotiator_rpc)
        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        await self._stack.aclose()


class SwitchApp(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_4.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mac_to_port = {}
        self.manager = AgileMeshNetworkManager()
        # TODO asyncio loop
        # self._stack = ExitStack()

    def start(self):
        super().start()
        # self._stack.enter_context(self.manager)

    def stop(self):
        self._stack.close()
        super().stop()

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
        self.add_flow(datapath, 0, match, actions)

        # TODO add ovpn tap to the bridge
        # TODO add flow via tap

    def add_flow(self, datapath, priority, match, actions):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]

        mod = parser.OFPFlowMod(
            datapath=datapath, priority=priority, match=match, instructions=inst
        )
        datapath.send_msg(mod)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        msg = ev.msg
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
        self.mac_to_port.setdefault(dpid, {})

        self.logger.info("packet in %s %s %s %s", dpid, src, dst, in_port)

        # learn a mac address to avoid FLOOD next time.
        self.mac_to_port[dpid][src] = in_port

        if dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][dst]
        else:
            out_port = ofproto.OFPP_FLOOD

        actions = [parser.OFPActionOutput(out_port)]

        # install a flow to avoid packet_in next time
        if out_port != ofproto.OFPP_FLOOD:
            match = parser.OFPMatch(in_port=in_port, eth_dst=dst, eth_src=src)
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
