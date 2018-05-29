import asyncio
import threading
from contextlib import ExitStack
from logging import getLogger

import ryu.lib.ovs.vsctl as ovs_vsctl
from async_exit_stack import AsyncExitStack
from ryu import cfg
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, set_ev_cls
from ryu.lib.ovs import bridge as ovs_bridge
from ryu.lib.packet import ether_types, ethernet, packet
from ryu.ofproto import ofproto_v1_4

from agile_mesh_network import settings
from agile_mesh_network.common.models import LayersDescriptionRpcModel
from agile_mesh_network.common.rpc import RpcUnixClient
from agile_mesh_network.ryu.topology_database import TopologyDatabase

logger = getLogger("amn_ryu_app")
# CONF = cfg.CONF['amn-app']
# TODO https://github.com/osrg/ryu/blob/master/ryu/services/protocols/bgp/application.py

OVSDB_PORT = 6640  # The IANA registered port for OVSDB [RFC7047]


class NetworkView:
    """Syncs the state between Negotiator, Topology Database and OVS.

    Must be thread-safe.
    """

    # TODO deal with L2 loops
    def __init__(
        self,
        topology_database: TopologyDatabase,
        negotiator_rpc: "NegotiatorRpc",
        ovs_manager: "OVSManager",
    ) -> None:
        self.topology_database = topology_database
        topology_database.add_local_db_synced_callback(self._event_db_synced)

        self.negotiator_rpc = negotiator_rpc
        negotiator_rpc.add_tunnels_changed_callback(
            self._event_negotiator_tunnels_update
        )

        self.ovs_manager = ovs_manager

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
            callback(topic="dump_tunnels_state", tunnel=None, tunnels=msg["tunnels"])
        # TODO return??

    async def _rpc_command_handler(self, session, msg):
        assert msg.keys() == {"tunnels"}
        for callback in self._tunnels_changed_callbacks:
            callback(topic=None, tunnel=None, tunnels=msg["tunnels"])


class AgileMeshNetworkManager:

    def __init__(self):
        self.topology_database = TopologyDatabase()
        self.negotiator_rpc = NegotiatorRpc(settings.NEGOTIATOR_RPC_UNIX_SOCK_PATH)
        self.ovs_manager = OVSManager(
            datapath_id=settings.OVS_DATAPATH_ID,
            # TODO ryu_app.CONF?
        )
        self.network_view = NetworkView(
            self.topology_database, self.negotiator_rpc, self.ovs_manager
        )
        self._stack = AsyncExitStack()

    async def __aenter__(self):
        try:
            self._stack.enter_context(self.ovs_manager)
            await self._stack.enter_async_context(self.topology_database)
            await self._stack.enter_async_context(self.negotiator_rpc)
        except:
            await self._stack.aclose()
            raise
        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        await self._stack.aclose()


class AgileMeshNetworkManagerThread:

    def __init__(self):
        self._manager = None
        self._thread = None
        self._thread_started_event = threading.Event()
        self._loop = None

    def __enter__(self):
        assert self._thread is None
        self._thread = threading.Thread(target=self._run)
        self._thread.start()
        self._thread_started_event.wait()
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        if self._thread is not None and self._thread.is_alive():
            assert self._loop
            # self._thread_interrupted = True
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
            manager = await stack.enter_async_context(AgileMeshNetworkManager())
            return manager

        stack = AsyncExitStack()
        try:
            self._manager = loop.run_until_complete(enter_stack(stack))
            self._thread_started_event.set()
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
            loop.run_until_complete(asyncio.gather(*pending))
            loop.close()


class RyuConfMock:
    """Can be used instead of ryu_app.CONF."""
    ovsdb_timeout = 2


class OVSManager:

    def __init__(self, datapath_id, CONF=RyuConfMock):
        ovsdb_addr = "tcp:%s:%d" % ("127.0.0.1", OVSDB_PORT)
        self.ovs = ovs_bridge.OVSBridge(
            CONF=CONF, datapath_id=datapath_id, ovsdb_addr=ovsdb_addr
        )
        self._lock = threading.Lock()

    def __enter__(self):
        self.ovs.init()
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        pass

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

    def _is_port_up(self, port_name):
        if self.ovs.get_ofport(port_name) < 0:
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


class SwitchApp(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_4.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mac_to_port = {}
        self.manager = AgileMeshNetworkManagerThread()
        self._stack = ExitStack()

    def start(self):
        self._stack.enter_context(self.manager)
        super().start()

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
