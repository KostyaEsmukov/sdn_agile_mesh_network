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
        self._thread = None
        self.unix_sock_path = unix_sock_path
        self._tunnels_changed_callbacks = []
        self._loop = None
        self._rpc_commands_queue = asyncio.Queue()
        self._thread_interrupted = False
        self._thread_started_event = threading.Event()
        self.is_connected_event = threading.Event()

    def add_tunnels_changed_callback(self, callback):
        self._tunnels_changed_callbacks.append(callback)

    def start_thread(self):
        assert self._thread is None
        self._thread_interrupted = False
        self._thread = threading.Thread(target=self._run)
        self._thread.start()
        self._thread_started_event.wait()

    def stopjoin_thread(self):
        if self._thread is not None and self._thread.is_alive():
            assert self._loop
            self._thread_interrupted = True
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join()
        self._thread = None
        self._thread_started_event.clear()
        self.is_connected_event.clear()

    def start_tunnel(
        self, src_mac, dst_mac, timeout, layers: LayersDescriptionRpcModel
    ) -> None:
        self._loop.call_soon_threadsafe(
            self._rpc_commands_queue.put_nowait,
            (
                "create_tunnel",
                {
                    "src_mac": src_mac,
                    "dst_mac": dst_mac,
                    "timeout": timeout,
                    "layers": layers.asdict(),
                },
            ),
        )

    def list_tunnels(self):
        self._loop.call_soon_threadsafe(
            self._rpc_commands_queue.put_nowait, ("dump_tunnels_state", {})
        )

    # TODO stop_tunnel

    def _run(self):

        async def command_cb(session, msg):
            assert msg.keys() == {"tunnels"}
            for callback in self._tunnels_changed_callbacks:
                callback(topic=None, tunnel=None, tunnels=msg["tunnels"])

        async def async_stack(stack):
            rpc_client = await stack.enter_async_context(
                RpcUnixClient(self.unix_sock_path, command_cb)
            )
            return rpc_client

        async def queue_processing(rpc_client):
            # TODO exc processing  !!!!!!!!!
            while True:
                # TODO use future??? don't block.
                name, kwargs = await self._rpc_commands_queue.get()
                msg = await rpc_client.session.issue_command(name, kwargs)
                self._rpc_commands_queue.task_done()
                assert msg.keys() == {"tunnel", "tunnels"}
                for callback in self._tunnels_changed_callbacks:
                    callback(topic=name, tunnel=msg["tunnels"], tunnels=msg["tunnels"])

        while not self._thread_interrupted:
            # asyncio.set_event_loop(None)
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)  # Related to the current thread only.
            self._loop = loop
            self._thread_started_event.set()

            stack = AsyncExitStack()
            try:
                rpc_client = loop.run_until_complete(async_stack(stack))
                self.is_connected_event.set()
                loop.run_until_complete(queue_processing(rpc_client))
                # asyncio.ensure_future(queue_processing(rpc_client), loop=loop)
                loop.run_forever()
            except Exception as e:
                self._handle_thread_exception(e)
            finally:
                logger.info("Shutting down RPC event loop")
                loop.run_until_complete(stack.aclose())
                loop.close()
                self.is_connected_event.clear()

    def _handle_thread_exception(self, e):
        logger.error("Error in RPC thread", exc_info=e)
        sleep(1)  # Avoid rapid looping on frequent failures


class AgileMeshNetworkManager:

    def __init__(self):
        self.topology_database = TopologyDatabase()
        self.negotiator_rpc = NegotiatorRpc(settings.NEGOTIATOR_RPC_UNIX_SOCK_PATH)
        self.network_view = NetworkView(self.topology_database, self.negotiator_rpc)

    def __enter__(self):
        self.topology_database.start_replication_thread()
        self.negotiator_rpc.start_thread()
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.topology_database.stopjoin_replication_thread()
        self.negotiator_rpc.stopjoin_thread()


class SwitchApp(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_4.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mac_to_port = {}
        self.manager = AgileMeshNetworkManager()
        self._stack = ExitStack()

    def start(self):
        super().start()
        self._stack.enter_context(self.manager)

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
