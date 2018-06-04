import asyncio
import threading
from contextlib import ExitStack
from logging import getLogger

from async_exit_stack import AsyncExitStack
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, set_ev_cls
from ryu.ofproto import ofproto_v1_4

from agile_mesh_network import settings
from agile_mesh_network.ryu import events
from agile_mesh_network.ryu.amn_manager import AgileMeshNetworkManager
from agile_mesh_network.ryu.events_scheduler import RyuAppEventLoopScheduler
from agile_mesh_network.ryu.flows_logic import (
    FlowsLogic, OFPriority, TunnelIntentionsProvider
)

logger = getLogger("amn_ryu_app")


class AgileMeshNetworkManagerThread(TunnelIntentionsProvider):

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

    @property
    def topology_database(self):
        return self._manager.topology_database

    def ask_for_tunnel(self, dst_mac):
        self._loop.call_soon_threadsafe(self._manager.ask_for_tunnel, dst_mac)

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


class SwitchApp(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_4.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mac_to_port = {}
        self.register_observer(events.EventActiveTunnelsList, self.name)
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
                is_relay=settings.IS_RELAY,
                ovs_manager=self.manager.ovs_manager,
                tunnel_intentions_provider=self.manager,
                topology_database=self.manager.topology_database,
            )
            self.manager.ovs_manager.set_controller()
        except:
            self._stack.close()
            raise
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
        self.flows_logic.set_datapath(datapath)
        self.flows_logic.add_flow(datapath, OFPriority.CONTROLLER, match, actions)
        self.manager.start_initialization()

    @set_ev_cls(ofp_event.EventOFPPortStatus, MAIN_DISPATCHER)
    def port_status_handler(self, ev):
        msg = ev.msg
        reason = msg.reason
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        ofp_port = msg.desc
        assert isinstance(ofp_port.name, bytes)
        tun = ofp_port.name.decode()

        port_desc = f"[ofport={ofp_port.port_no}, name={tun}]"

        if reason == ofproto.OFPPR_ADD:
            logger.info("port added %s", port_desc)
        elif reason == ofproto.OFPPR_DELETE:
            logger.info("port deleted %s", port_desc)
        elif reason == ofproto.OFPPR_MODIFY:
            logger.info("port modified %s", port_desc)
        else:
            logger.info("Illeagal port state %s %s", port_desc, reason)

        if reason == ofproto.OFPPR_DELETE:
            self.flows_logic.cleanup_flows_for_deleted_port(msg)
        elif reason == ofproto.OFPPR_ADD:
            self.flows_logic.add_flows_for_new_port(datapath, parser, tun)

    @set_ev_cls(events.EventActiveTunnelsList)
    def active_tunnels_list_handler(self, ev):
        self.flows_logic.sync_ovs_from_tunnels(ev.mac_to_tunswitch)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def packet_in_handler(self, ev):
        self.flows_logic.packet_in(ev.msg)
