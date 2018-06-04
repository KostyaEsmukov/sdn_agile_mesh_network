import asyncio
from concurrent.futures import CancelledError
from logging import getLogger
from typing import Mapping, Sequence

from async_exit_stack import AsyncExitStack

from agile_mesh_network import settings
from agile_mesh_network.common.models import (
    LayersDescriptionRpcModel, SwitchEntity, TunnelModel
)
from agile_mesh_network.ryu import events
from agile_mesh_network.ryu.events_scheduler import RyuAppEventLoopScheduler
from agile_mesh_network.ryu.negotiator_rpc import NegotiatorRpc
from agile_mesh_network.ryu.ovs_manager import OVSManager
from agile_mesh_network.ryu.topology_database import TopologyDatabase

logger = getLogger(__name__)


class AgileMeshNetworkManager:
    # Execution context: run entirely in the asyncio event loop,
    # no thread safety is required.

    def __init__(self, *, ryu_ev_loop_scheduler: RyuAppEventLoopScheduler) -> None:
        self.ryu_ev_loop_scheduler = ryu_ev_loop_scheduler
        self.topology_database = TopologyDatabase()
        self.negotiator_rpc = NegotiatorRpc(settings.NEGOTIATOR_RPC_UNIX_SOCK_PATH)
        self.ovs_manager = OVSManager(
            datapath_id=settings.OVS_DATAPATH_ID,
            # TODO ryu_app.CONF?
        )

        self._stack = AsyncExitStack()
        self._initialization_task = None
        self._tunnel_creation_tasks: Mapping[asyncio.Future, asyncio.Future] = {}

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
        for task in self._tunnel_creation_tasks:
            task.cancel()
        self._tunnel_creation_tasks.clear()
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
            events.EventActiveTunnelsList(
                {mac: (mac_to_tunnel[mac], mac_to_switch[mac]) for mac in valid}
            )
        )

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

    def ask_for_tunnel(self, dst_mac):
        try:
            switch = self.topology_database.find_switch_by_mac(dst_mac)
        except KeyError:
            logger.warning(
                f"Unable to connect to {dst_mac}: no such Switch in the database"
            )
            return

        t = None

        async def task():
            nonlocal t
            try:
                await self.connect_switch(switch)
            except CancelledError:
                return
            except:
                logger.warning(f"Failed to connect to {dst_mac}", exc_info=True)
            finally:
                self._tunnel_creation_tasks.pop(t, None)

        t = asyncio.ensure_future(task())
        self._tunnel_creation_tasks[t] = t
