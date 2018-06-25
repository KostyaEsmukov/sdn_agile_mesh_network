import asyncio
import functools
import random
import threading
from abc import ABCMeta, abstractmethod
from concurrent.futures import CancelledError, ThreadPoolExecutor
from logging import getLogger
from typing import Iterable, List, Sequence

from pymongo import MongoClient

from agile_mesh_network import settings
from agile_mesh_network.common.models import SwitchEntity

logger = getLogger(__name__)


class RemoteDatabase(metaclass=ABCMeta):
    @abstractmethod
    async def get_database(self) -> Iterable[SwitchEntity]:
        pass

    @abstractmethod
    async def aclose(self):
        pass


class MongoRemoteDatabase(RemoteDatabase):
    def __init__(self, *, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        self._executor = ThreadPoolExecutor(max_workers=1)
        self._executor_run = functools.partial(
            self._loop.run_in_executor, self._executor
        )
        self._client = None

    async def get_client(self):
        if not self._client:
            self._client = await self._executor_run(
                lambda: MongoClient(settings.REMOTE_DATABASE_MONGO_URI)
            )
        return self._client

    async def get_database(self) -> Iterable[SwitchEntity]:
        client = await self.get_client()

        def f():
            db = client.topology_database
            collection = db.switch_collection
            return [SwitchEntity.from_dict(doc) for doc in collection.find()]

        return await self._executor_run(f)

    async def aclose(self):
        if self._client:
            await self._executor_run(self._client.close)


class LocalTopologyDatabase:
    """In-memory storage."""

    def __init__(self):
        self.mac_to_switch = {}
        self.relay_switches = []
        self.is_filled = False
        self.is_filled_event = asyncio.Event()

    def update(self, switches):
        self.mac_to_switch = {switch.mac: switch for switch in switches}
        self.relay_switches = [switch for switch in switches if switch.is_relay]
        self.is_filled = True
        self.is_filled_event.set()

    def find_switch_by_mac(self, mac):
        """None if not found."""
        return self.mac_to_switch.get(mac)

    def find_switches_by_mac_list(self, mac_list):
        g = (self.mac_to_switch.get(mac) for mac in mac_list)
        return [sw for sw in g if sw is not None]

    def find_random_relay_switches(self, count=1):
        """Might be less than count."""
        return list(random.sample(self.relay_switches, count))


class TopologyDatabase:
    # Execution context: constructed in the asyncio thread, but find*
    # methods might be used in the Ryu thread, so they must be thread-safe,
    # fast and blocking.

    local_database = LocalTopologyDatabase
    remote_database = MongoRemoteDatabase
    database_sync_interval_seconds = settings.TOPOLOGY_DATABASE_SYNC_INTERVAL_SECONDS

    def __init__(self):
        self.remote = self.remote_database()
        self.local = self.local_database()
        # self._timer = None
        self._sync_stop_event = asyncio.Event()
        self._sync_task = None
        self._local_db_synced_callbacks = []
        self._lock = threading.Lock()

    def add_local_db_synced_callback(self, callback):
        self._local_db_synced_callbacks.append(callback)

    async def __aenter__(self):
        self._sync_stop_event.clear()
        self._sync_task = asyncio.ensure_future(self._sync())
        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        self._sync_stop_event.set()
        self._sync_task.cancel()
        await self.remote.aclose()

    async def _sync(self):
        await self._update_local_database()
        while True:
            try:
                await asyncio.wait_for(
                    self._sync_stop_event.wait(),
                    timeout=self.database_sync_interval_seconds,
                )
                break
            except asyncio.TimeoutError:
                pass  # Timeout is good! It means that the loop is not stopped yet.
            await self._update_local_database()

    async def _update_local_database(self):
        try:
            new_database = await self.remote.get_database()
            with self._lock:
                self.local.update(new_database)
            for callback in self._local_db_synced_callbacks:
                callback()
        except CancelledError:
            pass
        except Exception as e:
            logger.error("Exception in the database sync task", exc_info=True)

    def find_switch_by_mac(self, mac) -> SwitchEntity:
        with self._lock:
            switch = self.local.find_switch_by_mac(mac)
        if not switch:
            raise KeyError()
        return switch

    def find_switches_by_mac_list(self, mac_list) -> Sequence[SwitchEntity]:
        with self._lock:
            return self.local.find_switches_by_mac_list(mac_list)

    def find_random_relay_switches(self, count=1) -> List[SwitchEntity]:
        with self._lock:
            switches = list(self.local.find_random_relay_switches(count))
        if not switches:
            raise IndexError()
        return switches
