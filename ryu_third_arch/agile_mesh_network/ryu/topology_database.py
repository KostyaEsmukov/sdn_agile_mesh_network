import random
import threading
from abc import ABCMeta, abstractmethod
from collections import namedtuple
from logging import getLogger
from typing import Iterable, List

from pymongo import MongoClient

from agile_mesh_network import settings
from agile_mesh_network.common.models import SwitchEntity

logger = getLogger(__name__)


class RemoteDatabase(metaclass=ABCMeta):

    @abstractmethod
    def get_database(self) -> Iterable[SwitchEntity]:
        pass


class MongoRemoteDatabase(RemoteDatabase):

    def __init__(self):
        self.client = MongoClient(settings.REMOTE_DATABASE_MONGO_URI)

    def get_database(self) -> Iterable[SwitchEntity]:
        db = self.client.topology_database
        collection = db.switch_collection
        return [SwitchEntity.from_dict(doc) for doc in collection.find()]


class LocalTopologyDatabase:
    """In-memory storage. Must be thread-safe."""

    def __init__(self):
        self.mac_to_switch = {}
        self.relay_switches = []
        self.lock = threading.Lock()
        self.is_filled = False
        self.is_filled_event = threading.Event()

    def update(self, switches):
        with self.lock:
            self.mac_to_switch = {switch.mac: switch for switch in switches}
            self.relay_switches = [switch for switch in switches if switch.is_relay]
            self.is_filled = True
        self.is_filled_event.set()

    def find_switch_by_mac(self, mac):
        """None if not found."""
        with self.lock:
            return self.mac_to_switch.get(mac)

    def find_random_relay_switches(self, count=1):
        """Might be less than count."""
        with self.lock:
            return list(random.sample(self.relay_switches, count))


def update_local_database(topology_database: "TopologyDatabase"):
    try:
        topology_database.local.update(topology_database.remote.get_database())
        for callback in topology_database._local_db_synced_callbacks:
            callback()
    except Exception as e:
        logger.error("Exception in database sync thread", exc_info=True)


class TopologyDatabase:
    local_database = LocalTopologyDatabase
    remote_database = MongoRemoteDatabase
    database_sync_interval_seconds = settings.TOPOLOGY_DATABASE_SYNC_INTERVAL_SECONDS

    def __init__(self):
        self.remote = self.remote_database()
        self.local = self.local_database()
        self._timer = None
        self._local_db_synced_callbacks = []

    def add_local_db_synced_callback(self, callback):
        self._local_db_synced_callbacks.append(callback)

    def start_replication_thread(self):
        assert self._timer is None
        self._timer = SetIntervalThread(
            self.database_sync_interval_seconds,
            update_local_database,
            args=(self,),
            run_immediately=True,
        )
        self._timer.start()

    def stopjoin_replication_thread(self):
        if self._timer is not None:
            self._timer.cancel()
            self._timer.join()
        self._timer = None

    def find_switch_by_mac(self, mac) -> SwitchEntity:
        switch = self.local.find_switch_by_mac(mac)
        if not switch:
            raise KeyError()
        return switch

    def find_random_relay_switches(self, count=1) -> List[SwitchEntity]:
        switches = list(self.local.find_random_relay_switches(count))
        if not switches:
            raise IndexError()
        return switches


class SetIntervalThread(threading.Thread):

    def __init__(
        self, interval, function, args=None, kwargs=None, run_immediately=False
    ):
        super().__init__()
        self.interval = interval
        self.function = function
        self.args = args if args is not None else []
        self.kwargs = kwargs if kwargs is not None else {}
        self.finished = threading.Event()
        self.run_immediately = run_immediately

    def cancel(self):
        self.finished.set()

    def run(self):
        if self.run_immediately:
            self.function(*self.args, **self.kwargs)
        while not self.finished.wait(self.interval):
            self.function(*self.args, **self.kwargs)
