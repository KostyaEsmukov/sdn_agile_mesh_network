import random
import threading
from abc import ABCMeta, abstractmethod
from collections import namedtuple
from typing import Iterable, List

from pymongo import MongoClient

from agile_mesh_network import settings

__all__ = ("SwitchEntity", "TopologyDatabase")

# TODO maybe move to common.models?
SwitchEntity = namedtuple(
    "SwitchEntity", ["hostname", "is_relay", "layers_config", "mac"]
)


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
        return [SwitchEntity(**doc) for doc in collection.find()]


class LocalTopologyDatabase:
    """In-memory storage. Must be thread-safe."""

    def __init__(self):
        self.mac_to_switch = {}
        self.relay_switches = []
        self.lock = threading.Lock()

    def update(self, switches):
        with self.lock:
            self.mac_to_switch = {switch.mac: switch for switch in switches}
            self.relay_switches = [switch for switch in switches if switch.is_relay]

    def find_switch_by_mac(self, mac):
        """None if not found."""
        with self.lock:
            return self.mac_to_switch.get(mac)

    def find_random_relay_switches(self, count=1):
        """Might be less than count."""
        with self.lock:
            return list(random.sample(self.relay_switches, count))


def update_local_database(topology_database: 'TopologyDatabase'):
    topology_database.local.update(topology_database.remote.get_database())
    for callback in topology_database._local_db_synced_callbacks:
        callback()


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
        self._timer = threading.Timer(
            self.database_sync_interval_seconds, update_local_database, args=(self,)
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
