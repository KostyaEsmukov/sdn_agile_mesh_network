import threading
from functools import wraps
from typing import Sequence

from ryu.lib.ovs import bridge as ovs_bridge
from ryu.lib.ovs import vsctl as ovs_vsctl

from agile_mesh_network.common.types import MACAddress, TUNPortName
from agile_mesh_network.ryu.types import OFPort

OVSDB_PORT = 6640  # The IANA registered port for OVSDB [RFC7047]
RYU_PORT = 6633  # See also ofproto_v1_4.OFP_TCP_PORT


class RyuConfMock:
    """Can be used instead of ryu_app.CONF."""

    ovsdb_timeout = 2


def _synchronized(f):
    @wraps(f)
    def ff(self, *args, **kwargs):
        with self._lock:
            return f(self, *args, **kwargs)

    return ff


class OVSManager:
    # Execution context: shared between both Ryu and asyncio threads,
    # must be thread-safe, blocking and fast.

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

    @_synchronized
    def set_controller(self):
        self.ovs.set_controller([f"tcp:127.0.0.1:{RYU_PORT}"])

    @property
    def bridge_mac(self) -> MACAddress:
        if not self._bridge_mac:
            with self._lock:
                mac_in_use = self.ovs.db_get_val(
                    "Interface", self.ovs.br_name, "mac_in_use"
                )
            assert len(mac_in_use) == 1
            self._bridge_mac = mac_in_use[0]
        return self._bridge_mac

    @_synchronized
    def is_port_up(self, port_name: TUNPortName) -> bool:
        ports = set(self.ovs.get_port_name_list())
        if port_name not in ports:
            return False
        return self._is_port_up(port_name)

    @_synchronized
    def add_port_to_bridge(self, port_name: TUNPortName) -> None:
        command = ovs_vsctl.VSCtlCommand(
            "add-port", (self.ovs.br_name, port_name), "--may-exist"
        )
        self.ovs.run_command([command])

    @_synchronized
    def del_port_from_bridge(self, port_name: TUNPortName) -> None:
        command = ovs_vsctl.VSCtlCommand("del-port", (self.ovs.br_name, port_name))
        self.ovs.run_command([command])

    @_synchronized
    def get_ports_in_bridge(self) -> Sequence[TUNPortName]:
        return list(self.ovs.get_port_name_list())

    @_synchronized
    def get_ofport(self, port_name: TUNPortName) -> OFPort:
        return self.ovs.get_ofport(port_name)

    def get_ofport_ex(self, port_name: TUNPortName) -> OFPort:
        try:
            return self.get_ofport(port_name)
        except:
            # Exception: no row "tapanaaaaamzt16" in table Interface
            return OFPort(-1)

    def get_port_name_by_ofport(self, ofport: OFPort) -> TUNPortName:
        with self._lock:
            ports = self.ovs.get_port_name_list()
        for port_name in ports:
            ofport_ = self.get_ofport_ex(port_name)
            if ofport_ == ofport:
                return port_name
        raise KeyError("Port not found")

    def _is_port_up(self, port_name: TUNPortName) -> bool:
        if self.get_ofport_ex(port_name) < 0:
            # Interface is on the OVS DB, but is missing in the OS.
            return False
        with self._lock:
            link_state = self.ovs.db_get_val("Interface", port_name, "link_state")
        if link_state != ["up"]:
            # []  -- interface doesn't exist
            # ['down']  -- interface is down
            # ['up']  -- interface is up
            return False
        # Also there's `admin_state`.
        return True
