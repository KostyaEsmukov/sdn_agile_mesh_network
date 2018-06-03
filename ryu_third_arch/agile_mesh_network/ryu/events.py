from typing import Mapping, Tuple

from ryu.controller.event import EventBase

from agile_mesh_network.common.models import SwitchEntity, TunnelModel


class EventActiveTunnelsList(EventBase):

    def __init__(
        self, mac_to_tunswitch: Mapping[str, Tuple[TunnelModel, SwitchEntity]]
    ) -> None:
        self.mac_to_tunswitch = mac_to_tunswitch
