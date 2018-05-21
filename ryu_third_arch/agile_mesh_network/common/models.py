import os
from typing import Any, Sequence, Mapping

from dataclasses import dataclass, asdict, field

LayersList = Sequence[str]


@dataclass
class TunnelModel:
    src_mac: str
    dst_mac: str
    layers: LayersList
    is_dead: bool
    is_tunnel_active: bool

    asdict = asdict


@dataclass
class LayersDescriptionModel:
    protocol: str
    dest: Any
    layers: Mapping[str, Any]

    asdict = asdict


@dataclass
class NegotiationIntentionModel:
    src_mac: str
    dst_mac: str
    layers: Mapping[str, Any]

    # Prevent replay attack for the 'ack' response.
    nonce: str = field(default_factory=lambda: os.urandom(10))

    asdict = asdict
