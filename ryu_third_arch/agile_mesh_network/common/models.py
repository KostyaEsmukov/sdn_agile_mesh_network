import os
import base64
from typing import Any, Sequence, Mapping

from dataclasses import dataclass, asdict, field


def random_string():
    # Must be json-serializable (i.e. str, not bytes).
    return base64.b64encode(os.urandom(10)).decode()


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
    layers: Mapping[str, Any]

    asdict = asdict


@dataclass
class LayersDescriptionRpcModel(LayersDescriptionModel):
    dest: Any

    asdict = asdict


@dataclass
class NegotiationIntentionModel:
    src_mac: str
    dst_mac: str
    layers: Mapping[str, Any]

    # Prevent replay attack for the 'ack' response.
    nonce: str = field(default_factory=random_string)

    asdict = asdict
