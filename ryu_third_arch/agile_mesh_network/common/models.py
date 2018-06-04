import base64
import inspect
import os
from typing import Any, Mapping, NewType, Sequence, Tuple

from dataclasses import asdict, dataclass, field

from .types import MACAddress


def random_string():
    # Must be json-serializable (i.e. str, not bytes).
    return base64.b64encode(os.urandom(10)).decode()


LayersList = NewType("LayersList", Sequence[str])
LayersWithOptions = NewType("LayersWithOptions", Mapping[str, Any])


class AsDictMixin:
    asdict = asdict


class FromDictMixin:

    @classmethod
    def from_dict(cls, kwargs):
        """Like cls(**kwargs), but silently skips unknown arguments.
        """
        extra = set(kwargs) - set(inspect.signature(cls.__init__).parameters.keys())
        for key in extra:
            kwargs.pop(key)
        return cls(**kwargs)


@dataclass(order=True)
class TunnelModel(AsDictMixin, FromDictMixin):
    src_mac: MACAddress
    dst_mac: MACAddress
    layers: LayersList
    is_dead: bool
    is_tunnel_active: bool


@dataclass
class LayersDescriptionModel(AsDictMixin):
    protocol: str
    layers: LayersWithOptions


@dataclass
class LayersDescriptionRpcModel(LayersDescriptionModel, AsDictMixin, FromDictMixin):
    dest: Tuple[str, int]  # ip, port.


@dataclass
class NegotiationIntentionModel(AsDictMixin):
    src_mac: MACAddress
    dst_mac: MACAddress
    layers: LayersWithOptions

    # Prevent replay attack for the 'ack' response.
    nonce: str = field(default_factory=random_string)


@dataclass
class SwitchEntity(FromDictMixin):
    hostname: str
    is_relay: bool
    mac: MACAddress
    layers_config: Any  # TODO model. LayersDescriptionRpcModel?
    # TODO allow switches to dynamically set their own IP addresses.
