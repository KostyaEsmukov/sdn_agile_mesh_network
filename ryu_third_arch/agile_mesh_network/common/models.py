import base64
import inspect
import os
from enum import Enum
from typing import Any, Mapping, NewType, Sequence, Tuple

from dataclasses import asdict, dataclass, field, fields, is_dataclass

from .types import MACAddress


def random_string():
    # Must be json-serializable (i.e. str, not bytes).
    return base64.b64encode(os.urandom(10)).decode()


LayersList = NewType("LayersList", Sequence[str])
LayersWithOptions = NewType("LayersWithOptions", Mapping[str, Any])
DestHost = NewType("DestHost", Tuple[str, int])  # hostname, port.


class NegotiatorProtocol(Enum):
    TCP = "tcp"
    # TODO: udp, ws, wss, tcps, ...


NegotiatorProtocolValue = NewType("NegotiatorProtocolValue", str)  # value of the Enum.


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
        for f in fields(cls):
            if f.name not in kwargs:
                continue
            if is_dataclass(f.type) and not is_dataclass(kwargs[f.name]):
                assert issubclass(f.type, FromDictMixin)
                kwargs[f.name] = f.type.from_dict(kwargs[f.name])
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
    protocol: NegotiatorProtocolValue
    layers: LayersWithOptions


@dataclass
class LayersDescriptionRpcModel(LayersDescriptionModel, AsDictMixin, FromDictMixin):
    dest: DestHost


@dataclass
class NegotiationIntentionModel(AsDictMixin):
    src_mac: MACAddress
    dst_mac: MACAddress
    layers: LayersWithOptions

    # Prevent replay attack for the 'ack' response.
    nonce: str = field(default_factory=random_string)


@dataclass
class NegotiatorLayersConfig(FromDictMixin):
    negotiator: Mapping[NegotiatorProtocolValue, DestHost]
    layers: LayersWithOptions


@dataclass
class SwitchEntity(FromDictMixin):
    hostname: str
    is_relay: bool
    mac: MACAddress
    layers_config: NegotiatorLayersConfig
    # TODO allow switches to dynamically set their own IP addresses.
