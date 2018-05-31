import base64
import inspect
import os
from typing import Any, Mapping, Sequence

from dataclasses import asdict, dataclass, field


def random_string():
    # Must be json-serializable (i.e. str, not bytes).
    return base64.b64encode(os.urandom(10)).decode()


LayersList = Sequence[str]


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
    src_mac: str
    dst_mac: str
    layers: LayersList
    is_dead: bool
    is_tunnel_active: bool


@dataclass
class LayersDescriptionModel(AsDictMixin):
    protocol: str
    layers: Mapping[str, Any]


@dataclass
class LayersDescriptionRpcModel(LayersDescriptionModel, AsDictMixin, FromDictMixin):
    dest: Any


@dataclass
class NegotiationIntentionModel(AsDictMixin):
    src_mac: str
    dst_mac: str
    layers: Mapping[str, Any]

    # Prevent replay attack for the 'ack' response.
    nonce: str = field(default_factory=random_string)


@dataclass
class SwitchEntity(FromDictMixin):
    hostname: str
    is_relay: bool
    mac: str
    layers_config: Any  # TODO model. LayersDescriptionRpcModel?
    # TODO allow switches to dynamically set their own IP addresses.
