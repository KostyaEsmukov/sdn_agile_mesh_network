import asyncio
from abc import ABCMeta, abstractmethod
from typing import Awaitable, Generic, Tuple, TypeVar

from agile_mesh_network.common.models import (
    LayersDescriptionModel, LayersDescriptionRPCModel, LayersList, LayersWithOptions,
    NegotiationIntentionModel, NegotiatorProtocolValue, TunnelModel
)
from agile_mesh_network.common.types import MACAddress
from agile_mesh_network.negotiator.layers import ProcessManager
from agile_mesh_network.negotiator.tunnel_protocols import (
    InitiatorExteriorTcpProtocol, PipeContext, ResponderExteriorTcpProtocol
)


class BaseTunnel(metaclass=ABCMeta):
    def __init__(
        self, src_mac: MACAddress, dst_mac: MACAddress, layers: LayersList
    ) -> None:
        self.src_mac = src_mac
        self.dst_mac = dst_mac
        self.layers = layers
        self._cmp_tuple = tuple(sorted((src_mac, dst_mac)))

    @property
    @abstractmethod
    def is_dead(self) -> bool:
        """Tunnel won't be active. This is a final state."""
        pass

    @property
    @abstractmethod
    def is_tunnel_active(self) -> bool:
        """Is tunnel truly alive? Might temporary be False when is_dead
        is False -- which means that the tunnel is not alive, but might be
        up soon.
        """
        pass

    @abstractmethod
    def close(self) -> None:
        pass

    def model(self) -> TunnelModel:
        return TunnelModel(
            src_mac=self.src_mac,
            dst_mac=self.dst_mac,
            layers=self.layers,
            is_dead=self.is_dead,
            is_tunnel_active=self.is_tunnel_active,
        )

    def __str__(self):
        return f"{self.src_mac}<->{self.dst_mac}"

    def __hash__(self):
        return hash(self._cmp_tuple)

    def __eq__(self, other):
        if not isinstance(other, BaseTunnel):
            return False
        return self._cmp_tuple == other._cmp_tuple

    def __ne__(self, other):
        return not (self == other)


class TunnelIntention(BaseTunnel):
    @property
    def is_dead(self):
        # Mimic the Tunnel. TunnelIntention means that there's
        # no negotiation between the sides yet, but it's still in progress,
        # so the tunnel is definitely not dead.
        return False

    @property
    def is_tunnel_active(self):
        return False

    def close(self):
        pass  # TODO

    def to_negotiation_intention(
        self, layers: LayersWithOptions
    ) -> NegotiationIntentionModel:
        return NegotiationIntentionModel(
            src_mac=self.src_mac, dst_mac=self.dst_mac, layers=layers
        )

    @classmethod
    def from_negotiation_intention(
        cls,
        negotiation_intention: NegotiationIntentionModel,
        protocol: NegotiatorProtocolValue,
    ) -> Tuple["TunnelIntention", LayersDescriptionModel]:
        # src_mac of negotiation represents the MAC address of the initiator.
        # When converting negotiation to a tunnel (on responder side),
        # we need to swap them, as from the responder's point of view
        # the dst should be the initiator's MAC, not the src.
        tunnel_intention = cls(
            src_mac=negotiation_intention.dst_mac,
            dst_mac=negotiation_intention.src_mac,
            layers=LayersList(list(negotiation_intention.layers.keys())),
        )
        layers = LayersDescriptionModel(
            protocol=protocol, layers=negotiation_intention.layers
        )
        return tunnel_intention, layers


class Tunnel(BaseTunnel):
    def __init__(
        self, tunnel_intention: TunnelIntention, process_manager: ProcessManager
    ) -> None:
        super().__init__(
            tunnel_intention.src_mac, tunnel_intention.dst_mac, tunnel_intention.layers
        )
        self.process_manager = process_manager

    @property
    def is_dead(self):
        return self.process_manager.is_dead

    @property
    def is_tunnel_active(self):
        return self.process_manager.is_tunnel_active

    def close(self):
        self.process_manager.close()


T = TypeVar("T", bound=LayersDescriptionModel)


class PendingTunnel(Generic[T], metaclass=ABCMeta):
    @classmethod
    def tunnel_intention_for_initiator(
        cls,
        src_mac: MACAddress,
        dst_mac: MACAddress,
        layers: LayersDescriptionRPCModel,
        timeout: float,
    ) -> "PendingTunnel":
        tunnel_intention = TunnelIntention(
            src_mac, dst_mac, LayersList(list(layers.layers.keys()))
        )
        return _InitiatorPendingTunnel(tunnel_intention, layers, timeout)

    @classmethod
    def tunnel_intention_for_responder(
        cls
    ) -> Tuple[asyncio.Protocol, Awaitable["PendingTunnel"]]:
        pipe_context = PipeContext()
        protocol = ResponderExteriorTcpProtocol(pipe_context)
        return protocol, _ResponderPendingTunnel.negotiate(protocol)

    def __init__(self, tunnel_intention: TunnelIntention, layers: T) -> None:
        self.tunnel_intention = tunnel_intention
        self._layers: T = layers

    @abstractmethod
    async def create_tunnel(self) -> Tunnel:
        pass


class _InitiatorPendingTunnel(PendingTunnel):
    def __init__(
        self,
        tunnel_intention: TunnelIntention,
        layers: LayersDescriptionRPCModel,
        timeout: float,
    ) -> None:
        super().__init__(tunnel_intention, layers)
        self._timeout = timeout

    async def create_tunnel(self) -> Tunnel:
        assert self._layers.protocol == "tcp"
        loop = asyncio.get_event_loop()
        host, port = self._layers.dest
        pipe_context = PipeContext()
        neg = self.tunnel_intention.to_negotiation_intention(self._layers.layers)
        ext_prot = InitiatorExteriorTcpProtocol(pipe_context, neg)
        with pipe_context:
            await loop.create_connection(lambda: ext_prot, host, port)
            await ext_prot.fut_negotiated
            pm = ProcessManager.from_layers_initiator(
                self.tunnel_intention.dst_mac,
                self.tunnel_intention.src_mac,
                self._layers,
                pipe_context,
            )
            await pm.start(self._timeout)
            await pm.tunnel_started()  # TODO timeout??
            return Tunnel(self.tunnel_intention, pm)


class _ResponderPendingTunnel(PendingTunnel):
    def __init__(
        self,
        tunnel_intention: TunnelIntention,
        layers: LayersDescriptionModel,
        protocol: ResponderExteriorTcpProtocol,
    ) -> None:
        super().__init__(tunnel_intention, layers)
        self._protocol = protocol

    @classmethod
    async def negotiate(cls, protocol: ResponderExteriorTcpProtocol) -> PendingTunnel:
        with protocol.pipe_context:
            await protocol.fut_intention_read
            assert protocol.negotiation_intention
            # TODO validate MAC
            tunnel_intention, layers = TunnelIntention.from_negotiation_intention(
                protocol.negotiation_intention, protocol="tcp"
            )  # TODO protocol
            return cls(tunnel_intention, layers, protocol)

    async def create_tunnel(self) -> Tunnel:
        with self._protocol.pipe_context:
            pm = ProcessManager.from_layers_responder(
                self.tunnel_intention.dst_mac,
                self.tunnel_intention.src_mac,
                self._layers,
                self._protocol.pipe_context,
            )
            await pm.start()  # TODO timeout??
            self._protocol.write_ack()
            await pm.tunnel_started()  # TODO timeout??
            return Tunnel(self.tunnel_intention, pm)
