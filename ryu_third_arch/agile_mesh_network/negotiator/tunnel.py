import asyncio
import json
from abc import ABCMeta, abstractmethod
from typing import Awaitable, Tuple

from agile_mesh_network.common.models import (
    LayersList, TunnelModel, LayersDescriptionModel, NegotiationIntentionModel
)
from agile_mesh_network.negotiator.process_managers import ProcessManager
from agile_mesh_network.negotiator.tunnel_protocols import (
    InitiatorExteriorTcpProtocol, ResponderExteriorTcpProtocol, PipeContext,
)


class BaseTunnel(metaclass=ABCMeta):
    def __init__(self, src_mac, dst_mac, layers: LayersList):
        self.src_mac = src_mac
        self.dst_mac = dst_mac
        self.layers = layers
        self._cmp_tuple = tuple(sorted((src_mac, dst_mac)))

    @property
    @abstractmethod
    def is_dead(self):
        pass

    @property
    @abstractmethod
    def is_tunnel_active(self):
        pass

    def model(self):
        return TunnelModel(src_mac=self.src_mac, dst_mac=self.dst_mac,
                           layers=self.layers, is_dead=self.is_dead,
                           is_tunnel_active=self.is_tunnel_active)

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

    def to_negotiation_intention(self, layers: LayersDescriptionModel):
        return NegotiationIntentionModel(src_mac=self.src_mac,
                                         dst_mac=self.dst_mac,
                                         layers=layers)

    @classmethod
    def from_negotiation_intention(cls, negotiation_intention: NegotiationIntentionModel
                                   ) -> Tuple['TunnelIntention', LayersDescriptionModel]:
        tunnel_intention = cls(src_mac=negotiation_intention.src_mac,
                               dst_mac=negotiation_intention.dst_mac,
                               layers=list(negotiation_intention.layers.keys()))
        return tunnel_intention, negotiation_intention.layers


class Tunnel:
    def __init__(self, tunnel_intention: TunnelIntention,
                 process_manager: ProcessManager):
        self.src_mac = tunnel_intention.src_mac
        self.dst_mac = tunnel_intention.dst_mac
        self.process_manager = process_manager

    @property
    def is_dead(self):
        return self.process_manager.is_dead

    @property
    def is_tunnel_active(self):
        return self.process_manager.is_tunnel_active


class PendingTunnel(metaclass=ABCMeta):

    @classmethod
    def tunnel_intention_for_initiator(cls, src_mac, dst_mac,
                                       layers: LayersDescriptionModel, timeout) -> 'PendingTunnel':
        tunnel_intention = TunnelIntention(src_mac, dst_mac,
                                           list(layers.layers.keys()))
        return InitiatorPendingTunnel(tunnel_intention, layers, timeout)

    @classmethod
    def tunnel_intention_for_responder(cls) -> Tuple[asyncio.Protocol,
                                                     Awaitable['PendingTunnel']]:
        pipe_context = PipeContext()
        protocol = ResponderExteriorTcpProtocol(pipe_context)
        return protocol, ResponderPendingTunnel.negotiate(protocol)

    def __init__(self, tunnel_intention, layers: LayersDescriptionModel):
        self.tunnel_intention = tunnel_intention
        self._layers = layers

    @abstractmethod
    async def create_tunnel(self) -> Tunnel:
        pass


class InitiatorPendingTunnel(PendingTunnel):
    def __init__(self, tunnel_intention, layers: LayersDescriptionModel,
                 timeout):
        super().__init__(tunnel_intention, layers)
        self._timeout = timeout

    async def create_tunnel(self) -> Tunnel:
        assert self._layers.protocol == 'tcp'
        loop = asyncio.get_event_loop()
        host, port = self._layers.dest
        pipe_context = PipeContext()
        neg = self.tunnel_intention.to_negotiation_intention(self._layers.layers)
        ext_prot = InitiatorExteriorTcpProtocol(pipe_context, neg)
        try:
            await loop.create_connection(lambda: ext_prot, host, port)
            # TODO handle close?

            await ext_prot.fut_negotiated
            pm = ProcessManager.from_layers_initiator(
                self.tunnel_intention.dst_mac, self._layers, pipe_context)
            await pm.start(timeout)
            return Tunnel(self.tunnel_intention, pm)
        except:
            pipe_context.close()
            raise


class ResponderPendingTunnel(PendingTunnel):
    def __init__(self, tunnel_intention, layers: LayersDescriptionModel,
                 protocol: ResponderExteriorTcpProtocol):
        super().__init__(tunnel_intention, layers)
        self._protocol = protocol

    @classmethod
    async def negotiate(cls, protocol: ResponderExteriorTcpProtocol) -> PendingTunnel:
        await protocol.fut_intention_read
        # TODO validate MAC
        tunnel_intention, layers = TunnelIntention.from_negotiation_intention(
            protocol.negotiation_intention)
        return cls(tunnel_intention, layers, protocol)

    async def create_tunnel(self) -> Tunnel:
        pm = ProcessManager.from_layers_responder(
            self.tunnel_intention.dst_mac, self._layers, self._protocol.pipe_context)
        await pm.start()  # TODO timeout??
        self._protocol.write_ack()
        return Tunnel(self.tunnel_intention, pm)
