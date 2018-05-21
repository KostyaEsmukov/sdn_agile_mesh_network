import asyncio
import json
from abc import ABCMeta, abstractmethod
from typing import Awaitable, Tuple

from agile_mesh_network.common.reader import EncryptedNewlineReader
from agile_mesh_network.common.models import (
    LayersList, TunnelModel, LayersDescriptionModel, NegotiationIntentionModel
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


class Tunnel:  # TODO
    def __init__(self, src_mac, dst_mac, process_manager):
        self.src_mac = src_mac
        self.dst_mac = dst_mac
        self.process_manager = process_manager  # !!!

    @property
    def is_dead(self):
        """A final state. Tunnel won't be alive anymore.
        It needs to be stopped.
        """
        return self.process_manager.is_dead

    @property
    def is_tunnel_active(self):
        """Tunnel is alive."""
        return self.process_manager.is_tunnel_active


class PendingTunnel(metaclass=ABCMeta):

    @classmethod
    def tunnel_intention_for_initiator(cls, src_mac, dst_mac,
                                       layers: LayersDescriptionModel, timeout) -> 'PendingTunnel':
        tunnel_intention = TunnelIntention(src_mac, dst_mac,
                                           list(layers.layers.keys()))
        return InitiatorPendingTunnel(tunnel_intention, layers, timeout)

    @classmethod
    def tunnel_intention_for_responder(cls, transport) -> Tuple[asyncio.Protocol,
                                                                Awaitable['PendingTunnel']]:
        # TODO read neg
        # TODO validate neg

        pass

    def __init__(self, tunnel_intention):
        self.tunnel_intention = tunnel_intention

    @abstractmethod
    async def create_tunnel(self, *, loop) -> Tunnel:
        pass


class InitiatorPendingTunnel(PendingTunnel):
    def __init__(self, tunnel_intention, layers: LayersDescriptionModel,
                 timeout):
        super().__init__(tunnel_intention)
        self._layers = layers
        self._timeout = timeout

    async def create_tunnel(self, *, loop) -> Tunnel:
        assert self._layers.protocol == 'tcp'
        host, port = self._layers.dest
        neg = NegotiationIntentionModel(src_mac=self.tunnel_intention.src_mac,
                                        dst_mac=self.tunnel_intention.dst_mac,
                                        layers=self._layers.layers)
        int_prot = InitiatorInteriorTcpProtocol()
        ext_prot = InitiatorExteriorTcpProtocol(neg, int_prot)
        await loop.create_connection(lambda: ext_prot, host, port)
        # TODO handle close?

        await ext_prot.fut_negotiated
        server = await loop.create_server(int_prot, '127.0.0.1')

        assert 1 == len(server.sockets)
        _, port = server.sockets[0].getsockname()
        # TODO other layers
        # TODO below !!!!
        pm = OpenvpnProcessManager(dst_mac, **layers['openvpn'])
        lt = LocalTunnel(src_mac, dst_mac, pm)
        await pm.start(timeout)


class ResponderPendingTunnel(PendingTunnel):

    async def create_tunnel(self, *, loop) -> Tunnel:
        # TODO start ovpn
        # TODO connect to ovpn + pipe
        # TODO send ack
        pass


class InitiatorExteriorTcpProtocol(asyncio.Protocol):
    def __init__(self, negotiation_intention: NegotiationIntentionModel,
                 interior_protocol):
        self.negotiation_intention = negotiation_intention
        self.interior_protocol = interior_protocol
        self.enc_reader = EncryptedNewlineReader()
        self.is_negotiated = False
        self.fut_negotiated = asyncio.Future()

    def connection_made(self, transport):
        self.transport = transport
        self.interior_protocol.server_transport = transport
        # Send negotiation message.
        line = self.enc_reader.encrypt_line(
            json.dumps(self.negotiation_intention.asdict()).encode())
        self.transport.write(line + b'\n')

    def data_received(self, data):
        if not self.is_negotiated:
            self.enc_reader.feed_data(data)
            # Read ack message.
            for line in self.enc_reader:
                exp_ack = b'ack' + self.negotiation_intention.nonce
                if line != exp_ack:
                    logger.error('Bad ack! Expected: %r. Received: %r', exp_ack, line)
                    self.transport.close()
                    self.fut_negotiated.set_exception(OSError('bad ack'))
                    return
                break
            data = self.enc_reader.buf
            self.is_negotiated = True
            self.fut_negotiated.set_result()
        self.interior_protocol.write(data)

    def connection_lost(self, exc):
        self.fut_negotiated.set_exception(OSError('connection closed'))
        self.interior_protocol.close()


class InitiatorInteriorTcpProtocol(asyncio.Protocol):
    def __init__(self):
        self.write_q = []
        self.transport = None
        self.server_transport = None  # filled in the Exterior Protocol

    def connection_made(self, transport):
        self.transport = transport
        while self.write_q:
            self.transport.write(self.write_q.pop(0))
        self.write_q.clear()

    def data_received(self, data):
        self.server_transport.write(data)

    def connection_lost(self, exc):
        self.server_transport.close()

    def write(self, data):
        if self.transport is None:
            self.write_q.append(data)
            return
        self.transport.write(data)

    def close(self):
        if self.transport:
            self.transport.close()

class ResponderExteriorTcpProtocol(asyncio.Protocol):
    def __init__(self):
        pass

    def connection_made(self, transport):
        self.transport = transport
        # TODO

    def data_received(self, data):
        # TODO
        pass

    def connection_lost(self, exc):
        # TODO
        pass


class ResponderInteriorTcpProtocol(asyncio.Protocol):
    def __init__(self):
        pass

    def connection_made(self, transport):
        self.transport = transport
        # TODO

    def data_received(self, data):
        # TODO
        pass

    def connection_lost(self, exc):
        # TODO
        pass



class OpenvpnProcessManager:
    """Manages openvpn processes."""
    layers = ('openvpn',)

    def __init__(self, dst_mac, protocol, remote):
        # TODO setup configs, certs
        pass

    async def start(self, timeout):
        logger.warning('Pretending to start openvpn!')
        # TODO impl!

    @property
    def is_tunnel_active(self):
        pass

    @property
    def is_dead(self):
        pass
    # TODO stop??

# TODO process manager vs pipe client
# TODO complete impl in the top level
# TODO impl the shit below




