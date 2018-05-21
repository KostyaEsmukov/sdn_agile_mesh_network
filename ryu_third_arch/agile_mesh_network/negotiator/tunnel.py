from abc import ABCMeta


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
                                       layers: LayersDescriptionModel, timeout) -> PendingTunnel:
        tunnel_intention = TunnelIntention(src_mac, dst_mac,
                                           list(layers.layers.keys()))
        return InitiatorPendingTunnel(tunnel_intention, layers, timeout)

    @classmethod
    def tunnel_intention_for_responder(cls, transport) -> Tuple[asyncio.Protocol,
                                                                Awaitable[PendingTunnel]]:
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
                                        layers=self.layers.layers)
        ext_prot = InitiatorExteriorTcpProtocol(neg)
        await loop.create_connection(lambda: ext_prot, host, port)
        # TODO handle close?

        await ext_prot.fut_negotiated
        # TODO start int serv + pipe

        # TODO other layers
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
    def __init__(self, negotiation_intention: NegotiationIntentionModel):
        self.negotiation_intention = negotiation_intention
        self.enc_reader = EncryptedNewlineReader()
        self.is_negotiated = False
        self.fut_negotiated = asyncio.Future()

    def connection_made(self, transport):
        self.transport = transport
        line = self.enc_reader.encrypt_line(json.dumps(self.negotiation_intention.asdict()))
        self.transport.write(line + b'\n')
        # TODO

    def data_received(self, data):
        if not self.is_negotiated:
            self.enc_reader.feed_data(data)
            for line in self.enc_reader:
                exp_ack = bf'ack{self.negotiation_intention.nonce}'
                if line != exp_ack:
                    logger.error('Bad ack! Expected: %r. Received: %r', exp_ack, line)
                    self.transport.close()
                    self.fut_negotiated.set_exception(OSError('bad ack'))
                    return
                break
            data = self.enc_reader.buf
            self.is_negotiated = True
            self.fut_negotiated.set_result()

        # TODO

    def connection_lost(self, exc):
        self.fut_negotiated.set_exception(OSError('connection closed'))
        # TODO


class InitiatorInteriorTcpProtocol(asyncio.Protocol):
    def __init__(self):
        pass

    def connection_made(self, transport):
        self.transport = transport
        # TODO

    def data_received(self, data):
        # TODO

    def connection_lost(self, exc):
        # TODO


class ResponderExteriorTcpProtocol(asyncio.Protocol):
    def __init__(self):
        pass

    def connection_made(self, transport):
        self.transport = transport
        # TODO

    def data_received(self, data):
        # TODO

    def connection_lost(self, exc):
        # TODO


class ResponderInteriorTcpProtocol(asyncio.Protocol):
    def __init__(self):
        pass

    def connection_made(self, transport):
        self.transport = transport
        # TODO

    def data_received(self, data):
        # TODO

    def connection_lost(self, exc):
        # TODO



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


# class TcpPipeClient:
#     def __init__(self, host, port, src_mac, dst_mac, layers):
#         pass

#     async def negotiate(self):
#         # return host, port of the bound server.
#         pass

#     async def close_wait(self):
#         pass


# class ClientProtocol(asyncio.Protocol):
#     transport = None

#     def __init__(self, server_transport):
#         self.server_transport = server_transport
#         self.write_q = []

#     def connection_made(self, transport):
#         self.transport = transport
#         self.server_transport.resume_reading()
#         print('resume')
#         while self.write_q:
#             self.transport.write(self.write_q.pop(0))

#     def data_received(self, data):
#         self.server_transport.write(data)

#     def write(self, data):
#         if self.transport is None:
#             self.write_q.append(data)
#             return
#         self.transport.write(data)


# class ServerProtocol(asyncio.Protocol):
#     def connection_made(self, transport, message_handler):
#         self.transport = transport
#         self.is_negotiating = True
#         self.negotiation_data = b''
#         # self.transport.pause_reading()
#         # print('pause')
#         # self.client_protocol = ClientProtocol(self.transport)
#         # coro = loop.create_connection(lambda: self.client_protocol, '::1', 5201)
#         # loop.create_task(coro)

#     def data_received(self, data):
#         try:
#             if self.is_negotiating:
#                 self.negotiation_data += data
#                 line_rest = self.negotiation_data.split(b'\n', 1)
#                 if len(line_rest) != 2:
#                     return
#                 line, data = line_rest
#                 response = message_handler.accept(line)
#                 # TODO await ovpn is up
#                 self.transport.write(response)
#                 self.is_negotiating = False
#                 self.client_protocol = ClientProtocol(self.transport)
#                 # TODO create_connection
#                 if not data:
#                     return
#             self.client_protocol.write(data)
#         except:
#             self.transport.close()
#             raise

#     def connection_lost(self, exc):
#         pass



