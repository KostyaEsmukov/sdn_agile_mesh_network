import asyncio
import json
from abc import ABCMeta, abstractmethod
from typing import Awaitable, Tuple

from agile_mesh_network.common.reader import EncryptedNewlineReader
from agile_mesh_network.common.models import (
    LayersList, TunnelModel, LayersDescriptionModel, NegotiationIntentionModel
)


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
                data = self.enc_reader.buf
                self.is_negotiated = True
                self.fut_negotiated.set_result(None)
                if not data:
                    return
                break
            else:
                return
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
        self.negotiation_intention = None
        self.enc_reader = EncryptedNewlineReader()
        self.is_intention_read = False
        self.fut_intention_read = asyncio.Future()
        self.write_q = []

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        if not self.is_intention_read:
            self.enc_reader.feed_data(data)
            for line in self.enc_reader:
                self.negotiation_intention = \
                    NegotiationIntentionModel(**json.loads(line.decode()))
                data = self.enc_reader.buf
                self.is_intention_read = True
                self.fut_intention_read.set_result(None)
                if not data:
                    return
                break
            else:
                return
        self.write_interior(data)

    def connection_lost(self, exc):
        self.fut_intention_read.set_exception(OSError('connection closed'))
        # TODO
        pass

    def write_interior(self, data):
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

