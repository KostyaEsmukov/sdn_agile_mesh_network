import asyncio
import json
from abc import ABCMeta
from logging import getLogger

from agile_mesh_network.common.async_utils import (
    future_set_result_silent, future_set_exception_silent
)
from agile_mesh_network.common.reader import EncryptedNewlineReader
from agile_mesh_network.common.models import NegotiationIntentionModel

logger = getLogger(__name__)


class NegotiationMessages:
    @classmethod
    def compose_negotiation(cls,
                            negotiation_intention: NegotiationIntentionModel) -> bytes:
        return json.dumps(negotiation_intention.asdict()).encode()

    @classmethod
    def parse_negotiation(cls, line: bytes) -> NegotiationIntentionModel:
        return NegotiationIntentionModel(**json.loads(line.decode()))

    @classmethod
    def compose_ack(cls, negotiation_intention: NegotiationIntentionModel) -> bytes:
        return b'ack' + negotiation_intention.nonce

    @classmethod
    def validate_ack(cls, line: bytes,
                     negotiation_intention: NegotiationIntentionModel) -> str:
        exp_ack = b'ack' + negotiation_intention.nonce
        if line != exp_ack:
            logger.error('Bad ack! Expected: %r. Received: %r', exp_ack, line)
            raise ValueError('Invalid ack or nonce')


class BaseExteriorProtocol(asyncio.Protocol, metaclass=ABCMeta):
    def __init__(self):
        self.interior_transport = None
        self._enc_reader = EncryptedNewlineReader()
        self._write_q = []

    # Used by interior protocol
    def contribute_interior_transport(self, interior_transport):
        self.interior_transport = interior_transport
        while self._write_q:
            self.interior_transport.write(self.write_q.pop(0))

    def interior_write(self, data):
        if self.interior_transport is None:
            self._write_q.append(data)
            return
        self.interior_transport.write(data)

    def connection_lost(self, exc):  # Protocol method
        logger.info('%s: connection lost', type(self).__name__, exc_info=exc)
        self.close()

    def write(self, data):  # Used by interior protocol
        self.transport.write(data)

    def close(self):  # Used by interior protocol
        if self.transport:
            self.transport.close()
        if self.interior_transport:
            self.interior_transport.close()


class InitiatorExteriorTcpProtocol(BaseExteriorProtocol):
    def __init__(self, negotiation_intention: NegotiationIntentionModel):
        super().__init__()
        self.negotiation_intention = negotiation_intention
        self.is_negotiated = False
        self.fut_negotiated = asyncio.Future()

    def connection_made(self, transport):  # Protocol method
        self.transport = transport
        self.write_negotiation()

    def write_negotiation(self):
        line = self._enc_reader.encrypt_line(
            NegotiationMessages.compose_negotiation(self.negotiation_intention))
        self.transport.write(line + b'\n')

    def data_received(self, data):  # Protocol method
        if not self.is_negotiated:
            self._enc_reader.feed_data(data)
            # Read ack message.
            for line in self._enc_reader:
                try:
                    NegotiationMessages.validate_ack(line, self.negotiation_intention)
                except Exception as e:
                    self.close()
                    future_set_exception_silent(self.fut_negotiated, e)
                    return
                data = self._enc_reader.buf
                self.is_negotiated = True
                future_set_result_silent(self.fut_negotiated, None)
                if not data:
                    return
                break
            else:
                return
        self.interior_write(data)

    def close(self):
        future_set_exception_silent(self.fut_negotiated,
                                    OSError('connection closed'))
        super().close()


class ResponderExteriorTcpProtocol(BaseExteriorProtocol):
    def __init__(self):
        super().__init__()
        self.negotiation_intention = None
        self.is_intention_read = False
        self.fut_intention_read = asyncio.Future()

    def connection_made(self, transport):  # Protocol method
        self.transport = transport

    def write_ack(self):
        line = self._enc_reader.encrypt_line(
            NegotiationMessages.compose_ack(self.negotiation_intention))
        self.transport.write(line + b'\n')

    def data_received(self, data):  # Protocol method
        if not self.is_intention_read:
            self._enc_reader.feed_data(data)
            for line in self._enc_reader:
                self.negotiation_intention = NegotiationMessages.parse_negotiation(line)
                data = self._enc_reader.buf
                self.is_intention_read = True
                future_set_result_silent(self.fut_intention_read, None)
                if not data:
                    return
                break
            else:
                return
        self.interior_write(data)

    def close(self):
        future_set_exception_silent(self.fut_intention_read,
                                    OSError('connection closed'))
        super().close()
