import asyncio
import json
from abc import ABCMeta
from logging import getLogger
from typing import Awaitable, Optional

from agile_mesh_network.common.async_utils import (
    future_set_exception_silent, future_set_result_silent
)
from agile_mesh_network.common.models import NegotiationIntentionModel
from agile_mesh_network.common.reader import EncryptedNewlineReader

logger = getLogger(__name__)


class PipeContext:

    def __init__(self):
        self._closing_set = set()
        self.exterior_transport = None
        self.interior_transport = None
        self._write_q = []
        self._close_callbacks = []
        self.is_closed = False

    def contribute_exterior_transport(self, exterior_transport):
        assert self.exterior_transport is None
        self.exterior_transport = exterior_transport
        self.add_closing(exterior_transport)

    def contribute_interior_transport(self, interior_transport):
        assert self.interior_transport is None
        self.interior_transport = interior_transport
        self.add_closing(interior_transport)
        while self._write_q:
            self.interior_transport.write(self._write_q.pop(0))

    def write_to_exterior(self, data):
        self.exterior_transport.write(data)

    def write_to_interior(self, data):
        if self.interior_transport is None:
            self._write_q.append(data)
            return
        self.interior_transport.write(data)

    def add_closing(self, closing):
        assert hasattr(closing, "close")
        self._closing_set.add(closing)

    def add_close_callback(self, callback):
        self._close_callbacks.append(callback)

    def close(self):
        if self.is_closed:
            return
        logger.info("Closing pipe context")
        # TODO close process first, wait until it's terminated,
        # then close sockets.
        self.is_closed = True
        for closing in self._closing_set:
            closing.close()
        self._closing_set.clear()
        for callback in self._close_callbacks:
            callback()
        self._close_callbacks.clear()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        if exc_type:
            self.close()


class NegotiationMessages:

    @classmethod
    def compose_negotiation(
        cls, negotiation_intention: NegotiationIntentionModel
    ) -> bytes:
        return json.dumps(negotiation_intention.asdict()).encode()

    @classmethod
    def parse_negotiation(cls, line: bytes) -> NegotiationIntentionModel:
        return NegotiationIntentionModel(**json.loads(line.decode()))

    @classmethod
    def compose_ack(cls, negotiation_intention: NegotiationIntentionModel) -> bytes:
        return b"ack" + negotiation_intention.nonce.encode()

    @classmethod
    def validate_ack(
        cls, line: bytes, negotiation_intention: NegotiationIntentionModel
    ):
        exp_ack = b"ack" + negotiation_intention.nonce.encode()
        if line != exp_ack:
            logger.error("Bad ack! Expected: %r. Received: %r", exp_ack, line)
            raise ValueError("Invalid ack or nonce")


class BaseExteriorProtocol(asyncio.Protocol, metaclass=ABCMeta):

    def __init__(self, pipe_context: PipeContext) -> None:
        self.interior_transport = None
        self._enc_reader = EncryptedNewlineReader()
        self.pipe_context = pipe_context

    def connection_made(self, transport):
        self.transport = transport
        self.pipe_context.contribute_exterior_transport(transport)

    def connection_lost(self, exc):
        logger.info("%s: connection lost", type(self).__name__, exc_info=exc)
        self.pipe_context.close()


class InitiatorExteriorTcpProtocol(BaseExteriorProtocol):

    def __init__(
        self,
        pipe_context: PipeContext,
        negotiation_intention: NegotiationIntentionModel,
    ) -> None:
        super().__init__(pipe_context)
        self.negotiation_intention = negotiation_intention
        self.is_negotiated = False
        self.fut_negotiated: Awaitable[None] = asyncio.Future()
        pipe_context.add_close_callback(
            lambda: future_set_exception_silent(
                self.fut_negotiated, OSError("connection closed")
            )
        )

    def connection_made(self, transport):
        super().connection_made(transport)
        self._write_negotiation()

    def _write_negotiation(self):
        line = self._enc_reader.encrypt_line(
            NegotiationMessages.compose_negotiation(self.negotiation_intention)
        )
        self.transport.write(line + b"\n")

    def data_received(self, data):
        if not self.is_negotiated:
            self._enc_reader.feed_data(data)
            # Read ack message.
            for line in self._enc_reader:
                try:
                    NegotiationMessages.validate_ack(line, self.negotiation_intention)
                except Exception as e:
                    future_set_exception_silent(self.fut_negotiated, e)
                    self.pipe_context.close()
                    return
                data = self._enc_reader.buf
                self.is_negotiated = True
                future_set_result_silent(self.fut_negotiated, None)
                if not data:
                    return
                break
            else:
                return
        self.pipe_context.write_to_interior(data)


class ResponderExteriorTcpProtocol(BaseExteriorProtocol):

    def __init__(self, pipe_context: PipeContext) -> None:
        super().__init__(pipe_context)
        self.negotiation_intention: Optional[NegotiationIntentionModel] = None
        self.is_intention_read = False
        self.fut_intention_read: Awaitable[None] = asyncio.Future()
        pipe_context.add_close_callback(
            lambda: future_set_exception_silent(
                self.fut_intention_read, OSError("connection closed")
            )
        )

    def write_ack(self):
        line = self._enc_reader.encrypt_line(
            NegotiationMessages.compose_ack(self.negotiation_intention)
        )
        self.transport.write(line + b"\n")

    def data_received(self, data):
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
        self.pipe_context.write_to_interior(data)
