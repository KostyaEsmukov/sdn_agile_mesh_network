import asyncio
import builtins
import json
from abc import ABCMeta
from concurrent.futures import CancelledError
from logging import getLogger
from typing import Any, Awaitable

from .reader import NewlineReader
from .timeout_backoff import backoff

logger = getLogger(__name__)


# asyncio implementations of jsonrpc and 0mq are immature.


class BaseRPCMessage(metaclass=ABCMeta):
    def __init__(self, name, kwargs):
        self.name = name
        self.kwargs = kwargs

    def __repr__(self):
        return (
            f"{type(self).__name__}(name={repr(self.name)}, "
            f"kwargs={repr(self.kwargs)})"
        )

    def __eq__(self, other):
        if not other or not isinstance(other, type(self)):
            return False
        return self.name == other.name and self.kwargs == other.kwargs

    def __ne__(self, other):
        return not (self == other)


class RPCCommand(BaseRPCMessage):
    def __init__(self, name, kwargs, transport, msg_id):
        super().__init__(name, kwargs)
        self._msg_id = msg_id
        self._transport = transport

    async def respond(self, payload):
        assert self._msg_id
        if isinstance(payload, Exception):
            msg_type, data = "re", {"type": type(payload).__name__, "str": str(payload)}
        else:
            msg_type, data = "r", payload

        payload = ":".join((msg_type, self._msg_id, "", json.dumps(data))) + "\n"
        self._transport.write(payload.encode())
        self._msg_id = None


class RPCBroadcast(BaseRPCMessage):
    pass


class RPCSession:
    def __init__(self):
        self.transport = None
        self.msg_id = 0
        self.msg_id_to_future = {}
        self._is_closed = False

    def _replace_transport(self, transport, close=False):
        if transport is None and self.transport is None:
            # Idempotent close.
            return
        assert not self._is_closed
        if close:
            # It's important to do this before calling transport.close(),
            # which uses this flag. Otherwise close() might use the
            # not yet updated value.
            self._is_closed = True
        for fut in self.msg_id_to_future.values():
            if fut.done():
                assert fut.cancelled()
                continue
            fut.set_exception(OSError("connection closed"))
        self.msg_id_to_future.clear()
        if self.transport:
            self.transport.close()
        self.transport = transport

    async def issue_broadcast(self, name, kwargs={}) -> None:
        if not self.transport:
            raise ValueError("No transport. Remote end is not connected?")
        payload = ":".join(("", "", name, json.dumps(kwargs))) + "\n"
        self.transport.write(payload.encode())

    async def issue_command(self, name, kwargs={}) -> Any:
        if not self.transport:
            raise ValueError("No transport. Remote end is not connected?")
        self.msg_id += 1
        msg_id = str(self.msg_id)
        payload = ":".join(("c", msg_id, name, json.dumps(kwargs))) + "\n"
        self.transport.write(payload.encode())
        fut: Awaitable[Any] = asyncio.Future()
        self.msg_id_to_future[msg_id] = fut
        # Note that this future might get cancelled, that would leave it
        # in the self.msg_id_to_future dict in a finalized state.
        return await fut

    def close(self):
        self._replace_transport(None, close=True)

    @property
    def is_connected(self):
        if self._is_closed:
            return False
        if not self.transport:
            return False
        return not self.transport.is_closing()

    def _process_message(self, line):
        assert line
        assert self.transport
        msg_type, msg_id, name, kwargs_json = line.decode().split(":", 3)
        kwargs = json.loads(kwargs_json)
        if not msg_id:
            return RPCBroadcast(name, kwargs)
        if msg_type == "c":  # incoming command
            return RPCCommand(name, kwargs, self.transport, msg_id)

        fut = self.msg_id_to_future.pop(msg_id)
        if fut.done():
            assert fut.cancelled()
            return None

        if msg_type == "r":  # response to our command
            assert not name
            fut.set_result(kwargs)
            return None
        if msg_type == "re":  # erroneous response to our command
            assert not name
            type_str = kwargs["type"]
            type_ = getattr(builtins, type_str, None)
            if not type_ or not issubclass(type_, Exception):
                type_ = Exception
            str_ = kwargs["str"]
            fut.set_exception(type_(str_))
            return None
        raise AssertionError(f"Unknown msg_type {msg_type}")


class BaseRPCProtocol(asyncio.Protocol, metaclass=ABCMeta):
    # Must be re-entrable (i.e. the same instance can be used after
    # disconnect).

    def __init__(self, command_cb):
        self.transport = None
        self.session = RPCSession()
        self.command_cb = command_cb
        self.newline_reader = NewlineReader()

    def connection_made(self, transport):
        assert self.transport is None
        self.transport = transport
        self.session._replace_transport(transport)

    def data_received(self, data):
        logger.debug("recv data, %s", data)
        self.newline_reader.feed_data(data)
        for line in self.newline_reader:
            msg = self.session._process_message(line)
            if msg:
                asyncio.ensure_future(self._command_task(msg))

    def connection_lost(self, exc):
        self.on_close()

    async def _command_task(self, msg):
        try:
            await self.command_cb(self.session, msg)
        except:
            logger.error("RPC: error during command processing.", exc_info=True)
            self.on_close()

    def on_close(self):
        self.session._replace_transport(None)  # Closes the transport too.
        self.transport = None


class ClientRPCProtocol(BaseRPCProtocol):
    def __init__(self, command_cb, close_cb):
        super().__init__(command_cb)
        self.close_cb = close_cb

    def on_close(self):
        super().on_close()
        self.close_cb()


class ServerRPCProtocol(BaseRPCProtocol):
    def __init__(self, sessions, command_cb):
        super().__init__(command_cb)
        self.sessions = sessions

    def connection_made(self, transport):
        super().connection_made(transport)
        self.sessions.add(self.session)

    def on_close(self):
        self.sessions.discard(self.session)
        super().on_close()
        self.session.close()


class RPCUnixServer:
    def __init__(self, unix_sock_path, command_cb):
        self.server = None
        self.unix_sock_path = unix_sock_path
        self.command_cb = command_cb
        self.sessions = set()

    async def start(self):
        loop = asyncio.get_event_loop()
        self.server = await loop.create_unix_server(
            lambda: ServerRPCProtocol(self.sessions, self.command_cb),
            self.unix_sock_path,
        )

    async def close_wait(self):
        self.server.close()
        for session in self.sessions:
            session.close()
        await self.server.wait_closed()

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        await self.close_wait()


class RPCUnixClient:
    def __init__(self, unix_sock_path, command_cb):
        self.unix_sock_path = unix_sock_path
        self.command_cb = command_cb
        self._protocol = ClientRPCProtocol(self.command_cb, self._reconnect_or_close)
        self.session = self._protocol.session
        self._reconnect_task = None

    async def start(self):
        await self._connect()

    async def close_wait(self):
        self.session.close()

    async def _connect(self):
        loop = asyncio.get_event_loop()
        transport, protocol = await loop.create_unix_connection(
            lambda: self._protocol, self.unix_sock_path
        )

    def _reconnect_or_close(self):
        if self.session._is_closed:
            if self._reconnect_task:
                # Session is closed, but there's a pending
                # reconnection task. Cancel it.
                self._reconnect_task.cancel()
            return

        if self._reconnect_task:
            if not self._reconnect_task.done():
                return
            # Session is not closed, so we should reconnect.
            # But this reconnect task is not active, so cancel
            # it explicitly (just in case).
            self._reconnect_task.cancel()

        async def f():
            logger.warning("Lost connection to the RPC server. Reconnecting...")
            async for wait in backoff():
                try:
                    await self._connect()
                    logger.info("Connected to the RPC server.")
                    break
                except CancelledError:
                    return
                except:
                    logger.error(
                        "Failed to reconnect to the RPC server. Retrying in %s seconds.",
                        wait,
                        exc_info=True,
                    )

        self._reconnect_task = asyncio.ensure_future(f())

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        await self.close_wait()
