import asyncio
import json
from abc import ABCMeta
from typing import Any, Optional
from logging import getLogger

from .reader import NewlineReader

logger = getLogger(__name__)


# asyncio implementations of jsonrpc and 0mq are immature.


class BaseRpcMessage(metaclass=ABCMeta):
    def __init__(self, name, kwargs):
        self.name = name
        self.kwargs = kwargs


class RpcCommand(BaseRpcMessage):
    def __init__(self, name, kwargs, transport, msg_id):
        super().__init__(name, kwargs)
        self._msg_id = msg_id
        self._transport = transport

    async def respond(self, payload):
        assert self._msg_id
        if isinstance(payload, Exception):
            msg_type, data = 're', {"type": type(payload).__name__,
                                    "str": str(payload)}
        else:
            msg_type, data = 'r', payload

        payload = ':'.join((msg_type, self._msg_id, '',
                            json.dumps(data))) + '\n'
        self._transport.write(payload.encode())
        self._msg_id = None


class RpcBroadcast(BaseRpcMessage):
    pass


class RpcSession:
    def __init__(self, transport):
        self.transport = transport
        self.msg_id = 0
        self.msg_id_to_future = {}

    async def issue_broadcast(self, name, kwargs={}) -> None:
        payload = ':'.join(('', '', name, json.dumps(kwargs))) + '\n'
        self.transport.write(payload.encode())

    async def issue_command(self, name, kwargs={}) -> Any:
        self.msg_id += 1
        msg_id = str(self.msg_id)
        payload = ':'.join(('c', msg_id, name,
                            json.dumps(kwargs))) + '\n'
        self.transport.write(payload.encode())
        fut = self.msg_id_to_future[msg_id] = asyncio.Future()
        return await fut

    def close(self):
        for fut in self.msg_id_to_future.values():
            fut.set_exception(OSError('connection closed'))
        self.msg_id_to_future.clean()
        self.transport.close()

    def _process_message(self, line):
        assert line
        msg_type, msg_id, name, kwargs_json = line.decode().split(':', 3)
        kwargs = json.loads(kwargs_json)
        if not msg_id:
            return RpcBroadcast(name, kwargs)
        if msg_type == 'c':  # incoming command
            return RpcCommand(name, kwargs, self.transport, msg_id)
        if msg_type == 'r':  # response to our command
            assert not name
            fut = self.msg_id_to_future.pop(msg_id)
            fut.set_result(kwargs)
            return None
        raise AssertionError(f'Unknown msg_type {msg_type}')


class RpcProtocol(asyncio.Protocol):
    def __init__(self, sessions, command_cb):
        self.session = None
        self.sessions = sessions
        self.command_cb = command_cb
        self.newline_reader = NewlineReader()

    def connection_made(self, transport):
        self.transport = transport
        self.session = RpcSession(self.transport)
        self.sessions.add(self.session)

    def data_received(self, data):
        logger.debug('recv data, %s', data)
        self.newline_reader.feed_data(data)
        for line in self.newline_reader:
            msg = self.session._process_message(line)
            if msg:
                (asyncio.ensure_future(self.command_cb(self.session, msg))
                 .add_done_callback(self.command_future_callback))

    def connection_lost(self, exc):
        self.sessions.discard(self.session)
        self.session.close()

    def command_future_callback(self, fut):
        if fut.done() and fut.exception():
            logger.error('RPC: error during command '
                         'processing.', fut.exception())
        self.sessions.discard(self.session)
        self.session.close()


class RpcUnixServer:
    def __init__(self, unix_sock_path, command_cb, *, loop):
        self.server = None
        self.unix_sock_path = unix_sock_path
        self.command_cb = command_cb
        self.loop = loop
        self.sessions = set()

    async def start(self):
        self.server = await self.loop.create_unix_server(
            lambda: RpcProtocol(self.sessions, self.command_cb),
            self.unix_sock_path)

    async def close_wait(self):
        self.server.close()
        await self.server.wait_closed()


class RpcUnixClient:
    def __init__(self, unix_sock_path, command_cb, *, loop):
        self.session = None
        self.unix_sock_path = unix_sock_path
        self.command_cb = command_cb
        self.loop = loop

    async def start(self):
        transport, protocol = await self.loop.create_unix_connection(
            lambda: RpcProtocol(set(), self.command_cb),
            self.unix_sock_path)
        self.session = protocol.session

    async def close_wait(self):
        self.session.close()
