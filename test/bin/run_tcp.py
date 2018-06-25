#!/usr/bin/env python3
import argparse
import asyncio
import sys

parser = argparse.ArgumentParser(description="TCP client/server for testing.")
parser.add_argument("--mode", type=str, required=True, choices=("client", "server"))
parser.add_argument("--port", type=int, required=True)
parser.add_argument("--data", type=str, required=True)


async def handle(reader, writer, data):
    writer.write(data.encode())
    await writer.drain()
    while True:
        res = await reader.read(1)
        if not res:
            break
        sys.stdout.write(res.decode())
        sys.stdout.flush()
    writer.close()


async def tcp_client_local(port, data):
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    await handle(reader, writer, data)


async def tcp_server_local(port, data):
    done_future = asyncio.Future()

    async def handle_echo(reader, writer):
        try:
            await handle(reader, writer, data)
        except Exception as e:
            done_future.set_exception(e)
            raise
        else:
            done_future.set_result(None)

    server = await asyncio.start_server(handle_echo, "127.0.0.1", port)
    await done_future
    server.close()
    await server.wait_closed()


if __name__ == "__main__":
    args = parser.parse_args()
    loop = asyncio.get_event_loop()

    if args.mode == "client":
        coro = tcp_client_local(args.port, args.data)
    elif args.mode == "server":
        coro = tcp_server_local(args.port, args.data)
    else:
        raise AssertionError("Unknown mode")

    loop.run_until_complete(coro)
    loop.close()
