#!/usr/bin/env python3
import argparse
import asyncio

parser = argparse.ArgumentParser(description='TCP client/server for testing.')
parser.add_argument('--mode', type=str, required=True, choices=('client', 'server'))
parser.add_argument('--port', type=int, required=True)
parser.add_argument('--data', type=str, help='client only')


async def tcp_client_local(port, data):
    reader, writer = await asyncio.open_connection('127.0.0.1', port)
    writer.write(data.encode())
    await writer.drain()
    while not writer.is_closed():
        sys.stdout.write(await reader.read(1))
    writer.close()


async def tcp_server_local(port):
    done_future = asyncio.Future()

    async def handle_echo(reader, writer):
        try:
            while not writer.is_closed():
                sys.stdout.write(await reader.read(1))
            writer.close()
        except Exception as e:
            done.future.set_exception(e)
            raise
        else:
            done_future.set_result(None)

    server = await asyncio.start_server(handle_echo, '127.0.0.1', port)
    await done_future


if __name__ == '__main__':
    args = parser.parse_args()
    loop = asyncio.get_event_loop()

    if args.mode == 'client':
        if not args.data:
            raise ValueError('--data is required in --mode client')
        coro = tcp_client_local(args.port, args.data)
    elif args.mode == 'server':
        coro = tcp_server_local(args.port)
    else:
        raise AssertionError('Unknown mode')

    loop.run_until_complete(coro)
    loop.close()
