import asyncio
from concurrent.futures import CancelledError


async def backoff(starting=1, maximum=20):
    """Exponential backoff.

    Usage::

        async for wait in backoff():
            try:
                ...
                break
            except:
                logger.info(f'Retrying in {wait} seconds')

    """
    cur = starting
    while True:
        yield cur
        try:
            await _sleep(cur)
        except CancelledError:
            return
        cur = min(cur * 2, maximum)


def _sleep(time):  # mocked in tests
    return asyncio.sleep(time)
