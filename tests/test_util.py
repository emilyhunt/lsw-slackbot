"""Testing functions for things in util.py."""

import asyncio
from lsw_slackbot import util


def print_time():
    print(util.string_time())


async def test_periodic():
    """MANUAL TEST! For now anyway. This should output 5 times."""
    print("MANUAL Periodic TEST! You should see output 5 times, once per second.")
    task = util.Periodic(print_time, 1)
    await task.start()
    await asyncio.sleep(5.1)
    await task.stop()


async def test_scheduled():
    """MANUAL TEST! For now anyway. This should output 5 times."""
    print("MANUAL Scheduled TEST! You should see output roughly 5 times, once per 2 seconds, on every even second.")
    task = util.Scheduled(print_time, {"second": 0}, 2)
    await task.start()
    await asyncio.sleep(11.9)
    await task.stop()


if __name__ == "__main__":
    asyncio.run(test_periodic())
    asyncio.run(test_scheduled())
