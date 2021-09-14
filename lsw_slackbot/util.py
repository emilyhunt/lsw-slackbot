import socket
import asyncio
import logging
import datetime
from typing import Union, Optional

from slack_sdk.errors import SlackApiError
from contextlib import suppress


def string_time():
    return datetime.datetime.now().strftime("%Y-%m-%d--%H:%M:%S")


async def hello_world(client, channel):
    """Basic function to post an init message to a channel."""

    try:
        system_name = socket.gethostname()

        await client.chat_postMessage(
            channel=channel,
            text=f"Server time & date: {string_time()}\nApp is running on system {system_name}."
        )

    except SlackApiError as e:
        logging.exception(f"error from slack API when trying to send message: {e.response['error']}")


class Periodic:
    def __init__(self, func: callable, time: Union[int, datetime.timedelta], first_run=Optional[datetime.datetime],
                 args: Union[tuple, list] = tuple(), kwargs: dict = tuple()):
        """This class acts as a simple way to schedule tasks with asyncio. See the following StackOverflow answer for
        how it works: https://stackoverflow.com/a/37514633/12709989
        """
        if isinstance(time, datetime.timedelta):
            time = time.total_seconds()
        elif not isinstance(time, int):
            raise ValueError("periodic time for scheduling must be an int or a datetime.timedelta object.")

        self.func = func
        self.time = time
        self.is_started = False
        self._task = None
        self.first_run = first_run

        self.args = args
        self.kwargs = kwargs

    async def start(self):
        if not self.is_started:
            self.is_started = True
            # Start task to call func periodically:
            self._task = asyncio.ensure_future(self._run())

    async def stop(self):
        if self.is_started:
            self.is_started = False
            # Stop task and await it stopped:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task

    async def _run(self):

        # Work out when we need to run for the first time
        if self.first_run is not None:
            initial_sleep_time = (self.first_run - datetime.datetime.now()).total_seconds()
            if initial_sleep_time > 0:
                await asyncio.sleep(initial_sleep_time)

        # After the optional initial wait... it's time to loop!
        while True:
            # Before calling func(), we always work out when the next call should be. This means that if func() takes
            # a while to run, then we won't slip in time as much. (It's still imperfect though, since under heavy load
            # the end of asyncio.sleep() could take a while to be unblocked. However, that's just an unavoidable
            # side-effect, and in heavy load scenarios, you probably don't want to have all time taken up by periodic
            # tasks anyway... ;) )
            next_run = datetime.datetime.now() + datetime.timedelta(seconds=self.time)
            self.func(*self.args, **self.kwargs)

            # Sleep!
            sleep_time = (next_run - datetime.datetime.now()).total_seconds()
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
