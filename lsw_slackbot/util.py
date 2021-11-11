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
    def __init__(self, func: callable, time_interval: Union[int, datetime.timedelta],
                 first_run=Optional[datetime.datetime],
                 args: Union[tuple, list] = tuple(), kwargs: dict = tuple()):
        """This class acts as a simple way to schedule tasks with asyncio. See the following StackOverflow answer for
        how it works: https://stackoverflow.com/a/37514633/12709989
        """
        # Ensure time_interval is a datetime.timedelta object
        if isinstance(time_interval, int):
            time_interval = datetime.timedelta(seconds=time_interval)
        elif not isinstance(time_interval, datetime.timedelta):
            raise ValueError("periodic time for scheduling must be an int or a datetime.timedelta object.")

        self.func = func
        self.time_interval = time_interval
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
            next_run = datetime.datetime.now() + self.time_interval
            self.func(*self.args, **self.kwargs)

            # Sleep!
            sleep_time = (next_run - datetime.datetime.now()).total_seconds()
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)


class Scheduled(Periodic):
    def __init__(self, func: callable, first_run: datetime.datetime, time_interval: Union[int, datetime.timedelta],
                 args: Union[tuple, list] = tuple(), kwargs: dict = tuple()):
        """This class inherits from Periodic, and runs tasks in a slightly different way (i.e. scheduled at a set time
        interval instead of being every n seconds. Ideal for very rarely occuring tasks that ought to happen at
        e.g. the same time every day."""
        super().__init__(func, time_interval, first_run, args, kwargs)

    def _calculate_seconds_to_next_run(self):
        """Gets the next possible run time!"""
        first_to_now = (datetime.datetime.now() - self.first_run).total_seconds()
        intervals_to_add = int(first_to_now // self.time_interval.total_seconds() + 1)
        return (self.first_run + intervals_to_add * self.time_interval).total_seconds()

    async def _run(self):
        while True:
            await asyncio.sleep(self._calculate_seconds_to_next_run())
            self.func(*self.args, *self.kwargs)
