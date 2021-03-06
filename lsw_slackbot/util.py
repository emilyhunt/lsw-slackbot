import asyncio
import datetime
from typing import Union, Optional

from contextlib import suppress


def string_time():
    return datetime.datetime.now().strftime("%Y-%m-%d--%H:%M:%S")


class Periodic:
    def __init__(self, func: callable, time_interval: Union[int, datetime.timedelta],
                 first_run: Optional[datetime.datetime] = None,
                 args: Union[tuple, list] = tuple(), kwargs: Optional[dict] = None):
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

        if kwargs is None:
            self.kwargs = {}
        else:
            self.kwargs = kwargs

    async def start(self):
        if not self.is_started:
            self.is_started = True
            # Start task to call func periodically:
            self._task = asyncio.create_task(self._run())

    async def stop(self):
        if self.is_started:
            self.is_started = False
            # Stop task and await it stopped:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task

    async def _sleep_until_first_run(self):
        """Works out if we need to sleep until first_run and sleeps until then for you!"""
        if self.first_run is not None:
            initial_sleep_time = (self.first_run - datetime.datetime.now()).total_seconds()
            if initial_sleep_time > 0:
                await asyncio.sleep(initial_sleep_time)

    async def _run(self):
        """Main running loop that handles tasks."""
        await self._sleep_until_first_run()

        # After the optional initial wait... it's time to loop!
        while True:
            # Before calling func(), we always work out when the next call should be. This means that if func() takes
            # a while to run, then we won't slip in time as much. (It's still imperfect though, since under heavy load
            # the end of asyncio.sleep() could take a while to be unblocked. However, that's just an unavoidable
            # side-effect, and in heavy load scenarios, you probably don't want to have all time taken up by periodic
            # tasks anyway... ;) )
            next_run = datetime.datetime.now() + self.time_interval
            await self.func(*self.args, **self.kwargs)

            # Sleep!
            sleep_time = (next_run - datetime.datetime.now()).total_seconds()
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)


_POSSIBLE_DATETIME_KWARGS = ("year", "month", "day", "hour", "minute", "second", "microsecond")


class Scheduled(Periodic):
    def __init__(self, func: callable, first_run: Union[datetime.datetime, dict],
                 time_interval: Union[int, datetime.timedelta],
                 args: Union[tuple, list] = tuple(), kwargs: dict = tuple()):
        """This class inherits from Periodic, and runs tasks in a slightly different way (i.e. scheduled at a set time
        interval instead of being every n seconds. Ideal for very rarely occuring tasks that ought to happen at
        e.g. the same time every day."""
        # If first_run is a dict of datetime parameters, then we set first_run based on this
        if isinstance(first_run, dict):
            first_run = self.dict_to_first_run_datetime(first_run)
        elif not isinstance(first_run, datetime.datetime):
            raise ValueError("first_run must be a datetime or a dict that can be used to setup a datetime!")

        super().__init__(func, time_interval, first_run=first_run, args=args, kwargs=kwargs)

    @staticmethod
    def dict_to_first_run_datetime(first_run_dict):
        now = datetime.datetime.now()
        now_tuple = (now.year, now.month, now.day, now.hour, now.minute, now.second, now.microsecond)

        smallest_defined_time_interval_index = Scheduled.get_smallest_defined_time_interval(first_run_dict)
        parameters_from_now_to_use = _POSSIBLE_DATETIME_KWARGS[:smallest_defined_time_interval_index]

        first_run_dict_final = {
            k: v for k, v in zip(parameters_from_now_to_use, now_tuple[:smallest_defined_time_interval_index])}
        first_run_dict_final.update(first_run_dict)

        return datetime.datetime(**first_run_dict_final)

    @staticmethod
    def get_smallest_defined_time_interval(first_run_dict):
        for i, a_parameter in enumerate(_POSSIBLE_DATETIME_KWARGS[::-1]):
            if a_parameter in first_run_dict:
                # Deal with the fact that we always have to at least assign a year, month and day at minimum
                if i < 3:
                    return 3
                else:
                    return i

        raise ValueError("no valid time definitions were found in the first_run dictionary specifier!")

    def _calculate_seconds_to_next_run(self):
        """Gets the next possible run time!"""
        now = datetime.datetime.now()
        first_to_now = (now - self.first_run).total_seconds()
        intervals_to_add = int(first_to_now // self.time_interval.total_seconds() + 1)
        seconds_to_next_run = ((self.first_run + intervals_to_add * self.time_interval) - now).total_seconds()

        # Extra check just in case we ended up with a negative wait time
        if seconds_to_next_run < 0:
            return 0.0
        else:
            return seconds_to_next_run

    async def _run(self):
        while True:
            await asyncio.sleep(self._calculate_seconds_to_next_run())
            await self.func(*self.args, **self.kwargs)


class RunOnce(Periodic):
    """This class also inherits from Periodic. It only runs once! Either immediately or at first_run."""
    def __init__(self, func: callable, first_run: Optional[datetime.datetime] = None,
                 args: Union[tuple, list] = tuple(), kwargs: Optional[dict] = None):
        super().__init__(func, 0, first_run=first_run, args=args, kwargs=kwargs)

    async def _run(self):
        """Runs a function once after an optional sleep."""
        await self._sleep_until_first_run()

        await self.func(*self.args, **self.kwargs)
