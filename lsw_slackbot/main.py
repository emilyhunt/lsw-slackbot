import asyncio
import os
from datetime import datetime, timedelta
import logging
from pathlib import Path

from slack_sdk.web.async_client import AsyncWebClient
from slack_sdk.http_retry.builtin_async_handlers import AsyncRateLimitErrorRetryHandler

from .resources import sample_resource_usage
from .util import string_time, Periodic, Scheduled
from .slack import hello_world, send_resource_use_plot, _send_message, check_memory

# Get Slack tokens from environment variables (app should always be started like this for security reasons)
try:
    SLACK_API_TOKEN = os.environ['SLACK_API_TOKEN']
except KeyError:
    raise KeyError("Unable to find SLACK_API_TOKEN in set environment variables! Did you start the app correctly?")

try:
    SLACK_APP_TOKEN = os.environ['SLACK_APP_TOKEN']
except KeyError:
    raise KeyError("Unable to find SLACK_APP_TOKEN in set environment variables! Did you start the app correctly?")


# Setup logging
LOGGING_DIR = Path("./logs")
LOGGING_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    format="[%(asctime)s - %(levelname)s - %(name)s] %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
    filename=LOGGING_DIR / (string_time() + ".log"),
)

# Todo: add these to config file reading later instead
# Setup other directories
TEMP_DIR = Path("./temp")
# TEMP_DIR.mkdir(exist_ok=True)

DATA_DIR = Path("./data")
# DATA_DIR.mkdir(exist_ok=True)

# Setup channels
CHANNEL_ADMIN = "#computing-admin"
CHANNEL_GENERAL = "#computing-admin"


def _get_repeated_tasks(client):
    """Returns a runnable list of asynchronous tasks to use."""
    tasks = list()

    # Check up on resource use occasionally
    tasks.append(Periodic(sample_resource_usage, 300,
                          args=(DATA_DIR,),
                          kwargs={"measurement_time": 30}))

    # Send a resource usage plot to the main channel, with everything from the past day
    tasks.append(Scheduled(
        send_resource_use_plot,
        {"hour": 8},
        timedelta(days=1),
        args=(client,
              CHANNEL_GENERAL,
              {"data_location": DATA_DIR,
               "output_location": TEMP_DIR / "resources.png",
               "start_time": datetime.now() - timedelta(hours=32),
               "aggregation_level": "minute"}),
        kwargs={"title": "Resource usage in the past 32 hours!"}))

    # Send a resource usage plot to the main channel, everything from past week
    tasks.append(Scheduled(
        send_resource_use_plot,
        datetime(year=2021, month=11, day=8, hour=7, minute=59),
        timedelta(days=7),
        args=(client,
              CHANNEL_GENERAL,
              {"data_location": DATA_DIR,
               "output_location": TEMP_DIR / "resources.png",
               "start_time": datetime.now() - timedelta(days=7, hours=7),
               "aggregation_level": "hour"}),
        kwargs={"title": "Resource usage in the past week!"}))

    tasks.append(Periodic(
        _send_message, timedelta(hours=1),
        first_run=datetime.now() + timedelta(seconds=60),
        args=(client, CHANNEL_ADMIN, f"I have NOT crashed! \o/")))

    tasks.append(Periodic(
        check_memory, timedelta(seconds=15),
        args=(client, CHANNEL_ADMIN),
        kwargs={"memory_warn_fraction": 0.90, "sleep_time": 3600}
    ))

    # Start the tasks!
    return tasks


async def _start_repeated_tasks(tasks):
    for a_task in tasks:
        await a_task.start()

    await asyncio.sleep(3e10)  # Sleeps for 100 years lol


async def _stop_repeated_tasks(tasks):
    for a_task in tasks:
        await a_task.stop()


def client_loop():
    """Main client_loop loop. Must be called from your python start script."""

    print("Starting the lsw-slackbot... (see logs for fine-grained updates)")

    # App startup procedure
    logging.info("Starting app...")

    # Initialise the client, including adding a retry handler that tries a few times to reconnect (for upto around 20
    # minutes before giving up using these default values)
    client = AsyncWebClient(token=os.environ['SLACK_API_TOKEN'])
    client.retry_handlers.append(AsyncRateLimitErrorRetryHandler(max_retry_count=10))

    asyncio.run(hello_world(client, CHANNEL_ADMIN))

    # Setup repeated tasks...
    print("Getting tasks...")
    tasks = _get_repeated_tasks(client)

    # ... & wait for them!
    try:
        print("Running event loop!")
        asyncio.run(_start_repeated_tasks(tasks))

    # Keyboard interrupt properly, if desired
    except KeyboardInterrupt:
        print("KEYBOARD INTERRUPT: shutting down tasks & exiting application!")
        logging.info("Exiting application.")

    # Shuts stuff down!
    asyncio.run(_stop_repeated_tasks(tasks))
