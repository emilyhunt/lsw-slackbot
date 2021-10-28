import asyncio
import os
from datetime import datetime
import logging
from pathlib import Path

from slack_sdk.web.async_client import AsyncWebClient
from slack_sdk.http_retry.builtin_async_handlers import AsyncRateLimitErrorRetryHandler

from .util import hello_world, string_time, Periodic


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
LOGGING_DIR = Path("../logs")
LOGGING_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    format="[%(asctime)s - %(levelname)s - %(name)s] %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
    filename=LOGGING_DIR / (string_time() + ".log"),
)

# Setup other directories
TEMP_DIR = Path("../temp")
TEMP_DIR.mkdir(exist_ok=True)

DATA_DIR = Path("../data")
DATA_DIR.mkdir(exist_ok=True)

# Setup channels
CHANNEL_ADMIN = "#computing-admin"
CHANNEL_GENERAL = "#computing"


def client():
    """Main client loop. Must be called from your python start script."""

    print("Starting the lsw-slackbot... (see logs for fine-grained updates)")

    # App startup procedure
    logging.info("Starting app...")

    # Initialise the client, including adding a retry handler that tries a few times to reconnect (for upto around 20
    # minutes before giving up using these default values)
    client = AsyncWebClient(token=os.environ['SLACK_API_TOKEN'])
    client.retry_handlers.append(AsyncRateLimitErrorRetryHandler(max_retry_count=10))

    logging.info("Saying hello world!")
    asyncio.run(hello_world(client, CHANNEL_ADMIN))

    # Setup repeated tasks
    #  = Periodic(lambda )


    logging.info("Exiting application.")
