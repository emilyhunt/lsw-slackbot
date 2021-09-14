import asyncio
import os
from datetime import datetime
import logging
from pathlib import Path

from slack_sdk.web.async_client import AsyncWebClient

from .util import hello_world


# Get Slack token from environment variables (app should always be started like this for security reasons)
try:
    SLACK_API_TOKEN = os.environ['SLACK_API_TOKEN']
except KeyError:
    raise KeyError("Unable to find the Slack API token in set environment variables! Did you start the app correctly?")

# Setup logging
LOGGING_DIR = Path("../logs")
LOGGING_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    format="[%(asctime)s - %(levelname)s - %(name)s] %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
    filename=LOGGING_DIR / datetime.now().strftime("%Y-%m-%d--%H:%M:%S.log"),
)

# Setup other directories
TEMP_DIR = Path("../temp")
TEMP_DIR.mkdir(exist_ok=True)

TEMP_DIR = Path("../data")
TEMP_DIR.mkdir(exist_ok=True)

# Setup channels
CHANNEL_ADMIN = "#computing-admin"
CHANNEL_GENERAL = "#computing"


def client():
    """Main client loop. Must be called from your python start script."""

    print("Starting the lsw-slackbot... (see logs for fine-grained updates)")

    logging.info("Starting app...")
    client = AsyncWebClient(token=os.environ['SLACK_API_TOKEN'])

    logging.info("Saying hello world!")
    asyncio.run(hello_world(client, CHANNEL_ADMIN))

    logging.info("Exiting application.")
