"""Various functions that interact with Slack, e.g. posting messages."""

import logging
import socket
from pathlib import Path
from typing import Union

from slack_sdk.errors import SlackApiError

from lsw_slackbot.plots import plot_resource_use
from lsw_slackbot.util import string_time


async def _send_message(client, channel: str, message: str):
    """Sends a message to a channel, with basic logging & error handling."""

    try:
        await client.chat_postMessage(channel=channel, text=message)

    except SlackApiError as e:
        logging.exception(f"error from slack API when trying to send message: {e.response['error']}")


async def _send_file(client, channel: str, file: Union[Path, str], title):
    """Sends a file to a channel, with basic logging & error handling."""

    if isinstance(file, Path):
        file = str(file.absolute())

    try:
        await client.files_upload(channels=channel, file=file, title=title)

    except SlackApiError as e:
        logging.exception(f"error from slack API when trying to upload file: {e.response['error']}")


async def hello_world(client, channel: str):
    """Basic function to post an init message to a channel."""
    logging.info(f"Saying hello world in {channel}!")
    system_name = socket.gethostname()
    await _send_message(
        client, channel, f"Server time & date: {string_time()}\nApp is running on system {system_name}.")


async def send_resource_use_plot(client, channel: str, plot_kwargs: dict, title=None):
    """Sends a resource usage plot to a given channel."""

    if title is None:
        title = f"Resource usage plot generated at {string_time()}"
    else:
        title = title + f" (plot generated at {string_time()})"

    # Firstly, let's generate a plot
    logging.info("Generating a resource usage plot")
    logging.debug(f"plot kwargs: {plot_kwargs}")
    location_plot = await plot_resource_use(**plot_kwargs)

    # Now, let's try and send it to slack
    logging.info(f"Sending to Slack in channel {channel}")
    await _send_file(client, channel, location_plot, title)
