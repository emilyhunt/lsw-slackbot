import socket
from slack_sdk.errors import SlackApiError


async def hello_world(client, channel):
    """Basic function to post an init message to a channel."""

    try:
        system_name = socket.gethostname()

        await client.chat_postMessage(
            channel=channel,
            text=f"App now running on system {system_name}"
        )

    except SlackApiError as e:
        assert e.response["ok"] is False
        assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found'
        print(f"Got an error: {e.response['error']}")
