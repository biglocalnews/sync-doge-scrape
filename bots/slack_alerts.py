from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import os
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import logging

logger = logging.getLogger(__name__)

class SlackInternalAlert:
    def __init__(self, script_name):
        token = os.environ.get("SLACK_ERROR_TOKEN")
        self.slack_alert_channel_id = os.environ.get("SLACK_ERROR_CHANNEL_ID")
        self.client = WebClient(token=token)
        self.script_name = script_name

    def post(self, message, message_type="notice"):

        message_type_prefix = {
            "error": "🚨",
            "success": "👍",
            "notice" : "ℹ️"
        }
        formatted_message = f"{message_type_prefix[message_type]} {self.script_name}: {message}" 
        try:
            self.client.chat_postMessage(channel=self.slack_alert_channel_id, text=formatted_message)
        except SlackApiError as e:
            logger.error(f"Slack error: {e.response['error']} (channel: {self.slack_alert_channel_id})")
