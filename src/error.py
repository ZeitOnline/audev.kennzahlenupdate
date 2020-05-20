"""
Created by: humberg
Date:       19.05.20

This module contains all functions relevant to error handling.
"""
import json
import requests
import getpass


def send_error_slack(message):
    """
    This function sends message as error to slack using the app 'errorking'
    :param message: string of error
    :return:
    """
    webhook_url = 'https://hooks.slack.com/services/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'

    data = {
        "text": getpass.getuser() + "@audev.kennzahlenupdate :alert:",
        "attachments": [
            {
                "text": message,
                "color": "#f00",
                "footer": "<http://81.132.32.12|fix me, please>"
            }
        ]
    }

    requests.post(
        webhook_url, data=json.dumps(data),
        headers={'Content-Type': 'application/json'}
    )