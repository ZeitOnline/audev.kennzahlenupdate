"""
Created by: humberg
Date:       19.05.20

This module contains all functions relevant to error handling.
"""

import os
import sys
import json
import requests
import getpass

# add parent directory to sys.path in order to import modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))


def send_error_slack(message):
    """
    This function sends message as error to slack using the app 'errorking'
    :param message: string of error
    :return:
    """

    webhook_url = os.environ.get('SLACK_WEBHOOK')

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