"""
Created by: humberg
Date:       03.06.20

This module contains all functions to retrieve the ad impressions data from webtrekk (or ad manager
api)
"""

from src import api
import pandas as pd
import logging
from datetime import datetime

def get_data(date_from=api.get_datetime_yesterday(),
             date_to=api.get_datetime_yesterday()):
    """
    function to build analysisConfig and make api request
    :param date_from:
    :param date_to:
    :return: dataframe with relevant information
    """
    # build analysisConfig
    analysisConfig = {
        "hideFooters": [1],
        "startTime": date_from,
        "stopTime": date_to,
        "analysisObjects": [{
            "title": "Tage"
        }],
        "metrics": [{
            "title": "AI stationaer gesamt"
        }, {
            "title": "AI mobile gesamt"
        }, {
            "title": "AI HP stationaer"
        }, {
            "title": "AI HP mobile"
        }
        ]}

    # request data
    data = api.wt_get_data(analysisConfig)

    # parse data
    data = data["result"]["analysisData"]
    df = pd.DataFrame(data)
    col_names = ["date", "ai_stationaer", "ai_mobile", "ai_hp_stationaer", "ai_hp_mobile"]
    df.columns = col_names
    df.date = pd.to_datetime(df.date, format="%d.%m.%Y")

    convert_cols = df.columns.drop('date')
    df[convert_cols] = df[convert_cols].apply(pd.to_numeric, errors='coerce')

    logging.info(str(datetime.now()) + ' ad impressions imported from webtrekk')

    return df