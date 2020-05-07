"""
Created by: humberg
Date:       04.05.20

This module contains all functions getting and processing referrertraffic from webtrekk
"""
from src import api
import pandas as pd


def get_data(date_from=api.get_datetime_yesterday(),
             date_to=api.get_datetime_yesterday()):
    """

    :param date_from:
    :param date_to:
    :return: dataframe with
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
            "title": "Visits Google Organisch"
        }, {
            "title": "Visits Facebook (inkl. IAs)"
        }, {
            "title": "Visits Push"
        }, {
            "title": "Visits Google News"
        }, {
            "title": "Visits sonstige Referrer (all)"
        }, {
            "title": "Visits Flipboard"
        }, {
            "title": "Visits Firefox Recommendations"
        }
        ]}

    # request data
    data = api.wt_get_data(analysisConfig)

    # parse data
    data = data["result"]["analysisData"]
    df = pd.DataFrame(data)
    col_names = ["date", "google_organisch", "facebook", "push", "google_news", "sonst_referrer",
                 "flipboard", "firefox_rec"]
    df.columns = col_names
    df.date = pd.to_datetime(df.date, format="%d.%m.%Y")

    convert_cols = df.columns.drop('date')
    df[convert_cols] = df[convert_cols].apply(pd.to_numeric, errors='coerce')

    return df
