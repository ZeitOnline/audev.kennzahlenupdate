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
    function to build anlysisConfig and make api request
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
            "title": "Visits Facebook (inkl. IAs)"
        }, {
            "title": "Visits Firefox Recommendations"
        }, {
            "title": "Visits Flipboard"
        }, {
            "title": "Visits Google News"
        }, {
            "title": "Visits Google Organisch"
        }, {
            "title": "Visits Push"
        }, {
            "title": "Visits sonstige Referrer (all)"
        }, {
            "title": "Einstiege Facebook (inkl. IAs)"
        }, {
            "title": "Einstiege Firefox Recommendations"
        }, {
            "title": "Einstiege Flipboard"
        }, {
            "title": "Einstiege Google News"
        }, {
            "title": "Einstiege Google Organisch"
        }, {
            "title": "Einstiege Push"
        }, {
            "title": "Einstiege sonstige Referrer (all)"
        }
        ]}

    # request data
    data = api.wt_get_data(analysisConfig)

    # parse data
    data = data["result"]["analysisData"]
    df = pd.DataFrame(data)
    col_names = ["date", "visits_facebook", "visits_firefox", "visits_flipboard",
                 "visits_google_news", "visits_google_org", "visits_push", "visits_sonstige",
                 "einstiege_facebook", "einstiege_firefox", "einstiege_flipboard",
                 "einstiege_google_news", "einstiege_google_org", "einstiege_push",
                 "einstiege_sonstige"]
    df.columns = col_names
    df.date = pd.to_datetime(df.date, format="%d.%m.%Y")

    convert_cols = df.columns.drop('date')
    df[convert_cols] = df[convert_cols].apply(pd.to_numeric, errors='coerce')

    return df
