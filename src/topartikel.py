"""
Created by: humberg
Date:       03.06.20

This module contains all necessary functions to import topartikel in five categories:
- meistgelesen (topartikel)
- meistgelesen abopflichtig (topartikel_rot)
- meistgelesen registrierungspflichtig (topartikel_grau)
- meisten probeabos (topartikel_best)
- meisten registrierungen (topartikel_reg)
"""

from src import api
import pandas as pd
from datetime import datetime
import logging
import requests
from bs4 import BeautifulSoup


def get_data_top(date_from=api.get_datetime_yesterday(),
                 date_to=api.get_datetime_yesterday()):
    """
    function to build anlysisConfig and make api request; function retrieves top five most read
    articles fro  yesterday
    :param date_from:
    :param date_to:
    :return: dataframe with top five most read articles, their visits and their referrer
    """
    # build analysisConfig
    analysisConfig = {
        "hideFooters": [1],
        "startTime": date_from,
        "stopTime": date_to,
        "analysisFilter": {
            "filterRules": [{
                "objectTitle": "Seiten",
                "comparator": "=",
                "filter": "*.article.*"
            }]
        },
        "analysisObjects": [{
            "title": "Seiten",
            "rowLimit": 5
        }],
        "metrics": [{
            "title": "Visits *",
            "sortOrder": "desc"
        }, {
            "title": "Visits Direct"
        }, {
            "title": "Visits Stationaer"
        }, {
            "title": "Visits mobile"
        }, {
            "title": "Visits Chrome Content Suggestions"
        }, {
            "title": "Visits Direct iOS"
        }, {
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
            "title": "Visits Socialife"
        }, {
            "title": "Visits Upday"
        }, {
            "title": "Visits Twitter"
        }
        ]}

    # request data
    data = api.wt_get_data(analysisConfig)

    # parse data
    data = data["result"]["analysisData"]
    df = pd.DataFrame(data)
    col_names = ["url", "visits", "visits_direct", "visits_stationaer", "visits_mobile",
                 "visits_chrome_sugg", "visits_direct_ios", "visits_facebook", "visits_firefox",
                 "visits_flipboard", "visits_google_news", "visits_google_organisch", "visits_push",
                 "visits_socialife", "visits_upday", "visits_twitter"]
    df.columns = col_names

    # create date and rank
    df["date"] = pd.to_datetime(datetime.strftime(datetime.now(), '%Y-%m-%d'))
    df["rank"] = range(1, 1+len(df))

    # use only url of article and get title
    df.url = df.url.str.partition('|')[2]
    df["title"] = df.url.apply(lambda x: get_title_from_xml(x))

    # rearrange order of colummns
    cols = df.columns.tolist()
    cols = cols[-3:] + cols[:-3]
    df = df[cols]

    # convert to numeric columns
    convert_cols = df.columns.drop(['date', 'rank', 'title', 'url'])
    df[convert_cols] = df[convert_cols].apply(pd.to_numeric, errors='coerce')

    logging.info(str(datetime.now()) + ' topartikel imported from webtrekk')

    return df


def get_title_from_xml(url):
    """
    this functions retrieves the title from a given article url
    :param url: url of article
    :return: title of article
    """
    url = 'http://xml' + url.partition('www')[2]
    req = requests.get(url)
    soup = BeautifulSoup(req.content, 'lxml')
    title = soup.body.title.text
    return title

