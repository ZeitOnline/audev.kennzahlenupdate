"""
Created by: humberg
Date:       03.06.20

This module contains all necessary functions to import topartikel in three categories:
- meistgelesen (topartikel)
- meisten probeabos (topartikel_best)
- meisten registrierungen (topartikel_reg)
"""

from src import api, bigquery
import pandas as pd
from datetime import datetime, timedelta
import logging
import requests
from bs4 import BeautifulSoup


def get_data_top(date_from=api.get_datetime_yesterday(),
                 date_to=api.get_datetime_yesterday()):
    """
    function to build anlysisConfig and make api request; function retrieves top five most read
    articles from yesterday
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
            "title": "Visits Apple News (geschätzt)"                # vorher Visits Direct iOS
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
    df["date"] = pd.to_datetime(date_from)
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

    logging.info(str(datetime.now()) + ' topartikel imported from webtrekk for ' + date_from)

    return df


def get_title_from_xml(url):
    """
    this functions retrieves the title from a given article url
    :param url: url of article
    :return: title of article
    """
    url = 'http://xml' + url.partition('www')[2]
    #url = "http://cms-backend.zeit.de:9000/cms/work" + url.partition('www.zeit.de')[2]
    req = requests.get(url)
    soup = BeautifulSoup(req.content, 'xml')

    try:
        title = soup.article.body.title.text
        spitzmarke = soup.article.body.supertitle
    except:
        title = "Article has no title"
        spitzmarke = None

    if spitzmarke is not None:
        spitzmarke = spitzmarke.text
        title = spitzmarke + ': ' + title

    return title


def get_title_from_tms(url):
    """
    this function retrieves the title from a given url from the tms data in bigquery
    :param url: url of article
    :return: title of article
    """
    sql = 'SELECT title from `audev-webtrekk-for-content.wfc.tms` WHERE url = "' + url + '"'
    df_title = bigquery.get_data_wfc(sql)
    title = df_title.title[0]

    return title


def get_data_top_best(date_from=api.get_datetime_yesterday(),
                      date_to=api.get_datetime_yesterday()):
    """
    function to build anlysisConfig and make api request; function retrieves top five abo article
    with most orders
    :param date_from:
    :param date_to:
    :return: dataframe with top five abo articles with most orders and their PIs
    """
    # build analysisConfig
    analysisConfig = {
        "hideFooters": [1],
        "startTime": date_from,
        "stopTime": date_to,
        "analysisFilter": {
            "filterRules": [{
                "objectTitle": "Wall - Status",
                "comparator": "=",
                "filter": "paid"
            }]
        },
        "analysisObjects": [{
            "title": "Seiten",
            "rowLimit": 5
        }],
        "metrics": [{
            "title": "Anzahl Bestellungen mit Seitenbezug",
            "sortOrder": "desc"
        }, {
            "title": "Page Impressions"
        }
        ]}

    # request data
    data = api.wt_get_data(analysisConfig)

    # parse data
    data = data["result"]["analysisData"]
    df = pd.DataFrame(data)
    col_names = ["url", "bestellungen", "pis_schranke"]
    df.columns = col_names

    # create date and rank
    df["date"] = pd.to_datetime(date_from)
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

    logging.info(str(datetime.now()) + ' topartikel bestellungen imported from webtrekk for '
                 + date_from)

    return df


def get_data_top_reg(date_from=api.get_datetime_yesterday(),
                     date_to=api.get_datetime_yesterday()):
    """
    function to build anlysisConfig and make api request; function retrieves top five articles,
    which make the most registrations
    :param date_from:
    :param date_to:
    :return: dataframe with top five regigster articles with most registrations and their PIs
    """
    # build analysisConfig
    analysisConfig = {
        "hideFooters": [1],
        "startTime": date_from,
        "stopTime": date_to,
        "analysisObjects": [{
            "title": "Registrierung SSO",
            "rowLimit": 5
        }],
        "metrics": [{
            "title": "Anzahl Registrierungen Schranke",
            "sortOrder": "desc"
        }
        ]}

    # request data
    data = api.wt_get_data(analysisConfig)

    # parse data
    data = data["result"]["analysisData"]
    df = pd.DataFrame(data)
    col_names = ["url", "registrierungen"]
    df.columns = col_names

    # get rid of https in url
    df.url = df.url.str.partition('://')[2]

    # get PIs of most top five register article (all at once)
    df_pis = get_pis_of_url(df.url)

    # join registrierungen and their PIs
    df = df.join(df_pis.set_index('url'), on="url", how="left")

    # create date and rank
    df["date"] = pd.to_datetime(date_from)
    df["rank"] = range(1, 1+len(df))

    # get title
    df["title"] = df.url.apply(lambda x: get_title_from_xml(x))

    # rearrange order of colummns
    cols = df.columns.tolist()
    cols = cols[-3:] + cols[:-3]
    df = df[cols]

    # convert to numeric columns
    convert_cols = df.columns.drop(['date', 'rank', 'title', 'url'])
    df[convert_cols] = df[convert_cols].apply(pd.to_numeric, errors='coerce')

    logging.info(str(datetime.now()) + ' topartikel registrierungen imported from webtrekk for '
                 + date_from)

    return df


def get_pis_of_url(url,
                   date_from=api.get_datetime_yesterday(),
                   date_to=api.get_datetime_yesterday()):
    """
    this function retrieves the PIs of a given url on a specific day
    :param url: vector of five urls in order to only make one api call
    :param date_from:
    :param date_to:
    :return: the PIs for all five given urls as a dataframe
    """
    # build analysisConfig
    analysisConfig = {
        "hideFooters": [1],
        "startTime": date_from,
        "stopTime": date_to,
        "analysisObjects": [{
            "title": "Seiten",
            "rowLimit": 5
        }],
        "analysisFilter": {
            "filterRules": [{
                "objectTitle": "Wall - Status",
                "comparator": "=",
                "filter": "register"
            }, {
                "link": "and",
                "objectTitle": "Seiten",
                "comparator": "=",
                "filter": "*"+url[0]+"*"
            }, {
                "link": "or",
                "objectTitle": "Seiten",
                "comparator": "=",
                "filter": "*"+url[1]+"*"
            }, {
                "link": "or",
                "objectTitle": "Seiten",
                "comparator": "=",
                "filter": "*"+url[2]+"*"
            }, {
                "link": "or",
                "objectTitle": "Seiten",
                "comparator": "=",
                "filter": "*"+url[3]+"*"
            }, {
                "link": "or",
                "objectTitle": "Seiten",
                "comparator": "=",
                "filter": "*"+url[4]+"*"
            }
            ]
        },
        "metrics": [{
            "title": "Page Impressions",
            "sortOrder": "desc"
        }
        ]}

    # request data
    data = api.wt_get_data(analysisConfig)

    # parse data
    data = data["result"]["analysisData"]
    df_pis = pd.DataFrame(data)
    col_names = ["url", "pis_schranke"]
    df_pis.columns = col_names

    # display only url instead of content id
    df_pis.url = df_pis.url.str.partition('|')[2]

    return df_pis
