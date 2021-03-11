"""
Created by: humberg
Date:       29.04.20

This module contains all functions to import IVW data from reshin dashboard
"""
import logging

import pandas as pd

from src import api


def get_data(date_from=None, date_to=None):
    """
    function to import IVW data; if date_from and date_to exist, only the specific days will be
    processed in the advanced data (this is for the catchup job), otherwise the normal process of
    last seven days is handled
    :param: date_from: (optional)
    :param: date_to: (optional)
    :return: advanced and lifeview data
    """

    if date_from != None:
        # get data of specific day (missing day for catchup job)
        df_advanced = get_data_aggr(days_back=None, aggr="day",
                                    date_from=date_from, date_to=date_to)
        df_lifeview = dict()
    else:
        # get advanced data of last 7 days
        df_advanced = get_data_aggr(days_back=7, aggr="day")

        # get lifeview data of yesterday
        df_lifeview = get_data_aggr(days_back=1, aggr="hour")

    logging.info('ivw imported from f3 api for ' + date_from)

    return df_advanced, df_lifeview


def get_data_aggr(days_back, aggr,
                  date_from=None, date_to=None):
    """
    wrapper function to import advanced or lifeview data
    :param days_back: horizon for day:7 and hour:1; can be None
    :param aggr:advanced (aggr=day) or lifeview (aggr=hour) data
    :param: date_from: (optional)
    :param: date_to: (optional)
    :return: respective json
    """
    # get body
    body = api.f3_body(aggr, horizon=days_back,
                       date_from=date_from, date_to=date_to)
    # get data from api
    res = api.f3_call(body)
    return res


def parse_data(df_advanced, df_lifeview=None):
    """
    function to parse json data
    :param: df_advanced: json with rohdaten
    :param: df_lifeview: (optional) data with hochrechnungen
    :return: parsed data
    """

    # parse advanced data
    df_advanced = df_advanced["result"]["aggregations"]["2"]["buckets"]
    df = pd.DataFrame(columns=['date', 'zon_stationaer', 'zon_mobile', 'zon_android', 'zon_ios',
                               'zett_android', 'zett_ios'])

    for i in range(len(df_advanced)):
        df_cur = df_advanced[i]
        date = df_cur["key_as_string"]
        df = df.append({'date': date[0:date.find("T")],
                        'zon_stationaer': df_cur["3"]["buckets"]["stationaer"]["1"]["value"],
                        'zon_mobile': df_cur["3"]["buckets"]["mobile"]["1"]["value"],
                        'zon_android': df_cur["3"]["buckets"]["Android App"]["1"]["value"],
                        'zon_ios': df_cur["3"]["buckets"]["iOS App"]["1"]["value"],
                        'zett_android': df_cur["3"]["buckets"]["ZE.TT Android App"]["1"]["value"],
                        'zett_ios': df_cur["3"]["buckets"]["ZE.TT iOS App"]["1"]["value"]},
                       ignore_index=True)

    if df_lifeview != None:
        # parse lifeview data
        df_lifeview=df_lifeview["result"]["aggregations"]["2"]["buckets"][0]
        date = df_lifeview["key_as_string"]
        df = df.append({'date': date[0:date.find("T")],
                        'zon_stationaer': df_lifeview["3"]["buckets"]["stationaer"]["1"]["value"],
                        'zon_mobile': df_lifeview["3"]["buckets"]["mobile"]["1"]["value"],
                        'zon_android': df_lifeview["3"]["buckets"]["Android App"]["1"]["value"],
                        'zon_ios': df_lifeview["3"]["buckets"]["iOS App"]["1"]["value"],
                        'zett_android': df_lifeview["3"]["buckets"]["ZE.TT Android App"]["1"]["value"],
                        'zett_ios': df_lifeview["3"]["buckets"]["ZE.TT iOS App"]["1"]["value"]},
                       ignore_index=True)

    # convert date to datetime object
    df.date = pd.to_datetime(df.date, format="%Y-%m-%d")

    return df


