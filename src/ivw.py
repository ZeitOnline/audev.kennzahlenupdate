"""
Created by: humberg
Date:       29.04.20

This module contains all functions to import IVW data from reshin dashboard
"""
from src import api
import pandas as pd

def get_data():
    """
    function to import IVW data
    :return: advanced and lifeview data
    """

    # get advanced data of last 7 days
    df_advanced = get_data_aggr(days_back=7, aggr="day")

    # get lifeview data of yesterday
    df_lifeview = get_data_aggr(days_back=1, aggr="hour")

    return df_advanced, df_lifeview


def get_data_aggr(days_back, aggr):
    """
    wrapper function to import advanced or lifeview data
    :param days_back: horizon for day:7 and hour:1
    :param aggr:advanced (aggr=day) or lifeview (aggr=hour) data
    :return: respective json
    """
    # get body
    body = api.f3_body(aggr, horizon=days_back)
    # get data from api
    res = api.f3_call(body)
    return res


def parse_data(df_advanced, df_lifeview):
    """
    function to parse json data
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


