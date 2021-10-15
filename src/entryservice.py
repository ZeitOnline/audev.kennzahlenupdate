"""
Created by: humberg
Date:       17.06.20

This module contains all necessary functions to import and upload daily registrierungen and logins
per entry service
"""
import logging

import pandas as pd

from src import api


def get_data_reg(date_from=api.get_datetime_yesterday(),
                 date_to=api.get_datetime_yesterday()):
    """
    function to build anlysisConfig and make api request for registrations on entry service level
    :param date_from:
    :param date_to:
    :return: dataframe with relevant information
    """
    # build analysisConfig
    analysisConfig = {
        "hideFooters": [1],
        "startTime": date_from,
        "stopTime": date_to,
        "rowLimit": 10000,
        "analysisObjects": [{
            "title": "cb9 - Registrierung SSO - entry service"
        }],
        "metrics": [{
            "title": "Anzahl cb7 - Registrierung SSO"
        }, {
            "title": "Anzahl cb9 - Registrierung SSO – entry service"
        }]
    }

    # request data
    data = api.wt_get_data(analysisConfig)

    # parse data
    data = data["result"]["analysisData"]
    df = pd.DataFrame(data)
    col_names = ["entry_service", "reg_sso", "reg_sso_entry_service"]
    df.columns = col_names

    # create date
    df["date"] = pd.to_datetime(date_from)

    convert_cols = df.columns.drop(['date', 'entry_service'])
    df[convert_cols] = df[convert_cols].apply(pd.to_numeric, errors='coerce')

    # rearrange order of colummns
    cols = df.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    df = df[cols]

    logging.info('entry service registration imported from webtrekk for '
                 + date_from)

    return df


def get_data_login(date_from=api.get_datetime_yesterday(),
                   date_to=api.get_datetime_yesterday()):
    """
    function to build anlysisConfig and make api request for logins on entry service level
    :param date_from:
    :param date_to:
    :return: dataframe with relevant information
    """
    # build analysisConfig
    analysisConfig = {
        "hideFooters": [1],
        "startTime": date_from,
        "stopTime": date_to,
        "rowLimit": 10000,
        "analysisObjects": [{
            "title": "cb13 - Login SSO - entry service"
        }],
        "metrics": [{
            "title": "Anzahl cb12 - Login SSO"
        }, {
            "title": "Anzahl cb13 - Login SSO - entry service"
        }]
    }

    # request data
    data = api.wt_get_data(analysisConfig)

    # parse data
    data = data["result"]["analysisData"]
    df = pd.DataFrame(data)
    col_names = ["entry_service", "login_sso", "login_sso_entry_service"]
    df.columns = col_names

    # create date
    df["date"] = pd.to_datetime(date_from)

    convert_cols = df.columns.drop(['date', 'entry_service'])
    df[convert_cols] = df[convert_cols].apply(pd.to_numeric, errors='coerce')

    # rearrange order of colummns
    cols = df.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    df = df[cols]

    logging.info('entry service login imported from webtrekk for '
                 + date_from)

    return df
