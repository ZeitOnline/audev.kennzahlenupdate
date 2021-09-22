"""
Created by: humberg
Date:       30.04.20

This module contains all functions for data upload to bigquery. Note: before upload, last 7 days of
data are deleted, to avoid duplicates. This is necessary in order to update recent days.
"""
import os
import logging
from datetime import date, timedelta

import pandas as pd
import numpy as np
import google.cloud.bigquery as gcbq
from google.cloud.exceptions import NotFound

from src import api


def upload_data(df, table_id):
    """
    this function uploads a dataframe to bigquery
    :param table_id: dataset_id.table_id
    :param df: pandas dataframe
    :return returns True if upload was successful
    """
    # initialize client
    client = gcbq.Client(project="audev-217815")

    # get date of last entry in bigquery
    last_date = check_date(client, table_id)

    # define job_config for upload
    job_config = gcbq.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema_update_options = [
            gcbq.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        ]
    )

    # upload dataframe to bigquery table, but only if there are no entries of today already
    if last_date != df.date.iloc[-1]:
        client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
        logging.info('data uploaded to ' + table_id + '...')
        return True
    else:
        logging.info('no data to upload to ' + table_id)




def delete_recent_ivw(client):
    """
    Note: before upload, last 6 days of IVW
    data are deleted, to avoid duplicates. This is necessary in order to update recent days.
    :return:
    """

    # check if table exists
    try:
        client.get_table("project_kennzahlenupdate.ivw_visits")
        table_exists = True
    except NotFound:
        table_exists = False

    # if table exist, delete last 6 entries; last 6 entries are yesterday-6, so today-7
    if table_exists:
        last_six_days = str(date.today() - timedelta(days=7))
        sql = "DELETE FROM project_kennzahlenupdate.ivw_visits WHERE date >= '" + last_six_days + "'"
        client.query(sql)
    logging.info('last six days deleted in project_kennzahlenupdate.ivw_visits..')


def check_date(client, table_id):
    """
    checks last date in table engagement_score.monitor, in order to avoid uploading same day again
    :param client: bigquery client
    :param table_id: which table_id to check
    :return: date of last entry
    """
    # check if table exists
    try:
        client.get_table(table_id)
        table_exists = True
    except NotFound:
        table_exists = False

    # if table exist, get first ordered row in order to get max date
    if table_exists:
        sql = "SELECT MAX(date) as date FROM " + table_id
        last_date = client.query(sql).to_dataframe()
        last_date = last_date.date.values[0]
        return last_date
    else:
        return '1970-01-01'


def get_tables_list(dataset_id):
    """
    this function lists all tables for given dataset
    :param dataset_id: id of dataset
    :return: list with table names
    """
    # initialize client
    client = gcbq.Client(project="audev-217815")

    # get list of tables
    tables = client.list_tables(dataset_id)
    lst_tables = []
    for table in tables:
        lst_tables = lst_tables + [table.dataset_id + "." + table.table_id]

    return lst_tables


def get_missing_dates(table, min_date):
    """
    get missing dates of table (check distinct dates because of multiple entries in topartikel)
    :param table: dataset_id.table_idn as string
    :param min_date: minimum date from which on distinct dates should be checked (until today)
                    format is 'YYYY-MM-DD'
    :return: list of missing dates for specific table
    """
    # initialize client
    client = gcbq.Client(project="audev-217815")

    # check if table exists
    try:
        client.get_table(table)
        table_exists = True
    except NotFound:
        table_exists = False

    # if table exist, get distinct dates and check which dates are missing from min_date until now
    if table_exists:

        # get distinct dates
        sql = "SELECT DISTINCT(date) FROM " + table + " ORDER BY date asc"
        df = client.query(sql).to_dataframe()
        df.date = df.date.dt.strftime("%Y-%m-%d")
        dates = pd.date_range(start=min_date, end=api.get_datetime_yesterday())
        dates = dates.strftime("%Y-%m-%d").tolist()

        # make sure to check only entries which are greater than min_date
        df_check = df[df["date"] >= min_date]

        # remove existing dates
        for date in df_check.date:
            dates.remove(date)

        # log if there are missing dates
        if len(dates) == 0:
            logging.info('########## no missing dates in ' + table + ' ###########')
        else:
            logging.info('########## missing dates in ' + table + ' ###########')

        # return missing dates
        return dates
    else:
        logging.info(table + " doesn't exist")


def get_data(sql):
    """
    retrieves data for given sql sequence
    :param: sql: sql sequence
    :return: dataframe with data
    """
    # initialize client
    client = gcbq.Client(project="audev-217815")

    df = client.query(sql).to_dataframe()
    df.date = df.date.dt.strftime("%Y-%m-%d")

    return df


def update_data(df, table_id):
    """
    updates recent numbers for ivw_visits
    :param df: dataframe with data to update for each day
    :param table_id: so far only project_kennzahlenupdate.ivw_visits
    :return:
    """
    # initialize client
    client = gcbq.Client(project="audev-217815")

    # reduce df
    rel_col = ['date', 'zon_stationaer', 'zon_mobile', 'zon_android', 'zon_ios']
    df = df[rel_col]

    # erase falsy columns that contain zeros
    df = df.replace(0, np.nan).dropna(axis=0, how='any')

    if table_id == "project_kennzahlenupdate.ivw_visits":
        for cur_date in df.date:
            df_cur = df.loc[df.date == cur_date]
            dml_statement = (
                "UPDATE " + table_id + " "
                "SET zon_stationaer=" + str(df_cur.iloc[0].zon_stationaer) + ", "
                "zon_mobile=" + str(df_cur.iloc[0].zon_mobile) + ", "
                "zon_android=" + str(df_cur.iloc[0].zon_android) + ", "
                "zon_ios=" + str(df_cur.iloc[0].zon_ios) + " "
                "WHERE date='" + str(cur_date) + "'"
             )
            query_job = client.query(dml_statement)
            query_job.result()
            logging.info('updated ' + str(cur_date) + " in " + table_id)
    else:
        logging.info('WARNING update on wrong dataset ' + table_id)
