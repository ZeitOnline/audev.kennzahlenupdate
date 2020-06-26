"""
Created by: humberg
Date:       30.04.20

This module contains all functions for data upload to bigquery. Note: before upload, last 7 days of
data are deleted, to avoid duplicates. This is necessary in order to update recent days.
"""
import os
import google.cloud.bigquery as gcbq
from google.cloud.exceptions import NotFound
from datetime import datetime, date, timedelta
import logging
from src import api
import pandas as pd

# setup authentication for bigquery via JSON key file
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "audev-bigquery-default.json"


def upload_data(df, table_id):
    """
    this function uploads a dataframe to bigquery
    :param table_id: dataset_id.table_id
    :param df: pandas dataframe
    """
    # initialize client
    client = gcbq.Client()

    # get date of last entry in bigquery
    last_date = check_date(client, table_id)

    # if ivw data, delete recent days first
    if (table_id=="kennzahlenupdate.ivw_visits" and len(df)==7 and last_date != df.date.iloc[-1]):
        delete_recent_ivw(client)

    # define job_config for upload
    job_config = gcbq.LoadJobConfig(
        write_disposition="WRITE_APPEND",
    )

    # upload dataframe to bigquery table, but only if there are no entries of today already
    if last_date != df.date.iloc[-1]:
        client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
        logging.info(str(datetime.now()) + ' data uploaded to ' + table_id + '...')
    else:
        logging.info(str(datetime.now()) + ' no data to upload to ' + table_id)




def delete_recent_ivw(client):
    """
    Note: before upload, last 6 days of IVW
    data are deleted, to avoid duplicates. This is necessary in order to update recent days.
    :return:
    """

    # check if table exists
    try:
        client.get_table("kennzahlenupdate.ivw_visits")
        table_exists = True
    except NotFound:
        table_exists = False

    # if table exist, delete last 6 entries; last 6 entries are yesterday-6, so today-7
    if table_exists:
        last_six_days = str(date.today() - timedelta(days=7))
        sql = "DELETE FROM kennzahlenupdate.ivw_visits WHERE date >= '" + last_six_days + "'"
        client.query(sql)
    logging.info(str(datetime.now()) + ' last six days deleted in kennzahlenupdate.ivw_visits..')


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
    client = gcbq.Client()

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
    client = gcbq.Client()

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
            logging.info(
                str(datetime.now()) + ' ########## no missing dates in ' + table + ' ###########')
        else:
            logging.info(
                str(datetime.now()) + ' ########## missing dates in ' + table + ' ###########')

        # return missing dates
        return dates
    else:
        logging.info(str(datetime.now()) + table + " doesn't exist")


def get_data(sql):
    """
    retrieves data for given sql sequence
    :param: sql: sql sequence
    :return: dataframe with data
    """
    # initialize client
    client = gcbq.Client()

    df = client.query(sql).to_dataframe()
    df.date = df.date.dt.strftime("%Y-%m-%d")

    return df