"""
Created by: humberg
Date:       30.04.20

This module contains all functions for data upload to bigquery. Note: before upload, last 7 days of
data are deleted, to avoid duplicates. This is necessary in order to update recent days.
"""
import os
import google.cloud.bigquery as gcbq
from google.cloud.exceptions import NotFound
from datetime import datetime
import logging

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
        logging.info(str(datetime.now()) + ' no data to upload ' + table_id)




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

    # if table exist, delete last 6 entries
    if table_exists:
        sql = """ DELETE
                  FROM kennzahlenupdate.ivw_visits
                  ORDER BY date desc
                  LIMIT 6
              """
        client.query(sql)
    logging.info(str(datetime.now()) + ' last seven days deleted in kennzahlenupdate.ivw_visits..')


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
        sql = "SELECT date from " + table_id + " ORDER BY date desc LIMIT 1"
        last_date = client.query(sql).to_dataframe()
        last_date = last_date.date.values[0]
        return last_date
    else:
        return '1970-01-01'