"""
Created by: humberg
Date:       29.04.20

This module is the main module. It calls all necessary functions in order to get the necessary data
and upload it to bigquery
"""
from src import ivw, referrertraffic, usercentric, adimpressions, bigquery, error, topartikel, \
    entryservice
import logging
import traceback

# initialize log file
logging.basicConfig(filename="kennzahlenupdate.log", level=logging.INFO)

# handle IVW data
try:
    df_advanced, df_lifeview = ivw.get_data()
    df = ivw.parse_data(df_advanced, df_lifeview)
    bigquery.upload_data(df, 'kennzahlenupdate.ivw_visits')
except Exception:
    error.send_error_slack(traceback.format_exc())

# handle referrertraffic data
try:
    df = referrertraffic.get_data()
    bigquery.upload_data(df, 'kennzahlenupdate.referrertraffic')
except Exception:
    error.send_error_slack(traceback.format_exc())

# handle usercentric data
try:
    df = usercentric.get_data()
    bigquery.upload_data(df, 'kennzahlenupdate.usercentric')
except Exception:
    error.send_error_slack(traceback.format_exc())

# handle adimpressions data
try:
    df = adimpressions.get_data()
    bigquery.upload_data(df, 'kennzahlenupdate.adimpressions')
except Exception:
    error.send_error_slack(traceback.format_exc())

# handle topartikel (reichweite) data
try:
    df = topartikel.get_data_top()
    bigquery.upload_data(df, 'kennzahlenupdate.topartikel')
except Exception:
    error.send_error_slack(traceback.format_exc())

# handle topartikel bestellungen data
try:
    df = topartikel.get_data_top_best()
    bigquery.upload_data(df, 'kennzahlenupdate.topartikel_bestellungen')
except Exception:
    error.send_error_slack(traceback.format_exc())

# handle topartikel registrierungen data
try:
    df = topartikel.get_data_top_reg()
    bigquery.upload_data(df, 'kennzahlenupdate.topartikel_registrierungen')
except Exception:
    error.send_error_slack(traceback.format_exc())

# handle registrierungen entry service data
try:
    df = entryservice.get_data_reg()
    bigquery.upload_data(df, 'kennzahlenupdate.entryservice_registrierungen')
except Exception:
    error.send_error_slack(traceback.format_exc())

# handle logins entry service data
try:
    df = entryservice.get_data_login()
    bigquery.upload_data(df, 'kennzahlenupdate.entryservice_logins')
except Exception:
    error.send_error_slack(traceback.format_exc())
