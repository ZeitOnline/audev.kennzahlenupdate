"""
Created by: humberg
Date:       29.04.20

This module is the main module. It calls all necessary functions in order to get the necessary data
and upload it to bigquery
"""

import os
import sys
import logging
import traceback

# add parent directory to sys.path in order to import modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from src import ivw, referrertraffic, usercentric, \
    bigquery, error, topartikel, \
    entryservice

# initialize logging
logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)
logging.getLogger(__name__)


def run_ku():
    # handle IVW data
    try:
        df_advanced, df_lifeview = ivw.get_data()
        df_updates = ivw.parse_data(df_advanced)
        df_new = ivw.parse_data(df_lifeview)
        df_new = df_new.iloc[:1]

        result = bigquery.upload_data(df_new, 'project_kennzahlenupdate.ivw_visits')
        # only update rows, if upload of new data was successful
        if result:
            bigquery.update_data(df_updates, 'project_kennzahlenupdate.ivw_visits')
    except Exception:
        error.send_error_slack(traceback.format_exc())
        logging.warning(traceback.format_exc())

    # handle referrertraffic data
    try:
        df = referrertraffic.get_data()
        bigquery.upload_data(df, 'project_kennzahlenupdate.referrertraffic')
    except Exception:
        error.send_error_slack(traceback.format_exc())
        logging.warning(traceback.format_exc())

    # handle usercentric data
    try:
        df = usercentric.get_data()
        bigquery.upload_data(df, 'project_kennzahlenupdate.usercentric')
    except Exception:
        error.send_error_slack(traceback.format_exc())
        logging.warning(traceback.format_exc())

    # handle topartikel (reichweite) data
    try:
        df = topartikel.get_data_top()
        bigquery.upload_data(df, 'project_kennzahlenupdate.topartikel')
    except Exception:
        error.send_error_slack(traceback.format_exc())
        logging.warning(traceback.format_exc())

    # handle topartikel bestellungen data
    try:
        df = topartikel.get_data_top_best()
        bigquery.upload_data(df, 'project_kennzahlenupdate.topartikel_bestellungen')
    except Exception:
        error.send_error_slack(traceback.format_exc())
        logging.warning(traceback.format_exc())

    # handle topartikel registrierungen data
    try:
        df = topartikel.get_data_top_reg()
        bigquery.upload_data(df, 'project_kennzahlenupdate.topartikel_registrierungen')
    except Exception:
        error.send_error_slack(traceback.format_exc())
        logging.warning(traceback.format_exc())

    # handle registrierungen entry service data
    try:
        df = entryservice.get_data_reg()
        bigquery.upload_data(df, 'project_kennzahlenupdate.entryservice_registrierungen')
    except Exception:
        error.send_error_slack(traceback.format_exc())
        logging.warning(traceback.format_exc())

    # handle logins entry service data
    try:
        df = entryservice.get_data_login()
        bigquery.upload_data(df, 'project_kennzahlenupdate.entryservice_logins')
    except Exception:
        error.send_error_slack(traceback.format_exc())
        logging.warning(traceback.format_exc())


if __name__ == "__main__":
    run_ku()
