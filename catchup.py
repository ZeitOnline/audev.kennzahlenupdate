"""
Created by: humberg
Date:       08.06.20

This module finds missing dates in bigquery and adds the missing data (if api didn't work)
"""
from src import bigquery, usercentric, topartikel, referrertraffic, ivw, adimpressions
import logging
from datetime import datetime

# initialize log file
logging.basicConfig(filename="kennzahlenupdate_catchup.log", level=logging.INFO)

# set min_date for which the tables should be filled
min_date = '2020-05-18'

# get list of all tables in bigquery dataset 'kennzahlenupdate'
tables = bigquery.get_tables_list("kennzahlenupdate")

# remove ivw from list
tables.remove("kennzahlenupdate.ivw_visits")

# loop through all tables
for table in tables:

    # get distinct dates from table
    missing_dates = bigquery.get_missing_dates(table, min_date)

    # add missing data for each missing day
    for date in missing_dates:

        if table == "kennzahlenupdate.usercentric":
            df = usercentric.get_data(date_from=date, date_to=date)
        elif table == "kennzahlenupdate.referrertraffic":
            df = referrertraffic.get_data(date_from=date, date_to=date)
        elif table == "kennzahlenupdate.adimpressions":
            df = adimpressions.get_data(date_from=date, date_to=date)
        elif table == "kennzahlenupdate.topartikel":
            df = topartikel.get_data_top(date_from=date, date_to=date)
        elif table == "kennzahlenupdate.topartikel_bestellungen":
            df = topartikel.get_data_top_best(date_from=date, date_to=date)
        elif table == "kennzahlenupdate.topartikel_registrierungen":
            df = topartikel.get_data_top_reg(date_from=date, date_to=date)
        else:
            logging.info(str(datetime.now()) + table + " not listed in if statements")
        bigquery.upload_data(df, table)
        logging.info(str(datetime.now()) + " " + date + ' data added to ' + table)

