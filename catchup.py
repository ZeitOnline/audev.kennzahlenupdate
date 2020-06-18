"""
Created by: humberg
Date:       08.06.20

This module finds missing dates in bigquery and adds the missing data (if api didn't work)
"""
from src import bigquery, usercentric, topartikel, referrertraffic, ivw, adimpressions
import logging
from datetime import datetime
import sys

# initialize log file
logging.basicConfig(filename="kennzahlenupdate_catchup.log", level=logging.INFO)

# check if any arguments are set by user, otherwise set min_date for which the tables should be
# filled
if len(sys.argv) - 1 > 0:
    min_date = sys.argv[1]
else:
    min_date = "2020-05-10"

# get list of all tables in bigquery dataset 'kennzahlenupdate'
tables = bigquery.get_tables_list("kennzahlenupdate")

# remove topartikel (history not needed)
tables.remove("kennzahlenupdate.topartikel")
tables.remove("kennzahlenupdate.topartikel_bestellungen")
tables.remove("kennzahlenupdate.topartikel_registrierungen")

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
        elif table == "kennzahlenupdate.ivw_visits":
            df_advanced, df_lifeview = ivw.get_data(date_from=date, date_to=date)
            df = ivw.parse_data(df_advanced)
        else:
            logging.info(str(datetime.now()) + table + " not listed in if statements")
        bigquery.upload_data(df, table)

