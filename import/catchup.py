"""
Created by: humberg
Date:       08.06.20

This module finds missing dates in bigquery and adds the missing data (if api didn't work)
"""
import os
import sys
import logging
from dotenv import load_dotenv

load_dotenv()

# add parent directory to sys.path in order to import modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from src import bigquery, usercentric, topartikel, referrertraffic, \
    ivw, entryservice


# initialize logging
logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)
logging.getLogger(__name__)

# check if any arguments are set by user, otherwise set min_date for which
# the tables should be filled
if len(sys.argv) - 1 > 0:
    min_date = sys.argv[1]
else:
    min_date = "2020-07-01"

# get list of all tables in bigquery dataset 'project_kennzahlenupdate'
tables = bigquery.get_tables_list("project_kennzahlenupdate")

# remove topartikel (history not needed)
tables.remove("project_kennzahlenupdate.topartikel")
tables.remove("project_kennzahlenupdate.topartikel_bestellungen")
tables.remove("project_kennzahlenupdate.topartikel_registrierungen")
tables.remove("project_kennzahlenupdate.ivw_visits_predictions")
tables.remove("project_kennzahlenupdate.planzahlen")

# loop through all tables
for table in tables:

    # get distinct dates from table
    missing_dates = bigquery.get_missing_dates(table, min_date)

    # add missing data for each missing day
    for date in missing_dates:

        if table == "project_kennzahlenupdate.usercentric":
            df = usercentric.get_data(date_from=date, date_to=date)
        elif table == "project_kennzahlenupdate.referrertraffic":
            df = referrertraffic.get_data(date_from=date, date_to=date)
        elif table == "project_kennzahlenupdate.topartikel":
            df = topartikel.get_data_top(date_from=date, date_to=date)
        elif table == "project_kennzahlenupdate.topartikel_bestellungen":
            df = topartikel.get_data_top_best(date_from=date, date_to=date)
        elif table == "project_kennzahlenupdate.topartikel_registrierungen":
            df = topartikel.get_data_top_reg(date_from=date, date_to=date)
        elif table == "project_kennzahlenupdate.ivw_visits":
            df_advanced, df_lifeview = ivw.get_data(date_from=date, date_to=date)
            df = ivw.parse_data(df_advanced)
        elif table == "project_kennzahlenupdate.entryservice_registrierungen":
            df = entryservice.get_data_reg(date_from=date, date_to=date)
        elif table == "project_kennzahlenupdate.entryservice_logins":
            df = entryservice.get_data_login(date_from=date, date_to=date)
        else:
            logging.info(table + " not listed in if statements")
        bigquery.upload_data(df, table)

