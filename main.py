"""
Created by: humberg
Date:       29.04.20

This module does...
"""
from src import ivw, referrertraffic, usercentric, bigquery
import logging

# initialize log file
logging.basicConfig(filename="kennzahlenupdate.log", level=logging.INFO)

# handle IVW data
df_advanced, df_lifeview = ivw.get_data()
df = ivw.parse_data(df_advanced, df_lifeview)
bigquery.upload_data(df, 'kennzahlenupdate.ivw_visits')

# handle referrertraffic data
df = referrertraffic.get_data()
bigquery.upload_data(df, 'kennzahlenupdate.referrertraffic_visits')

# handle usercentric data
df = usercentric.get_data()
bigquery.upload_data(df, 'kennzahlenupdate.usercentric')

