"""
Created by: humberg
Date:       24.06.20

This module contains all functions to upload Planzahlen to bigquery
- planzahlen are imported from csv
- append to existing bigquery table kennzahlenupdate.planzahlen
- this has to be done once a year
"""

import pandas as pd

from src import bigquery

df = pd.read_csv("/Users/humberg/Downloads/planzahlen_zon.csv", encoding='ISO-8859-1', delimiter=";")

# drop unnecessary columns
df = df[df.columns.drop(["year", "month"])]

# convert date
df.date = pd.to_datetime(df.date, format="%Y-%m")

# upload to bigquery
bigquery.upload_data(df, 'kennzahlenupdate.planzahlen')