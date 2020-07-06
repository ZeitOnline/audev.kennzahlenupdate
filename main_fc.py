"""
Created by: humberg
Date:       01.07.20

This module runs actual forecasting functions
"""

from src import forecast3
import pandas as pd
from datetime import datetime
import warnings
from src import bigquery
from numpy import round

df = forecast3.get_data()

warnings.filterwarnings('ignore')
pred_stat = forecast3.arima_model(df.zon_stationaer, arima_order=(6,0,6), horizon=31)
pred_mobile = forecast3.arima_model(df.zon_mobile, arima_order=(6,0,6), horizon=31)
pred_android = forecast3.arima_model(df.zon_android, arima_order=(6,0,6), horizon=31)
pred_ios = forecast3.arima_model(df.zon_ios, arima_order=(6,0,6), horizon=31)



# create data frame
df_fc = pd.DataFrame({'date': [datetime.today().strftime('%Y-%m-%d') for x in range(31)],
                      'date_to_predict': pd.date_range(datetime.today().strftime('%Y-%m-%d'),
                                                       periods=31),
                      'pred_stationaer': round(pred_stat, 2),
                      'pred_mobile': round(pred_mobile, 2),
                      'pred_android': round(pred_android, 2),
                      'pred_ios': round(pred_ios, 2)})
# convert date
df_fc.date = pd.to_datetime(df_fc.date, format="%Y-%m-%d")

# upload forecast to bigquery
bigquery.upload_data(df_fc, "kennzahlenupdate.ivw_visits_predictions")
