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
from dateutil.relativedelta import relativedelta

df = forecast3.get_data()

# train models and get predictions
warnings.filterwarnings('ignore')
pred_stat = forecast3.arima_model(df.zon_stationaer, arima_order=(6,0,6), horizon=31)
pred_mobile = forecast3.arima_model(df.zon_mobile, arima_order=(6,0,6), horizon=31)
pred_android = forecast3.arima_model(df.zon_android, arima_order=(6,0,6), horizon=31)
pred_ios = forecast3.arima_model(df.zon_ios, arima_order=(6,0,6), horizon=31)

# get real values from past days this month
cur_month = datetime.today().strftime("%Y-%m")
df_real = df[df.date >= cur_month]
df_real = pd.DataFrame({"date": [datetime.today().strftime('%Y-%m-%d') for x in range(len(df_real))],
                        "date_to_predict": df_real.date,
                        'pred_stationaer': df_real.zon_stationaer,
                        'pred_mobile': df_real.zon_mobile,
                        'pred_android': df_real.zon_android,
                        'pred_ios': df_real.zon_ios
                        })


# create data frame with predictions
df_fc = pd.DataFrame({'date': [datetime.today().strftime('%Y-%m-%d') for x in range(31)],
                      'date_to_predict': pd.date_range(datetime.today().strftime('%Y-%m-%d'),
                                                       periods=31),
                      'pred_stationaer': round(pred_stat, 2),
                      'pred_mobile': round(pred_mobile, 2),
                      'pred_android': round(pred_android, 2),
                      'pred_ios': round(pred_ios, 2)})
# convert date
df_fc.date = pd.to_datetime(df_fc.date, format="%Y-%m-%d")
df_real.date = pd.to_datetime(df_real.date, format="%Y-%m-%d")

# get only forecast values for this month
next_month = datetime.today() + relativedelta(months=+1)
next_month = next_month.strftime("%Y-%m")
df_fc = df_fc[df_fc.date_to_predict < next_month]

# append past real values with future forecast values in order to sum correctly in visualisation
df_upload = df_real.append(df_fc)

# upload forecast to bigquery
bigquery.upload_data(df_upload, "kennzahlenupdate.ivw_visits_predictions")
