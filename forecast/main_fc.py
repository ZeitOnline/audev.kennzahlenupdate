"""
Created by: humberg
Date:       01.07.20

This module runs actual forecasting functions
"""
import os
import sys
import warnings
import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
from statsmodels.tools.sm_exceptions import ConvergenceWarning

import ray
import pandas as pd
from numpy import round

# add parent directory to sys.path in order to import modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from src import forecast3, bigquery

# initialize log file
logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)
logging.getLogger(__name__)


def run_forecast():
    ray.init()

    # get data
    df = forecast3.get_data()

    # filter warnings
    warnings.filterwarnings('ignore')
    warnings.simplefilter('ignore', ConvergenceWarning)

    # train models and get predictions in parallel
    @ray.remote
    def func_pred_stat():
        return forecast3.arima_model(df.zon_stationaer, dataset_name='stationaer')

    @ray.remote
    def func_pred_mobile():
        return forecast3.arima_model(df.zon_mobile, dataset_name='mobile')

    @ray.remote
    def func_pred_android():
        return forecast3.arima_model(df.zon_android, dataset_name='android')

    @ray.remote
    def func_pred_ios():
        return forecast3.arima_model(df.zon_ios, dataset_name='ios')

    res_func_stat = func_pred_stat.remote()
    res_func_mobile = func_pred_mobile.remote()
    res_func_android = func_pred_android.remote()
    res_func_ios = func_pred_ios.remote()

    pred_stat, pred_mobile, pred_android, pred_ios = ray.get([res_func_stat, res_func_mobile,
                                                              res_func_android, res_func_ios])

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


if __name__ == "__main__":
    run_forecast()



