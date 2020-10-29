"""
Created by: humberg
Date:       01.07.20

This module contains all functions neccessary to make ivw visit forecast; this is done using a ARIMA
model
"""

import pandas as pd
from src import bigquery
from statsmodels.tsa.arima_model import ARIMA
import logging
from datetime import datetime


def get_data():
	"""
	Fetches all data in kennzahlenupdate.ivw_visits and adds ze.tt to zon accordingly
	:return: dataframe with all data from 2015-01-01 onwards
	"""
	sql = "SELECT * FROM kennzahlenupdate.ivw_visits WHERE date >= '2015-01-01' ORDER BY date asc"
	df = bigquery.get_data(sql)

	# add ze.tt values to corresponding zon values
	df.zon_android = df.zon_android.add(df.zett_android, fill_value=0)
	df.zon_ios = df.zon_ios.add(df.zett_ios, fill_value=0)

	# drop ze.tt values
	df = df[df.columns.drop(["zett_android", "zett_ios"])]

	# convert to date object
	df.date = pd.to_datetime(df.date, format="%Y-%m-%d")

	return df


def difference(dataset, interval=1):
	"""
	differences seasonal data
	:param dataset: dataset to difference
	:param interval: difference interval; dependent on dataset dimension
	:return: dataframe with differened data
	"""
	diff = list()
	for i in range(interval, len(dataset)):
		value = dataset[i] - dataset[i - interval]
		diff.append(value)
	return diff


def inverse_difference(history, yhat, interval=1):
	"""
	reverses differenced data
	:param history: original data
	:param yhat: prediction
	:param interval: interval of seasonality
	:return:
	"""
	return yhat + history[-interval]


def arima_model(df, dataset_name):
	"""
	this function trains and makes predictions using the arima model
	:param lst: [df: dataframe to be predicted,
				 dataset_name: one of stationaer, mobile, android, ios]
	:return: returns vector with predictions; len=horizon
	"""
	# unpack arguments
	#df = lst[0]
	#dataset_name = lst[1]
	arima_order = (6, 0, 6)
	horizon = 31

	print('start forecasting ' + dataset_name)

	# prepare training dataset
	X = df.astype("float32")
	history = [x for x in X]
	# make predictions
	predictions = list()
	for t in range(horizon):
		# difference data
		days = 7
		diff = difference(history, days)
		model = ARIMA(diff, order=arima_order)
		model_fit = model.fit(trend='nc', disp=0)
		yhat = model_fit.forecast()[0]
		yhat = inverse_difference(history, yhat, days)
		predictions.append(yhat)
		history.append(predictions[t])

	logging.info(str(datetime.now()) + ' forecasting finished for ' + dataset_name)
	print('forecasting finished for ' + dataset_name)
	return {dataset_name: predictions}


