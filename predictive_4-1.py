#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This script attempts to answer question 4.1 of the City of Cape Town - Data Science Unit Code Challenge:

Please use sr_hex.csv dataset, only looking at requests from the Water and Sanitation Services department.

Time series challenge: Predict the weekly number of expected service requests per hex for the next 4 weeks.

Output is an html report containing the predicted number of requests for the next 4 weeks for each hex location.

@author: ssalie
email: ssalie@ska.ac.za

"""
# import libraries
import numpy as np
import os
import requests
import pandas as pd
from statsmodels.tsa.arima.model import ARIMA
import warnings
import timeit
warnings.simplefilter(action='ignore', category=FutureWarning)
from statsmodels.tools.sm_exceptions import ConvergenceWarning
warnings.simplefilter('ignore', ConvergenceWarning)
warnings.filterwarnings("ignore")

# functions
# modelling and prediction function using Autoregressive modelling and forecasting modules
def predict_next(series,pred=4):
    """    
    Parameters
    ----------
    series : data seriess of each hex containing list of week number and asscoiated number of service requests for that week
        
    pred : forecast of how many values of future predictions requested. The default is 4 (4 weeks).

    Returns
    -------
    Prediction series of the weekly number of expected service requests per hex for the next 4 weeks
    """    
    X = series.values
    size = int(len(X) * 0.66)
    train, test = X[0:size], X[size:len(X)]
    history = [x for x in train]
    predictions = list()
    # walk-forward validation
    for t in range(len(test)):
    	model = ARIMA(history, order=(5,1,0))
    	model_fit = model.fit()
    	output = model_fit.forecast()
    	yhat = output[0]
    	predictions.append(yhat)
    	obs = test[t]
    	history.append(obs)
    return model_fit.forecast(pred)

# functions to write to html
def append_html(input_data,output_path):
    with open(output_path,'a') as f:
        f.writelines(input_data)
        
def print2html(*args):
    op_str = ''
    tables = ''
    for arg in args:
        op_str+=arg
    op_str = "<p >"+op_str+" "+"</p>"+"\n"
    op_str+=tables
    append_html(op_str,html_report_path)

# downlaod datasets
print("Downlaoding service request data files")
starttime = timeit.default_timer()

sr_hex_csv_gz_url = "https://cct-ds-code-challenge-input-data.s3.af-south-1.amazonaws.com/sr_hex.csv.gz"
sr_hex_csv_gz = "sr_hex.csv.gz"
if os.path.exists(sr_hex_csv_gz):
    print("sr_hex.csv.gz already downloaded")
else:
    r = requests.get(sr_hex_csv_gz_url,stream=True)
    with open(sr_hex_csv_gz, "wb") as f:
        r = requests.get(sr_hex_csv_gz_url)
        f.write(r.content)

print("opening sr_hex.gz dataset in a dataframe")
df = pd.read_csv(sr_hex_csv_gz, compression='zip', header=0, sep=',', quotechar='"')
df = df[df["department"] == "Water and Sanitation"] # select only sr for the water and sanitation department
df = df[df["h3_level8_index"] != '0'] # select only data with valid location data
df["CreationDate"] = pd.to_datetime(df["CreationDate"]) #force dates to be in datetime format
df = df.sort_values(["h3_level8_index","CreationDate"],inplace=False) #sort data by h3_level8_index and CreationDate
df = df.reset_index(drop=True)
df["WeekNumber"] = ((df["CreationDate"] - min(df["CreationDate"]))/7).dt.days.astype('int16') #create a new column that shows the asscociated week number for each request (week 0 being the week for the earliest sr date)

print("Creating dictionaries to save calculated/predicted data")
# create a location dictionary where each hex location can be saved and the calculated number of requests per weeknumber for each hex
location = {}
for hex_str in df["h3_level8_index"].unique():
    location.setdefault(hex_str,[])

# create a location dictionary where each hex location can be saved and the predicted next 4 weeks number of requests per each hex
location_prediction = {}
for hex_str in df["h3_level8_index"].unique():
    location_prediction.setdefault(hex_str,[])


print("developing forecast model and predicting future number requests per week for next 4 weeks")
print("this may take a while to complete, see html report for updated progress")
#for each hex location, save an array with the week number and number of requests for that week
html_report_path = "predictive_q4-1.html"
if os.path.exists(html_report_path):
    os.remove(html_report_path)
count = 0
for key in location.keys():
    tmp = df[df["h3_level8_index"] == key]["WeekNumber"]
    x = tmp.value_counts()
    x.sort_index(inplace=True)
    xarr = np.zeros([len(x),2])
    xarr[:,0] = x.index.values
    xarr[:,1] = x.values
    location[key] = xarr
    series = pd.DataFrame(location[key], columns=['weeknumber', 'count'])
    series.set_index('weeknumber',inplace=True)
    # predict number requests for next 4 weeks. NB if training dataset is insufficient, assign future request of the mean of the series data request per week for next 4 weeks
    try:
        location_prediction[key].append(predict_next(series,4))
        pred = location_prediction[key][0]
        print2html("h3_level8_index {} is forecasted to have {:.0f}, {:.0f}, {:.0f}, {:.0f} service requests respectively per week over the next 4 weeks".format(key,pred[0],pred[1],pred[2],pred[3]))
    except:
        # not enough data to make prediction, assign mean of series for next 4 weeks
        pred_fail = series.mean()
        location_prediction[key].append([pred_fail.values[0],pred_fail.values[0],pred_fail.values[0],pred_fail.values[0]])
        pred = location_prediction[key][0]
        print2html("h3_level8_index {} is forecasted to have {:.0f}, {:.0f}, {:.0f}, {:.0f} service requests respectively per week over the next 4 weeks".format(key,pred[0],pred[1],pred[2],pred[3]))
    count+=1
    if count%100==0:
        print("now by hex number {} out of {}".format(count,len(location)))
print("------------------------------------------------------------------------")        
print("The time taken to complete this script for data extraction is {:.2f} minutes".format((timeit.default_timer() - starttime)/60))
