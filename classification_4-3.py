#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This script attempts to answer question 4.3 of the City of Cape Town - Data Science Unit Code Challenge:

Please use sr_hex.csv dataset, only looking at requests from the Water and Sanitation Services department.

Classification challenge: Classify a hex as formal, informal or rural based on the data derived from the service request data.

Output is an html report that classifies each hex location as formal, informal or rural based on the sr data given

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

print("Creating dictionaries to sort classification to per hex")
location = {}
for hex_str in df["h3_level8_index"].unique():
    location.setdefault(hex_str,[])

print("classifying hex location to be either RURAL, INFORMAL or FORMAL based on sr data")
print("if Suburb contains FARM, classification will assign RURAL location")
print("if the above condition fails, but if CodeGroup contains INFORMAL, classification will assign INFORMAL location")
print("all other locations that do not satisfy the above two conditions is classified as FORMAL")
print("this may take a while to complete, see html report for updated progress")
#for each hex location, save an array with the week number and number of requests for that week
num_rural = 0
num_informal = 0
num_formal = 0
html_report_path = "classification_q4-3.html"
if os.path.exists(html_report_path):
    os.remove(html_report_path)
count = 0
df_hex = pd.DataFrame(location.keys()) # create dataframe of unique hex numbers
for key in location.keys():
    tmp_codegroup = df[df["h3_level8_index"] == key]["CodeGroup"].unique()
    tmp_suburb = df[df["h3_level8_index"] == key]["OfficialSuburbs"].unique()
    if (any("FARM" in string for string in tmp_suburb)):
        df_hex[key] = 'rural'
        print2html("{} is classified as a RURAL location".format(key))
        num_rural+=1
    
    elif (("WATER  - INFORMAL SETTLEMENTS" in tmp_codegroup) | ("SEWER  - INFORMAL SETTLEMENTS" in tmp_codegroup)):
        df_hex[key] = 'informal'
        print2html("{} is classified as a INFORMAL location".format(key))
        num_informal+=1
        
    else:
        df_hex[key] = 'formal'
        print2html("{} is classified as a FORMAL location".format(key))
      
        num_formal+=1
    count+=1
    if count%100==0:
        print("now by hex number {} out of {}".format(count,len(location)))

print("------------------------------------------------------------------------")
print("The time taken to complete this script for data extraction is {:.2f} minutes".format((timeit.default_timer() - starttime)/60)) 
