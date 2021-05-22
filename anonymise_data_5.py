#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This script attempts to answer question 5 of the City of Cape Town - Data Science Unit Code Challenge:
    
Write a script which anonymises the sr_hex.csv file, but preserves the following resolutions (You may use H3 indexes or lat/lon coordinates for your spatial data):

    location accuracy to within approximately 500m
    temporal accuracy to within 6 hours
    scrubs any columns which may contain personally identifiable information.

Output is an anonymised service request csv file

@author: ssalie
email: ssalie@ska.ac.za

"""
# Import libraries
import pandas as pd
import numpy as np
import datetime
from datetime import timedelta
import timeit
import os
import requests

# functions

def rand_hours(N,H=6):
    """
    function to create a random datetime object between -6 to +6 hours
    """

    max_val, min_val = 1, -1
    range_size = (max_val - min_val)  # 2
    return ((np.random.rand(N) * range_size + min_val) *H)[0]

def create_random_point(x0,y0,distance=500):
    """
    Function to anonymise geo lat/long data
    This function generates a random lat/long point within 500m of given lat/long
    """   
    r = distance/ 111300
    u = np.random.uniform(0,1)
    v = np.random.uniform(0,1)
    w = r * np.sqrt(u)
    t = 2 * np.pi * v
    x = w * np.cos(t)
    x1 = x / np.cos(y0)
    y = w * np.sin(t)
    return (x0+x1, y0 +y)


def anon_timeseries(time_str,res=6):
    """
    function to anonymise timeseries data
    """
    t1 = datetime.datetime.fromisoformat(time_str)
    delta = timedelta(hours=rand_hours(1,res))
    return t1+delta

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


# anonymize data and save to csv
print("opening sr_hex.gz dataset in a dataframe")
df = pd.read_csv(sr_hex_csv_gz, compression='zip', header=0, sep=',', quotechar='"')

print("drop Notification column because its values are unique for every service request/row")
df.drop(columns=["NotificationNumber"], inplace=True) # drop Notification column because its values are unique for every service request/row

print("based on lat/long values from each sr, create a random coordinate point around it within 500m")
tmp_cord = create_random_point(df['Latitude'],df['Longitude'],distance=500) #based on lat/long values from each sr, create a random coordinate point around it within 500m

# anonymise the timestamp data values by modifyng its datetime to a random datetime within 6 hours
print("anonymise the timeseries data values by modifyng its datetime to a random datetime within 6 hours")
print("this may take a few minutes to complete")
tmp_modtime = []
for i in df['ModificationTimestamp']:
    try:
        tmp_modtime.append(anon_timeseries(i))
    except:
        tmp_modtime.append(i)
    
tmp_comptime = []
for i in df['CompletionTimestamp']:
    try:
        tmp_comptime.append(anon_timeseries(i))
    except:
        tmp_comptime.append(i)

tmp_creattime = []
for i in df['CreationTimestamp']:
    try:
        tmp_creattime.append(anon_timeseries(i))
    except:
        tmp_creattime.append(i)

# modify dataframe columns with the randomised geolocation and timeseries data
print("modify dataframe columns with the randomised geolocation and timeseries data")
df['Latitude'] = tmp_cord[0]
df['Longitude'] = tmp_cord[1]
df['ModificationTimestamp'] = tmp_modtime
df['CompletionTimestamp'] = tmp_comptime
df['CreationTimestamp'] = tmp_creattime

#save anonymised data to csv file
print("this may take a few minutes to complete")
print("save anonymised data to csv file")
df.to_csv('sr_hex_anonymised.csv')

print("------------------------------------------------------------------------")
print("The time taken to complete this script for data extraction is {:.2f} minutes".format((timeit.default_timer() - starttime)/60)) 
print("------------------------------------------------------------------------")
print("Anonymise process followed:\n  - Notificationnumber column was dropeed as it was a unique number for each request\n - Geolocation data was anonymised by generating a random lat/long point within 500m of the true lat/long location\n - Timeseries data was anonymised by generating a random timeseries within 6 hours of the true timeseries data")
"""
Anonymise process followed
Notificationnumber column was dropeed as it was a unique number for each request
geolocation data was anonymised by generating a random lat/long point within 500m of the true lat/long location
the timeseries data was anonymised by generating a random timeseries within 6 hours of the true timeseries data
"""