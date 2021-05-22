#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This script attempts to answer question 2 of the City of Cape Town - Data Science Unit Code Challenge:
    
Join the file city-hex-polygons-8.geojson to the service request dataset, such that each service request is assigned to a single H3 hexagon. Use the sr_hex.csv file to validate your work.

For any requests where the Latitude and Longitude fields are empty, set the index value to 0.

Output is the calculated h3_level8_index for each service request from the sr.csv file, and validated against the h3_level8_index from the sr_hex.csv file

@author: ssalie
email: ssalie@ska.ac.za

"""
#%% import libraries
import timeit
import h3
import csv
import os
import requests
import zipfile

#download sr.csv and sr_hex.csv files and extract zipped files
print("Downlaoding service request data files")
starttime = timeit.default_timer()

sr_csv_gz_url = "https://cct-ds-code-challenge-input-data.s3.af-south-1.amazonaws.com/sr.csv.gz"
sr_hex_csv_gz_url = "https://cct-ds-code-challenge-input-data.s3.af-south-1.amazonaws.com/sr_hex.csv.gz"

sr_csv_gz = "sr.csv.gz"
sr_hex_csv_gz = "sr_hex.csv.gz"

if os.path.exists(sr_csv_gz):
    print("sr.csv.gz already downloaded")
else:
    r = requests.get(sr_csv_gz_url,stream=True)
    with open(sr_csv_gz, "wb") as f:
        r = requests.get(sr_csv_gz_url)
        f.write(r.content)
    with zipfile.ZipFile(sr_csv_gz, 'r') as zip_ref:
        zip_ref.extractall('.')

if not(os.path.exists("sr.csv")):
    with zipfile.ZipFile('sr.csv.gz', 'r') as zip_ref:
        zip_ref.extractall('.')
        
if os.path.exists(sr_hex_csv_gz):
    print("sr_hex.csv.gz already downloaded")
else:
    r = requests.get(sr_hex_csv_gz_url,stream=True)
    with open(sr_hex_csv_gz, "wb") as f:
        r = requests.get(sr_hex_csv_gz_url)
        f.write(r.content)
    with zipfile.ZipFile(sr_hex_csv_gz, 'r') as zip_ref:
        zip_ref.extractall('.')

if not(os.path.exists("sr_hex.csv")):
    with zipfile.ZipFile("sr_hex.csv.gz", 'r') as zip_ref:
        zip_ref.extractall('.')

print("open and read in csv files to data variables, takes a few minutes")
# read in sr_csv file
with open("sr.csv",'r') as datafile:
    datareader = csv.reader(datafile, delimiter=',')
    data_sr = []
    for row in datareader:
        data_sr.append(row)

# open the sr_hex.csv file to use to validate calculated hex numbers from sr.csv file
with open("sr_hex.csv",'r') as datafile_hex:
    datareader = csv.reader(datafile_hex, delimiter=',')
    data_hex = []
    for row in datareader:
        data_hex.append(row)

print("convert extracted csv data to data lists, takes a minute or two")
#convert list to dictionary
data_sr_dict = {}
for i in range(len(data_sr[0])):
    data_sr_dict.setdefault(data_sr[0][i],[])

    for idx in range(len(data_sr[1:])):
        data_sr_dict[data_sr[0][i]].append(data_sr[idx+1][i])

del data_sr
    
print("from sr.csv lat/long data, determine the appropriate hex number for it and save to variable")
h3_index = []
h3_index.append("h3_level8_index")

# for each lat/long coordinate point for each service request, determine the asscociated h3_level8_index from the h3.geo_to_h3 function and append to h3_index variable
# if lat/long == nan, fill h3 _index variable with '0'
for idx in range(0,len(data_sr_dict["Latitude"])):
    if data_sr_dict["Latitude"][idx]=="nan":
        h3_index.append('0')        
    else:
        index_hash = h3.geo_to_h3(float(data_sr_dict["Latitude"][idx]),float(data_sr_dict["Longitude"][idx]),8)
        h3_index.append(index_hash)

print("create h3_index_original variable and fill with h3_level8_index from sr_hex.csv file to use to validate calculated hex numbers")
# create h3_index_original variable and fill with h3_level8_index from sr_hex.csv file to use to validate calculated hex numbers
h3_index_original = []
for row in data_hex:
    h3_index_original.append(row[-1])

print("data validation")
# data validation
count = 0
for i in range(0,len(h3_index)):
    if h3_index[i] == h3_index_original[i]:
        count+=1
print("------------------------------------------------------------------------")    
print("calculated h3_level8_index values match to those from sr_hex.csv file for {:.2f}% of total service request".format(100*count/len(h3_index)))        
print("------------------------------------------------------------------------")
print("h3_level8_index column from sr_hex.csv matches that of the h3_level8_index column developed from sr.csv latitude/longitude points. Thus validated")
print("------------------------------------------------------------------------")
print("The time taken to complete this script for data extraction is {:.2f} seconds".format(timeit.default_timer() - starttime))


