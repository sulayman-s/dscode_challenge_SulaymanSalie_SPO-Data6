#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This script attempts to answer question 6 of the City of Cape Town - Data Science Unit Code Challenge:
    
Select a subset of columns (including the H3 index column) from the sr_hex.csv or the anonymised file created in the task above, and write it to the write-only S3 bucket.

Be sure to name your output file something that is recognisable as your work, and unlikely to collide with the names of others.

Output is a subset csv file uploaded to the AWS s3 bucket

@author: ssalie
email: ssalie@ska.ac.za

"""
# import libraries
import boto3
import timeit
import os
import requests
import pandas as pd

starttime = timeit.default_timer()
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

# create a subset of the sr_hex dataset and save to new csv file
print("create a subset of the sr_hex dataset and save to new csv file")
df_sub = df[["NotificationNumber","Latitude","Longitude","h3_level8_index"]][:1000]
df_sub.to_csv("sr_hex_subset_SSalie.csv")

print("upload the subset csv file to AWS s3 bucket")
BUCKET_NAME='cct-ds-code-challenge-input-data'
OBJECT_NAME='sr_hex_subset_SSalie.csv'

s3 = boto3.client('s3',
                  region_name="af-south-1",
                  aws_access_key_id='AKIAYH57YDEWMHW2ESH2',
                  aws_secret_access_key='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')


with open("sr_hex_subset_SSalie.csv", "rb") as f:
    s3.upload_fileobj(f, BUCKET_NAME, OBJECT_NAME)

print("------------------------------------------------------------------------")
print("The time taken to complete this script for data extraction is {:.2f} minutes".format((timeit.default_timer() - starttime)/60)) 