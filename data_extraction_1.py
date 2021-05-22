#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
This script attempts to answer question 1 of the City of Cape Town - Data Science Unit Code Challenge:    
    
1. Data Extraction

Use the AWS S3 SELECT command to read in the H3 resolution 8 data from city-hex-polygons-8-10.geojson. Use the city-hex-polygons-8.geojson file to validate your work.

Output is a geojson file matching to and validated against city-hex-polygons-8.geojson file

@author: ssalie
email: ssalie@ska.ac.za

"""
#%% import libraries
import boto3
import timeit
import os
import json

# Retrieve data from AWS bucket
starttime = timeit.default_timer()
print("Accessing AWS COCT bucket to rerieve requested data")

BUCKET_NAME='cct-ds-code-challenge-input-data'
KEY_NAME='city-hex-polygons-8-10.geojson'

s3 = boto3.client('s3',
                  region_name="af-south-1",
                  aws_access_key_id='AKIAYH57YDEWMHW2ESH2',
                  aws_secret_access_key='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')

#Extract only the H3 resolution 8 data from KEY_NAME in AWS BUCKET_NAME
resp = s3.select_object_content(
    Bucket=BUCKET_NAME,
    Key=KEY_NAME,
    ExpressionType='SQL',
    Expression="select * from s3object[*].features[*] s where s.properties.resolution = 8", 
    InputSerialization={"JSON": {"Type": "DOCUMENT"}, "CompressionType": "NONE"},
    OutputSerialization={'JSON': {}},
)

# extract the data of interest and write to variable and file
print("extract the data of interest and write to variable and file")
filePath1 = "asd.json"  #temo file to store json data
filePath2 = "city-hex-polygons-8_SSalieExtracted.geojson" #save extracted data from AWS bucket to a .geojson file

# remove .json files if they exist
try:
    os.remove(filePath1)
except:
    print("{} does not exist".format(filePath1))
    
try:
    os.remove(filePath2)
except:
    print("{} does not exist".format(filePath2))    

# extract payload data from AWS bucket and write to a temp asd.json file
for event in resp['Payload']:
    with open('asd.json', 'a') as f_:
        if 'Records' in event:
            records = event['Records']['Payload'].decode('utf-8')
            if not str(records) == "":
                f_.write(records)

# dump the extracted H3 reolustion 8 data from th etemp asd.json file to a geojson file
print("dump the extracted H3 reolustion 8 data from th temp asd.json file to a geojson file")
features = []
with open('asd.json', 'r') as f_:
    for line in f_:
        json_line = json.loads(line)
        del json_line['properties']['resolution']       #delete the resolution field
        features.append(json_line)

print("writing city-hex-polygons-8_SSalieExtracted.geojson file to directory")
with open(filePath2, 'a') as f_:
    f_.write(json.dumps(features))
    
try:
    os.remove("asd.json")
except:
    print("Error while deleting file ", "asd.json")


#%%Data extraction and validation
failed = False
# Open city-hex-polygons-8.geojson file to validate against extracted data
print("validating the downloaded and extracted city-hex-polygons-8_SSalieExtracted.geojson file from AWS against the downloaded city-hex-polygons-8.geojson file")
with open('city-hex-polygons-8.geojson') as data_file:
    city_hex = json.loads(data_file.read())
    for my_item, val_item in zip(features, city_hex['features']):
        if my_item != val_item:
            failed = True
            print('failed to verify {} against \n {}'.format(my_item, val_item))
        
    if failed:
        print('city-hex-polygons-8.geojson != city-hex-polygons-8_SSalieExtracted.json, data extraction failed and not validated')
    else:
        print('city-hex-polygons-8.geojson == city-hex-polygons-8_SSalieExtracted.json, data extraction validated')

print("------------------------------------------------------------------------")        
print("The time taken to complete this script for data extraction is {:.2f} seconds".format(timeit.default_timer() - starttime))

