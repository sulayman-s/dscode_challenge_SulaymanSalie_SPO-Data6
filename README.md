# dscode_challenge_SulaymanSalie_SPO-Data6
This repository contains code and files developed by Sulayman Salie for submission for ds code challenge

##Setup:
Firstly, using pip3, install all dependencies and required libraries from the requirements.txt file
run:
* pip3 install -r [requirements.txt](https://github.com/sulayman-s/dscode_challenge_SulaymanSalie_SPO-Data6/blob/main/requirements.txt)

##Question 1: Data Extraction
This script attempts to answer question 1 of the City of Cape Town - Data Science Unit Code Challenge:
run:
* [data_extraction_1.py](https://github.com/sulayman-s/dscode_challenge_SulaymanSalie_SPO-Data6/blob/main/data_extraction_1.py)
Output is a geojson file matching to and validated against city-hex-polygons-8.geojson file

##Question 2: Data Transformation
This script attempts to answer question 2 of the City of Cape Town - Data Science Unit Code Challenge:
run:
* [data_transformation_2.py](https://github.com/sulayman-s/dscode_challenge_SulaymanSalie_SPO-Data6/blob/main/data_transformation_2.py)
Output is the calculated h3_level8_index for each service request from the sr.csv file, and validated against the h3_level8_index from the sr_hex.csv file

##Question 3: Data Analysis
This file attempts to answer question 3 of the City of Cape Town - Data Science Unit Code Challenge:
open:
* [Data_analysis_report](https://github.com/sulayman-s/dscode_challenge_SulaymanSalie_SPO-Data6/blob/main/SPO-Data%20Analyst.pdf) 

##Question 4.1: Data Prediction
This script attempts to answer question 4.1 of the City of Cape Town - Data Science Unit Code Challenge:
run:
* [predictive_4-1.py](https://github.com/sulayman-s/dscode_challenge_SulaymanSalie_SPO-Data6/blob/main/predictive_4-1.py)
Output is an html report [predictive_q4-1.html](https://github.com/sulayman-s/dscode_challenge_SulaymanSalie_SPO-Data6/blob/main/predictive_q4-1.html) containing the predicted number of requests for the next 4 weeks for each hex location.

##Question 4.3: Data Classification
This script attempts to answer question 4.3 of the City of Cape Town - Data Science Unit Code Challenge:
run:
* [classification_4-3.py](https://github.com/sulayman-s/dscode_challenge_SulaymanSalie_SPO-Data6/blob/main/classification_4-3.py)
Output is an html report [classification_q4-3.html](https://github.com/sulayman-s/dscode_challenge_SulaymanSalie_SPO-Data6/blob/main/classification_q4-3.html) that classifies each hex location as formal, informal or rural based on the sr data given

##Question 5: Data Transformation - Anonymise Data
This script attempts to answer question 5 of the City of Cape Town - Data Science Unit Code Challenge:
run:
* [anonymise_data_5.py](https://github.com/sulayman-s/dscode_challenge_SulaymanSalie_SPO-Data6/blob/main/anonymise_data_5.py)
Output is an anonymised service request csv file

##Question 6: Data Loading - Upload to AWS s3 Bucket
This script attempts to answer question 6 of the City of Cape Town - Data Science Unit Code Challenge:
run:
* [data_uploadAWS_6.py](https://github.com/sulayman-s/dscode_challenge_SulaymanSalie_SPO-Data6/blob/main/data_uploadAWS_6.py)
Output is a subset csv file uploaded to the AWS s3 bucket
