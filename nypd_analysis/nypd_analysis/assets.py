import json
import csv
import os 

import requests
from dagster import asset

headers = { "X-App-Token": "WGwrytV3FHVDoouYC91Qp7X8c" }

@asset
def arrests_json() -> None:
    nypd_arrests_url = "https://data.cityofnewyork.us/resource/uip8-fykc.json?$limit=200000"
    nypd_arrests = requests.get(nypd_arrests_url, headers = headers).json()

    os.makedirs("data", exist_ok = True)
    with open("data/arrests.json", "w") as f:
        json.dump(nypd_arrests, f)

@asset
def complaints_csv() -> None:
    nypd_complaints_url = "https://data.cityofnewyork.us/resource/5uac-w243.csv"
    nypd_complaints = requests.get(nypd_complaints_url).iter_content()
    

    os.makedirs("data", exist_ok = True)
    with open('data/complaints.csv', 'wb') as csv_file:
        csv_file.write(nypd_complaints)








@asset
def shooting_json() -> None:
        nypd_shooting_url = "https://data.cityofnewyork.us/resource/5ucz-vwe8.json"
        nypd_shootings = requests.get(nypd_shooting_url).json()

        os.makedirs("data",exist_ok = True)
        with open("data/shootings.json", "w") as s:
             json.dump(nypd_shootings,s)




from dagster import get_dagster_logger, job, op, Out
from pymongo import MongoClient, errors

mongo_connection_string = "mongodb://dap:dap@127.0.0.1"
logger = get_dagster_logger()

@asset(deps=[shooting_json])
def extract_shootings() -> bool:
    result = True
    try:
        #connect the mongodb databse
        client = MongoClient(mongo_connection_string)
        #connect to the shootings database
        shootings_db = client["nypd_analysis"]
        #connect to the nypd_analysis collection
        shootings_collection = shootings_db["shootings"]
        #open the file containing the data
        with open("data/shootings.json","r") as sj:
            #load the josn data from the file
            data = json.load(sj)

        for shooting in data:
            try:
                shootings_collection.insert_one(shooting)


            except errors.DuplicateKeyError as err:
                logger.error("Error: %s" % err)
                continue

        # Trap and handle other errors
    except Exception as err:
        logger.error("Error: %s" % err)
        result = False

    # Return a Boolean indicating success or failure
    return result

@asset(deps = [arrests_json])
def extract_arrests() -> bool:
    result = True
    try:
        client = MongoClient(mongo_connection_string)
        arrests_db = client["nypd_analysis"]
        arrests_collection = arrests_db["arrests"]
        with open("data/arrests.json","r") as aj:
            data = json.load(aj)
        for arrest in data:
            try:
                arrests_collection.insert_one(arrest)

            except errors.DuplicateKeyError as err:
                logger.error("Error: %s" % err)
                continue
    except Exception as err:
        logger.error("Error: %s" % err)
        result = False
            
    return result

@job
def load_files_into_db_job():
    extract_shootings()





