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
        

