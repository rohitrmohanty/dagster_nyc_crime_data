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
    nypd_complaints_url = "https://data.cityofnewyork.us/resource/5uac-w243.csv?$limit=40000000"
    nypd_complaints = requests.get(nypd_complaints_url, headers = headers)

    os.makedirs("data", exist_ok = True)
    with open('data/complaints.csv', 'w') as csv_file:
        csv_file.write(nypd_complaints.text)

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

from sqlalchemy import create_engine

postgres_connection_string = "postgresql+psycopg2://dap:dap@127.0.0.1:5432/nypd"
@asset(deps = [complaints_csv])
def extract_complaints() -> bool:
    result = True
    complaints = pd.read_csv('data/complaints.csv') 
    try:
        engine = create_engine(postgres_connection_string)
        complaints.to_sql('Complaints', engine, if_exists = 'replace')                
    except Exception as err:
        logger.error("Error: %s" % err)
        result = False

    return result

@job
def load_files_into_db_job():
    extract_shootings()



from dagster import MaterializeResult, MetadataValue, AssetExecutionContext
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from io import BytesIO
import base64

@asset(deps=[extract_shootings])
def shootings_df(
    context: AssetExecutionContext
) -> pd.DataFrame:
    
    client = MongoClient(mongo_connection_string)
    shootings_db = client["nypd_analysis"]
    shootings_collection = shootings_db["shootings"]
    collection_name = "shootings"
    collection=shootings_db[collection_name]
    data = list(collection.find())
    client.close()
    shootings_df = pd.DataFrame(data)
    context.add_output_metadata(
            metadata={
                "num_records": len(shootings_df),
                "preview": MetadataValue.md(shootings_df.head().to_markdown()),
            }
        )

    return shootings_df

@asset
def shootings_per_month_df(
    context: AssetExecutionContext,
    shootings_df
) -> pd.api.typing.DataFrameGroupBy:
    shootings_df['month'] = pd.to_datetime(shootings_df['occur_date']).dt.month
    group_month = shootings_df.groupby('month')['incident_key'].count()
    context.add_output_metadata(
            metadata={
                "num_records": len(shootings_df),
                "preview": MetadataValue.md(group_month.to_markdown()),
            }
        )    
    
    return group_month

@asset
def histogram_shootings_per_month_hist(shootings_per_month_df) -> MaterializeResult:
    shootings_per_month_df = pd.DataFrame(shootings_per_month_df)
    sns.barplot(data = shootings_per_month_df, x="month", y="incident_key",color='skyblue')
    plt.xlabel('Month')
    plt.ylabel('Counts')
    buffer = BytesIO()
    plt.savefig(buffer, format = "png")
    image_data = base64.b64encode(buffer.getvalue())

    md_content = f"![img](data:image/png;base64,{image_data.decode()})"


    return MaterializeResult(
        metadata={"Shootings Per Month Hist": MetadataValue.md(md_content)}
    )


@asset
def value_counts_of_shooting_fatalities(context: AssetExecutionContext, shootings_df) -> pd.Series:
    shootings_df['statistical_murder_flag'] = shootings_df['statistical_murder_flag'].map({'N':'No','Y':'Yes'})
    sh_vc = shootings_df['statistical_murder_flag'].value_counts()
    context.add_output_metadata(metadata = {"Value Counts of Shooting Fatalities": MetadataValue.md(sh_vc.to_markdown())})
    return sh_vc

@asset
def piechart_percentages_of_fatalities(value_counts_of_shooting_fatalities) -> MaterializeResult:
    plt.pie(value_counts_of_shooting_fatalities,labels = value_counts_of_shooting_fatalities.index, autopct ='%1.1f%%', colors = ['skyblue','lightcoral'])
    plt.axis('equal')

    buffer = BytesIO()
    plt.savefig(buffer, format = "png")
    image_data = base64.b64encode(buffer.getvalue())

    md_content = f"![img](data:image/png;base64,{image_data.decode()})"


    return MaterializeResult(
        metadata={"Percentage Of Fatalities vs Non Fatal Shootings": MetadataValue.md(md_content)}
    )
    
