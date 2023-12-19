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
        collection_name = "shootings"
        shootings_collection = shootings_db[collection_name]

        if collection_name in shootings_db.list_collection_names() and shootings_collection.count!= 0:
            shootings_collection.drop()

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
        collection_name = "arrests"
        arrests_collection = arrests_db[collection_name]

        if collection_name in arrests_db.list_collection_names() and arrests_collection.count() != 0:
            arrests_collection.drop()

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
        complaints.to_sql('complaints', engine, if_exists = 'replace')                
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
) -> pd.DataFrame:
    shootings_df['month'] = pd.to_datetime(shootings_df['occur_date']).dt.month
    group_month = shootings_df.groupby('month')['incident_key'].count()
    context.add_output_metadata(
            metadata={
                "num_records": len(shootings_df),
                "preview": MetadataValue.md(group_month.to_markdown()),
            }
        )    
    
    shootings_per_month_df = pd.DataFrame(group_month)
    return shootings_per_month_df

@asset
def histogram_shootings_per_month_hist(shootings_per_month_df) -> MaterializeResult:
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
def pie_chart_percentages_of_fatalities(value_counts_of_shooting_fatalities) -> MaterializeResult:
    plt.pie(value_counts_of_shooting_fatalities,labels = value_counts_of_shooting_fatalities.index, autopct ='%1.1f%%', colors = ['skyblue','lightcoral'])
    plt.axis('equal')

    buffer = BytesIO()
    plt.savefig(buffer, format = "png")
    image_data = base64.b64encode(buffer.getvalue())

    md_content = f"![img](data:image/png;base64,{image_data.decode()})"


    return MaterializeResult(
        metadata={"Percentage Of Fatalities vs Non Fatal Shootings": MetadataValue.md(md_content)}
    )

import pandas.io.sql as sqlio

@asset(deps=[extract_complaints])
def complaints_df(
    context: AssetExecutionContext
):
    engine = create_engine(postgres_connection_string)
    with engine.connect() as connection:
        complaints_df = sqlio.read_sql_query("SELECT * FROM complaints", connection)
    
    context.add_output_metadata(
        metadata = {
            "num_records": len(complaints_df),
            "preview": MetadataValue.md(complaints_df.head().to_markdown()),            
        }
    )

    return complaints_df

@asset(deps=[extract_arrests])
def arrests_df(
    context: AssetExecutionContext
) -> pd.DataFrame:
    
    client = MongoClient(mongo_connection_string)
    arrests_db = client["nypd_analysis"]
    arrests_collection = arrests_db["arrests"]
    collection_name = "arrests"
    collection = arrests_db[collection_name]
    data = list(collection.find())
    client.close()
    arrests_df = pd.DataFrame(data)
    context.add_output_metadata(
            metadata={
                "num_records": len(arrests_df),
                "preview": MetadataValue.md(arrests_df.head().to_markdown()),
            }
        )

    return arrests_df

@asset
def value_counts_of_complaints_offenses(
    context: AssetExecutionContext,
    complaints_df
) -> pd.DataFrame:
    complaints_df['law_cat_cd'] = complaints_df['law_cat_cd'].map({'MISDEMEANOR':'Misdemeanor','FELONY':'Felony','VIOLATION':'Violation'}) 

    complaint_offense_count = complaints_df['law_cat_cd'].value_counts()
    complaint_offense_count_dataframe = pd.DataFrame(complaint_offense_count)
    complaint_offense_count_dataframe.index.names = ['OFFENSE']
    complaint_offense_count_dataframe.loc['Infraction'] = 0

    context.add_output_metadata(
            metadata={
                "Complaint Counts Per Offense": MetadataValue.md(complaint_offense_count_dataframe.to_markdown()),
            }
        )    
    return complaint_offense_count_dataframe

@asset
def value_counts_of_arrests_offenses(
    context: AssetExecutionContext,
    arrests_df
) -> pd.DataFrame:
    arrests_offenses_count  =  arrests_df['law_cat_cd'].value_counts()
    arrests_offenses_count_dataframe = pd.DataFrame(arrests_offenses_count)
    arrests_offenses_count_dataframe.index.names = ['OFFENSE']
    arrests_offenses_count_dataframe.loc['law_cat_cd' == '9']
    context.add_output_metadata(
        metadata={
            "Arrest Counts Per Offense": MetadataValue.md(arrests_offenses_count_dataframe.to_markdown()),
        }
    )

    return arrests_offenses_count_dataframe

@asset
def value_counts_of_arrests_and_complaints(
    context: AssetExecutionContext,
    value_counts_of_arrests_offenses,
    value_counts_of_complaints_offenses
):
    complaint_arrest_joined = pd.merge(value_counts_of_complaints_offenses, value_counts_of_arrests_offenses, left_index = True, right_index = True)
    complaint_arrest_joined = complaint_arrest_joined.rename(columns = {'LAW_CAT_CD_x':'Complaint Offense','LAW_CAT_CD_y':'Arrest Offense'})

    context.add_output_metadata(
        metadata={
            "Joined Dataframe for Arrests and Complaints per Offense": MetadataValue.md(complaint_arrest_joined.to_markdown()),
        }
    )
    return complaint_arrest_joined

@asset
def bar_chart_types_of_offenses_in_arrests_and_complaints(value_counts_of_arrests_and_complaints):
    fig, ax = plt.subplots(figsize=(10, 6))

    value_counts_of_arrests_and_complaints.plot(kind='bar', ax=ax, width=0.8)

    # Adding labels and title
    ax.set_xlabel('OFFENSE')
    ax.set_ylabel('Count')
    ax.set_title('Comparison of Offense')
    #for p in plt.gca().patches:
    #    plt.gca().annotate(f'{p.get_height()}', (p.get_x() + p.get_width() / 2., p.get_height()),
    #                       ha='center', va='bottom', color='black', fontsize=10)
        
    buffer = BytesIO()
    plt.savefig(buffer, format = "png")
    image_data = base64.b64encode(buffer.getvalue())

    md_content = f"![img](data:image/png;base64,{image_data.decode()})"


    return MaterializeResult(
        metadata={"Comparison of Offenses in Complaints and Arrests": MetadataValue.md(md_content)}
    )

