# import for creating the data directory where the downloaded json and csv files are to be stored
import os

# import for dumping the response json from the apis into json files in memory
import json

# import for fetching the data from their respective APIs using the request package
import requests

# imports from dagster to be used for creating the metadata to be stored or materialise some result on the dagster UI
from dagster import asset, get_dagster_logger, MaterializeResult, MetadataValue, AssetExecutionContext

# Database related imports
from pymongo import MongoClient, errors
from sqlalchemy import create_engine
import pandas.io.sql as sqlio

# Analysis and Plot related imports
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from io import BytesIO
import base64

# imports for creating a map of arrests and shootings
import geopandas

# Token for New York Open Data (Needs to be in an env file but didn't get enough time)
headers = {"X-App-Token": "WGwrytV3FHVDoouYC91Qp7X8c"}

# connection string for postgres, change the username and password depending on your installation and create a database called nypd
postgres_connection_string = "postgresql+psycopg2://dap:dap@127.0.0.1:5432/nypd"

# connection string for mongo db, change the username and password according to your mondo db installation
mongo_connection_string = "mongodb://dap:dap@127.0.0.1"
logger = get_dagster_logger()

# Asset for fetching the arrests data from the API endpoint and loading it into a json file


@asset
def arrests_json() -> None:
    nypd_arrests_url = "https://data.cityofnewyork.us/resource/uip8-fykc.json?$limit=200000"
    nypd_arrests = requests.get(nypd_arrests_url, headers=headers).json()

    # creates a directory called data for storing the files if it doesn't already exist
    os.makedirs("data", exist_ok=True)

    # open a file called arrests.json in write mode and then dumps all the json from the response into the file
    with open("data/arrests.json", "w") as f:
        json.dump(nypd_arrests, f)

# Asset for fetching the complaints data from the API endpoint and loading it into a csv file


@asset
def complaints_csv() -> None:
    nypd_complaints_url = "https://data.cityofnewyork.us/resource/5uac-w243.csv?$limit=40000000"
    nypd_complaints = requests.get(nypd_complaints_url, headers=headers)

    # creates a directory called data for storing the files if it doesn't already exist
    os.makedirs("data", exist_ok=True)
    # open a file called complaints.csv in write mode and then dumps all the csv data from the response into the file
    with open('data/complaints.csv', 'w') as csv_file:
        csv_file.write(nypd_complaints.text)

# Asset for fetching the shootings data from the API endpoint and loading it into a json file


@asset
def shooting_json() -> None:
    nypd_shooting_url = "https://data.cityofnewyork.us/resource/5ucz-vwe8.json"
    nypd_shootings = requests.get(nypd_shooting_url).json()

    # creates a directory called data for storing the files if it doesn't already exist
    os.makedirs("data", exist_ok=True)
    # open a file called shootings.json in write mode and then dumps all the json from the response into the file
    with open("data/shootings.json", "w") as s:
        json.dump(nypd_shootings, s)


@asset(deps=[shooting_json])
def extract_shootings() -> bool:
    result = True
    try:
        # connect the mongodb databse
        client = MongoClient(mongo_connection_string)
        # connect to the shootings database
        shootings_db = client["nypd_analysis"]
        # connect to the nypd_analysis collection
        collection_name = "shootings"
        shootings_collection = shootings_db[collection_name]

        # if the shootings collection already exists and it's not empty it deletes the collection to avoid duplication of data
        if collection_name in shootings_db.list_collection_names() and shootings_collection.count != 0:
            shootings_collection.drop()

        # open the file containing the data
        with open("data/shootings.json", "r") as sj:
            # load the json data from the file
            data = json.load(sj)

        # reads every object in the json separately
        for shooting in data:
            try:
                # inserts each json object into the mongodb collection one by one
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


@asset(deps=[arrests_json])
def extract_arrests() -> bool:
    result = True
    try:
        client = MongoClient(mongo_connection_string)
        arrests_db = client["nypd_analysis"]
        collection_name = "arrests"
        arrests_collection = arrests_db[collection_name]

        if collection_name in arrests_db.list_collection_names() and arrests_collection.count() != 0:
            arrests_collection.drop()

        with open("data/arrests.json", "r") as aj:
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


@asset(deps=[complaints_csv])
def extract_complaints() -> bool:
    result = True
    # reads the csv into a dataframe to be loaded into postgres
    complaints = pd.read_csv('data/complaints.csv')
    try:
        # creates a connection to the postgres server
        engine = create_engine(postgres_connection_string)
        # uses the pandas to_sql function to load the dataframe into postgres
        complaints.to_sql('complaints', engine, if_exists='replace')
    except Exception as err:
        logger.error("Error: %s" % err)
        result = False

    return result


#
#
#    Shootings Related Assets
#
#

@asset(deps=[extract_shootings])
def shootings_df(
    context: AssetExecutionContext
) -> pd.DataFrame:

    # creates a connection to the mongo db server
    client = MongoClient(mongo_connection_string)
    shootings_db = client["nypd_analysis"]
    collection_name = "shootings"
    collection = shootings_db[collection_name]
    # finds all the records in the collection and loads it into a dataframe
    data = list(collection.find())
    client.close()
    shootings_df = pd.DataFrame(data)
    # add some basic metadata about the retrieved dataframe as a dagster asset
    context.add_output_metadata(
        metadata={
            "num_records": len(shootings_df),
            "preview": MetadataValue.md(shootings_df.head().to_markdown()),
        }
    )

    return shootings_df


# Creating the dataframe to store the value counts of shootings by month
@asset
def shootings_per_month_df(
    context: AssetExecutionContext,
    shootings_df
) -> pd.DataFrame:
    # creates a new column for the month numbers since it was stored as a full date string
    shootings_df['month'] = pd.to_datetime(shootings_df['occur_date']).dt.month
    # creates a new groupby object of month numbers and their corresponding counts
    group_month = shootings_df.groupby('month')['incident_key'].count()
    # adds some metadata about the created dataframe as a dagster asset
    context.add_output_metadata(
        metadata={
            "num_records": len(shootings_df),
            "preview": MetadataValue.md(group_month.to_markdown()),
        }
    )
    # converts the groupBy object into a pandas dataframe to be stored in postgres and returned by dagster for downstream assets
    shootings_per_month_df = pd.DataFrame(group_month)
    # creates a connection to the postgres server
    engine = create_engine(postgres_connection_string)
    # uses the pandas to_sql function to load the dataframe into postgres
    shootings_per_month_df.to_sql(
        'shootings_per_month', engine, if_exists='replace')
    return shootings_per_month_df


# Asset for creating the line plot of Shootings Per Month
@asset
def lineplot_shootings_per_month_hist(shootings_per_month_df) -> MaterializeResult:
    # Calculate the max y value
    max_y_value = shootings_per_month_df['incident_key'].max()

    # Plot the line chart
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=shootings_per_month_df, x="month", y="incident_key",
                 marker='o', color='blue', linestyle='-', markersize=8)
    plt.xlabel("Months")
    plt.ylabel('Count of shooting incidents')
    # Set y-axis limits to start from 0
    plt.ylim(0, max_y_value + 10)
    # starts a buffer stream to take the created plot as input
    buffer = BytesIO()
    # saves the created plot as a png file
    plt.savefig(buffer, format="png")
    # encodes the created plot in b64 encoding
    image_data = base64.b64encode(buffer.getvalue())
    # creating a string to be stored in markdown by decoding the created b64 encoding
    md_content = f"![img](data:image/png;base64,{image_data.decode()})"

    # materializing the created plot as a string output in markdown to be stored as a dagster asset
    return MaterializeResult(
        metadata={
            "Shootings Per Month Trend for 2023": MetadataValue.md(md_content)}
    )


# Asset for creating the datat frame of counts of Fatalities vs Non Fatalities in the shootings dataset
@asset
def value_counts_of_shooting_fatalities(context: AssetExecutionContext, shootings_df) -> pd.Series:
    # converts the Y and N values to full strings Yes and No to be showing in the plot
    shootings_df['statistical_murder_flag'] = shootings_df['statistical_murder_flag'].map({
                                                                                          'N': 'No', 'Y': 'Yes'})
    # creates a series object of the value counts of each Yes and No values
    sh_vc = shootings_df['statistical_murder_flag'].value_counts()
    # adds some data about the created output
    context.add_output_metadata(
        metadata={"Value Counts of Shooting Fatalities": MetadataValue.md(
            sh_vc.to_markdown())}
    )
    # creates a connection to the postgres server
    engine = create_engine(postgres_connection_string)
    # uses the pandas to_sql function to load the dataframe into postgres
    sh_vc.to_sql(
        'value_counts_of_shooting_fatalities', engine, if_exists='replace')
    return sh_vc


@asset
def pie_chart_percentages_of_fatalities(value_counts_of_shooting_fatalities) -> MaterializeResult:
    # creates a pie chart
    plt.pie(value_counts_of_shooting_fatalities, labels=value_counts_of_shooting_fatalities.index,
            autopct='%1.1f%%', colors=['skyblue', 'lightcoral'])
    plt.axis('equal')
    # starts a buffer stream to take the created plot as input
    buffer = BytesIO()
    # saves the created plot as a png file
    plt.savefig(buffer, format="png")
    # encodes the created plot in b64 encoding
    image_data = base64.b64encode(buffer.getvalue())
    # creating a string to be stored in markdown by decoding the created b64 encoding
    md_content = f"![img](data:image/png;base64,{image_data.decode()})"
    # materializing the created plot as a string output in markdown to be stored as a dagster asset
    return MaterializeResult(
        metadata={
            "Percentage Of Fatalities vs Non Fatal Shootings": MetadataValue.md(md_content)}
    )


# Asset for creating a dataframe consisting of value counts of shooting by borough
@asset
def value_counts_of_shootings_by_area(
    context: AssetExecutionContext,
    shootings_df
) -> pd.DataFrame:
    # creates a series object consisting of the value counts per BORO
    # converts the series object into a dataframe
    shoots_boro = pd.DataFrame(shootings_df['boro'].value_counts())
    # Renames the BORO field to shootings to remember which dataset it came from when it is merged with arrests
    shoots_boro = shoots_boro.rename(columns={'boro': 'SHOOTINGS'})
    # renames the index to BORO for easier merging with the arrests dataframe of the same data type
    shoots_boro.index.names = ['BORO']

    # add some basic metadata about the query as a dagster asset
    context.add_output_metadata(
        metadata={
            "num_records": len(shoots_boro),
            "preview": MetadataValue.md(shoots_boro.to_markdown()),
        }
    )

    # creates a connection to the postgres server
    engine = create_engine(postgres_connection_string)
    # uses the pandas to_sql function to load the dataframe into postgres
    shoots_boro.to_sql('value_counts_of_shootings_by_area',
                       engine, if_exists='replace')

    return shoots_boro


#
#
#  Complaints Related Assets
#
#

# Asset for creating the complaints dataframe with all the data to be used subsequently by downstream assets
@asset(deps=[extract_complaints])
def complaints_df(
    context: AssetExecutionContext
) -> pd.DataFrame:
    # creates a connection to the postgres server
    engine = create_engine(postgres_connection_string)
    # using the connection you send the query to the server and read the output
    with engine.connect() as connection:
        complaints_df = sqlio.read_sql_query(
            "SELECT * FROM complaints", connection)

    # add some basic metadata about the query as a dagster asset
    context.add_output_metadata(
        metadata={
            "num_records": len(complaints_df),
            "preview": MetadataValue.md(complaints_df.head().to_markdown()),
        }
    )

    return complaints_df


# Asset for creating a dataframe of value counts of Each Type of Offense in Complaints
@asset
def value_counts_of_complaints_offenses(
    context: AssetExecutionContext,
    complaints_df
) -> pd.DataFrame:
    # converts the values of the law_cat_cd column to appropriately formatted strings
    complaints_df['law_cat_cd'] = complaints_df['law_cat_cd'].map(
        {'MISDEMEANOR': 'Misdemeanor', 'FELONY': 'Felony', 'VIOLATION': 'Violation'})
    # creates a series object of value counts of the law
    complaint_offense_count = complaints_df['law_cat_cd'].value_counts()
    # converts it into a dataframe
    complaint_offense_count_dataframe = pd.DataFrame(complaint_offense_count)
    # changes the index column name to be merged later with arrests data
    complaint_offense_count_dataframe.index.names = ['OFFENSE']
    # creates a new row since the arrests data has an extra offense type
    complaint_offense_count_dataframe.loc['Infraction'] = 0
    # adds some metadata about the created dataframe as a dagster asset
    context.add_output_metadata(
        metadata={
            "Complaint Counts Per Offense": MetadataValue.md(complaint_offense_count_dataframe.to_markdown()),
        }
    )

    # creates a connection to the postgres server
    engine = create_engine(postgres_connection_string)
    # uses the pandas to_sql function to load the dataframe into postgres
    complaint_offense_count_dataframe.to_sql(
        'value_counts_of_complaints_offenses', engine, if_exists='replace')

    return complaint_offense_count_dataframe


# Asset for creating a groupby object which converts into a dataframe consisting of the complaints by age group
@asset
def complaints_by_age_group(
    context: AssetExecutionContext,
    complaints_df
) -> pd.DataFrame:
    # drops the observation for which vic_sex is unknown
    complaints_df.drop(
        complaints_df[(complaints_df['vic_sex'] == 'U')].index, inplace=True)
    # formats the vic_sex values into proper string for the visualisation
    complaints_df['vic_sex'] = complaints_df['vic_sex'].map(
        {'F': 'Female', 'M': 'Male', 'L': 'Unknown', 'E': 'Anonymous'})
    # creates the age groups to be filtered since there are many age groups
    real_age_group = ['<18', '18-24', '25-44', '45-64', '65+']
    # filters the data frame for age groups within our defined set of age groups
    complaints_by_age_group = complaints_df[complaints_df['vic_age_group'].isin(
        real_age_group)]
    # adds some metadata about the created dataframe as a dagster asset
    context.add_output_metadata(
        metadata={
            "Cleaned DataFrame for Select Age Groups": MetadataValue.md(complaints_by_age_group.head().to_markdown()),
        }
    )

    # creates a connection to the postgres server
    engine = create_engine(postgres_connection_string)
    # uses the pandas to_sql function to load the dataframe into postgres
    complaints_by_age_group.to_sql(
        "complaints_by_age_group", engine, if_exists='replace')

    return complaints_by_age_group


# Asset for creating a Bar Chart of Complaints by Age Group and Gender
@asset
def bar_chart_complaints_by_age_group_and_gender(complaints_by_age_group) -> MaterializeResult:
    # creates and Age Group array of the age groups of interest
    real_age_group = ['<18', '18-24', '25-44', '45-64', '65+']
    # sets the theme for better colors
    sns.set_theme(style="darkgrid")
    # increases size of the plot
    fig, ax = plt.subplots(figsize=(10, 8))
    # creates the actual count plot using vic_sex for the legend and orders it according to our chosen age groups
    sns.countplot(data=complaints_by_age_group, x='vic_age_group',
                  hue='vic_sex', palette='viridis', order=real_age_group)
    # sets the X Label
    plt.xlabel('Victims Age Group')
    # Sets the Y Label
    plt.ylabel('Count of complaints')

    # rotates the x values on top of each bar for better visibility
    ax.bar_label(ax.containers[0], rotation=90, padding=10)
    ax.bar_label(ax.containers[1], rotation=90, padding=10)
    ax.bar_label(ax.containers[2], rotation=90, padding=10)
    ax.bar_label(ax.containers[3], rotation=90, padding=10)
    plt.tight_layout()

    # starts a buffer stream to take the created plot as input
    buffer = BytesIO()
    # saves the created plot as a png file
    plt.savefig(buffer, format="png")
    # encodes the created plot in b64 encoding
    image_data = base64.b64encode(buffer.getvalue())
    # creating a string to be stored in markdown by decoding the created b64 encoding
    md_content = f"![img](data:image/png;base64,{image_data.decode()})"

    # materializing the created plot as a string output in markdown to be stored as a dagster asset
    return MaterializeResult(
        metadata={
            "Complaints by Age Group and Gender": MetadataValue.md(md_content)}
    )


# Asset for creating a dataframe containing only the observations for the most common age group 25-44
@asset
def complaints_for_most_common_age_group(
    context: AssetExecutionContext,
    complaints_by_age_group
) -> pd.DataFrame:
    # creates a subset of the filtered dataframe to include only the age group of interest
    complaints_for_most_common_age_group = complaints_by_age_group[
        complaints_by_age_group['vic_age_group'] == '25-44']
    # adds some metadata about the created dataframe as a dagster asset
    context.add_output_metadata(
        metadata={
            "Complaints For Most Common Age Group": MetadataValue.md(complaints_for_most_common_age_group.head().to_markdown()),
        }
    )

    # creates a connection to the postgres server
    engine = create_engine(postgres_connection_string)
    # uses the pandas to_sql function to load the dataframe into postgres
    complaints_for_most_common_age_group.to_sql(
        'complaints_for_most_common_age_group', engine, if_exists='replace')

    return complaints_for_most_common_age_group


# Asset for creating the bar chart for the Complaints made by the most common age group 25-44
@asset
def bar_chart_complaints_for_most_common_age_group(complaints_for_most_common_age_group) -> MaterializeResult:
    # Query the dataset only for Female or Male observations
    age_com_data_gender = pd.DataFrame(complaints_for_most_common_age_group).query(
        'vic_sex== "Female" or vic_sex == "Male"')
    sns.set_theme(style="darkgrid")
    plt.subplots(figsize=(22, 14))
    # Create a list of Races for which to create the legend and groups with gender
    original_race = ['UNKNOWN', 'BLACK', 'WHITE', 'ASIAN / PACIFIC ISLANDER',
                     'WHITE HISPANIC', 'AMERICAN INDIAN/ALASKAN NATIVE',
                     'BLACK HISPANIC']
    # plot the bar charts with sex and gender as the grouping
    sns.countplot(data=age_com_data_gender, x='vic_race',
                  hue='vic_sex', palette='viridis', order=original_race)
    plt.xlabel('Victims Race')
    plt.ylabel('Count of complaints')
    # plots the values on top of each bar in the chart
    for p in plt.gca().patches:
        plt.gca().annotate(f'{p.get_height()}', (p.get_x() + p.get_width() / 2., p.get_height()),
                           ha='center', va='bottom', color='black', fontsize=10)

    plt.tight_layout()
    # starts a buffer stream to take the created plot as input
    buffer = BytesIO()
    # saves the created plot as a png file
    plt.savefig(buffer, format="png")
    # encodes the created plot in b64 encoding
    image_data = base64.b64encode(buffer.getvalue())
    # creating a string to be stored in markdown by decoding the created b64 encoding
    md_content = f"![img](data:image/png;base64,{image_data.decode()})"

    # materializing the created plot as a string output in markdown to be stored as a dagster asset
    return MaterializeResult(
        metadata={
            "Complaints by Age Group and Gender": MetadataValue.md(md_content)}
    )

#
#
#    Arrests Related Assets
#
#


# Asset for creating the assets dataframe to be used by all downstream assets
@asset(deps=[extract_arrests])
def arrests_df(
    context: AssetExecutionContext
) -> pd.DataFrame:

    # creates a connection to mongodb server
    client = MongoClient(mongo_connection_string)
    arrests_db = client["nypd_analysis"]
    collection_name = "arrests"
    collection = arrests_db[collection_name]
    # finds all the related documents in the collection
    data = list(collection.find())
    client.close()
    # creates a data frame to be used
    arrests_df = pd.DataFrame(data)
    # adds some metadata related to the created data frame to be stored as a dagster asset
    context.add_output_metadata(
        metadata={
            "num_records": len(arrests_df),
            "preview": MetadataValue.md(arrests_df.head().to_markdown()),
        }
    )

    return arrests_df


# Asset for creating a dataframe consisting of the value counts of each type of felony in the arrests dataset
@asset
def value_counts_of_arrests_offenses(
    context: AssetExecutionContext,
    arrests_df
) -> pd.DataFrame:
    # creates a series object of the value counts of each type of felony in the law_cat_cd column
    arrests_offenses_count = arrests_df['law_cat_cd'].value_counts()
    # converts the series object into a dataframe
    arrests_offenses_count_dataframe = pd.DataFrame(arrests_offenses_count)
    # changes the index of the created dataframe to OFFENSES for merging with the similar dataframe created in complaints
    arrests_offenses_count_dataframe.index.names = ['OFFENSE']
    # drops the irregular values of felony types for which the value was an integer called 9
    arrests_offenses_count_dataframe.drop(['9'], inplace=True)
    # formats the values inthe dataframe to proper strings to be used in the visuals as well as match the ones used in the complaints_offenses_count dataframe
    arrests_offenses_count_dataframe.rename(
        index={'M': 'Misdemeanor', 'F': 'Felony', 'V': 'Violation', 'I': 'Infraction'}, inplace=True)
    # adds some metadata about the created dataframe to be used
    context.add_output_metadata(
        metadata={
            "Arrest Counts Per Offense": MetadataValue.md(arrests_offenses_count_dataframe.to_markdown()),
        }
    )

    # creates a connection to the postgres server
    engine = create_engine(postgres_connection_string)
    # uses the pandas to_sql function to load the dataframe into postgres
    arrests_offenses_count_dataframe.to_sql(
        "arrests_offenses_counts", engine, if_exists='replace')

    return arrests_offenses_count_dataframe


# Asset for creating the bar chart of Arrest by Age Group and The genders within each age group
@asset
def bar_chart_arrests_by_age_and_gender(arrests_df) -> MaterializeResult:
    arrests_df.perp_sex = arrests_df.perp_sex.map({'F': 'Female', 'M': 'Male'})
    arrests_df.dropna(subset=['perp_sex'], inplace=True)
    arrest_age_group = ['<18', '18-24', '25-44', '45-64', '65+']

    plt.figure(figsize=(8, 6))
    sns.set_theme(style='darkgrid')
    sns.countplot(data=arrests_df, x='age_group',
                  hue='perp_sex', order=arrest_age_group)
    plt.title('Arrests Count by Age Group and Gender')
    plt.xlabel('Age Group')
    plt.ylabel('Count of Arrests')
    plt.tight_layout()
    for p in plt.gca().patches:
        plt.gca().annotate(f'{p.get_height()}', (p.get_x() + p.get_width() / 2., p.get_height()),
                           ha='center', va='bottom', color='black', fontsize=10)

    # starts a buffer stream to take the created plot as input
    buffer = BytesIO()
    # saves the created plot as a png file
    plt.savefig(buffer, format="png")
    # encodes the created plot in b64 encoding
    image_data = base64.b64encode(buffer.getvalue())
    # creating a string to be stored in markdown by decoding the created b64 encoding
    md_content = f"![img](data:image/png;base64,{image_data.decode()})"

    # materializing the created plot as a string output in markdown to be stored as a dagster asset
    return MaterializeResult(
        metadata={
            "Arrests by Age Group and Gender": MetadataValue.md(md_content)}
    )


# Asset for creating the dataframe consisting of the observations only related to the most common age group 25-44
@asset
def arrests_for_most_common_age_group(
    context: AssetExecutionContext,
    arrests_df
) -> pd.DataFrame:
    # creates a subset of the dataframe consisting of the observations only related to the most common age group
    arrests_for_most_common_age_group = arrests_df[arrests_df['age_group'] == '25-44']
    # adds some metadata about the created dataframe to be viewed inside dagster
    context.add_output_metadata(
        metadata={
            "Arrests For The Most Common Age Group (25-44)": MetadataValue.md(arrests_for_most_common_age_group.head().to_markdown()),
        }
    )
    # drops the leftover column from mongodb
    arrests_for_most_common_age_group.drop(
        ['_id', 'geocoded_column'], axis=1, inplace=True)
    # creates a connection to the postgres server
    engine = create_engine(postgres_connection_string)
    # uses the pandas to_sql function to load the dataframe into postgres
    arrests_for_most_common_age_group.to_sql('arrests_for_most_common_age_group',
                                             engine, if_exists='replace')

    return arrests_for_most_common_age_group


# Asset for creating the count plot of felony types per race in the arrests dataset
@asset
def bar_chart_arrests_by_felony_type_for_most_common_age_group(arrests_for_most_common_age_group) -> MaterializeResult:
    sns.set_theme(style="whitegrid")
    plt.subplots(figsize=(22, 14))
    # creates the plot for the arrests by race for the most common age group 25044
    sns.countplot(data=arrests_for_most_common_age_group,
                  x='perp_race', hue='perp_sex', palette='pastel')
    plt.xlabel('Race of arrested people')
    plt.ylabel('Count of Arrests')
    plt.tight_layout()
    for p in plt.gca().patches:
        plt.gca().annotate(f'{p.get_height()}', (p.get_x() + p.get_width() / 2., p.get_height()),
                           ha='center', va='bottom', color='black', fontsize=10)

    # starts a buffer stream to take the created plot as input
    buffer = BytesIO()
    # saves the created plot as a png file
    plt.savefig(buffer, format="png")
    # encodes the created plot in b64 encoding
    image_data = base64.b64encode(buffer.getvalue())
    # creating a string to be stored in markdown by decoding the created b64 encoding
    md_content = f"![img](data:image/png;base64,{image_data.decode()})"

    # materializing the created plot as a string output in markdown to be stored as a dagster asset
    return MaterializeResult(
        metadata={
            "Arrests by Age Group and Gender": MetadataValue.md(md_content)}
    )


# Asset for creating a dataframe consisting of value counts of arrests by borough
@asset
def value_counts_of_arrests_by_area(
    context: AssetExecutionContext,
    arrests_df
) -> pd.DataFrame:

    arrests_df['arrest_boro'] = arrests_df['arrest_boro'].map(
        {'K': 'BROOKLYN', 'B': 'BRONX', 'M': 'MANHATTAN', 'Q': 'QUEENS', 'S': 'STATEN ISLAND'})
    arrest_boro_dataframe = pd.DataFrame(
        arrests_df['arrest_boro'].value_counts())
    arrest_boro_dataframe = arrest_boro_dataframe.rename(
        columns={'ARREST_BORO': 'ARRESTS'})
    arrest_boro_dataframe.index.names = ['BORO']

    # add some basic metadata about the query as a dagster asset
    context.add_output_metadata(
        metadata={
            "num_records": len(arrest_boro_dataframe),
            "preview": MetadataValue.md(arrest_boro_dataframe.to_markdown()),
        }
    )

    # creates a connection to the postgres server
    engine = create_engine(postgres_connection_string)
    # uses the pandas to_sql function to load the dataframe into postgres
    arrest_boro_dataframe.to_sql(
        'value_counts_of_arrests_by_area', engine, if_exists='replace')

    return arrest_boro_dataframe


#
#
#  Arrests AND Complaints Related Assets
#
#

# Asset for creating the merged dataframe of Counts of Felony Types in both the Arrests and Complaints Datasets
@asset
def value_counts_of_arrests_and_complaints(
    context: AssetExecutionContext,
    value_counts_of_arrests_offenses,
    value_counts_of_complaints_offenses
) -> pd.DataFrame:
    # merges the two dataframes using the OFFENSES index
    complaint_arrest_joined = pd.merge(value_counts_of_complaints_offenses,
                                       value_counts_of_arrests_offenses, left_index=True, right_index=True)
    # renames the count columns into more understandable column names
    complaint_arrest_joined = complaint_arrest_joined.rename(
        columns={'count_x': 'Complaint Offense', 'count_y': 'Arrest Offense'})
    # adds some metadata related to the created dataframe to be stored in the dagster asset
    context.add_output_metadata(
        metadata={
            "Joined Dataframe for Arrests and Complaints per Offense": MetadataValue.md(complaint_arrest_joined.to_markdown()),
        }
    )

    # creates a connection to the postgres server
    engine = create_engine(postgres_connection_string)
    # uses the pandas to_sql function to load the dataframe into postgres
    complaint_arrest_joined.to_sql(
        'value_counts_of_arrests_and_complaints', engine, if_exists='replace')
    return complaint_arrest_joined

# Asset for creating the bar chart using the merged dataframe of felony type for Arrests and Complaints


@asset
def bar_chart_types_of_offenses_in_arrests_and_complaints(value_counts_of_arrests_and_complaints) -> MaterializeResult:
    # increases the size of the plot
    fig, ax = plt.subplots(figsize=(10, 6))
    # creates the plot with the merged dataframe for types of felonies in both complaints and arrests
    value_counts_of_arrests_and_complaints.plot(kind='bar', ax=ax, width=0.8)

    # Adding labels and title
    ax.set_xlabel('OFFENSE')
    ax.set_ylabel('Count')
    ax.set_title('Comparison of Offense')
    for p in plt.gca().patches:
        plt.gca().annotate(f'{p.get_height()}', (p.get_x() + p.get_width() / 2., p.get_height()),
                           ha='center', va='bottom', color='black', fontsize=10)
    plt.tight_layout()
    # starts a buffer stream to take the created plot as input
    buffer = BytesIO()
    # saves the created plot as a png file
    plt.savefig(buffer, format="png")
    # encodes the created plot in b64 encoding
    image_data = base64.b64encode(buffer.getvalue())
    # creating a string to be stored in markdown by decoding the created b64 encoding
    md_content = f"![img](data:image/png;base64,{image_data.decode()})"

    # materializing the created plot as a string output in markdown to be stored as a dagster asset
    return MaterializeResult(
        metadata={
            "Comparison of Offenses in Complaints and Arrests": MetadataValue.md(md_content)}
    )


#
#
#  Arrests AND Shootings Related Assets
#
#


# Asset for creating a dataframe consisting of value counts of shootings and arrests by borough
@asset
def value_counts_of_arrests_and_shootings_by_area(
    context: AssetExecutionContext,
    value_counts_of_arrests_by_area,
    value_counts_of_shootings_by_area
) -> pd.DataFrame:
    arrests_and_shootings_by_area = pd.merge(
        value_counts_of_shootings_by_area, value_counts_of_arrests_by_area, left_index=True, right_index=True)

    arrests_and_shootings_by_area = arrests_and_shootings_by_area.rename(
        columns={'count_x': 'Shootings', 'count_y': 'Arrests'})

    # add some basic metadata about the query as a dagster asset
    context.add_output_metadata(
        metadata={
            "num_records": len(arrests_and_shootings_by_area),
            "preview": MetadataValue.md(arrests_and_shootings_by_area.to_markdown()),
        }
    )

    # creates a connection to the postgres server
    engine = create_engine(postgres_connection_string)
    # uses the pandas to_sql function to load the dataframe into postgres
    arrests_and_shootings_by_area.to_sql(
        'value_counts_of_arrests_and_shootings_by_area', engine, if_exists='replace')

    return arrests_and_shootings_by_area


# Asset for creating a merged data frame consisting of value counts of shootings and arrests by borough as well as geopandas polygon data
@asset
def geopandas_dataframe_of_arrests_and_shootings_by_area(
    context: AssetExecutionContext,
    value_counts_of_arrests_and_shootings_by_area
) -> pd.DataFrame:
    nyc_shp = geopandas.read_file(geopandas.datasets.get_path('nybb'))
    # create a dataframe
    con_fa_nyc = pd.DataFrame()
    con_fa_nyc['BoroName'] = ['Bronx', 'Brooklyn',
                              'Manhattan', 'Queens', 'Staten Island']
    con_fa_nyc['Shootings'] = value_counts_of_arrests_and_shootings_by_area['Shootings'].tolist()
    con_fa_nyc['Arrests'] = value_counts_of_arrests_and_shootings_by_area['Arrests'].tolist()
    con_fa_nyc['longitude'] = [985000, 1000000, 970000, 1040000, 925000]
    con_fa_nyc['latitude'] = [180000, 250000, 220000, 200000, 150000]
    # merge con_fa_nyc and nyc_shp
    nyc_shp = nyc_shp.merge(con_fa_nyc, on='BoroName')

    # add some basic metadata about the query as a dagster asset
    context.add_output_metadata(
        metadata={
            "num_records": len(nyc_shp),
            "preview": MetadataValue.md(nyc_shp.to_markdown())
        }
    )

    return nyc_shp


# Asset for creating a map consisting of value counts of shootings and arrests by borough as well as geopandas polygon data
@asset
def map_of_arrests_and_shootings_by_area(geopandas_dataframe_of_arrests_and_shootings_by_area) -> MaterializeResult:

    # plot new york city
    ax = geopandas_dataframe_of_arrests_and_shootings_by_area.plot(column='Arrests', figsize=(
        10, 10), alpha=0.5, edgecolor='k', cmap='Reds', legend=True, scheme="quantiles")
    # add boroughs' names with numbers of confirmed cases and fatalities
    for i in range(len(geopandas_dataframe_of_arrests_and_shootings_by_area)):
        plt.text(geopandas_dataframe_of_arrests_and_shootings_by_area.longitude[i], geopandas_dataframe_of_arrests_and_shootings_by_area.latitude[i], "{}\nArrests: {}\nShootings: {}".format(
            geopandas_dataframe_of_arrests_and_shootings_by_area.BoroName[i], geopandas_dataframe_of_arrests_and_shootings_by_area.Arrests[i], geopandas_dataframe_of_arrests_and_shootings_by_area.Shootings[i]), size=13)
    leg = ax.get_legend()
    leg.set_bbox_to_anchor((1.3, 1))
    plt.tight_layout()
    # starts a buffer stream to take the created plot as input
    buffer = BytesIO()
    # saves the created plot as a png file
    plt.savefig(buffer, format="png")
    # encodes the created plot in b64 encoding
    image_data = base64.b64encode(buffer.getvalue())
    # creating a string to be stored in markdown by decoding the created b64 encoding
    md_content = f"![img](data:image/png;base64,{image_data.decode()})"

    # materializing the created plot as a string output in markdown to be stored as a dagster asset
    return MaterializeResult(
        metadata={
            "Shootings And Arrests By Area for 2023": MetadataValue.md(md_content)}
    )
