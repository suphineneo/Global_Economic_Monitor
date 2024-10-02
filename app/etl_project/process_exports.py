# import requests
import pandas as pd
import requests
import yaml
from dotenv import load_dotenv
import os
from pathlib import Path
from sqlalchemy import Column, Float, Integer, MetaData, String, Table, create_engine
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import URL
from etl_project.connectors.postgresql import PostgreSqlClient
from etl_project.assets.pipeline_logging import PipelineLogging
import schedule
import time


def extract(indicator, date_range):

    print("Starting extract")
    base_url = f"https://api.worldbank.org/v2/countries/all/indicators/{indicator}"

    params = {"date": date_range, "format": "json", "page": 1}  # Start at page 1

    export_data = []

    while True:
        response = requests.get(base_url, params=params)

        if response.status_code != 200:
            print(f"Error: Unable to fetch data (Status code: {response.status_code})")
            break
        # print(response.status_code)
        response_data = response.json()

        if len(response_data) < 2 or not response_data[1]:  # Check if there's data
            print("No data available.")
            break

        # Add current page data to all_data
        export_data.extend(response_data[1])
        # Update parameters for the next page
        params["page"] += 1

    df_export = pd.json_normalize(data=export_data)

    return pd.DataFrame(df_export)


def transform(df: pd.DataFrame) -> pd.DataFrame:
    print("Starting transform for exports")

    df_selected = df[
        [
            "date",
            "countryiso3code",
            "country.value",
            "indicator.id",
            "indicator.value",
            "value",
        ]
    ]

    df_renamed = df_selected.rename(
        columns={
            "date": "year",
            "countryiso3code": "country_code",
            "country.value": "country_name",
            "indicator.id": "indicator_id",
            "indicator.value": "indicator_value",
        }
    )

    # Remove NaN from the Year and value column
    df_cleaned = df_renamed.dropna(subset=["year"]).dropna(subset=["value"])

    # Filter the DataFrame to include only rows where 'country' is Australia, New Zealand, or Italy
    df_cleaned = df_cleaned[
        df_cleaned["country_name"].isin(
            ["Australia", "New Zealand", "Italy", "United States"]
        )
    ]
    df_cleaned.reset_index(drop=True, inplace=True)

    df_cleaned = df_cleaned.astype({"year": int})

    try:
        df_cleaned.to_csv("data/cleaned_export_data.csv", index=False)
        print("Data saved successfully to cleaned_export_data.csv")
    except Exception as e:
        print(f"Error saving data to CSV: {e}")

    return df_cleaned


def load(
    df: pd.DataFrame,
    postgresql_client: PostgreSqlClient,
    table: Table,
    metadata: MetaData,
    load_method: str = "overwrite",
):
    # Create the upsert statement
    if load_method == "insert":
        postgresql_client.insert(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    elif load_method == "upsert":
        postgresql_client.upsert(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    elif load_method == "overwrite":
        postgresql_client.overwrite(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    else:
        raise Exception(
            "Please specify a correct load method: [insert, upsert, overwrite]"
        )


# Define a unified function to run the entire ETL pipeline
def run_pipeline():
    pipeline_logging.logger.info("Starting ETL pipeline")

    # Execute Extract
    df_extracted = extract(
        indicator=config["indicator_export"], date_range=config["date_range"]
    )
    pipeline_logging.logger.info("Extract step completed")

    # Execute Transform
    df_transformed = transform(df_extracted)
    pipeline_logging.logger.info("Transform step completed")

    # Execute Load
    postgresql_client = PostgreSqlClient(
        server_name=SERVER_NAME,
        database_name=DATABASE_NAME,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        port=PORT,
    )

    metadata = MetaData()
    export_table = Table(
        "exports",
        metadata,
        Column("year", Integer, primary_key=True),
        Column("country_code", String, primary_key=True),
        Column("country_name", String),
        Column("indicator_id", String),
        Column("indicator_value", String),
        Column("value", Float),
    )

    load(df_transformed, postgresql_client, export_table, metadata)
    pipeline_logging.logger.info("Load step completed")
    pipeline_logging.logger.info("Pipeline run complete")


if __name__ == "__main__":

    load_dotenv()
    # Retrieve the database configurations from environment variables
    DB_USERNAME = os.getenv("DB_USERNAME")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    SERVER_NAME = os.getenv("SERVER_NAME")
    DATABASE_NAME = os.getenv("DATABASE_NAME")
    PORT = os.environ.get("PORT")

    # Define the path to the YAML configuration file
    yaml_file_path = "etl_project/gem.yaml"

    if Path(yaml_file_path).exists():
        with open(yaml_file_path) as yaml_file:
            config = yaml.safe_load(yaml_file)
    else:
        raise Exception(
            f"Missing {yaml_file_path} file! Please create the yaml file with at least a `name` key for the pipeline name."
        )

    pipeline_logging = PipelineLogging(
        pipeline_name="worldbankdata_exports", log_folder_path="etl_project/logs"
    )
    pipeline_logging.logger.info("Making api connection")

    # set schedule
    schedule.every(config.get("schedule").get("run_seconds")).seconds.do(run_pipeline)

    while True:
        schedule.run_pending()
        time.sleep(config.get("schedule").get("poll_seconds"))
