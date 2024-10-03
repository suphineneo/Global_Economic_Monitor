import pandas as pd
import numpy as np
import os
import requests
import openpyxl
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter, Retry
import sqlalchemy
import logging
import time
from sqlalchemy import Column, Float, Integer, MetaData, String, Table, create_engine
from etl_project.connectors.postgres_industrial import PostgreSqlClient
from etl_project.assets.pipeline_logging import PipelineLogging
from dotenv import load_dotenv


def setup_pipeline_logging(pipeline_name: str, log_folder_path: str):
    # Initialize logger
    logger = logging.getLogger(pipeline_name)
    logger.setLevel(logging.INFO)

    # Create log file path
    file_path = f"{log_folder_path}/{pipeline_name}_{time.time()}.log"

    # Create handlers
    file_handler = logging.FileHandler(file_path)
    stream_handler = logging.StreamHandler()

    # Set logging levels
    file_handler.setLevel(logging.INFO)
    stream_handler.setLevel(logging.INFO)

    # Create formatters and add them to the handlers
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)

    # Add handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger, file_path


def get_logs(file_path: str) -> str:
    with open(file_path, "r") as file:
        return "".join(file.readlines())

class WorldBankDataLoader:
    def __init__(self, indicator, start_year, end_year):
        """
        Initialize the WorldBankDataLoader with the indicator, start year, and end year.
        
        Args:
        - indicator (str): The indicator code for the World Bank API (e.g., 'NV.IND.TOTL.KD.ZG').
        - start_year (str): The starting year for the data.
        - end_year (str): The ending year for the data.
        """
        self.indicator = indicator
        self.date_range = f"{start_year}:{end_year}"
        self.base_url = f"https://api.worldbank.org/v2/countries/all/indicators/{self.indicator}?"
        self.params = {
            "date": self.date_range,
            "format": "json",
            "page": 1  # Start at page 1
        }
        self.all_data = []  # To store paginated data

    def fetch_data(self):
        """
        Fetch data from the World Bank API, handling pagination until all data is retrieved.
        
        Returns:
        - pd.DataFrame: A pandas DataFrame containing all the data fetched from the API.
        """
        while True:
            response = requests.get(self.base_url, params=self.params)
            response_data = response.json()

            # Check if valid data is returned
            if len(response_data) < 2 or not response_data[1]:
                break

            # Extend the all_data list with the current page's data
            self.all_data.extend(response_data[1])

            # Increment page number for the next API call
            self.params["page"] += 1

        # Normalize and return the data as a pandas DataFrame
        return pd.json_normalize(data=self.all_data)
    
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and filter the data for specific countries, remove NaN values, 
        and save the result to a CSV file.
        
        Args:
        - df (pd.DataFrame): The DataFrame to be transformed.
        
        Returns:
        - pd.DataFrame: The cleaned and filtered DataFrame.
        """
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

        df_cleaned.reset_index(drop=True, inplace=True)

        df_cleaned = df_cleaned.astype({"year": int})

        print("Completed transform")

        try:
            df_cleaned.to_csv("etl_project/data/industrial_cleaned_export_data.csv", index=False)
            print("Data saved successfully to cleaned_export_data.csv")
        except Exception as e:
            print(f"Error saving data to CSV: {e}")

        return df_cleaned
    
    def load(self, df: pd.DataFrame, postgresql_client, table: Table, metadata: MetaData, load_method: str = "overwrite"):
        """
        Load the cleaned data into a PostgreSQL database.
        
        Args:
        - df (pd.DataFrame): The cleaned DataFrame to be loaded.
        - postgresql_client: A PostgreSQL client instance to execute the load.
        - table (Table): The SQLAlchemy Table object representing the target table.
        - metadata (MetaData): The SQLAlchemy MetaData object representing the schema.
        - load_method (str): The method for loading data: 'insert', 'upsert', or 'overwrite'.
        """
        # Create the upsert statement based on the load method
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


# Example usage
if __name__ == "__main__":

    load_dotenv()
    DB_USERNAME = os.getenv("DB_USERNAME")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    SERVER_NAME = os.getenv("SERVER_NAME")
    DATABASE_NAME = os.getenv("DATABASE_NAME")
    PORT = os.environ.get("PORT")
    # Create an instance of the loader for the specific indicator and date range
    loader = WorldBankDataLoader(
        indicator="NV.IND.TOTL.KD.ZG", start_year="2020", end_year="2024"
    )

    logger, log_file = setup_pipeline_logging("worldbankdata_industrial", "etl_project/logs")
    logger.info("Making api connection")
    logs = get_logs(log_file)
    print(logs)

    # Fetch the data
    logger.info("Starting to fetch data")
    df_data = loader.fetch_data()
    logger.info("Data has been fetched")

    logger.info("Starting Transformation")
    logger.info("Progressing through my transformations")
    cleaned_df = loader.transform(df_data)
    logger.info("Transformations have been finished")

    logger.info("Starting to load into my Postgres")
    logger.info("gathering my postgres server info")
    postgresql_client = PostgreSqlClient(
    server_name=SERVER_NAME,
    database_name=DATABASE_NAME,
    username=DB_USERNAME,
    password=DB_PASSWORD,
    port=PORT,
    )
    logger.info("completed gathering my postgres credential info")
    logger.info("Calling MetaData and creating industrial_data table")
    metadata = MetaData()
    export_table = Table(
        "industrial_data",
        metadata,
        Column("year", Integer, primary_key=True),
        Column("country_code", String, primary_key = True),
        Column("country_name", String),
        Column("indicator_id", String),
        Column("indicator_value", String),
        Column("value", Float),
    )
    logger.info("Doing the Load:::::::>>>>>>>")
    loader.load(
        cleaned_df,
        postgresql_client=postgresql_client,
        table=export_table,
        metadata=metadata,
        load_method="upsert"
    ) 
    logger.info("Load ================>>>>>>>100 percent complete")
    logger.info("pipeline finished")
