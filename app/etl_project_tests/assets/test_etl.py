import os
from pathlib import Path
from etl_project.assets.extract_load_transform import extract, transform, load
import pytest
import requests
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import Table, MetaData, Column, String, Integer
from etl_project.connectors.postgresql import PostgreSqlClient
from etl_project.connectors.data_fetcher import fetch_data_from_api


# test connection to API
@pytest.fixture
def setup_api_connection():
    load_dotenv()


def test_fetch_data_from_api(setup_api_connection):
    # Use real parameters suitable for your API call
    wb_indicator = "SL.UEM.TOTL.ZS"  # Example indicator
    wb_daterange = "2023:2023"  # Example date range

    # Call the fetch_data_from_api function
    df = fetch_data_from_api(wb_indicator, wb_daterange)

    # Perform assertions
    assert not df.empty, "DataFrame is empty"
    assert "date" in df.columns, "Date column is missing in the DataFrame"
    assert "value" in df.columns, "Value column is missing in the DataFrame"


@pytest.fixture
def setup_extract():
    load_dotenv()


def test_extract(setup_extract):
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    PORT = os.environ.get("PORT")

    postgresql_client = PostgreSqlClient(
        server_name=SERVER_NAME,
        database_name=DATABASE_NAME,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        port=PORT,
    )

    df = extract(
        postgresql_client, "full", "year", "unemployment", "SL.UEM.TOTL.ZS", "2023:2023"
    )

    assert not df.empty, "DataFrame is empty"
    assert len(df) == 266  # 266 rows before transformation


@pytest.fixture
def setup_input_GEM_df():
    return pd.DataFrame(
        [
            {
                "countryiso3code": "SGP",
                "date": "2023",
                "value": 3.472,
                "unit": "",
                "obs_status": "",
                "decimal": 1,
                "indicator.id": "SL.UEM.TOTL.ZS",
                "indicator.value": "Unemployment, total (% of total labor force) (modeled ILO estimate)",
                "country.id": "SG",
                "country.value": "Singapore",
            },
            {
                "countryiso3code": "NZL",
                "date": "2023",
                "value": 3.737,
                "unit": "",
                "obs_status": "",
                "decimal": 1,
                "indicator.id": "SL.UEM.TOTL.ZS",
                "indicator.value": "Unemployment, total (% of total labor force) (modeled ILO estimate)",
                "country.id": "NZ",
                "country.value": "New Zealand",
            },
        ]
    )


# columns in original df = ['countryiso3code', 'date', 'value', 'unit', 'obs_status', 'decimal',
#       'indicator.id', 'indicator.value', 'country.id', 'country.value'],
#      dtype='object')


@pytest.fixture
def setup_transformed_gem_df():
    data = [
        {
            "year": 2023,
            "country_code": "SGP",
            "country_name": "Singapore",
            "indicator_id": "SL.UEM.TOTL.ZS",
            "indicator_value": "Unemployment, total (% of total labor force) (modeled ILO estimate)",
            "value": 3.472,
            "region": "East Asia & Pacific",
        },
        {
            "year": 2023,
            "country_code": "NZL",
            "country_name": "New Zealand",
            "indicator_id": "SL.UEM.TOTL.ZS",
            "indicator_value": "Unemployment, total (% of total labor force) (modeled ILO estimate)",
            "value": 3.737,
            "region": "East Asia & Pacific",
        },
    ]

    df = pd.DataFrame(data)
    df["year"] = df["year"].astype("int32")
    return df


def test_transform(setup_input_GEM_df, setup_transformed_gem_df):
    region_file_path = r"..\\data\\CLASS_CSV.csv"
    expected_df = setup_transformed_gem_df
    df = transform(df=setup_input_GEM_df, region_file_path=region_file_path)
    pd.testing.assert_frame_equal(left=df, right=expected_df, check_exact=True)
