import os
import requests
import pandas as pd
from datetime import datetime, timezone
import yaml
from pathlib import Path
from importlib import import_module


# extract
def extract_unemp(wb_indicator, wb_daterange) -> pd.DataFrame:

    indicator = wb_indicator
    date_range = wb_daterange
    # base_url = f"https://api.worldbank.org/v2/countries/all/indicators/{indicator}?{date_range}&format=json"
    base_url = f"https://api.worldbank.org/v2/countries/all/indicators/{indicator}?"
    params = {"date": date_range, "format": "json", "page": 1}  # Start at page 1

    # response_data = []
    all_data = []

    while True:
        response = requests.get(base_url, params=params)
        response_data = response.json()

        if len(response_data) < 2 or not response_data[1]:  # Check if there's data
            break

        all_data.extend(response_data[1])  # Add current page data to all_data

        # Update parameters for the next page
        params["page"] += 1

    df_unemp = pd.json_normalize(data=all_data)
    print(df_unemp)
    return df_unemp


# transfom
def transform_unemp(df_unemp: pd.DataFrame, region_file_path) -> pd.DataFrame:
    # rename
    df_unemp_renamed = df_unemp.rename(
        columns={
            "date": "year",
            "countryiso3code": "country_code",
            "country.value": "country_name",
            "indicator.id": "indicator_id",
            "indicator.value": "indicator_value",
            "value": "value",
        }
    )

    # keep only selected columns
    df_unemp_selected = df_unemp_renamed[
        [
            "year",
            "country_code",
            "country_name",
            "indicator_id",
            "indicator_value",
            "value",
        ]
    ]

    df_region = pd.read_csv(region_file_path)  # "data/CLASS_CSV.csv"

    # merge with region class file
    df_final = pd.merge(
        left=df_unemp_selected,
        right=df_region,
        left_on="country_code",
        right_on="Code",
    )

    print(df_final)


# load into postgres


if __name__ == "__main__":
    # load_dotenv()
    # LOGGING_SERVER_NAME = os.environ.get("LOGGING_SERVER_NAME")
    # LOGGING_DATABASE_NAME = os.environ.get("LOGGING_DATABASE_NAME")
    # LOGGING_USERNAME = os.environ.get("LOGGING_USERNAME")
    # LOGGING_PASSWORD = os.environ.get("LOGGING_PASSWORD")
    # LOGGING_PORT = os.environ.get("LOGGING_PORT")

    # get config variables

    yaml_file_path = "gem.yaml"

    if Path(yaml_file_path).exists():
        with open(yaml_file_path) as yaml_file:
            config = yaml.safe_load(yaml_file)
            wb_config = config.get("config")
            wb_indicator = wb_config.get("indicator_unemp")
            wb_daterange = wb_config.get("date_range")
            region_file_path = wb_config.get("region_classification_path")

            # pipeline_config = yaml.safe_load(yaml_file)
            # PIPELINE_NAME = pipeline_config.get("indicator")
    else:
        raise Exception(
            f"Missing {yaml_file_path} file! Please create the yaml file with at least a `name` key for the pipeline name."
        )

    # extract
    df_unemp = extract_unemp(wb_indicator=wb_indicator, wb_daterange=wb_daterange)
    # transform
    transform_unemp(df_unemp, region_file_path=region_file_path)
    # load into postgres
    # ---will use load code from ming/gianby's file
