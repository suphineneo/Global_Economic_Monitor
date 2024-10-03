import pandas as pd
import requests
from pathlib import Path
from sqlalchemy import Table, MetaData
from etl_project.connectors.postgresql import PostgreSqlClient


def extract(indicator, date_range):
    """
    Extract data from the monitor database
    """
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
    """
    Transform the raw data
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

    # Filter the DataFrame to include only rows where 'country' is Australia, New Zealand, or Italy
    df_cleaned = df_cleaned[
        df_cleaned["country_name"].isin(
            ["Australia", "New Zealand", "Italy", "United States"]
        )
    ]
    df_cleaned.reset_index(drop=True, inplace=True)

    df_cleaned = df_cleaned.astype({"year": int})

    # try:
    #     df_cleaned.to_csv("../data/cleaned_export_data.csv", index=False)
    #     print("Data saved successfully to cleaned_export_data.csv")
    # except Exception as e:
    #     print(f"Error saving data to CSV: {e}")

    return df_cleaned


def load(
    df: pd.DataFrame,
    postgresql_client: PostgreSqlClient,
    table: Table,
    metadata: MetaData,
    load_method: str = "overwrite",
):
    """
    Load dataframe to a database.

        Args:
            df: dataframe to load
            postgresql_client: postgresql client
            table: sqlalchemy table
            metadata: sqlalchemy metadata
            load_method: supports one of: [insert, upsert, overwrite]
    """
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
