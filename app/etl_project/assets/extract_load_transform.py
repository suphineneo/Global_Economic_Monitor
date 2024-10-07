from jinja2 import Environment, Template
import pandas as pd
import requests
from sqlalchemy import Table, MetaData, inspect, text
from sqlalchemy.engine import URL, Engine
from etl_project.connectors.postgresql import PostgreSqlClient


# extract from WB
def extract(
    postgresql_client: PostgreSqlClient,
    extract_type,
    incremental_column,
    table_name,
    wb_indicator,
    wb_daterange,
) -> pd.DataFrame:
    """
    Extract data from the monitor database
    """
    print("Starting extract")

    # goal is for our tables to fetch incremental data from WB
    if extract_type == "full":
        date_range = wb_daterange  # use the date range specified in yaml
    elif extract_type == "incremental":
        # get max year in postgres table since year is the incremental column.
        # first, check if table exists
        if postgresql_client.table_exists(table_name):
            sql_response = postgresql_client.run_sql(
                text(
                    f"select max({incremental_column}) as incremental_value from {table_name}"
                )
            )
            incremental_value = sql_response[0].get("incremental_value")
            date_range = (
                f"{incremental_value + 1}:{incremental_value + 1}"  # max year + 1
            )
        else:
            date_range = wb_daterange  # if table doesn't exist, use the full date range specified in yaml

    print(f"Date range param for api: {date_range}")

    indicator = wb_indicator
    base_url = f"https://api.worldbank.org/v2/countries/all/indicators/{indicator}?"
    params = {"date": date_range, "format": "json", "page": 1}  # Start at page 1

    all_data = []

    while True:
        response = requests.get(base_url, params=params)
        response_data = response.json()

        if len(response_data) < 2 or not response_data[1]:  # Check if there's data
            break

        all_data.extend(response_data[1])  # Add current page data to all_data

        # Update parameters for the next page
        params["page"] += 1

    df = pd.json_normalize(data=all_data)

    if df.empty:  # this means our table is already updated with latest data in WB
        print(
            f"Incremental extract is empty. {date_range} data is not yet available in World Bank."
        )
    else:
        distinct_years = df["date"].unique()
        print(f"Year extracted from World Bank api: {distinct_years}")

    print("Completed extract")
    return pd.DataFrame(df)


# transfom
def transform(df: pd.DataFrame, region_file_path) -> pd.DataFrame:
    if df.empty:
        print("Incremental extract is empty. No data to transform.")
    else:
        print("Starting transform")

        # select some columns
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

        # rename column names
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

        # change datatype of year
        df_cleaned = df_cleaned.astype({"year": "int64"})

        df_region = pd.read_csv(
            region_file_path, usecols=["Code", "Region"]
        )  # "data/CLASS_CSV.csv"

        df_region = df_region.rename(columns={"Region": "region"})

        # merge with region class file
        df_final = pd.merge(
            left=df_cleaned,
            right=df_region,
            left_on="country_code",
            right_on="Code",
        )

        df_final = df_final.drop(["Code"], axis=1)

        print("Completed transform")
        df = df_final

    return pd.DataFrame(df)


# load into postgres
def load(
    df: pd.DataFrame,
    postgresql_client: PostgreSqlClient,
    table: Table,
    metadata: MetaData,
    load_method,
) -> pd.DataFrame:
    """
    Load dataframe to a database.
        Args:
            df: dataframe to load
            postgresql_client: postgresql client
            table: sqlalchemy table
            metadata: sqlalchemy metadata
            load_method: supports one of: [insert, upsert, overwrite]
    """

    if df.empty:
        print("Incremental extract is empty. No data to load.")
    else:
        print("Starting load")
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
        print("Completed load")


# do further transformation using jinja and partition - create an unemployment_ranked table
def transform_sql(
    table_name: str, postgresql_client: PostgreSqlClient, environment: Environment
):

    transform_sql_template = environment.get_template(f"{table_name}.sql")

    exec_sql = f"""
        drop table if exists {table_name};
        create table {table_name} as (
             {transform_sql_template.render()}
        )
    """
    postgresql_client.execute_sql(exec_sql)
