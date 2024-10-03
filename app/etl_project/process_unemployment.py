from jinja2 import Environment, FileSystemLoader, Template
from dotenv import load_dotenv
import os
import requests
import pandas as pd
import yaml
from pathlib import Path
from importlib import import_module
from sqlalchemy import (
    Column,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    text,
    inspect,
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import URL, Engine
from sqlalchemy.schema import CreateTable


# extract from WB
def extract_unemp(
    engine: Engine, sql_template: Template, wb_indicator, wb_daterange
) -> pd.DataFrame:
    # general assumption is that we want our tables to be as updated as WB

    extract_type = sql_template.make_module().config.get(
        "extract_type"
    )  # first, get the extract type
    if extract_type == "full":
        date_range = wb_daterange  # use the full date range specified in yaml
    elif extract_type == "incremental":
        # get max year in postgres table. year is the incremental column.
        # first, check if table exists
        source_table_name = sql_template.make_module().config.get("source_table_name")
        if inspect(engine).has_table(source_table_name):
            incremental_column = sql_template.make_module().config.get(
                "incremental_column"
            )
            with engine.connect() as connection:
                with connection.begin():  # Start a transaction
                    incremental_value = connection.execute(
                        text(
                            f"select max({incremental_column}) as incremental_value from {source_table_name}"
                        )
                    ).scalar()

                    date_range = incremental_value + 1  # max year + 1
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

    df_unemp = pd.json_normalize(data=all_data)

    if df_unemp.empty:
        print(f"{date_range} data is not yet available in World Bank.")
    else:
        distinct_years = df_unemp["date"].unique()
        print(f"Year extracted from World Bank api: {distinct_years}")

    print("Completed extract")
    return df_unemp


# transfom
def transform_unemp(df_unemp: pd.DataFrame, region_file_path) -> pd.DataFrame:

    # select some columns
    df_selected = df_unemp[
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
    df_cleaned = df_cleaned.astype({"year": int})

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
    return df_final


# load into postgres


def load(df: pd.DataFrame, engine):

    print("Starting load")

    # it is not automatic with pandas, we need to write exactly what the table looks like
    meta = MetaData()
    export_table = Table(
        "unemployment",
        meta,
        Column("year", Integer, primary_key=True),
        Column("country_code", String, primary_key=True),
        Column("country_name", String),
        Column("indicator_id", String),
        Column("indicator_value", String),
        Column("value", Float),
        Column("region", String),  # ADDING REGION!
    )

    meta.create_all(engine)  # creates table if it does not exists

    # Create the upsert statement
    insert_statement = postgresql.insert(export_table).values(
        df.to_dict(orient="records")
    )

    # Set up the conflict resolution statement
    upsert_statement = insert_statement.on_conflict_do_update(
        index_elements=["year", "country_code"],
        set_={
            c.key: c
            for c in insert_statement.excluded
            if c.key not in ["year", "country_code"]
        },
    )

    # Execute the upsert statement
    # with engine.connect() as connection:
    #   connection.execute(upsert_statement)
    try:
        with engine.connect() as connection:
            with connection.begin():  # Start a transaction
                connection.execute(upsert_statement)
                print("Upsert operation completed successfully!")
    except Exception as e:
        print(f"An error occurred during upsert: {e}")

    print("Completed load")


# do further transformation using jinja and partition - create an unemployment_ranked table
def transform_sql(engine: Engine, sql_template: Template, table_name: str):
    exec_sql = f"""
        drop table if exists {table_name};
        create table {table_name} as (
             {sql_template.render()}
        )
    """

    try:
        with engine.connect() as connection:
            with connection.begin():  # Start a transaction
                connection.execute(text(exec_sql))
                print(f"Table {table_name} created successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    load_dotenv()

    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    PORT = os.environ.get("PORT")

    connection_url = URL.create(
        drivername="postgresql+pg8000",
        username=DB_USERNAME,
        password=DB_PASSWORD,
        host=SERVER_NAME,
        port=PORT,
        database=DATABASE_NAME,
    )
    # creates the engine to connect to the db
    target_engine = create_engine(connection_url)

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

    # incremental extract from api
    extract_environment = Environment(loader=FileSystemLoader("sql/extract"))
    extract_template_name = "unemployment_incremental"
    extract_sql_template = extract_environment.get_template(
        f"{extract_template_name}.sql"
    )

    df_unemp = extract_unemp(
        engine=target_engine,
        sql_template=extract_sql_template,
        wb_indicator=wb_indicator,
        wb_daterange=wb_daterange,
    )

    if df_unemp.empty:
        print("Incremental extract is empty. No data to transform or load.")
    else:
        # transform
        df_transformed = transform_unemp(df_unemp, region_file_path=region_file_path)

        # load into postgres
        load(df=df_transformed, engine=target_engine)

        # do further transformation i.e., create a unemployment_ranked table using jinja and partition

        transform_environment = Environment(loader=FileSystemLoader("sql/transform"))
        transform_table_name = "unemployment_ranked"
        transform_sql_template = transform_environment.get_template(
            f"{transform_table_name}.sql"
        )
        transform_sql(
            engine=target_engine,
            sql_template=transform_sql_template,
            table_name=transform_table_name,
        )
