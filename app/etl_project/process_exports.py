# import requests
from pipeline_logging import setup_pipeline_logging, get_logs
import pandas as pd
import requests
import yaml
from dotenv import load_dotenv
import os
from pathlib import Path
from sqlalchemy import Column, Float, Integer, MetaData, String, Table, create_engine
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import URL


def extract_export(indicator, date_range):

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

    print("Completed extract")
    return pd.DataFrame(df_export)


def transform(df: pd.DataFrame) -> pd.DataFrame:
    print("Starting transform")

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

    print("Completed transform")

    try:
        df_cleaned.to_csv("data/cleaned_export_data.csv", index=False)
        print("Data saved successfully to cleaned_export_data.csv")
    except Exception as e:
        print(f"Error saving data to CSV: {e}")

    return df_cleaned


def load(df: pd.DataFrame):
    print("Starting load")
    connection_url = URL.create(
        drivername="postgresql+pg8000",
        username=db_user,
        password=db_password,
        host=db_server_name,
        port=port,  # Default PostgreSQL port
        database=db_database_name,
    )

    # creates the engine to connect to the db
    engine = create_engine(connection_url)

    # # append using pandas
    # df.to_sql("exports", engine, if_exists="append", index=False)

    # # replace using pandas
    # df.to_sql("exports", engine, if_exists="replace")

    # it is not automatic with pandas, we need to write exactly what the table looks like
    meta = MetaData()
    export_table = Table(
        "exports",
        meta,
        Column("year", Integer, primary_key=True),
        Column("country_code", String, primary_key=True),
        Column("country_name", String),
        Column("indicator_id", String),
        Column("indicator_value", String),
        Column("value", Float),
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
    with engine.connect() as connection:
        connection.execute(upsert_statement)

    print("Completed load")


if __name__ == "__main__":

    load_dotenv()
    # Retrieve the database configurations from environment variables
    db_user = os.getenv("DB_USERNAME")
    db_password = os.getenv("DB_PASSWORD")
    db_server_name = os.getenv("SERVER_NAME")
    db_database_name = os.getenv("DATABASE_NAME")
    port = os.environ.get("PORT")

    # Define the path to the YAML configuration file
    yaml_file_path = "gem.yaml"

    if Path(yaml_file_path).exists():
        with open(yaml_file_path) as yaml_file:
            config = yaml.safe_load(yaml_file)
            config = config.get("config")
            indicator = config.get("indicator_export")
            date_range = config.get("date_range")

    else:
        raise Exception(
            f"Missing {yaml_file_path} file! Please create the yaml file with at least a `name` key for the pipeline name."
        )

    logger, log_file = setup_pipeline_logging("worldbankdata_exports", "logs")
    logger.info("Making api connection")
    # logs = get_logs(log_file)

    # Execute the ETL pipeline
    df = extract_export(indicator, date_range)
    df_transformed = transform(df)
    logger.info("Finishing Transformations")
    logger.info("Loading Data into Postgres")
    load(df_transformed)
    logger.info("Finished loading into postgres")
    logger.info("Pipeline Run Complete")
