# import requests
import pandas as pd
import requests
from secrets_config import (
    db_database_name,
    db_password,
    db_server_name,
    db_user,
)

from sqlalchemy import Column, Float, Integer, MetaData, String, Table, create_engine
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import URL


def extract(indicator, date_range):
    print("Starting extract")
    api_url = f"https://api.worldbank.org/v2/countries/all/indicators/{indicator}"

    params = {"date": date_range, "format": "json", "page": 1}  # Start at page 1

    export_data = []

    while True:
        response = requests.get(api_url, params=params)

        if response.status_code != 200:
            print(f"Error: Unable to fetch data (Status code: {response.status_code})")
            break

        response_data = response.json()

        if len(response_data) < 2 or not response_data[1]:
            print("No more data available.")
            break

        export_data.extend(response_data[1])
        params["page"] += 1

    df_export = pd.json_normalize(export_data)

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
    return df_cleaned


def load(df: pd.DataFrame):
    print("Starting load")
    # create connection to database
    connection_url = URL.create(
        drivername="postgresql+pg8000",  # "postgresql+pg8000" indicates the driver to be used.
        username=db_user,
        password=db_password,
        host=db_server_name,
        port=5432,  # Ensure the port number is correct (default for PostgreSQL is 5432).
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
    df = extract("TX.VAL.MRCH.XD.WD", "2020:2024")
    df_transformed = transform(df)
    load(df_transformed)
