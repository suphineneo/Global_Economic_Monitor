# import requests
import pandas as pd 
import requests
import wbgapi as wb
import json
from secrets_config import (
    
    db_database_name,
    db_password,
    db_server_name,
    db_user,
)
from secrets_config import (
    
    indicator,
    date_range,
)

from sqlalchemy import Column, Float, Integer, MetaData, String, Table, create_engine
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import URL
from sqlalchemy.schema import CreateTable


def extract(indicator, date_range):
    api_url = f"https://api.worldbank.org/v2/countries/all/indicators/{indicator}"
    
    params = {
        "date": date_range,
        "format": "json",
        "page": 1  # Start at page 1
    }
    
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

    return pd.DataFrame(export_data)

 

def transform(all_data: pd.DataFrame) -> pd.DataFrame:
    # understand the columns
    df_export.columns

    # remove leading and trailing spaces
    df_export.columns = df_export.columns.str.strip()
    df_export.columns

    #only select columns needed
    df_cleaned = df_export[['date', 'value', 'country.value']]

    # Rename the columns
    df_cleaned.rename(columns={
        'date': 'year',
        'country.value': 'country_name'
    }, inplace=True)
    df_cleaned

    # Remove NaN from the Year and value column
    df_cleaned = df_cleaned.dropna(subset=['year']).dropna(subset=['value'])
    df_cleaned.reset_index(drop=True, inplace=True)

    # Filter the DataFrame to include only rows where 'country' is Australia, New Zealand, or Italy
    df_cleaned = df_cleaned[df_cleaned['country_name'].isin(['Australia', 'New Zealand', 'Italy', 'United States' ])]
    df_cleaned

    pass


def join_data()

def load(df):
    # load (overwrite) data to a csv file
    df_export.to_parquet('data\clean_data\df_export.parquet')

    
   # create connection to database 
    connection_url = URL.create(
        drivername="postgresql+pg8000",  # "postgresql+pg8000" indicates the driver to be used.
        username=db_user,
        password=db_password,
        host=db_server_name,
        port=5432,  # Ensure the port number is correct (default for PostgreSQL is 5432).
        database=db_database_name,
    )
    #creates the engine to connect to the db
    engine = create_engine(connection_url)

    # using pandas
    df.to_sql("data", engine, if_exists="append")

    # using pandas
    df.to_sql("data", engine, if_exists="replace")

    # it is not authomaic with pandas, we need to write exactly what the table looks like  

    meta = MetaData()
    export_table = Table(
        "export", meta, 
        Column("id", Integer, primary_key=True),
        Column("year", Integer, primary_key=True),
        Column("Country", Float),
        Column("Country", Float),
        Column("Country", Float),
        
    )
    meta.create_all(engine) # creates table if it does not existsmeta = MetaData()
        
    # Create the upsert statement
    insert_statement = postgresql.insert(export_table).values(pivoted_df.to_dict(orient='records'))

    # Set up the conflict resolution statement
    upsert_statement = insert_statement.on_conflict_do_update(
        index_elements=["id", "year"],
        set_={c.key: c for c in insert_statement.excluded if c.key not in ["id", "year"]})

    # Execute the upsert statement
    with engine.connect() as connection:
        connection.execute(upsert_statement)


if __name__ == "__main__":
   data = extract()
   df = transform()
   load(df)
