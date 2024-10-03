# import requests
import pandas as pd
import requests
import yaml
from dotenv import load_dotenv
import os
from pathlib import Path
from etl_project.assets.export import (
    extract,
    transform,
    load,
)
from sqlalchemy import Column, Float, Integer, MetaData, String, Table, create_engine
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import URL
from etl_project.connectors.postgresql import PostgreSqlClient
from etl_project.assets.metadata_logging import MetaDataLogging, MetaDataLoggingStatus
from etl_project.assets.pipeline_logging import PipelineLogging
import schedule
import time


# Define a unified function to run the entire ETL pipeline
def pipeline(config: dict, pipeline_logging: PipelineLogging):
    pipeline_logging.logger.info("Starting ETL pipeline")
    # set up environment variables
    pipeline_logging.logger.info("Getting pipeline environment variables")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    PORT = os.environ.get("PORT")

    # Execute Extract, also has the api request
    pipeline_logging.logger.info("Extracting data from database monitor API")
    df_extracted = extract(
        indicator=config["indicator_export"], date_range=config["date_range"]
    )
    pipeline_logging.logger.info("Extract step completed")

    # Execute Transform
    pipeline_logging.logger.info("Transforming dataframes")
    df_transformed = transform(df_extracted)
    pipeline_logging.logger.info("Transform step completed")

    # Execute Load
    pipeline_logging.logger.info("Loading data to postgres")
    postgresql_client = PostgreSqlClient(
        server_name=SERVER_NAME,
        database_name=DATABASE_NAME,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        port=PORT,
    )
    metadata = MetaData()
    table = Table(
        "exports",
        metadata,
        Column("year", Integer, primary_key=True),
        Column("country_code", String, primary_key=True),
        Column("country_name", String),
        Column("indicator_id", String),
        Column("indicator_value", String),
        Column("value", Float),
    )
    load(
        df=df_transformed,
        postgresql_client=postgresql_client,
        table=table,
        metadata=metadata,
        load_method="upsert",
    )
    pipeline_logging.logger.info("Pipeline run successful")


def run_pipeline(
    pipeline_name: str,
    postgresql_logging_client: PostgreSqlClient,
    pipeline_config: dict,
):
    pipeline_logging = PipelineLogging(
        pipeline_name=pipeline_config.get("name"),
        log_folder_path=pipeline_config.get("config").get("log_folder_path"),
    )
    metadata_logger = MetaDataLogging(
        pipeline_name=pipeline_name,
        postgresql_client=postgresql_logging_client,
        config=pipeline_config.get("config"),
    )
    try:
        metadata_logger.log()  # log start
        pipeline(
            config=pipeline_config.get("config"), pipeline_logging=pipeline_logging
        )
        metadata_logger.log(
            status=MetaDataLoggingStatus.RUN_SUCCESS, logs=pipeline_logging.get_logs()
        )  # log end
        pipeline_logging.logger.handlers.clear()
    except BaseException as e:
        pipeline_logging.logger.error(f"Pipeline run failed. See detailed logs: {e}")
        metadata_logger.log(
            status=MetaDataLoggingStatus.RUN_FAILURE, logs=pipeline_logging.get_logs()
        )  # log error
        pipeline_logging.logger.handlers.clear()


if __name__ == "__main__":
    load_dotenv()
    LOGGING_SERVER_NAME = os.environ.get("LOGGING_SERVER_NAME")
    LOGGING_DATABASE_NAME = os.environ.get("LOGGING_DATABASE_NAME")
    LOGGING_USERNAME = os.environ.get("LOGGING_USERNAME")
    LOGGING_PASSWORD = os.environ.get("LOGGING_PASSWORD")
    LOGGING_PORT = os.environ.get("LOGGING_PORT")

    postgresql_logging_client = PostgreSqlClient(
        server_name=LOGGING_SERVER_NAME,
        database_name=LOGGING_DATABASE_NAME,
        username=LOGGING_USERNAME,
        password=LOGGING_PASSWORD,
        port=LOGGING_PORT,
    )

    # get config variables
    yaml_file_path = "etl_project/pipelines/gem.yaml"
    if Path(yaml_file_path).exists():
        with open(yaml_file_path) as yaml_file:
            pipeline_config = yaml.safe_load(yaml_file)
            config = pipeline_config.get("config")
            PIPELINE_NAME = pipeline_config.get("name")
    else:
        raise Exception(
            f"Missing {yaml_file_path} file! Please create the yaml file with at least a `name` key for the pipeline name."
        )

    # set schedule
    schedule.every(config.get("schedule").get("run_seconds")).seconds.do(
        run_pipeline,
        pipeline_name=PIPELINE_NAME,
        postgresql_logging_client=postgresql_logging_client,
        pipeline_config=pipeline_config,
    )

    while True:
        schedule.run_pending()
        time.sleep(config.get("schedule").get("poll_seconds"))
