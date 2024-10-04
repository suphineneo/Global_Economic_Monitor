from jinja2 import Environment, FileSystemLoader, Template
from dotenv import load_dotenv
import os
import requests
import pandas as pd
import yaml
from pathlib import Path
from importlib import import_module
from sqlalchemy import (Column, Float, Integer, MetaData, String, Table)
from etl_project.connectors.postgresql import PostgreSqlClient
from etl_project.assets.metadata_logging import MetaDataLogging, MetaDataLoggingStatus
from etl_project.assets.pipeline_logging import PipelineLogging
from etl_project.assets.extract_load_transform import (
    extract,
    transform,
    load,
    transform_sql
)
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

    postgresql_client = PostgreSqlClient(
        server_name=SERVER_NAME,
        database_name=DATABASE_NAME,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        port=PORT,
    )

    # Execute Extract, also has the api request
    pipeline_logging.logger.info("Extracting data from database monitor API")
    df_extracted = extract(
        postgresql_client=postgresql_client,
        extract_type = extract_type,
        incremental_column = incremental_column,
        table_name = extract_table_name,
        wb_indicator=wb_indicator,
        wb_daterange=wb_daterange,
    )
    pipeline_logging.logger.info("Extract step completed")

    # Execute Transform
    pipeline_logging.logger.info("Transforming dataframes")
    df_transformed = transform(df_extracted,region_file_path=region_file_path)
    pipeline_logging.logger.info("Transform step completed")

    # Execute Load
    pipeline_logging.logger.info("Loading data to postgres")
    
    metadata = MetaData()
    table = Table(
        "unemployment",
        metadata,
        Column("year", Integer, primary_key=True),
        Column("country_code", String, primary_key=True),
        Column("country_name", String),
        Column("indicator_id", String),
        Column("indicator_value", String),
        Column("value", Float),
        Column("region", String)
    )

    load(
        df=df_transformed,
        postgresql_client=postgresql_client,
        table=table,
        metadata=metadata,
        load_method="upsert",
    )

    #Execute 2nd-level transformation i.e., create a unemployment_ranked table using jinja and partition
    transform_environment = Environment(
        loader=FileSystemLoader("etl_project/sql/transform")
    )
    transform_table_name = "unemployment_ranked"
    transform_sql(
        table_name=transform_table_name,
        postgresql_client=postgresql_client,
        environment=transform_environment
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
            
            wb_indicator = config.get("indicator_unemp")
            wb_daterange = config.get("date_range")
            region_file_path = config.get("region_classification_path")

            incremental_column = pipeline_config.get("extract").get("incremental_column")
            extract_type = pipeline_config.get("extract").get("extract_type")
            extract_table_name = "unemployment" #? how do we make this dynamic to include all our tables

    else:
        raise Exception(
            f"Missing {yaml_file_path} file! Please create the yaml file with at least a `name` key for the pipeline name."
        )
    
    # set schedule
    schedule.every(pipeline_config.get("schedule").get("run_seconds")).seconds.do(
        run_pipeline,
        pipeline_name=PIPELINE_NAME,
        postgresql_logging_client=postgresql_logging_client,
        pipeline_config=pipeline_config
    )

    while True:
        schedule.run_pending()
        time.sleep(pipeline_config.get("schedule").get("poll_seconds"))
