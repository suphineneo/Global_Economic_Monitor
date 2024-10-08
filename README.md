## Note for instructors: Pls ignore 'process_exports.py' under pipelines and 'export.py' under assets. We are in the midst of troubleshooting something. They are not used or called by global_economic_monitor.py

## Overview

This is the code repository for one of the DataEngineerCamp `2024-09` Project-1 groups. It builds up data pipelines to fetch data from [World Bank Open Data](https://data.worldbank.org/), specifically the [Global Economic Monitor](https://datacatalog.worldbank.org/search/dataset/0037798/Global-Economic-Monitor) data set.
For more details, refer to the [Project Plan](Project_Template.md)

## Setup

```bash
git clone git@github.com:suphineneo/Global_Economic_Monitor.git
cd Global_Economic_Monitor

python -m pip install -r requirements.txt

cd app
```

Clone `template.env` into `.env` file and update it with your environment variables
- This will be used for local development and testing
- The `.env` file will not be committed into the Git repository


## Run on local machine
- queries World Bank API for 5 types of indicators
- outputs to DB tables
- stores pipeline logs to DB
- runs on a schedule
- does 2 levels of transforms, including SQL window functions `rank()`

```bash
python -m etl_project.pipelines.global_economic_monitor
```


## Test
```bash
cd etl_project_tests
python -m pytest
```


## Build Docker containers
- Build and run locally
- Change the Dockerfile to specify which `process_*` pipeline to be built and run
```bash
docker build --platform=linux/amd64 -t global_economic_monitor_etl .
docker run --env-file .env global_economic_monitor_etl:latest
```

- Build and push to ECR
```bash
docker build --platform=linux/amd64 -t global_economic_monitor_etl:process_exports .
# docker build --platform=linux/amd64 -t global_economic_monitor_etl:process_unemployment .
```


## Run on local machine in a docker-compose cluster
- refer to `template.env` for specifying the `.env` file for a `docker-compose` setup
```bash
# start postgres and etl containers and link them to each other
docker-compose up

# connect your pgAdmin to localhost port 5433

# query the databases for new data, e.g.
# SELECT * FROM unemployment_ranked

# Cmd/Ctrl+C to cancel docker-compose, then cleanup the stopped containers
docker-compose down
```


## Deploy and run on AWS
```bash
docker tag global_economic_monitor_etl:latest 084375572515.dkr.ecr.ap-southeast-1.amazonaws.com/global_economic_monitor_etl:latest

docker push 084375572515.dkr.ecr.ap-southeast-1.amazonaws.com/global_economic_monitor_etl:latest
```
