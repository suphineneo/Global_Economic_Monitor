## Overview

This is the code repository for one of the DataEngineerCamp `2024-09` Project-1 groups. It builds up data pipelines to fetch data from [World Bank Open Data](https://data.worldbank.org/), specifically the [Global Economic Monitor](https://datacatalog.worldbank.org/search/dataset/0037798/Global-Economic-Monitor) data set.
For more details, refer to the [Project Plan](Project_Template.md)

## Setup

```bash
git clone git@github.com:suphineneo/Global_Economic_Monitor.git
cd Global_Economic_Monitor

python -m pip install -r requirements.txt

cd app
python -m etl_project.process_exports
```

Clone `template.env` into `.env` file and update it with your environment variables
- This will be used for local development and testing
- The `.env` file will not be committed into the Git repository


## Test
```bash
# pytest ...
```


## Build and run on local machine
```bash
docker build --platform=linux/amd64 -t global_economic_monitor_etl .
docker run global_economic_monitor_etl:latest
# Hello World
```

- if you specify a `USERNAME` environment variable, the container should output `Hello ${USERNAME}`, else it will output `Hello World`
```bash
docker run -e USERNAME=ming global_economic_monitor_etl:latest
# Hello ming
```

## Deploy and run on AWS
```bash
docker tag global_economic_monitor_etl:latest 084375572515.dkr.ecr.ap-southeast-1.amazonaws.com/global_economic_monitor_etl:latest
docker push 084375572515.dkr.ecr.ap-southeast-1.amazonaws.com/global_economic_monitor_etl:latest
```
