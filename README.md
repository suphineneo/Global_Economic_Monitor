## Overview

This is the code repository for one of the DataEngineerCamp `2024-09` Project-1 groups. It builds up data pipelines to fetch data from [World Bank Open Data](https://data.worldbank.org/), specifically the [Global Economic Monitor](https://datacatalog.worldbank.org/search/dataset/0037798/Global-Economic-Monitor) data set.
For more details, refer to the [Project Plan](Project_Template.md)

## Setup

```bash
git clone git@github.com:suphineneo/Global_Economic_Monitor.git
cd Global_Economic_Monitor/app

python -m pip install -r requirements.txt
python -m etl_project.main
```


## Test
```bash
# pytest ...
```


## Build and run on local machine
```bash
docker build --platform=linux/amd64 -t global_economic_monitor_etl .
docker run global_economic_monitor_etl:latest
```
