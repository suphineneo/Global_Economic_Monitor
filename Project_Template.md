# Project plan

## Objective

The objective of our project is to provide analytical datasets from World Bank's Global Economic Monitors.

## Consumers

What users would find your data useful? How do they want to access the data?

> The users of our datasets are Data Analysts in the Economics Research team. 

## Questions

What questions are you trying to answer with your data? How will your data support your users?

> - How has unemployment rate changed for each country over the years?
> - What changes have happened in the industrial world for each country?
> - Which country had the highest import in 2023?
> - which countries have had the fastest Consumer Price Index over time?
> - What is the relationship between the different economic indicators?


## Source datasets

What datasets are you sourcing from? How frequently are the source datasets updating?

> https://datacatalog.worldbank.org/search/dataset/0037798/Global-Economic-Monitor
>
> Specifically, we will start exploring these data sets
> - Unemployment Rate
> - Industrial Production
> - Exports Merchandise
> - CPI Price
> - GDP
> 
> The source datasets are available via API. The data is updated at a yearly granularity.


## Solution architecture

> (to-do)

How are we going to get data flowing from source to serving? What components and services will we combine to implement the solution? How do we automate the entire running of the solution?

> - What data extraction patterns are you going to be using? Incremental Extract
> - What data loading patterns are you going to be using? Upsert
> - What data transformation patterns are you going to be performing?
>   - transform raw data: filter, rename, dropna, change column type, merge
>   - transform_sql : using SQL queries and window function rank()

## Breakdown of tasks

> See GitHub project [Kanban board](https://github.com/users/suphineneo/projects/1/views/1?visibleFields=%5B%22Title%22%2C%22Assignees%22%2C%22Status%22%2C135941698%2C135941700%2C135941699%2C%22Labels%22%5D)


## Deployment to Amazon Web Services

1. Elastic Container Registry (ECR) - screenshot of image in ECR

    ![images/01_ecr_container_images.png](images/01_ecr_container_images.png)

2. Elastic Container Service (ECS) - screenshot of scheduled task in ECS

    ![images/02_ecs_task_definition.png](images/02_ecs_task_definition.png)
    ![images/03_ecs_running_tasks.png](images/03_ecs_running_tasks.png)
    ![images/04_ecs_running_task_configuration.png](images/04_ecs_running_task_configuration.png)
    ![images/05_ecs_running_task_logs.png](images/05_ecs_running_task_logs.png)

3. Relational Database Service (RDS) - screenshot of dataset in target storage
    - We set up a Postgres instance in RDS with no public access
    - We also set up an EC2 instance with public IP as a gateway/bastion server and SSH into it to connect to the Postgres instance
    - The following screenshots show the tables and data in the Postgres instance.

    ![images/06_rds_storage_01.png](images/06_rds_storage_01.png)
    ![images/07_rds_storage_02.png](images/07_rds_storage_02.png)
    ![images/08_rds_storage_03.png](images/08_rds_storage_03.png)

4. IAM Role - screenshot of created role

    ![images/09_iam_role.png](images/09_iam_role.png)

5. S3 for `.env` file - screenshot of `.env` file in S3

    ![images/10_s3_env_file.png](images/10_s3_env_file.png)
