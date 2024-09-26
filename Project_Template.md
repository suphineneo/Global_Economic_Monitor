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
> - Unemployment Rate, seas. adj.
> - Industrial Production, constant 2010 US$, seas. adj.
> - Exports Merchandise, Customs, Price, US$, seas. adj.
> - CPI Price, nominal, seas. adj.
> 
> The source datasets are available as static files and also via API.
> The data is updated at a daily granularity.


## Solution architecture

> (t.b.d)

How are we going to get data flowing from source to serving? What components and services will we combine to implement the solution? How do we automate the entire running of the solution?

- What data extraction patterns are you going to be using? TBC
- What data loading patterns are you going to be using?
- What data transformation patterns are you going to be performing?

## Breakdown of tasks

> See GitHub project [Kanban board](https://github.com/users/suphineneo/projects/1/views/1?visibleFields=%5B%22Title%22%2C%22Assignees%22%2C%22Status%22%2C135941698%2C135941700%2C135941699%2C%22Labels%22%5D)
