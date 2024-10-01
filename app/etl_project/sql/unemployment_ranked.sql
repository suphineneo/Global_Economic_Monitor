select
    year,
    country_code,
    country_name,
    region,
    value,
    avg(value) over (partition by year) as avg_unemployment_by_year,
    avg(value) over (partition by region) as avg_unemployment_by_region,
    rank() over (partition by year order by value desc) as rank_unemployment_by_year,
    rank() over (partition by year, region order by value desc) as rank_unemployment_by_region
from unemployment
order by year, country_code