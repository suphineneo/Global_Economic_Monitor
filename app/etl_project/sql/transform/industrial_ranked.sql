select
    year,
    country_code,
    country_name,
    region,
    value as "industrial",
    avg(value) over (partition by year) as avg_industrial_by_year,
    avg(value) over (partition by region) as avg_industrial_by_region,
    rank() over (partition by year order by value desc) as rank_industrial_by_year,
    rank() over (partition by year, region order by value desc) as rank_industrial_by_region
from industrial
where region <> 'nan'
order by year, country_code