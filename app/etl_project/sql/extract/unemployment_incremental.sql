-- this jinja file for incremental extract is simple 
-- because we are reading from our API endpoint with a query parameter e.g. `api?date=2023`.

{% set config = {
    "extract_type": "incremental",
    "incremental_column": "year",
    "source_table_name": "unemployment"
} %}


