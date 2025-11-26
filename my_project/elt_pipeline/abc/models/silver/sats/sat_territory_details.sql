-- models/silver/sats/sat_territory_details.sql
{{
    config(
        materialized = 'table'
    )
}}

select
    territory_hk,
    loaddate,
    recordsource,
    name,
    country_region_code,
    "group",
    sales_ytd,
    sales_last_year,
    cost_ytd,
    cost_last_year
from {{ ref('stg_sales__sales_territory') }}