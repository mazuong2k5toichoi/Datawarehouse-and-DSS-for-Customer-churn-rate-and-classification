-- models/silver/staging/stg_sales__sales_territory.sql
with source as (
    select
        "TerritoryID" as territory_id,
        "Name" as name,
        "CountryRegionCode" as country_region_code,
        "Group" as "group",
        "SalesYTD" as sales_ytd,
        "SalesLastYear" as sales_last_year,
        "CostYTD" as cost_ytd,
        "CostLastYear" as cost_last_year
    from {{ source('bronze_adventureworks', 'sales_sales_territory') }}
),

with_loaddate as (
    select
        *,
        current_timestamp as loaddate
    from source
),

derived_columns as (
    select
        territory_id,
        name,
        country_region_code,
        "group",
        sales_ytd,
        sales_last_year,
        cost_ytd,
        cost_last_year,
        loaddate,
        {{ dbt_utils.generate_surrogate_key(['territory_id']) }} as territory_hk,
        'Sales.SalesTerritory' as recordsource
    from with_loaddate
)

select *
from derived_columns
