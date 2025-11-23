-- models/silver/staging/stg_sales__sales_territory.sql
with source as (
    select
        "TerritoryID" as territory_id
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
        loaddate,
        {{ dbt_utils.generate_surrogate_key(['territory_id']) }} as territory_hk,
        'Sales.SalesTerritory' as recordsource
    from with_loaddate
)

select *
from derived_columns
