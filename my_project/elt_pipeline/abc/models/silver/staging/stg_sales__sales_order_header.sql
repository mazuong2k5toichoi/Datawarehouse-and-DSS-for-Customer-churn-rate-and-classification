-- models/silver/staging/stg_sales__sales_order_header.sql
with source as (
    select
        "SalesOrderID" as salesorder_id
    from {{ source('bronze_adventureworks', 'sales_sales_order_header') }}
),

with_loaddate as (
    select
        *,
        current_timestamp as loaddate
    from source
),

derived_columns as (
    select
        salesorder_id,
        loaddate,
        {{ dbt_utils.generate_surrogate_key(['salesorder_id']) }} as salesorder_hk,
        'Sales.SalesOrderHeader' as recordsource
    from with_loaddate
)

select *
from derived_columns
