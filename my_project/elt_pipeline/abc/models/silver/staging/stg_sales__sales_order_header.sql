-- models/silver/staging/stg_sales__sales_order_header.sql

with source as (
    select
        "SalesOrderID"    as salesorder_id,
        "CustomerID"      as customer_id,
        "OrderDate"       as order_date,
        "DueDate"         as due_date,
        "ShipDate"        as ship_date,
        "Status"          as status,
        "OnlineOrderFlag" as online_order_flag,
        "SubTotal"        as sub_total,
        "TotalDue"        as total_due,
        "TerritoryID"     as territory_id
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
        customer_id,
        order_date,
        due_date,
        ship_date,
        status,
        online_order_flag,
        sub_total,
        total_due,
        territory_id,
        loaddate,
        {{ dbt_utils.generate_surrogate_key(['salesorder_id']) }} as salesorder_hk,
        'Sales.SalesOrderHeader' as recordsource
    from with_loaddate
)

select *
from derived_columns
