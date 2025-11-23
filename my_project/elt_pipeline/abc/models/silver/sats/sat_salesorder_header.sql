-- models/silver/sats/sat_salesorder_header.sql
{{
    config(
        materialized = 'table'
    )
}}

select
    salesorder_hk,
    loaddate,
    recordsource,
    order_date,
    due_date,
    ship_date,
    status,
    online_order_flag,
    sub_total,
    total_due,
    territory_id
from {{ ref('stg_sales__sales_order_header') }}
