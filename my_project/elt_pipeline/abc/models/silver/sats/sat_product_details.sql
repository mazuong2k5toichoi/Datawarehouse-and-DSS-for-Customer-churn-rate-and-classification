-- models/silver/sats/sat_product_details.sql
{{
    config(
        materialized = 'table'
    )
}}

select
    product_hk,
    loaddate,
    recordsource,
    product_number,
    product_name,
    color,
    list_price,
    standard_cost,
    product_subcategory_id
from {{ ref('stg_production__product') }}
