-- models/silver/hubs/hub_salesorder.sql
{{
    config(
        materialized='incremental',
        unique_key='salesorder_hk'
    )
}}

select distinct
    salesorder_hk,
    salesorder_id,
    loaddate,
    recordsource
from {{ ref('stg_sales__sales_order_header') }}
{% if is_incremental() %}
where salesorder_hk not in (select salesorder_hk from {{ this }})
{% endif %}
