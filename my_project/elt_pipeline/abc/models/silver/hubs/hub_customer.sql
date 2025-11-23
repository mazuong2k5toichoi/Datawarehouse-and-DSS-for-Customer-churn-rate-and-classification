-- models/silver/hubs/hub_customer.sql
{{
    config(
        materialized='incremental',
        unique_key='customer_hk'
    )
}}

select
    customer_hk,
    customer_id,
    loaddate,
    recordsource
from {{ ref('stg_sales__customer') }}
{% if is_incremental() %}
where customer_hk not in (select customer_hk from {{ this }})
{% endif %}
