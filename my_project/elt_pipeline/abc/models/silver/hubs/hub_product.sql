-- models/silver/hubs/hub_product.sql
{{
    config(
        materialized='incremental',
        unique_key='product_hk'
    )
}}

select distinct
    product_hk,
    product_id,
    loaddate,
    recordsource
from {{ ref('stg_production__product') }}
{% if is_incremental() %}
where product_hk not in (select product_hk from {{ this }})
{% endif %}
