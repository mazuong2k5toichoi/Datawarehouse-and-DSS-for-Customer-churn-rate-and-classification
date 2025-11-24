-- models/silver/hubs/hub_special_offer.sql
{{
    config(
        materialized='incremental',
        unique_key='special_offer_hk'
    )
}}

select distinct
    special_offer_hk,
    special_offer_id,
    loaddate,
    recordsource
from {{ ref('stg_sales__special_offer') }}
{% if is_incremental() %}
where special_offer_hk not in (select special_offer_hk from {{ this }})
{% endif %}