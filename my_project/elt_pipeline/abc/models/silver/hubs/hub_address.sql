-- models/silver/hubs/hub_address.sql
{{
    config(
        materialized='incremental',
        unique_key='address_hk'
    )
}}

select distinct
    address_hk,
    address_id,
    loaddate,
    recordsource
from {{ ref('stg_person__address') }}
{% if is_incremental() %}
where address_hk not in (select address_hk from {{ this }})
{% endif %}
