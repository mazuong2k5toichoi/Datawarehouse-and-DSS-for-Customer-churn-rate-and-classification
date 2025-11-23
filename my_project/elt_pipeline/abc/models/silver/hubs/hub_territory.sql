-- models/silver/hubs/hub_territory.sql
{{
    config(
        materialized='incremental',
        unique_key='territory_hk'
    )
}}

select distinct
    territory_hk,
    territory_id,
    loaddate,
    recordsource
from {{ ref('stg_sales__sales_territory') }}
{% if is_incremental() %}
where territory_hk not in (select territory_hk from {{ this }})
{% endif %}
