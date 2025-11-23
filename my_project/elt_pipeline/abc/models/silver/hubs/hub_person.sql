-- models/silver/hubs/hub_person.sql
{{
    config(
        materialized='incremental',
        unique_key='person_hk'
    )
}}

select distinct
    person_hk,
    business_entity_id,
    loaddate,
    recordsource
from {{ ref('stg_person__person') }}
{% if is_incremental() %}
where person_hk not in (select person_hk from {{ this }})
{% endif %}
