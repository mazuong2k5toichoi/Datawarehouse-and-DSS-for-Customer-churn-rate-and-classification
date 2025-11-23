-- models/silver/sats/sat_address_details.sql
{{
    config(
        materialized = 'table'
    )
}}

select
    address_hk,
    loaddate,
    recordsource,
    address_line1,
    address_line2,
    city,
    state_province_id,
    postal_code
from {{ ref('stg_person__address') }}
