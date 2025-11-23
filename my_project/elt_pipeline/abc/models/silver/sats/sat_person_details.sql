-- models/silver/sats/sat_person_details.sql
{{
    config(
        materialized = 'table'
    )
}}

select
    person_hk,
    loaddate,
    recordsource,
    person_type,
    title,
    first_name,
    middle_name,
    last_name,
    suffix,
    email_promotion
from {{ ref('stg_person__person') }}
