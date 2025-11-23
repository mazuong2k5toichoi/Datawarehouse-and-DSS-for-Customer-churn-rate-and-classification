-- models/silver/sats/sat_person_contact.sql
{{
    config(
        materialized = 'table'
    )
}}

with email as (
    select
        business_entity_id,
        email_address_id,
        email_address,
        loaddate,
        recordsource
    from {{ ref('stg_person__email_address') }}
),

persons as (
    select
        business_entity_id,
        person_hk
    from {{ ref('stg_person__person') }}
),

joined as (
    select
        p.person_hk,
        e.email_address_id,
        e.email_address,
        e.loaddate     as loaddate,
        e.recordsource as recordsource
    from email e
    join persons p
      on e.business_entity_id = p.business_entity_id
)

select *
from joined
