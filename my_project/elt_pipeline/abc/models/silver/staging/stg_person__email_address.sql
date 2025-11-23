-- models/silver/staging/stg_person__email_address.sql

with source as (
    select
        "BusinessEntityID" as business_entity_id,
        "EmailAddressID"   as email_address_id,
        "EmailAddress"     as email_address
    from {{ source('bronze_adventureworks', 'person_email_address') }}
),

with_loaddate as (
    select
        *,
        current_timestamp as loaddate
    from source
),

derived_columns as (
    select
        business_entity_id,
        email_address_id,
        email_address,
        loaddate,
        'Person.EmailAddress' as recordsource
    from with_loaddate
)

select *
from derived_columns
