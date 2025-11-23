-- models/silver/staging/stg_person__business_entity_address.sql
with source as (
    select
        "BusinessEntityID" as business_entity_id,
        "AddressID"        as address_id,
        "AddressTypeID"    as address_type_id
    from {{ source('bronze_adventureworks', 'person_business_entity_address') }}
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
        address_id,
        address_type_id,
        loaddate,
        'Person.BusinessEntityAddress' as recordsource
    from with_loaddate
)

select *
from derived_columns
