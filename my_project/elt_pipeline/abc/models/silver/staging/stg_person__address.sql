-- models/silver/staging/stg_person__address.sql
with source as (
    select
        "AddressID" as address_id
    from {{ source('bronze_adventureworks', 'person_address') }}
),

with_loaddate as (
    select
        *,
        current_timestamp as loaddate
    from source
),

derived_columns as (
    select
        address_id,
        loaddate,
        {{ dbt_utils.generate_surrogate_key(['address_id']) }} as address_hk,
        'Person.Address' as recordsource
    from with_loaddate
)

select *
from derived_columns
