-- models/silver/staging/stg_person__address.sql

with source as (
    select
        "AddressID"       as address_id,
        "AddressLine1"    as address_line1,
        "AddressLine2"    as address_line2,
        "City"            as city,
        "StateProvinceID" as state_province_id,
        "PostalCode"      as postal_code
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
        address_line1,
        address_line2,
        city,
        state_province_id,
        postal_code,
        loaddate,
        {{ dbt_utils.generate_surrogate_key(['address_id']) }} as address_hk,
        'Person.Address' as recordsource
    from with_loaddate
)

select *
from derived_columns
