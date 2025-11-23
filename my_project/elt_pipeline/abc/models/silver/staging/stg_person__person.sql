-- models/silver/staging/stg_person__person.sql

with source as (
    select
        "BusinessEntityID" as business_entity_id,
        "PersonType"       as person_type,
        "Title"            as title,
        "FirstName"        as first_name,
        "MiddleName"       as middle_name,
        "LastName"         as last_name,
        "Suffix"           as suffix,
        "EmailPromotion"   as email_promotion
    from {{ source('bronze_adventureworks', 'person_person') }}
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
        person_type,
        title,
        first_name,
        middle_name,
        last_name,
        suffix,
        email_promotion,
        loaddate,
        {{ dbt_utils.generate_surrogate_key(['business_entity_id']) }} as person_hk,
        'Person.Person' as recordsource
    from with_loaddate
)

select *
from derived_columns
