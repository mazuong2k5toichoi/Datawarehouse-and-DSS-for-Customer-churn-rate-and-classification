-- models/silver/staging/stg_person__person.sql
with source as (
    select
        "BusinessEntityID" as business_entity_id
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
        loaddate,
        {{ dbt_utils.generate_surrogate_key(['business_entity_id']) }} as person_hk,
        'Person.Person' as recordsource
    from with_loaddate
)

select *
from derived_columns
