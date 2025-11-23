-- models/silver/links/link_person_address.sql
{{
    config(
        materialized = 'incremental',
        unique_key   = 'personaddress_lk'
    )
}}

with bea as (
    select
        business_entity_id,
        address_id,
        loaddate
    from {{ ref('stg_person__business_entity_address') }}
    where business_entity_id is not null
      and address_id is not null
),

persons as (
    select
        business_entity_id,
        person_hk
    from {{ ref('stg_person__person') }}
),

addresses as (
    select
        address_id,
        address_hk
    from {{ ref('stg_person__address') }}
),

joined as (
    select
        b.business_entity_id,
        b.address_id,
        p.person_hk,
        a.address_hk,
        b.loaddate,
        'Person.BusinessEntityAddress' as recordsource
    from bea b
    join persons   p on b.business_entity_id = p.business_entity_id
    join addresses a on b.address_id        = a.address_id
),

final as (
    select distinct
        {{ dbt_utils.generate_surrogate_key(['business_entity_id', 'address_id']) }} as personaddress_lk,
        person_hk,
        address_hk,
        loaddate,
        recordsource
    from joined
)

select *
from final
{% if is_incremental() %}
where personaddress_lk not in (select personaddress_lk from {{ this }})
{% endif %}
