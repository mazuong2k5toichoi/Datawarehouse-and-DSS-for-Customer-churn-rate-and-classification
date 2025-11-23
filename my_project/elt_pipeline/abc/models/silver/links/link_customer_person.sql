-- models/silver/links/link_customer_person.sql
{{
    config(
        materialized = 'incremental',
        unique_key   = 'customerperson_lk'
    )
}}

with customer as (
    select
        customer_id,
        person_id,
        customer_hk,
        loaddate
    from {{ ref('stg_sales__customer') }}
    where person_id is not null
),

person as (
    select
        business_entity_id,
        person_hk
    from {{ ref('stg_person__person') }}
),

joined as (
    select
        c.customer_id,
        c.person_id,
        c.customer_hk,
        p.person_hk,
        c.loaddate,
        'Sales.Customer' as recordsource
    from customer c
    join person p
      on c.person_id = p.business_entity_id
),

final as (
    select distinct
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'person_id']) }} as customerperson_lk,
        customer_hk,
        person_hk,
        loaddate,
        recordsource
    from joined
)

select *
from final
{% if is_incremental() %}
where customerperson_lk not in (select customerperson_lk from {{ this }})
{% endif %}
