-- models/silver/links/link_customer_territory.sql
{{
    config(
        materialized = 'incremental',
        unique_key   = 'customerterritory_lk'
    )
}}

with customers as (
    select
        customer_id,
        customer_hk,
        territory_id,
        loaddate
    from {{ ref('stg_sales__customer') }}
    where territory_id is not null
),

territories as (
    select
        territory_id,
        territory_hk
    from {{ ref('stg_sales__sales_territory') }}
),

joined as (
    select
        c.customer_id,
        c.customer_hk,
        c.territory_id,
        t.territory_hk,
        c.loaddate,
        'Sales.Customer' as recordsource
    from customers c
    join territories t
      on c.territory_id = t.territory_id
),

final as (
    select distinct
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'territory_id']) }} as customerterritory_lk,
        customer_hk,
        territory_hk,
        loaddate,
        recordsource
    from joined
)

select *
from final
{% if is_incremental() %}
where customerterritory_lk not in (select customerterritory_lk from {{ this }})
{% endif %}
