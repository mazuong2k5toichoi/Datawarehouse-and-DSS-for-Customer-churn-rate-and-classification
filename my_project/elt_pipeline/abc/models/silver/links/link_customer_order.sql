-- models/silver/links/link_customer_order.sql
{{
    config(
        materialized = 'incremental',
        unique_key   = 'customerorder_lk'
    )
}}

with orders as (
    select
        salesorder_id,
        customer_id,
        salesorder_hk,
        loaddate
    from {{ ref('stg_sales__sales_order_header') }}
    where customer_id is not null
),

customers as (
    select
        customer_id,
        customer_hk
    from {{ ref('stg_sales__customer') }}
),

joined as (
    select
        o.salesorder_id,
        o.customer_id,
        o.salesorder_hk,
        c.customer_hk,
        o.loaddate,
        'Sales.SalesOrderHeader' as recordsource
    from orders o
    join customers c
      on o.customer_id = c.customer_id
),

final as (
    select distinct
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'salesorder_id']) }} as customerorder_lk,
        customer_hk,
        salesorder_hk,
        loaddate,
        recordsource
    from joined
)

select *
from final
{% if is_incremental() %}
where customerorder_lk not in (select customerorder_lk from {{ this }})
{% endif %}
