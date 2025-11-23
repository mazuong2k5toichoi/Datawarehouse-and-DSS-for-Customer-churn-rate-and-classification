-- models/silver/links/link_order_product.sql
{{
    config(
        materialized = 'incremental',
        unique_key   = 'orderproduct_lk'
    )
}}

with details as (
    select
        salesorder_id,
        product_id,
        loaddate
    from {{ ref('stg_sales__sales_order_detail') }}
    where salesorder_id is not null
      and product_id is not null
),

orders as (
    select
        salesorder_id,
        salesorder_hk
    from {{ ref('stg_sales__sales_order_header') }}
),

products as (
    select
        product_id,
        product_hk
    from {{ ref('stg_production__product') }}
),

joined as (
    select
        d.salesorder_id,
        d.product_id,
        o.salesorder_hk,
        p.product_hk,
        d.loaddate,
        'Sales.SalesOrderDetail' as recordsource
    from details d
    join orders  o on d.salesorder_id = o.salesorder_id
    join products p on d.product_id   = p.product_id
),

final as (
    select distinct
        {{ dbt_utils.generate_surrogate_key(['salesorder_id', 'product_id']) }} as orderproduct_lk,
        salesorder_hk,
        product_hk,
        salesorder_id,
        product_id,
        loaddate,
        recordsource
    from joined
)

select *
from final
{% if is_incremental() %}
where orderproduct_lk not in (select orderproduct_lk from {{ this }})
{% endif %}
