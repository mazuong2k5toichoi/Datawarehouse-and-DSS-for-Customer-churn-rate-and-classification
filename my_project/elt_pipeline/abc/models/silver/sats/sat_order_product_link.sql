-- models/silver/sats/sat_order_product_link.sql
{{
    config(
        materialized = 'table'
    )
}}

with link as (
    select
        orderproduct_lk,
        salesorder_id,
        product_id,
        loaddate     as link_loaddate,
        recordsource
    from {{ ref('link_order_product') }}
),

detail as (
    select
        salesorder_id,
        product_id,
        order_qty,
        unit_price,
        unit_price_discount,
        line_total,
        specialoffer_id,
        loaddate as detail_loaddate
    from {{ ref('stg_sales__sales_order_detail') }}
),

joined as (
    select
        l.orderproduct_lk,
        coalesce(d.detail_loaddate, l.link_loaddate) as loaddate,
        l.recordsource,
        d.order_qty,
        d.unit_price,
        d.unit_price_discount,
        d.line_total,
        d.specialoffer_id
    from link l
    left join detail d
      on l.salesorder_id = d.salesorder_id
     and l.product_id    = d.product_id
)

select *
from joined
