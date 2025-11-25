-- models/gold/facts/fact_sales_order_line.sql
{{
    config(
        materialized='incremental',
        unique_key='sales_order_line_sk',
        tags=['gold', 'fact']
    )
}}

with order_line_base as (
    select
        lop.orderproduct_lk,
        lop.salesorder_hk,
        lop.product_hk,
        sopl.order_qty,
        sopl.unit_price,
        sopl.unit_price_discount,
        sopl.line_total,
        sopl.specialoffer_id
    from {{ ref('link_order_product') }} lop
    left join {{ ref('sat_order_product_link') }} sopl on lop.orderproduct_lk = sopl.orderproduct_lk
),

order_header as (
    select
        hso.salesorder_hk,
        hso.salesorder_id,
        ssh.order_date,
        ssh.territory_id
    from {{ ref('hub_salesorder') }} hso
    left join {{ ref('sat_salesorder_header') }} ssh on hso.salesorder_hk = ssh.salesorder_hk
),

customer_order as (
    select
        lco.customer_hk,
        lco.salesorder_hk,
        hc.customer_id
    from {{ ref('link_customer_order') }} lco
    join {{ ref('hub_customer') }} hc on lco.customer_hk = hc.customer_hk
),

product_order as (
    select
        lop.orderproduct_lk,
        lop.product_hk,
        hp.product_id
    from {{ ref('link_order_product') }} lop
    join {{ ref('hub_product') }} hp on lop.product_hk = hp.product_hk
),

territory_order as (
    select
        hso.salesorder_hk,
        ht.territory_id
    from {{ ref('sat_salesorder_header') }} ssh
    join {{ ref('hub_salesorder') }} hso on ssh.salesorder_hk = hso.salesorder_hk
    left join {{ ref('hub_territory') }} ht on ssh.territory_id = ht.territory_id
),

combined as (
    select
        olb.orderproduct_lk,
        oh.salesorder_id,
        oh.order_date,
        terr.territory_id,
        co.customer_id,
        po.product_id,
        olb.order_qty,
        olb.unit_price,
        olb.unit_price_discount,
        olb.line_total,
        olb.specialoffer_id
    from order_line_base olb
    join order_header oh on olb.salesorder_hk = oh.salesorder_hk
    join customer_order co on olb.salesorder_hk = co.salesorder_hk
    join product_order po on olb.orderproduct_lk = po.orderproduct_lk
    left join territory_order terr on olb.salesorder_hk = terr.salesorder_hk
),

with_sks as (
    select
        c.*,
        {{ dbt_utils.generate_surrogate_key(['salesorder_id', 'product_id']) }} as sales_order_line_sk
    from combined c
)

select *
from with_sks
{% if is_incremental() %}
where order_date > (select max(order_date) from {{ this }})
{% endif %}
