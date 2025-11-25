-- models/gold/facts/fact_customer_churn.sql
{{
    config(
        materialized='table',
        tags=['gold', 'fact']
    )
}}

-- Snapshot date variable
{% set snapshot_date = var('snapshot_date', '2014-06-30') %}

with fact_sales as (
    select * from {{ ref('fact_sales_order_line') }}
),

dim_customer as (
    select * from {{ ref('dim_customer') }}
),

stg_sales_order_header as (
    select * from {{ ref('stg_sales__sales_order_header') }}
),

customer_last_purchase as (
    select
        customer_sk,
        max(order_date) as last_order_date
    from fact_sales
    group by 1
),

-- RFM calculations over the last 12 months
twelve_month_window as (
    select
        fs.customer_sk,
        count(distinct fs.salesorder_sk) as frequency,
        sum(fs.line_total) as monetary,
        count(distinct fs.product_sk) as distinct_product_count
    from fact_sales fs
    where fs.order_date >= ('{{ snapshot_date }}'::date - interval '12 months')
      and fs.order_date < '{{ snapshot_date }}'::date
    group by 1
),

-- Churn and other metrics
customer_metrics as (
    select
        h.customer_id,
        count(distinct case when h.status in (4, 6) then h.salesorder_id end) as complaint_cancel_orders,
        count(distinct case when h.status = 3 then h.salesorder_id end) as backorders,
        count(distinct h.salesorder_id) as total_orders,
        sum(case when h.status in (4, 6) then od.line_total end) as potential_loss_value
    from stg_sales_order_header h
    join {{ ref('stg_sales__sales_order_detail') }} od on h.salesorder_id = od.salesorder_id
    group by 1
),

final as (
    select
        dc.customer_sk,
        dc.territory_sk,
        da.address_sk,

        -- Recency
        ('{{ snapshot_date }}'::date - clp.last_order_date)::int as recency,

        -- RFM
        coalesce(rfm.frequency, 0) as frequency,
        coalesce(rfm.monetary, 0) as monetary,
        {{ safe_divide('rfm.monetary', 'rfm.frequency') }} as avg_order_value,
        coalesce(rfm.distinct_product_count, 0) as distinct_product_count,

        -- Churn: no orders in last 12 months
        case
            when clp.last_order_date < ('{{ snapshot_date }}'::date - interval '12 months') then 1
            else 0
        end as is_churned,

        -- Ratios
        {{ safe_divide('cm.complaint_cancel_orders', 'cm.total_orders') }} as complaint_cancel_ratio,
        {{ safe_divide('cm.backorders', 'cm.total_orders') }} as backorder_ratio,
        coalesce(cm.potential_loss_value, 0) as potential_loss_value

    from dim_customer dc
    left join customer_last_purchase clp on dc.customer_sk = clp.customer_sk
    left join twelve_month_window rfm on dc.customer_sk = rfm.customer_sk
    left join customer_metrics cm on dc.customer_id = cm.customer_id
    left join {{ ref('dim_address') }} da on dc.customer_id = da.address_id
)

select * from final
