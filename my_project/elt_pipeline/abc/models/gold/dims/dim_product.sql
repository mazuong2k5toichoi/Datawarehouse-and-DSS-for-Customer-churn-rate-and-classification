-- models/gold/dims/dim_product.sql
{{
    config(
        materialized='table',
        tags=['gold', 'dim']
    )
}}

with product_details as (
    select
        hp.product_hk,
        hp.product_id,
        spd.product_number,
        spd.product_name,
        spd.color,
        spd.list_price,
        spd.standard_cost,
        spd.product_subcategory_id
    from {{ ref('hub_product') }} hp
    left join {{ ref('sat_product_details') }} spd on hp.product_hk = spd.product_hk
), -- OK

subcategory as (
    select
        sps.product_subcategory_id,
        sps.name as subcategory_name,
        spc.product_category_id,
        spc.name as category_name
    from {{ ref('stg_production__product_subcategory') }} sps
    left join {{ ref('stg_production__product_category') }} spc on sps.product_category_id = spc.product_category_id
),

combined as (
    select
        pd.product_id,
        pd.product_number,
        pd.product_name,
        pd.color,
        pd.list_price,
        pd.standard_cost,
        sc.subcategory_name,
        sc.category_name
    from product_details pd
    left join subcategory sc on pd.product_subcategory_id = sc.product_subcategory_id
)

select *
from combined
