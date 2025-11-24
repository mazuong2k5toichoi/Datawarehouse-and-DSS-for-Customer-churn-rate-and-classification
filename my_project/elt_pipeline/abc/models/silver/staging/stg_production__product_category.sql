-- models/silver/staging/stg_production__product_category.sql
with source as (
    select
        "ProductCategoryID" as product_category_id,
        "Name" as name
    from {{ source('bronze_adventureworks', 'production_product_category') }}
),

with_loaddate as (
    select
        *,
        current_timestamp as loaddate
    from source
),

derived_columns as (
    select
        product_category_id,
        name,
        loaddate,
        {{ dbt_utils.generate_surrogate_key(['product_category_id']) }} as product_category_hk,
        'Production.ProductCategory' as recordsource
    from with_loaddate
)

select *
from derived_columns