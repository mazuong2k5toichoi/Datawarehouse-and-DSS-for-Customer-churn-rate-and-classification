-- models/silver/staging/stg_production__product_subcategory.sql
with source as (
    select
        "ProductSubcategoryID" as product_subcategory_id,
        "ProductCategoryID" as product_category_id,
        "Name" as name
    from {{ source('bronze_adventureworks', 'production_product_subcategory') }}
),

with_loaddate as (
    select
        *,
        current_timestamp as loaddate
    from source
),

derived_columns as (
    select
        product_subcategory_id,
        product_category_id,
        name,
        loaddate,
        {{ dbt_utils.generate_surrogate_key(['product_subcategory_id']) }} as product_subcategory_hk,
        'Production.ProductSubcategory' as recordsource
    from with_loaddate
)

select *
from derived_columns