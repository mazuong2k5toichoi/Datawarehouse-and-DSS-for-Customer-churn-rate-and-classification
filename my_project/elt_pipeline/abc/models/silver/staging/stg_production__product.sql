-- models/silver/staging/stg_production__product.sql

with source as (
    select
        "ProductID"           as product_id,
        "ProductNumber"       as product_number,
        "Name"                as product_name,
        "Color"               as color,
        "ListPrice"           as list_price,
        "StandardCost"        as standard_cost,
        "ProductSubcategoryID" as product_subcategory_id
    from {{ source('bronze_adventureworks', 'production_product') }}
),

with_loaddate as (
    select
        *,
        current_timestamp as loaddate
    from source
),

derived_columns as (
    select
        product_id,
        product_number,
        product_name,
        color,
        list_price,
        standard_cost,
        product_subcategory_id,
        loaddate,
        {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_hk,
        'Production.Product' as recordsource
    from with_loaddate
)

select *
from derived_columns
