-- models/silver/staging/stg_production__product.sql
with source as (
    select
        "ProductID" as product_id
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
        loaddate,
        {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_hk,
        'Production.Product' as recordsource
    from with_loaddate
)

select *
from derived_columns
