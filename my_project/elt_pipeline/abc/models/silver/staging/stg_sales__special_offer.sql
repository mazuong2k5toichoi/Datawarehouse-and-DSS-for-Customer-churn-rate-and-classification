-- models/silver/staging/stg_sales__special_offer.sql
with source as (
    select
        "SpecialOfferID" as special_offer_id,
        "Description" as description,
        "DiscountPct" as discount_pct,
        "Type" as type,
        "Category" as category,
        "StartDate" as start_date,
        "EndDate" as end_date
    from {{ source('bronze_adventureworks', 'sales_special_offer') }}
),

with_loaddate as (
    select
        *,
        current_timestamp as loaddate
    from source
),

derived_columns as (
    select
        special_offer_id,
        description,
        discount_pct,
        type,
        category,
        start_date,
        end_date,
        loaddate,
        {{ dbt_utils.generate_surrogate_key(['special_offer_id']) }} as special_offer_hk,
        'Sales.SpecialOffer' as recordsource
    from with_loaddate
)

select *
from derived_columns