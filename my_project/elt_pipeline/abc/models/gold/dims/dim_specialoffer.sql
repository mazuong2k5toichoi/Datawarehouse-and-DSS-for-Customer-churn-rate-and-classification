{{
    config(
        materialized='table',
        tags=['gold', 'dim']
    )
}}
select "SpecialOfferID", "Description","DiscountPct","Type","Category"
from {{ source('bronze_adventureworks', 'sales_special_offer') }}