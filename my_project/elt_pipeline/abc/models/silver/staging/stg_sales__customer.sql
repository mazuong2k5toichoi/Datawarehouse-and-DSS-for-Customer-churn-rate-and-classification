-- models/silver/staging/stg_sales__customer.sql
with source as (
    select
        "CustomerID"  as customer_id,
        "PersonID"    as person_id,
        "TerritoryID" as territory_id
    from {{ source('bronze_adventureworks', 'sales_customer') }}
),

with_loaddate as (
    select
        *,
        current_timestamp as loaddate
    from source
),

derived_columns as (
    select
        customer_id,
        person_id,
        territory_id,
        loaddate,
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_hk,
        'Sales.Customer' as recordsource
    from with_loaddate
)

select *
from derived_columns
