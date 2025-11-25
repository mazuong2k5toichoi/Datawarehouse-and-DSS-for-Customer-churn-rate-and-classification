-- models/silver/staging/stg_sales__sales_order_detail.sql

with source as (
  select
      "SalesOrderID"      as salesorder_id,
      "ProductID"         as product_id,
      "OrderQty"          as order_qty,
      ("UnitPrice")::numeric         as unit_price,
      ("UnitPriceDiscount")::numeric as unit_price_discount,
      ("LineTotal")::numeric         as line_total,
      "SpecialOfferID"    as specialoffer_id
  from {{ source('bronze_adventureworks', 'sales_sales_order_detail') }}
),

with_loaddate as (
    select
        *,
        current_timestamp as loaddate
    from source
),

derived_columns as (
    select
        salesorder_id,
        product_id,
        order_qty,
        unit_price,
        unit_price_discount,
        line_total,
        specialoffer_id,
        loaddate,
        'Sales.SalesOrderDetail' as recordsource
    from with_loaddate
)

select *
from derived_columns
