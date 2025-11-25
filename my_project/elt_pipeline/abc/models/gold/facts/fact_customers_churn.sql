{{
    config(
        materialized='table',
        tags=['gold', 'fact']
    )
}}

WITH LastOrder AS (
    SELECT 
        a.customer_id, 
        -- CAST THE SOURCE DATA HERE
        MAX(a.order_date::date) AS LastPurchase
    FROM {{ ref('stg_sales__sales_order_header') }} a
    GROUP BY a.customer_id
)
SELECT 
    customer_id,
    CASE
        -- Now both sides of the '<' operator are dates
        WHEN LastPurchase < ('2014-06-30'::date - interval '1 year') OR LastPurchase IS NULL THEN 'Churned'
        ELSE 'Active'
    END AS CustomerStatus
FROM LastOrder;

