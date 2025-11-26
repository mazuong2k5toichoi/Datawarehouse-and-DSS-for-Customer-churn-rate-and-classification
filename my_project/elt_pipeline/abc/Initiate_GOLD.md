# Initiate GOLD Layer

## Overview
The Gold layer transforms the Silver Data Vault into analytics-ready star schema dimensions and facts for sales performance, customer behavior, and churn prediction.

## Architecture
- **Schema**: `gold`
- **Materialization**: Dimensions as `table`, Facts as `incremental`
- **Tags**: `gold`, `dim`, `fact`

## Dimensions

### DimDate
- **Grain**: One row per day (2000-01-01 to 2030-12-31)
- **Source**: Generated via dbt_utils.date_spine
- **Key Columns**:
  - date_sk (SK)
  - date, year, month, day, etc.

### DimCustomer
- **Grain**: One row per customer
- **Source**: hub_customer + person/person details + territory + address
- **Key Columns**:
  - customer_sk (SK), customer_id (BK)
  - first_name, last_name, email, territory_name, address_line1, etc.

### DimProduct
- **Grain**: One row per product
- **Source**: hub_product + sat_product_details + category/subcategory staging
- **Key Columns**:
  - product_sk (SK), product_id (BK)
  - product_name, color, list_price, category_name, subcategory_name

### DimTerritory
- **Grain**: One row per territory
- **Source**: hub_territory + sat_territory_details
- **Key Columns**:
  - territory_sk (SK), territory_id (BK)
  - name, country_region_code, group, sales_ytd, etc.

### DimAddress
- **Grain**: One row per address
- **Source**: hub_address + sat_address_details
- **Key Columns**:
  - address_sk (SK), address_id (BK)
  - address_line1, city, postal_code

### DimSpecialOffer (Optional)
- **Grain**: One row per special offer
- **Source**: hub_special_offer + staging
- **Key Columns**:
  - special_offer_sk (SK), special_offer_id (BK)
  - description, type, category, discount_pct

## Facts

### FactSalesOrderLine
- **Grain**: One row per order line (order + product)
- **Source**: link_order_product + sat_order_product_link + hubs
- **Key Columns**:
  - sales_order_line_sk (SK)
  - salesorder_sk, product_sk, customer_sk, territory_sk, order_date_sk
  - order_qty, unit_price, line_total, etc.

### FactCustomerChurnMonthly
- **Grain**: One row per customer per month
- **Source**: Aggregated from facts
- **Key Columns**:
  - customer_sk, month_sk
  - is_churned, recency_days, frequency_cum, monetary_cum

## Churn Logic
- **Definition**: Customer churned if no purchases for 12+ months from snapshot month.
- **Features**: Recency (days since last purchase), Frequency (total orders), Monetary (total spend).

## Build Steps
1. Build Silver: `dbt run --select path:models/silver`
2. Build Dims: `dbt run --select tag:gold --select tag:dim`
3. Build Facts: `dbt run --select tag:gold --select tag:fact`
4. Test: `dbt test --select tag:gold`
5. Docs: `dbt docs generate && dbt docs serve`

## Power BI & ML
- Connect to `gold` schema for star schema relationships.
- Use FactCustomerChurnMonthly for ML training (target: is_churned).