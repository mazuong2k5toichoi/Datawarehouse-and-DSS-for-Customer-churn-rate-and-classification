# Updated Data Vault Implementation Guide

## Phase 1: Analysis of the Bronze Layer (Current State)

*   **Data Loading**: A Python script (`main.py`) connects to an MS SQL Server source, extracts data from 21 tables using Polars, and loads it into a destination PostgreSQL database.
*   **Bronze Schema Structure**: The script loads data into a `bronze` schema using underscore-separated table names:
    *   `bronze.person_person`
    *   `bronze.sales_customer`
    *   `bronze.production_product`
    *   And 18 other tables following the pattern `schema_table_name`
*   **dbt Project (`elt_pipeline/abc`)**:
    *   **`dbt_project.yml`**: The project is named `abc`.
    *   **`profiles.yml`**: Already configured to connect to the PostgreSQL database (`companyxdb`) and builds models in the `silver` schema.
*   **Source Data for Silver Layer**: Our Silver dbt models will read from these underscore-separated table names within the `bronze` schema of the `companyxdb` database.

## Phase 2: Local Setup and dbt Project Configuration

**Step 1: Bronze Layer Status**
✅ The bronze layer is already configured and running with the correct table structure.

**Step 2: dbt Configuration Status**
✅ `profiles.yml` is already configured for the `silver` schema.

**Step 3: dbt Project Structure**
✅ The folder structure is already scaffolded:
```
models/silver/
├── staging/
├── hubs/
├── links/
└── sats/
```

**Step 4: Bronze Tables as dbt Sources**
✅ `sources.yml` is already updated with the correct table names from `main.py`.

## Phase 3: Step-by-Step Implementation of the Silver Data Vault

### Staging Layer Implementation

**Why a Staging Layer?**
Instead of selecting directly from the Bronze sources in our Hubs, Links, and Satellites, we'll create staging models to:
- Select and rename columns from the source
- Perform light data type casting
- Generate the surrogate keys (Hash Keys) and hash diffs

**Generating Keys: Using a Macro**
We'll use hashing to create our surrogate keys. Create `macros/generate_surrogate_key.sql`:

```sql
{% macro generate_surrogate_key(field_list) %}
    md5(
        {% for field in field_list %}
            coalesce(cast({{ field }} as varchar), '')
            {% if not loop.last %} || '-' || {% endif %}
        {% endfor %}
    )
{% endmacro %}
```

### A. Hubs (Business Keys)

#### Hub_Customer Implementation

**1. Staging Model for Customer**

*File: `models/silver/staging/stg_sales__customer.sql`*
```sql
with source as (
    select * from {{ source('bronze_adventureworks', 'sales_customer') }}
),

derived_columns as (
    select
        "CustomerID" as customer_id,
        "PersonID" as person_id,
        "TerritoryID" as territory_id,
        "rowguid" as loaddate,
        {{ generate_surrogate_key(['customer_id']) }} as customer_hk,
        'Sales.Customer' as recordsource
    from source
)

select * from derived_columns
```

**2. Hub Model: `Hub_Customer`**

*File: `models/silver/hubs/hub_customer.sql`*
```sql
{{
    config(
        materialized='incremental',
        unique_key='customer_hk'
    )
}}

select
    customer_hk,
    customer_id,
    loaddate,
    recordsource
from {{ ref('stg_sales__customer') }}
{% if is_incremental() %}
where customer_hk not in (select customer_hk from {{ this }})
{% endif %}
```

**3. Schema and Tests**

*File: `models/silver/hubs/schema.yml`*
```yaml
version: 2

models:
  - name: hub_customer
    description: "Hub for customers, one row per customer."
    columns:
      - name: customer_hk
        description: "Surrogate key for the customer."
        data_tests:
          - unique
          - not_null
      - name: customer_id
        description: "Business key for the customer from the source system."
        data_tests:
          - unique
          - not_null
```

### Additional Hubs to Implement

Repeat the same pattern for these key hubs:

1. **Hub_Person** - from `bronze.person_person`
2. **Hub_Product** - from `bronze.production_product`
3. **Hub_SalesOrder** - from `bronze.sales_sales_order_header`
4. **Hub_Territory** - from `bronze.sales_sales_territory`
5. **Hub_Address** - from `bronze.person_address`

### B. Links (Relationships)

#### Link_Customer_Person

*File: `models/silver/links/link_customer_person.sql`*
```sql
{{
    config(
        materialized='incremental',
        unique_key='customer_person_hk'
    )
}}

with customer as (
    select * from {{ ref('stg_sales__customer') }}
),

person as (
    select * from {{ ref('stg_person__person') }}
),

link as (
    select
        {{ generate_surrogate_key(['customer.customer_id', 'person.person_id']) }} as customer_person_hk,
        customer.customer_hk,
        person.person_hk,
        customer.loaddate,
        'Sales.Customer' as recordsource
    from customer
    inner join person on customer.person_id = person.person_id
)

select * from link
{% if is_incremental() %}
where customer_person_hk not in (select customer_person_hk from {{ this }})
{% endif %}
```

### C. Satellites (Descriptive Attributes)

#### Sat_Customer_Details

*File: `models/silver/sats/sat_customer_details.sql`*
```sql
{{
    config(
        materialized='incremental',
        unique_key='customer_hk',
        incremental_strategy='merge'
    )
}}

with source as (
    select * from {{ ref('stg_sales__customer') }}
),

final as (
    select
        customer_hk,
        territory_id,
        {{ generate_surrogate_key(['customer_hk', 'territitory_id']) }} as hashdiff,
        loaddate,
        'Sales.Customer' as recordsource
    from source
)

select * from final
```

## Phase 4: Execution Plan and Next Steps

### Sequence of Work:

1. **Setup (✅ Completed)**
   - [x] Bronze layer running with correct table names
   - [x] dbt profiles.yml configured for silver schema
   - [x] Project structure scaffolded
   - [x] sources.yml updated with correct table names

2. **Implement Staging and Hubs**
   - Create staging models for all 21 bronze tables
   - Implement all Hub models (hub_customer, hub_person, hub_product, etc.)
   - Run and test: `dbt run --select tag:staging+` and `dbt test --select tag:hub`

3. **Implement Links**
   - Create Link models for key relationships
   - Materialize as incremental
   - Add relationship tests
   - Run and test: `dbt run --select tag:link` and `dbt test --select tag:link`

4. **Implement Satellites**
   - Create Satellite models for descriptive attributes
   - Implement hashdiff for change tracking
   - Run and test: `dbt run --select tag:sat` and `dbt test --select tag:sat`

### Key Differences from Old Documentation:

1. **Table Names**: Using underscore-separated names (`person_person`) instead of quoted schema.table names (`"Person.person"`)
2. **Schema Configuration**: Already configured for silver schema
3. **Source Structure**: 21 tables mapped from AdventureWorks2022 with proper naming convention
4. **Project Structure**: Already scaffolded and ready for implementation

### Bronze Table Mapping:

| Source Table | Bronze Table | Description |
|-------------|-------------|-------------|
| Person.Person | person_person | Personal information |
| Sales.Customer | sales_customer | Customer data |
| Production.Product | production_product | Product information |
| Sales.SalesOrderHeader | sales_sales_order_header | Order headers |
| Sales.SalesOrderDetail | sales_sales_order_detail | Order details |
| Person.Address | person_address | Address data |
| ... | ... | 15 additional tables |

This updated documentation reflects the actual implementation state and provides the correct table names and schema structure for building the Data Vault silver layer.
