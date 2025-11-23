1.  **Phase 1: Analysis of the Bronze Layer** - Understanding the current state.
2.  **Phase 2: Local Setup and dbt Project Scaffolding** - Preparing the environment for Silver.
3.  **Phase 3: Step-by-Step Implementation of the Silver Data Vault** - Building the models.
4.  **Phase 4: Execution Plan and Next Steps** - Sequencing the work and looking ahead to Gold.

---

### Phase 1: Analysis of the Bronze Layer

I have already analyzed the repository. Here's a summary of how the Bronze layer is configured and what it means for our Silver implementation.

*   **Data Loading**: A Python script (`main.py`) connects to an MS SQL Server source, extracts data from ~20 tables using Polars, and loads it into a destination PostgreSQL database.
*   **Bronze Schema Structure**: The script loads data into a `bronze` schema, but it also preserves the source schema names, creating tables like:
    *   `bronze."Person.person"`
    *   `bronze."Sales.customer"`
    *   `bronze."Production.product"`
    *   *(Note: The table names are quoted because they contain dots)*.
*   **dbt Project (`elt_pipeline/abc`)**:
    *   **`dbt_project.yml`**: The project is named `abc`.
    *   **`profiles.yml`**: It's configured to connect to the PostgreSQL database (`companyxdb`) and, by default, builds models in a schema named `test`. We will change this for our Silver models.
*   **Source Data for Silver Layer**: Based on this analysis, our Silver dbt models will need to read from these quoted table names within the `bronze` schema of the `companyxdb` database.

---

### Phase 2: Local Setup and dbt Project Scaffolding

Let's prepare your local environment and dbt project to build the Silver layer.

**Step 1: Get the Bronze Layer Running**

You have already completed most of this. For completeness:
1.  Ensure PostgreSQL is running in Docker.
2.  Ensure your MS SQL Server is running and accessible.
3.  Create your `.env` file with the correct credentials for both databases (remembering to set `DATABASE_NAME='MyDB'`).
4.  Activate your Python virtual environment (`source venv/bin/activate`).
5.  Run the Bronze pipeline to load the data: `python main.py`.
6.  Connect with DBeaver to your PostgreSQL instance and verify that tables like `bronze."Person.person"` exist and contain data.

**Step 2: Configure dbt for the Silver Layer**

1.  **Navigate to the dbt project:**
    ```bash
    cd elt_pipeline/abc
    ```
2.  **Update `profiles.yml`:** Change the default schema from `test` to `silver`. This ensures our new models are built in the correct place.

    *   **Edit `profiles.yml`:**
        ```yaml
        abc:
          outputs:
            dev:
              dbname: companyxdb
              host: localhost
              pass: P@ssW0rd
              port: 55432
              schema: silver # <-- CHANGE THIS FROM 'test' to 'silver'
              threads: 1
              type: postgres
              user: companyx
          target: dev
        ```

**Step 3: Scaffold the dbt Project Structure**

Create a clean folder structure within your `models` directory. This will keep the project organized.

```bash
# Inside the 'elt_pipeline/abc' directory
mkdir -p models/silver/staging
mkdir -p models/silver/hubs
mkdir -p models/silver/links
mkdir -p models/silver/sats
# Optional: remove the example models
rm -rf models/example
```

**Step 4: Define Bronze Tables as dbt Sources**

This is a critical best practice. We will create a `sources.yml` file to declare our Bronze tables. This allows us to reference them using the `{{ source() }}` function, track lineage, and test the freshness of our source data.

*   **Create the file `models/silver/staging/sources.yml`:**
    ```yaml
    version: 2

    sources:
      - name: bronze_adventureworks # This is an arbitrary group name
        database: companyxdb # The database name
        schema: bronze # The schema name
        tables:
          - name: "Person.person"
          - name: "Sales.customer"
          - name: "Sales.salesorderheader"
          - name: "Production.product"
          - name: "Sales.salesterritory"
          - name: "Person.address"
          - name: "Sales.salesorderdetail"
          - name: "Person.businessentityaddress"
          - name: "Person.emailaddress"
          # Add other tables as needed...
    ```
    *(You will add all the source tables you need to this file as we proceed.)*

---

### Phase 3: Step-by-Step Implementation of the Silver Data Vault

Now we'll build the Data Vault. A key architectural decision is to create an intermediate **staging layer** first.

**Why a Staging Layer?**
Instead of selecting directly from the Bronze sources in our Hubs, Links, and Satellites, we'll create staging models. This is where we will:
-   Select and rename columns from the source.
-   Perform light data type casting.
-   **Generate the surrogate keys (Hash Keys) and hash diffs.**

This keeps our final DV models clean, simple, and focused only on loading data.

**Generating Keys: Using a Macro**
We'll use hashing to create our surrogate keys. dbt's `dbt_utils.generate_surrogate_key` is perfect for this. If `dbt_utils` isn't installed, we can create our own simple macro.

*   **Create `macros/generate_surrogate_key.sql`:**
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

Now, let's implement the models.

#### A. Hubs (Business Keys)

We'll start with `Hub_Customer`.

**1. Staging Model for Customer**

*   **File:** `models/silver/staging/stg_sales__customer.sql`
*   **Purpose:** Prepare customer data for the hub.
*   **SQL:**
    ```sql
    -- models/silver/staging/stg_sales__customer.sql
    with source as (
        select
            "CustomerID" as customer_id,
            "PersonID" as person_id,
            "TerritoryID" as territory_id,
            _airbyte_extracted_at as loaddate -- Assuming a load timestamp from Bronze
        from {{ source('bronze_adventureworks', 'Sales.customer') }}
    ),

    derived_columns as (
        select
            *,
            {{ generate_surrogate_key(['customer_id']) }} as customer_hk,
            'Sales.Customer' as recordsource
        from source
    )

    select * from derived_columns
    ```

**2. Hub Model: `Hub_Customer`**

*   **File:** `models/silver/hubs/hub_customer.sql`
*   **Materialization:** `incremental` (Hubs only grow; they are never updated).
*   **SQL:**
    ```sql
    -- models/silver/hubs/hub_customer.sql
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

*   **File:** `models/silver/hubs/schema.yml` (you'll add all hubs to this file)
*   **YAML:**
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

**4. How to Run and Test**

1.  **Run the staging model and the hub:**
    ```bash
    # From the elt_pipeline/abc directory
    dbt run --select stg_sales__customer+
    ```
    *(The `+` at the end tells dbt to run the specified model AND all its downstream dependents, so it will run `stg_sales__customer` and then `hub_customer`.)*

2.  **Test the hub:**
    ```bash
    dbt test --select hub_customer
    ```

You would now **repeat this exact pattern for all other hubs**:
1.  Create a `stg_...` model for the source (`Person.person`, `Sales.salesorderheader`, etc.).
2.  In the staging model, generate the `_HK` hash key.
3.  Create the `hub_...` model, materialized as `incremental`.
4.  Add tests to `schema.yml`.
5.  Run and test using `dbt run --select stg_...` and `dbt test --select hub_...`.

---

### Phase 4: Execution Plan and Next Steps

Here is the recommended sequence for implementing the entire Silver layer and preparing for Gold.

**Sequence of Work:**

1.  **Setup (You are here):**
    *   [X] Get Bronze layer running.
    *   [X] Configure dbt `profiles.yml` for the `silver` schema.
    *   [X] Scaffold the dbt project directories.
    *   [X] Create `sources.yml` and the `generate_surrogate_key` macro.

2.  **Implement Staging and Hubs:**
    *   Implement all staging models (`stg_*`) first.
    *   Then, implement all Hub models (`hub_*`).
    *   Run and test all of them together: `dbt run --select tag:staging+` followed by `dbt test --select tag:hub`. (This requires you to add `tags: ["staging"]` and `tags: ["hub"]` to your model configs).

3.  **Implement Links:**
    *   Create Link models (`link_*`) in `models/silver/links`.
    *   These models will join the necessary **staging** tables to get the required Hub Keys (`_HK`).
    *   Materialize them as `incremental`.
    *   Add `relationship` tests in your `schema.yml` to ensure the foreign keys in the links correctly reference the primary keys in the hubs.
    *   Run and test: `dbt run --select tag:link` and `dbt test --select tag:link`.

4.  **Implement Satellites:**
    *   Create Satellite models (`sat_*`) in `models/silver/sats`.
    *   These models will select descriptive attributes from the staging tables.
    *   **Crucially**, for tracking history (SCD2), you need a `hashdiff` in your staging layer. This is a hash of all the attribute columns in the satellite.
    *   Your satellite's incremental logic will then be based on loading a new record only if the `hashdiff` has changed for a given Hub Key.
    *   Run and test: `dbt run --select tag:sat` and `dbt test --select tag:sat`.

**Preparing for the Gold Layer (Star Schema)**

Once your Data Vault is built and tested, creating the Gold layer becomes much simpler.
-   **Dimension Tables (`dim_customer`, `dim_product`):** These will be built by joining a Hub with its relevant Satellites. For example, `dim_customer` would be created by joining `hub_customer` with `sat_customer_details` (if you create one), `link_customer_person`, `hub_person`, and `sat_person_details`.
-   **Fact Tables (`fct_sales`):** These are typically built from Links and their Satellites. For example, `fct_sales` would be built from `link_order_product` and its satellite `sat_order_product_link`, bringing in the measures like `OrderQty` and `LineTotal`.

This structured approach ensures your data warehouse is robust, auditable, and ready to support the complex queries needed for customer churn prediction in Power BI and ML models.
