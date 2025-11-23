## Run the Silver **staging** models

These staging models read from Bronze (`schema: bronze`) and add:

* clean column names
* load timestamps (`loaddate`)
* hash keys for hubs (`*_hk`)

Run staging:

```bash
dbt run --select 'path:models/silver/staging'
```

That will build:

* `stg_sales__customer`
* `stg_person__person`
* `stg_person__address`
* `stg_person__email_address`
* `stg_sales__sales_order_header`
* `stg_sales__sales_order_detail`
* `stg_sales__sales_territory`
* `stg_production__product`
* `stg_person__business_entity_address`
* (and any other staging models in that folder)

---

## Build all **hubs**

Hubs are under `models/silver/hubs`. Run:

```bash
dbt run --select 'path:models/silver/hubs'
```

This will build (incremental):

* `hub_customer`
* `hub_person`
* `hub_salesorder`
* `hub_product`
* `hub_territory`
* `hub_address`

To run tests on hubs:

```bash
dbt test --select 'path:models/silver/hubs'
```

You should see:

* `unique` & `not_null` passing on each `*_hk`
* `unique` & `not_null` on the business keys (e.g. `customer_id`)

---

## Build all **links**

Links are under `models/silver/links`. Run:

```bash
dbt run --select 'path:models/silver/links'
```

This will build (incremental):

* `link_customer_person`
* `link_customer_order`
* `link_customer_territory`
* `link_order_product`
* `link_person_address`

Then test them:

```bash
dbt test --select 'path:models/silver/links'
```

What you should see:

* `orderproduct_lk`, `customerorder_lk`, etc. are `unique` and `not_null`
* `relationships` tests passing:

  * `customer_hk` → `hub_customer`
  * `person_hk` → `hub_person`
  * `salesorder_hk` → `hub_salesorder`
  * `product_hk` → `hub_product`
  * `territory_hk` → `hub_territory`
  * `address_hk` → `hub_address`

> If you change link schemas (e.g. add columns like `salesorder_id` / `product_id`), use `--full-refresh` on that specific link to rebuild the table.

---

## Build all **satellites**

Satellites are under `models/silver/sats`. Run:

```bash
dbt run --select 'path:models/silver/sats'
```

This builds satellites like:

* `sat_person_details`
* `sat_person_contact`
* `sat_address_details`
* `sat_product_details`
* `sat_salesorder_header`
* `sat_order_product_link`

Then test them:

```bash
dbt test --select 'path:models/silver/sats'
```

You should see:

* `relationships` from each satellite back to the correct hub or link:

  * `person_hk` → `hub_person`
  * `address_hk` → `hub_address`
  * `product_hk` → `hub_product`
  * `salesorder_hk` → `hub_salesorder`
  * `orderproduct_lk` → `link_order_product`

---

## 8. How do you know you “have the Silver layer”?

Once the steps above are done and tests are green:

1. **All Silver tables exist in Postgres**, typically in schema `silver`:

   * `silver.hub_*`
   * `silver.link_*`
   * `silver.sat_*`
   * plus `silver.stg_*` views/tables.

2. **dbt tests pass**:

   ```bash
   dbt test --select 'path:models/silver/hubs' 'path:models/silver/links' 'path:models/silver/sats'
   ```

3. Optionally, open the **dbt docs graph** to visually inspect the DV:

   ```bash
   dbt docs generate
   dbt docs serve
   ```

   * In the browser UI, search for `hub_customer`, `link_order_product`, `sat_order_product_link`, etc.
   * You should see the DAG: Bronze source → staging → hub/link → satellite.

At that point, your **Silver (Data Vault) layer is ready** to be used to build **Gold** models (star schemas) for analytics / churn features. You can tell teammates:

> “Run the Bronze ETL once, then from `abc/`:
> `dbt deps`, `dbt run --select 'path:models/silver/*'`, `dbt test --select 'path:models/silver/*'`.
> When tests are green and you see `hub_*`, `link_*`, `sat_*` in schema `silver`, the Silver layer is good to go.”
