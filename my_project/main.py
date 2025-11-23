import logging
from elt_pipeline.ingest.psql_ingest import extract_data_from_sql, load_data_to_postgres


def setup_logging():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    return logging.getLogger(__name__)


def main():
    logger = setup_logging()
    logger.info("Starting ETL process")

    src_prefix = ""

    list_tables_extract = [
        "Person.Person",
        "Person.BusinessEntityAddress",
        "Person.AddressType",
        "Person.BusinessEntityContact",
        "Person.StateProvince",
        "Person.CountryRegion",
        "Person.EmailAddress",
        "Person.PersonPhone",
        "Person.PhoneNumberType",
        "Person.ContactType",
        "Person.BusinessEntity",
        "Sales.Customer",
        "Sales.SalesOrderHeader",
        "Sales.SalesOrderDetail",
        "Sales.SalesTerritory",
        "Sales.SpecialOffer",
        "Sales.SpecialOfferProduct",
        "Sales.SalesReason",
        "Sales.SalesOrderHeaderSalesReason",
        "Production.Product",
        "Production.ProductSubcategory",
        "Production.ProductCategory",
    ]

    list_tables_load = [
        ("bronze", "person_person"),
        ("bronze", "person_business_entity_address"),
        ("bronze", "person_address_type"),
        ("bronze", "person_business_entity_contact"),
        ("bronze", "person_state_province"),
        ("bronze", "person_country_region"),
        ("bronze", "person_email_address"),
        ("bronze", "person_person_phone"),
        ("bronze", "person_phone_number_type"),
        ("bronze", "person_contact_type"),
        ("bronze", "person_business_entity"),
        ("bronze", "sales_customer"),
        ("bronze", "sales_sales_order_header"),
        ("bronze", "sales_sales_order_detail"),
        ("bronze", "sales_sales_territory"),
        ("bronze", "sales_special_offer"),
        ("bronze", "sales_special_offer_product"),
        ("bronze", "sales_sales_reason"),
        ("bronze", "sales_sales_order_header_sales_reason"),
        ("bronze", "production_product"),
        ("bronze", "production_product_subcategory"),
        ("bronze", "production_product_category"),
    ]

    for i, tbl in enumerate(list_tables_extract):
        full_src = f"{src_prefix}{tbl}"
        query = f"SELECT * FROM {full_src}"
        logger.info("Extracting from %s", full_src)
        try:
            data = extract_data_from_sql(query)
            row_count = len(data) if data is not None else 0
            logger.info("Extracted %s rows from %s", row_count, full_src)

            dest_schema, dest_table = list_tables_load[i]
            logger.info("Loading data into %s.%s", dest_schema, dest_table)
            load_data_to_postgres(data, schema_name=dest_schema, table_name=dest_table)
            logger.info("Loaded data into %s.%s", dest_schema, dest_table)
        except Exception:
            logger.exception("Failed processing table %s", full_src)

    # Extract for only people address table
    address_query = f"""SELECT [AddressID]
      ,[AddressLine1]
      ,[AddressLine2]
      ,[City]
      ,[StateProvinceID]
      ,[PostalCode]
      ,[rowguid]
      ,[ModifiedDate] FROM {src_prefix}Person.Address"""
    try:
        logger.info("Extracting Person.Address")
        data = extract_data_from_sql(address_query)
        row_count = len(data) if data is not None else 0
        logger.info("Extracted %s rows from Person.Address", row_count)

        dest_schema = "bronze"
        dest_table = "person_address"
        logger.info("Loading data into %s.%s", dest_schema, dest_table)
        load_data_to_postgres(data, schema_name=dest_schema, table_name=dest_table)
        logger.info("Loaded data into %s.%s", dest_schema, dest_table)
    except Exception:
        logger.exception("Failed processing Person.Address")

    logger.info("ETL process finished")


if __name__ == "__main__":
    main()
