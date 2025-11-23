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
        "bronze.Person.person",
        "bronze.Person.business_entity_address",
        "bronze.Person.address_type",
        "bronze.Person.business_entity_contact",
        "bronze.Person.state_province",
        "bronze.Person.country_region",
        "bronze.Person.email_address",
        "bronze.Person.person_phone",
        "bronze.Person.phone_number_type",
        "bronze.Person.contact_type",
        "bronze.Person.business_entity",
        "bronze.Sales.customer",
        "bronze.Sales.sales_order_header",
        "bronze.Sales.sales_order_detail",
        "bronze.Sales.sales_territory",
        "bronze.Sales.special_offer",
        "bronze.Sales.special_offer_product",
        "bronze.Sales.sales_reason",
        "bronze.Sales.sales_order_header_sales_reason",
        "bronze.Production.product",
        "bronze.Production.product_subcategory",
        "bronze.Production.product_category",
    ]

    for i, tbl in enumerate(list_tables_extract):
        full_src = f"{src_prefix}{tbl}"
        query = f"SELECT * FROM {full_src}"
        logger.info("Extracting from %s", full_src)
        try:
            data = extract_data_from_sql(query)
            row_count = len(data) if data is not None else 0
            logger.info("Extracted %s rows from %s", row_count, full_src)

            dest_table = list_tables_load[i]
            logger.info("Loading data into %s", dest_table)
            load_data_to_postgres(data, table_name=dest_table)
            logger.info("Loaded data into %s", dest_table)
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

        logger.info("Loading data into bronze.Person.address")
        load_data_to_postgres(data, table_name="bronze.Person.address")
        logger.info("Loaded data into bronze.Person.address")
    except Exception:
        logger.exception("Failed processing Person.Address")

    logger.info("ETL process finished")


if __name__ == "__main__":
    main()
