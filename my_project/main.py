import sys
from elt_pipeline.ingest.psql_ingest import extract_data_from_sql, load_data_to_postgres

def main():
    src_prefix = "AdventureWorks2022."  # put the prefix in a variable

    list_tables_extract = [
        'Person.Person',
        'Person.BusinessEntityAddress',
        'Person.AddressType',
        'Person.BusinessEntityContact',
        'Person.StateProvince',
        'Person.CountryRegion',
        'Person.EmailAddress',
        'Person.PersonPhone',
        'Person.PhoneNumberType',
        'Person.ContactType',
        'Person.BusinessEntity',
        'Sales.Customer',
        'Sales.SalesOrderHeader',
        'Sales.SalesOrderDetail',
        'Sales.SalesTerritory',
        'Sales.SpecialOffer',
        'Sales.SpecialOfferProduct',
        'Sales.SalesReason',
        'Sales.SalesOrderHeaderSalesReason',
        'Production.Product',
        'Production.ProductSubcategory',
        'Production.ProductCategory'
    ]
    list_tables_load = [
        'bronze.Person.person',
        'bronze.Person.business_entity_address',
        'bronze.Person.address_type',
        'bronze.Person.business_entity_contact',
        'bronze.Person.state_province',
        'bronze.Person.country_region',
        'bronze.Person.email_address',
        'bronze.Person.person_phone',
        'bronze.Person.phone_number_type',
        'bronze.Person.contact_type',
        'bronze.Person.business_entity',
        'bronze.Sales.customer',
        'bronze.Sales.sales_order_header',
        'bronze.Sales.sales_order_detail',
        'bronze.Sales.sales_territory',
        'bronze.Sales.special_offer',
        'bronze.Sales.special_offer_product',
        'bronze.Sales.sales_reason',
        'bronze.Sales.sales_order_header_sales_reason',
        'bronze.Production.product',
        'bronze.Production.product_subcategory',
        'bronze.Production.product_category'  
    ]

    for i, tbl in enumerate(list_tables_extract):
        query = f"SELECT * FROM {src_prefix}{tbl}"
        data = extract_data_from_sql(query)
        load_data_to_postgres(data, table_name=list_tables_load[i])

    # Extract for only people address table
    query = f"""SELECT [AddressID]
      ,[AddressLine1]
      ,[AddressLine2]
      ,[City]
      ,[StateProvinceID]
      ,[PostalCode]
      ,[rowguid]
      ,[ModifiedDate] FROM {src_prefix}Person.Address"""
    data = extract_data_from_sql(query)
    load_data_to_postgres(data, table_name='bronze.Person.address')

if __name__ == "__main__":
    main()
