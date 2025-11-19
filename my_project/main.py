import sys
from elt_pipeline.ingest.psql_ingest import extract_data_from_sql, load_data_to_postgres

def main():
    query = "SELECT  * FROM AdventureWorks2022.Person.Person"
    data = extract_data_from_sql(query)
    # print(f"Extracted {len(data)} records from source database {data}.")
    load_data_to_postgres(data, table_name="person_data")

if __name__ == "__main__":
    main()
