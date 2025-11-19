import os
from dotenv import load_dotenv
load_dotenv()

import pyodbc
import psycopg2

# Do not open connections at import time; provide factory functions instead.

def get_sql_conn():
    SQLSERVER_DRIVER = os.getenv("DRIVER_NAME")
    SERVER = os.getenv("SERVER_NAME")
    UID = os.getenv("USERNAME")
    PWD = os.getenv("PASSWORD")
    TRUST_CERT = os.getenv("TRUST_CERT", "no")
    conn_str = f"DRIVER={SQLSERVER_DRIVER};SERVER={SERVER};UID={UID};PWD={PWD};TrustServerCertificate={TRUST_CERT};"
    return pyodbc.connect(conn_str)

def get_pg_conn():
    PG_HOST = os.getenv("PG_HOST")
    PG_PORT = os.getenv("PG_PORT", "5432")
    PG_DB = os.getenv("PG_DB")
    PG_USER = os.getenv("PG_USER")
    PG_PASSWORD = os.getenv("PG_PASSWORD")
    return psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD)


# Test connections (uncomment to test)
# if __name__ == "__main__":
#     sqlserver_table = "AdventureWorks2022.Person.Address"  # Example table
#     dst_schema = "AdventureWorks2022"
#     dst_table = "your_pg_table"

#     import polars as pl
#     import io

#     # Ingest data from SQL Server
#     sql_conn = get_sql_conn()
#     if sql_conn is None:
#         raise Exception("Failed to connect to SQL Server")
    
#     sql_query = f"select top 100 * from AdventureWorks2022.Person.Person p ;"
    
#     df = pl.read_database(sql_query, connection=sql_conn)
#     print(f"Read {df.height} rows from SQL Server table {df}")
#     sql_conn.close()

#     # Map Polars dtypes to Postgres types

