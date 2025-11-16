import polars as pl
import pypyodbc as odbc
import pyodbc
import psycopg2
# import pandas as pd
# import matplotlib.pyplot as plt
# import seaborn as sns
import os
# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

#SQLSERVER CONNECTION
SQLSERVER_DRIVER = os.getenv("DRIVER_NAME")
SERVER = os.getenv("SERVER_NAME")
UID = os.getenv("USERNAME")
PWD = os.getenv("PASSWORD")
TRUST_CERT = os.getenv("TRUST_CERT")

conn = pyodbc.connect(f"DRIVER={SQLSERVER_DRIVER};SERVER={SERVER};UID={UID};PWD={PWD};TrustServerCertificate={TRUST_CERT};")
cursor = conn.cursor()
print("SQL Server connection successful")
conn.close()

# POSTGRESQL CONNECTION
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

try:
    pg_conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )
    pg_cur = pg_conn.cursor()
    print("Postgres connection successful")
except Exception as e:
    print("Postgres connection error:", e)
finally:
    try:
        pg_cur.close()
    except NameError:
        pass
    try:
        pg_conn.close()
    except NameError:
        pass