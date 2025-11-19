import sys
from pathlib import Path
# Ensure project root is on sys.path so  package can be imported when running the script directly
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import polars as pl
import io
from ..connection import get_sql_conn, get_pg_conn

def extract_data_from_sql(query: str) -> pl.DataFrame:
    conn = get_sql_conn()
    df = pl.read_database(query, connection=conn)
    conn.close()

    
    # Read the CSV data into a Polars DataFrame
    return df
def load_data_to_postgres(df: pl.DataFrame, table_name: str):
    conn = get_pg_conn()
    # Write the DataFrame to a table_name
    # ...existing code...
    from psycopg2 import sql, extras

    # Map Polars dtypes (string form) to PostgreSQL types
    type_map = {
        "Int64": "BIGINT",
        "Int32": "INTEGER",
        "Int16": "SMALLINT",
        "UInt64": "BIGINT",
        "UInt32": "INTEGER",
        "Float64": "DOUBLE PRECISION",
        "Float32": "REAL",
        "Utf8": "TEXT",
        "Boolean": "BOOLEAN",
        "Date": "DATE",
        "Datetime": "TIMESTAMP",
        "Time": "TIME",
        "Categorical": "TEXT",
    }

    cur = conn.cursor()
    try:
        # Build CREATE TABLE statement using psycopg2.sql to safely quote identifiers
        cols = []
        for name, dtype in zip(df.columns, df.dtypes):
            pg_type = type_map.get(str(dtype), "TEXT")
            cols.append(sql.SQL("{} {}").format(sql.Identifier(name), sql.SQL(pg_type)))

        create_stmt = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
            sql.Identifier(table_name),
            sql.SQL(", ").join(cols),
        )
        cur.execute(create_stmt)
        conn.commit()

        # If there's no data, we're done after creating the table
        if df.height == 0:
            return

        # Ensure categorical columns are cast to Utf8 so values become strings
        cat_cols = [c for c, d in zip(df.columns, df.dtypes) if str(d).startswith("Categorical")]
        if cat_cols:
            df = df.with_columns([pl.col(c).cast(pl.Utf8) for c in cat_cols])

        # Prepare records for bulk insert using Polars-native iteration (no CSV, no pandas)
        # df.iter_rows() yields Python-native tuples (uses None for nulls)
        records = list(df.iter_rows())

        # Use psycopg2.extras.execute_values for fast bulk insert
        cols_ident = [sql.Identifier(c) for c in df.columns]
        insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
            sql.Identifier(table_name),
            sql.SQL(", ").join(cols_ident),
        )

        extras.execute_values(cur, insert_sql.as_string(conn), records, template=None, page_size=1000)
        conn.commit()
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass