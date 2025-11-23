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


def load_data_to_postgres(df: pl.DataFrame, schema_name: str, table_name: str):
    conn = get_pg_conn()
    from psycopg2 import sql, extras

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
        # Ensure schema exists
        create_schema_stmt = sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
            sql.Identifier(schema_name)
        )
        cur.execute(create_schema_stmt)

        # Build CREATE TABLE statement
        cols = []
        for name, dtype in zip(df.columns, df.dtypes):
            pg_type = type_map.get(str(dtype), "TEXT")
            cols.append(sql.SQL("{} {}").format(sql.Identifier(name), sql.SQL(pg_type)))

        table_ident = sql.Identifier(schema_name, table_name)

        create_stmt = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
            table_ident,
            sql.SQL(", ").join(cols),
        )
        cur.execute(create_stmt)
        conn.commit()

        if df.height == 0:
            return

        # Cast categorical columns to Utf8
        cat_cols = [
            c for c, d in zip(df.columns, df.dtypes) if str(d).startswith("Categorical")
        ]
        if cat_cols:
            df = df.with_columns([pl.col(c).cast(pl.Utf8) for c in cat_cols])

        records = list(df.iter_rows())

        cols_ident = [sql.Identifier(c) for c in df.columns]
        insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
            table_ident,
            sql.SQL(", ").join(cols_ident),
        )

        extras.execute_values(
            cur, insert_sql.as_string(conn), records, template=None, page_size=1000
        )
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

