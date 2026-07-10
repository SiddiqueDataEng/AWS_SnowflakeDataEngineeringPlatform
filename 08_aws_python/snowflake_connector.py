"""
08_aws_python/snowflake_connector.py
======================================
Local simulation of all Section 10 Python → Snowflake patterns:

  1. python-sf-local.py     → direct connector query
  2. pandas-sf.py           → read SQL to DataFrame
  3. glue-python.py         → AWS Glue Python job simulation
  4. glue-python-params.py  → parameterized Glue job
  5. pyspark-sf.py          → PySpark-style processing (pandas substitute)
  6. pyspark_orders_transform.py → join + aggregate + write back

All connect to local SQLite (pk_ecommerce.db) instead of real Snowflake.
"""

import sqlite3
import os
import pandas as pd
from datetime import datetime

BASE    = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH = os.path.join(BASE, "local_db", "pk_ecommerce.db")


# ── Connection factory (simulates snowflake.connector.connect) ────────────────

class LocalSnowflakeConnection:
    """
    Mimics snowflake.connector.connect() API.
    Replace with real snowflake.connector.connect(...) for production.
    """
    def __init__(self, database: str = "pk_ecommerce_db", schema: str = "pk_ecommerce_dev"):
        self.database  = database
        self.schema    = schema
        self._conn     = sqlite3.connect(DB_PATH)
        self._conn.row_factory = sqlite3.Row
        print(f"  [CONNECT] {database}.{schema} (local SQLite: {os.path.basename(DB_PATH)})")

    def cursor(self):
        return LocalCursor(self._conn)

    def read_sql(self, sql: str) -> pd.DataFrame:
        return pd.read_sql_query(sql, self._conn)

    def close(self):
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


class LocalCursor:
    def __init__(self, conn: sqlite3.Connection):
        self._conn   = conn
        self._cursor = conn.cursor()
        self.sfqid   = f"local-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"

    def execute(self, sql: str, params=None):
        if params:
            self._cursor.execute(sql, params)
        else:
            self._cursor.execute(sql)

    def fetchall(self):
        return [dict(r) for r in self._cursor.fetchall()]

    def fetchone(self):
        r = self._cursor.fetchone()
        return dict(r) if r else None

    def get_results_from_sfqid(self, qid: str):
        pass  # no-op in local simulation

    def close(self):
        self._cursor.close()


# ── 1. python-sf-local.py — basic query ──────────────────────────────────────

def demo_basic_query():
    print("\n── 1. Basic Python → Snowflake query (python-sf-local.py) ──")
    conn = LocalSnowflakeConnection()
    cs = conn.cursor()
    try:
        cs.execute("""
            SELECT li.L_ORDERKEY, li.L_QUANTITY, li.L_EXTENDEDPRICE,
                   li.L_SHIPDATE, li.L_COURIER, s.S_NAME as SUPPLIER
            FROM LINEITEM li
            JOIN SUPPLIER s ON li.L_SUPPKEY = s.S_SUPPKEY
            LIMIT 5
        """)
        rows = cs.fetchall()
        print(f"  Fetched {len(rows)} rows:")
        for r in rows:
            print(f"    {r}")
    finally:
        cs.close()
        conn.close()


# ── 2. pandas-sf.py — DataFrame query ────────────────────────────────────────

def demo_pandas_query():
    print("\n── 2. Pandas → Snowflake query (pandas-sf.py) ──")
    with LocalSnowflakeConnection() as conn:
        sql = """
            SELECT
                o.O_ORDERDATE,
                o.O_ORDERPRIORITY,
                c.C_CITY,
                c.C_PROVINCE,
                round(o.O_TOTALPRICE, 2) as TOTAL_PKR
            FROM ORDERS o
            JOIN CUSTOMER c ON o.O_CUSTKEY = c.C_CUSTKEY
            WHERE o.O_ORDERSTATUS = 'F'
            ORDER BY o.O_TOTALPRICE DESC
            LIMIT 10
        """
        df = conn.read_sql(sql)
        print(df.to_string(index=False))


# ── 3. glue-python.py — AWS Glue Python job simulation ───────────────────────

def demo_glue_job():
    print("\n── 3. AWS Glue Python job (glue-python.py) ──")
    conn = LocalSnowflakeConnection(schema="pk_ecommerce_dev")
    cursor = conn.cursor()
    sql = "SELECT * FROM LINEITEM LIMIT 5"
    try:
        cursor.execute(sql)
        query_id = cursor.sfqid
        cursor.get_results_from_sfqid(query_id)
        results = cursor.fetchall()
        print(f"  QueryID: {query_id}")
        print(f"  First row: {results[0] if results else 'No data'}")
    except Exception as e:
        print(f"  [ERROR] {e}")
        raise
    finally:
        conn.close()


# ── 4. glue-python-params.py — parameterized Glue job ────────────────────────

def demo_glue_params(ship_date: str, supplier_key: int):
    print(f"\n── 4. Parameterized Glue job (glue-python-params.py) ship_date={ship_date} suppkey={supplier_key} ──")
    conn = LocalSnowflakeConnection()
    cursor = conn.cursor()
    sql = """
        SELECT *
        FROM LINEITEM
        WHERE L_SHIPDATE = ? AND L_SUPPKEY = ?
        LIMIT 3
    """
    try:
        cursor.execute(sql, (ship_date, supplier_key))
        results = cursor.fetchall()
        print(f"  Rows returned: {len(results)}")
        if results:
            print(f"  Sample: {results[0]}")
    finally:
        conn.close()


# ── 5 & 6. PySpark-style join + aggregate (pyspark_orders_transform.py) ──────

def demo_pyspark_transform():
    print("\n── 5. PySpark orders transform (pyspark_orders_transform.py) ──")
    with LocalSnowflakeConnection() as conn:
        df_orders = conn.read_sql("""
            SELECT O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE
            FROM ORDERS
        """)
        df_lineitems = conn.read_sql("""
            SELECT L_ORDERKEY, L_SHIPDATE, L_SHIPMODE, L_COURIER, L_EXTENDEDPRICE
            FROM LINEITEM
        """)

    # Join (mirrors df_lineitems.join(df_orders, on=..., how='inner'))
    df = df_lineitems.merge(df_orders, left_on="L_ORDERKEY", right_on="O_ORDERKEY", how="inner")

    # Aggregate by shipmode + courier + date (mirrors df_agg in pyspark_orders_transform.py)
    df_agg = (
        df.groupby(["L_SHIPMODE", "L_COURIER", "L_SHIPDATE"])
          .agg(
              TOTAL_REVENUE_PKR=("L_EXTENDEDPRICE", "sum"),
              ORDER_COUNT=("O_ORDERKEY", "count"),
          )
          .reset_index()
          .sort_values("TOTAL_REVENUE_PKR", ascending=False)
    )

    print(f"  Aggregated {len(df_agg)} shipmode/courier/date combinations.")
    print(df_agg.head(5).to_string(index=False))

    # Write-back to local DB (mirrors df_agg.write.format("snowflake").save())
    out_path = os.path.join(BASE, "local_db", "pk_ecommerce.db")
    engine_conn = sqlite3.connect(out_path)
    df_agg.to_sql("AGGREGATED_DAILY_SALES", engine_conn, if_exists="replace", index=False)
    engine_conn.close()
    print(f"  [WRITE-BACK] AGGREGATED_DAILY_SALES → {out_path}")


# ── Main ──────────────────────────────────────────────────────────────────────

def run_all():
    print("\n🐍  AWS Python ↔ Snowflake Integration Demos\n" + "─" * 48)
    demo_basic_query()
    demo_pandas_query()
    demo_glue_job()

    # Pick a real date + supplier from data
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute("SELECT L_SHIPDATE, L_SUPPKEY FROM LINEITEM LIMIT 1").fetchone()
    conn.close()
    if row:
        demo_glue_params(row[0], row[1])

    demo_pyspark_transform()
    print("\n✅  All AWS Python demos complete.")


if __name__ == "__main__":
    run_all()
