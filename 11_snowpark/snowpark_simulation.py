"""
11_snowpark/snowpark_simulation.py
====================================
Local simulation of Section 13: Snowpark (Python DataFrame API).

Mirrors:
  - Df-Basic-Operations.ipynb  → DataFrame transformations
  - Stored-Procedures.ipynb    → Python stored procs
  - UDF.ipynb                  → Snowpark UDFs
  - Machine-Learning           → simple model training + deployment

Pakistani context:
  - Analyze customer order patterns by province
  - Train a simple sales prediction model
  - Demonstrate Snowpark-style DataFrame API using pandas + SQLite
"""

import sqlite3
import os
import pandas as pd
import json
from datetime import datetime

BASE    = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH = os.path.join(BASE, "local_db", "pk_ecommerce.db")


# ── Snowpark-style Session factory ───────────────────────────────────────────

class LocalSnowparkSession:
    """
    Mimics snowflake.snowpark.Session.builder.configs({...}).create()
    For production: replace with real Snowpark session.
    """
    def __init__(self):
        self._conn = sqlite3.connect(DB_PATH)
        print("  [SESSION] Snowpark session created (local SQLite simulation)")

    def table(self, name: str) -> pd.DataFrame:
        """Mirrors: session.table('ORDERS')"""
        return pd.read_sql_query(f"SELECT * FROM {name}", self._conn)

    def sql(self, query: str) -> pd.DataFrame:
        """Mirrors: session.sql('SELECT ...')"""
        return pd.read_sql_query(query, self._conn)

    def write_pandas(self, df: pd.DataFrame, table_name: str, overwrite: bool = True):
        """Mirrors: session.write_pandas(df, table_name, overwrite=True)"""
        df.to_sql(table_name, self._conn, if_exists="replace" if overwrite else "append", index=False)
        self._conn.commit()
        print(f"  [WRITE] {len(df)} rows → {table_name}")

    def close(self):
        self._conn.close()


# ── DataFrame basic operations (Df-Basic-Operations.ipynb) ────────────────────

def demo_dataframe_ops(session: LocalSnowparkSession):
    print("\n── DataFrame Basic Operations ──")

    df = session.table("ORDERS")
    print(f"  ORDERS shape: {df.shape}")
    print(f"  Schema:\n{df.dtypes.to_string()}")

    # Filter (mirrors df.filter(col('O_ORDERSTATUS') == 'F'))
    fulfilled = df[df["O_ORDERSTATUS"] == "F"]
    print(f"\n  Fulfilled orders: {len(fulfilled)}")

    # Select + rename (mirrors df.select('O_ORDERKEY', 'O_TOTALPRICE').rename(...))
    summary = df[["O_ORDERKEY", "O_TOTALPRICE", "O_ORDERDATE", "O_ORDERPRIORITY"]].copy()
    summary.columns = ["ORDER_ID", "TOTAL_PKR", "ORDER_DATE", "PRIORITY"]
    print(f"\n  Selected columns sample:\n{summary.head(3).to_string(index=False)}")

    # Aggregate (mirrors df.group_by('O_ORDERPRIORITY').agg(avg('O_TOTALPRICE')))
    agg = df.groupby("O_ORDERPRIORITY").agg(
        AVG_TOTAL_PKR=("O_TOTALPRICE", "mean"),
        ORDER_COUNT=("O_ORDERKEY", "count"),
    ).reset_index().sort_values("AVG_TOTAL_PKR", ascending=False)
    print(f"\n  Orders by Priority:\n{agg.to_string(index=False)}")

    return df


# ── Stored Procedure simulation (Stored-Procedures.ipynb) ─────────────────────

def sproc_load_daily_summary(session: LocalSnowparkSession, ship_date: str) -> str:
    """
    Simulates a Snowpark Python Stored Procedure.
    In Snowflake:
        session.sproc.register(func, name='load_daily_summary', ...)
        session.call('load_daily_summary', '2024-03-15')
    """
    print(f"\n── Stored Procedure: load_daily_summary('{ship_date}') ──")
    df = session.sql(f"""
        SELECT
            li.L_SHIPDATE,
            li.L_SHIPMODE,
            li.L_COURIER,
            s.S_PROVINCE,
            sum(li.L_QUANTITY)                                       as TOTAL_QTY,
            sum(li.L_EXTENDEDPRICE)                                  as TOTAL_BASE_PKR,
            sum(li.L_EXTENDEDPRICE * (1-li.L_DISCOUNT))             as DISC_REVENUE_PKR,
            count(distinct li.L_ORDERKEY)                            as ORDER_COUNT
        FROM LINEITEM li
        JOIN SUPPLIER s ON li.L_SUPPKEY = s.S_SUPPKEY
        WHERE li.L_SHIPDATE = '{ship_date}'
        GROUP BY li.L_SHIPDATE, li.L_SHIPMODE, li.L_COURIER, s.S_PROVINCE
    """)

    if df.empty:
        return f"No data for {ship_date}"

    session.write_pandas(df, "SPROC_DAILY_SUMMARY", overwrite=False)
    return f"SUCCESS: {len(df)} rows loaded for {ship_date}"


# ── Snowpark UDF simulation (UDF.ipynb) ───────────────────────────────────────

def demo_snowpark_udfs(session: LocalSnowparkSession):
    print("\n── Snowpark UDF Demo ──")

    df = session.table("LINEITEM")

    # UDF: classify order size in PKR
    def classify_order(price_pkr: float) -> str:
        if price_pkr < 5000:    return "Small"
        if price_pkr < 50000:   return "Medium"
        if price_pkr < 200000:  return "Large"
        return "Enterprise"

    # UDF: compute net amount after discount + GST
    def net_amount_pkr(price: float, discount: float, tax: float) -> float:
        return round(price * (1 - discount) * (1 + tax), 2)

    df["ORDER_SIZE"]     = df["L_EXTENDEDPRICE"].apply(classify_order)
    df["NET_AMOUNT_PKR"] = df.apply(
        lambda r: net_amount_pkr(r["L_EXTENDEDPRICE"], r["L_DISCOUNT"], r["L_TAX"]), axis=1
    )

    print(df[["L_ORDERKEY", "L_EXTENDEDPRICE", "NET_AMOUNT_PKR", "ORDER_SIZE"]].head(5).to_string(index=False))
    return df


# ── Machine Learning — sales prediction (Deploy-HousePricing-Model style) ─────

def demo_ml_sales_forecast(session: LocalSnowparkSession):
    print("\n── Machine Learning: Provincial Sales Forecast ──")

    try:
        from sklearn.linear_model import LinearRegression
        from sklearn.model_selection import train_test_split
        import numpy as np

        df = session.sql("""
            SELECT
                strftime('%Y', li.L_SHIPDATE) as YEAR,
                strftime('%m', li.L_SHIPDATE) as MONTH,
                s.S_PROVINCE,
                sum(li.L_EXTENDEDPRICE * (1-li.L_DISCOUNT)) as REVENUE_PKR,
                count(*) as ITEM_COUNT
            FROM LINEITEM li
            JOIN SUPPLIER s ON li.L_SUPPKEY = s.S_SUPPKEY
            GROUP BY 1, 2, 3
        """)

        if len(df) < 10:
            print("  (Not enough data for ML — load full dataset first)")
            return

        df["YEAR"]  = df["YEAR"].astype(int)
        df["MONTH"] = df["MONTH"].astype(int)

        province_enc = {p: i for i, p in enumerate(df["S_PROVINCE"].unique())}
        df["PROV_ENC"] = df["S_PROVINCE"].map(province_enc)

        X = df[["YEAR", "MONTH", "PROV_ENC", "ITEM_COUNT"]].values
        y = df["REVENUE_PKR"].values

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        model = LinearRegression()
        model.fit(X_train, y_train)
        score = model.score(X_test, y_test)

        print(f"  Model: LinearRegression  R²={score:.4f}")
        print(f"  Provinces encoded: {province_enc}")

        # Save predictions
        df["PREDICTED_PKR"] = model.predict(X)
        session.write_pandas(
            df[["YEAR", "MONTH", "S_PROVINCE", "REVENUE_PKR", "PREDICTED_PKR"]],
            "ML_SALES_FORECAST",
            overwrite=True,
        )
        print(df[["YEAR", "MONTH", "S_PROVINCE", "REVENUE_PKR", "PREDICTED_PKR"]].head(5).to_string(index=False))

    except ImportError:
        print("  scikit-learn not installed. Run: pip install scikit-learn")
        print("  Showing statistical summary instead.")
        df = session.sql("""
            SELECT S_PROVINCE, round(avg(L_EXTENDEDPRICE), 0) as AVG_PRICE_PKR,
                   count(*) as ITEMS
            FROM LINEITEM li JOIN SUPPLIER s ON li.L_SUPPKEY = s.S_SUPPKEY
            GROUP BY S_PROVINCE ORDER BY AVG_PRICE_PKR DESC
        """)
        print(df.to_string(index=False))


# ── Main ──────────────────────────────────────────────────────────────────────

def run_demo():
    print("\n❄️   Snowpark Simulation — Pakistani Ecommerce Analytics\n" + "─" * 55)
    session = LocalSnowparkSession()

    demo_dataframe_ops(session)
    demo_snowpark_udfs(session)

    # Run stored proc for a real date from data
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute("SELECT DISTINCT L_SHIPDATE FROM LINEITEM LIMIT 1").fetchone()
    conn.close()
    if row:
        result = sproc_load_daily_summary(session, row[0])
        print(f"  SPROC result: {result}")

    demo_ml_sales_forecast(session)
    session.close()
    print("\n✅  Snowpark demo complete.")


if __name__ == "__main__":
    run_demo()
