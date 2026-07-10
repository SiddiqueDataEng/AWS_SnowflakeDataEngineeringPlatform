"""
08_aws_python/airflow_dags/pk_sales_dag.py
============================================
Local simulation of Section 10 Airflow DAG.

Original: snowflake_automation_dag.py
  → copy_data (SnowflakeOperator COPY INTO) >> glue_task (PythonOperator → boto3 Glue)

Pakistani version:
  → pk_copy_orders   (simulate COPY INTO pk_ecommerce_db.pk_ecommerce_dev.ORDERS)
  >> pk_glue_transform (simulate PySpark aggregation Glue job)

Can be run standalone (no Airflow server needed — simulates the DAG steps).
To use with real Airflow, pip install apache-airflow and set up snowflake_conn.
"""

import os
import sys
import sqlite3
import pandas as pd
import json
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BASE    = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DB_PATH = os.path.join(BASE, "local_db", "pk_ecommerce.db")
S3_BASE = os.path.join(BASE, "local_s3", "pk_ecommerce_dev")

# ── Task: simulate SnowflakeOperator (COPY INTO) ──────────────────────────────

SNOWFLAKE_SQL = [
    "-- use role sysadmin;",
    "-- use schema pk_ecommerce_db.pk_ecommerce_dev;",
    """
    -- COPY INTO orders from @stg_orders file_format=pk_csv_format ON_ERROR=ABORT_STATEMENT
    -- (Simulated locally: re-load orders from CSV)
    """,
]


def task_copy_orders(**kwargs):
    """Simulates SnowflakeOperator → COPY INTO ORDERS from S3 stage."""
    logger.info("Task: pk_copy_orders — loading orders CSV → SQLite")

    csv_path = os.path.join(S3_BASE, "orders", "orders.csv")
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Orders CSV not found: {csv_path}. Run data generator first.")

    df = pd.read_csv(csv_path)
    conn = sqlite3.connect(DB_PATH)
    df.to_sql("ORDERS", conn, if_exists="replace", index=False)
    conn.close()

    logger.info("COPY INTO ORDERS — %d rows loaded", len(df))
    return {"rows_loaded": len(df), "table": "ORDERS", "timestamp": datetime.utcnow().isoformat()}


# ── Task: simulate boto3 → Glue PySpark job ──────────────────────────────────

def task_glue_pyspark_transform(**kwargs):
    """
    Simulates: client.start_job_run(JobName='pyspark_pk_sales_agg')
    In production: boto3.client('glue').start_job_run(JobName=...) on ap-south-1
    """
    logger.info("Task: pk_glue_transform — running PySpark-style aggregation")

    conn = sqlite3.connect(DB_PATH)

    df_orders = pd.read_sql_query(
        "SELECT O_ORDERKEY, O_CUSTKEY, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY FROM ORDERS",
        conn,
    )
    df_li = pd.read_sql_query(
        "SELECT L_ORDERKEY, L_SHIPDATE, L_SHIPMODE, L_COURIER, L_EXTENDEDPRICE, L_DISCOUNT FROM LINEITEM",
        conn,
    )
    df_cust = pd.read_sql_query(
        "SELECT C_CUSTKEY, C_CITY, C_PROVINCE FROM CUSTOMER",
        conn,
    )
    conn.close()

    # Join lineitems → orders → customers
    df = df_li.merge(df_orders, left_on="L_ORDERKEY", right_on="O_ORDERKEY")
    df = df.merge(df_cust, left_on="O_CUSTKEY", right_on="C_CUSTKEY")

    # Aggregate: daily revenue by shipmode + courier + province
    df["NET_REVENUE_PKR"] = df["L_EXTENDEDPRICE"] * (1 - df["L_DISCOUNT"])
    df_agg = (
        df.groupby(["L_SHIPDATE", "L_SHIPMODE", "L_COURIER", "C_PROVINCE"])
          .agg(TOTAL_REVENUE_PKR=("NET_REVENUE_PKR", "sum"),
               ORDER_COUNT=("O_ORDERKEY", "nunique"))
          .reset_index()
          .sort_values("TOTAL_REVENUE_PKR", ascending=False)
    )

    # Write-back (mirrors df_agg.write.format("snowflake").save())
    out_conn = sqlite3.connect(DB_PATH)
    df_agg.to_sql("PK_DAILY_SALES_AGG", out_conn, if_exists="replace", index=False)
    out_conn.close()

    logger.info("Glue job complete — %d aggregation rows written to PK_DAILY_SALES_AGG", len(df_agg))
    return {"rows": len(df_agg), "table": "PK_DAILY_SALES_AGG"}


# ── DAG definition (Airflow-compatible + standalone) ─────────────────────────

DAG_ARGS = {
    "owner":      "DataEng-PK",
    "start_date": datetime(2024, 1, 1),
    "retries":    1,
    "retry_delay": timedelta(minutes=5),
}


def run_dag_standalone():
    """Run the DAG tasks sequentially without Airflow (local simulation)."""
    print("\n✈️  Airflow DAG — pk_sales_pipeline (standalone mode)\n" + "─" * 50)
    print(f"  DAG: pk_sales_pipeline  schedule: @daily")
    print(f"  Tasks: pk_copy_orders >> pk_glue_transform\n")

    logger.info("=== Task 1/2: pk_copy_orders ===")
    result1 = task_copy_orders()
    logger.info("Result: %s", result1)

    logger.info("=== Task 2/2: pk_glue_transform ===")
    result2 = task_glue_pyspark_transform()
    logger.info("Result: %s", result2)

    # Show sample output
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(
        "SELECT * FROM PK_DAILY_SALES_AGG ORDER BY TOTAL_REVENUE_PKR DESC LIMIT 5",
        conn,
    )
    conn.close()
    print("\n── Top 5 revenue rows from PK_DAILY_SALES_AGG ──")
    print(df.to_string(index=False))
    print("\n✅  DAG simulation complete.")


try:
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator

    dag = DAG(
        dag_id="pk_sales_pipeline",
        default_args=DAG_ARGS,
        schedule_interval="@daily",
        description="Pakistani ecommerce: COPY INTO + Glue PySpark aggregation",
    )

    with dag:
        copy_task = PythonOperator(
            task_id="pk_copy_orders",
            python_callable=task_copy_orders,
            provide_context=True,
        )
        glue_task = PythonOperator(
            task_id="pk_glue_transform",
            python_callable=task_glue_pyspark_transform,
            provide_context=True,
            execution_timeout=timedelta(minutes=30),
        )
        copy_task >> glue_task

except ImportError:
    pass   # Airflow not installed — use run_dag_standalone() instead


if __name__ == "__main__":
    run_dag_standalone()
