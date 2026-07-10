"""
run_all.py — Pakistani AWS + Snowflake Data Engineering Platform
=================================================================
Master script that runs the full pipeline end-to-end:

  1.  Generate Pakistani contextual data (customers, orders, lineitems)
  2.  Load into local SQLite (simulated Snowflake)
  3.  Streams & CDC demo (change tracking, transactional streams)
  4.  Tasks & Scheduling demo (daily aggregation pipeline)
  5.  UDF demo (scalar + tabular)
  6.  External Functions demo (PKR currency conversion Lambda)
  7.  AWS Python integrations (connector, pandas, glue, pyspark-style)
  8.  Airflow DAG simulation
  9.  Kafka streaming (produce 50 events, consume + sink)
  10. Data Governance & Security (masking, row policy, time-travel)
  11. Snowpark analytics + ML forecast

Run with: python run_all.py
"""

import os
import sys
import time

BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE)


def section(title: str, n: int):
    print(f"\n{'═'*60}")
    print(f"  STEP {n}: {title}")
    print(f"{'═'*60}")


def main():
    print("\n🇵🇰  Pakistani AWS + Snowflake Data Platform — Full Pipeline")
    print("=" * 60)
    start_total = time.time()

    # ── Step 1: Data generation ────────────────────────────────────
    section("Generate Pakistani Contextual Data", 1)
    from aws_snowflake_data_eng.two_data_generator import pk_data_generator
    pk_data_generator.generate_all(BASE)

    # ── Step 2: Load into local Snowflake (SQLite) ─────────────────
    section("Setup DB & Load Data (COPY INTO simulation)", 2)
    from aws_snowflake_data_eng.three_data_loading import setup_db
    setup_db.setup()

    # ── Step 3: Streams & CDC ──────────────────────────────────────
    section("Streams & Change Data Capture", 3)
    from aws_snowflake_data_eng.four_streams_cdc import streams_cdc
    streams_cdc.run_demo()

    # ── Step 4: Tasks & Scheduling ─────────────────────────────────
    section("Tasks & Query Scheduling", 4)
    from aws_snowflake_data_eng.five_tasks_scheduling import tasks
    tasks.run_task_chain(["2024-03-15", "2024-06-01", "2024-09-20"])

    # ── Step 5: UDFs ───────────────────────────────────────────────
    section("User Defined Functions (Scalar + Tabular)", 5)
    from aws_snowflake_data_eng.six_udf import udf_simulation
    udf_simulation.run_demo()

    # ── Step 6: External Functions / Lambda ───────────────────────
    section("External Functions — PKR Currency Conversion", 6)
    from aws_snowflake_data_eng.seven_external_functions import lambda_currency
    lambda_currency.run_demo()

    # ── Step 7: AWS Python Integrations ───────────────────────────
    section("AWS Python → Snowflake (connector, pandas, glue, pyspark)", 7)
    from aws_snowflake_data_eng.eight_aws_python import snowflake_connector
    snowflake_connector.run_all()

    # ── Step 8: Airflow DAG simulation ────────────────────────────
    section("Airflow DAG — COPY INTO + Glue Transform", 8)
    from aws_snowflake_data_eng.eight_aws_python.airflow_dags import pk_sales_dag
    pk_sales_dag.run_dag_standalone()

    # ── Step 9: Kafka Streaming ────────────────────────────────────
    section("Kafka Streaming — Produce + Consume Pakistani Sales Events", 9)
    from aws_snowflake_data_eng.nine_kafka_streaming import pk_kafka_producer, pk_kafka_consumer
    pk_kafka_producer.stream_events(n=50, delay_s=0.01)
    pk_kafka_consumer.consume_and_sink()

    # ── Step 10: Governance & Security ────────────────────────────
    section("Data Governance & Security (Masking, RBAC, Time-Travel)", 10)
    from aws_snowflake_data_eng.ten_governance_security import governance
    governance.run_demo()

    # ── Step 11: Snowpark ─────────────────────────────────────────
    section("Snowpark Analytics + ML Forecast", 11)
    from aws_snowflake_data_eng.eleven_snowpark import snowpark_simulation
    snowpark_simulation.run_demo()

    elapsed = time.time() - start_total
    print(f"\n{'═'*60}")
    print(f"  ✅  Full pipeline complete in {elapsed:.1f}s")
    print(f"  DB : {os.path.join(BASE, 'local_db', 'pk_ecommerce.db')}")
    print(f"  S3 : {os.path.join(BASE, 'local_s3')}")
    print(f"{'═'*60}\n")


if __name__ == "__main__":
    main()
