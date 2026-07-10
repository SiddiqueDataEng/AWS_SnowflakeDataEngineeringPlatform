"""
run_pipeline.py
===============
Runs the full Pakistani AWS + Snowflake pipeline by invoking each
module directly (no package import issues — just run this file).

Usage:
    cd aws_snowflake_data_eng
    python run_pipeline.py
"""

import os
import sys
import time

# Add each module dir to path
THIS = os.path.dirname(os.path.abspath(__file__))
for subdir in [
    "02_data_generator", "03_data_loading", "04_streams_cdc",
    "05_tasks_scheduling", "06_udf", "07_external_functions",
    "08_aws_python", "09_kafka_streaming",
    "10_governance_security", "11_snowpark",
]:
    sys.path.insert(0, os.path.join(THIS, subdir))

# Override BASE in each module to point to THIS
os.chdir(THIS)


def hdr(n: int, title: str):
    print(f"\n{'═'*62}")
    print(f"  STEP {n:>2} │ {title}")
    print(f"{'═'*62}")


def main():
    print("\n🇵🇰  Pakistani AWS + Snowflake Data Engineering Platform")
    print("     Local Simulation — Full End-to-End Pipeline")
    t0 = time.time()

    # ── 1: Generate data ──────────────────────────────────────────
    hdr(1, "Generate Pakistani Contextual Data")
    import pk_data_generator
    pk_data_generator.generate_all(THIS)

    # ── 2: Setup DB & load ────────────────────────────────────────
    hdr(2, "Setup SQLite DB & COPY INTO (bulk load)")
    import setup_db
    setup_db.setup()

    # ── 3: Streams & CDC ─────────────────────────────────────────
    hdr(3, "Streams & Change Data Capture (telecom subscribers)")
    import streams_cdc
    streams_cdc.run_demo()

    # ── 4: Tasks & Scheduling ─────────────────────────────────────
    hdr(4, "Scheduled Tasks — Daily Sales Aggregation")
    import tasks
    tasks.run_task_chain(["2024-03-15", "2024-06-01", "2024-09-20"])

    # ── 5: UDFs ───────────────────────────────────────────────────
    hdr(5, "User Defined Functions (PKR conversion, GST, tabular)")
    import udf_simulation
    udf_simulation.run_demo()

    # ── 6: External Functions ─────────────────────────────────────
    hdr(6, "External Functions — PKR Currency Conversion Lambda")
    import lambda_currency
    lambda_currency.run_demo()

    # ── 7: AWS Python ─────────────────────────────────────────────
    hdr(7, "AWS Python Integrations (connector, pandas, glue, pyspark)")
    import snowflake_connector
    snowflake_connector.run_all()

    # ── 8: Airflow DAG ────────────────────────────────────────────
    hdr(8, "Airflow DAG Simulation (COPY INTO >> Glue Transform)")
    sys.path.insert(0, os.path.join(THIS, "08_aws_python", "airflow_dags"))
    import pk_sales_dag
    pk_sales_dag.run_dag_standalone()

    # ── 9: Kafka Streaming ────────────────────────────────────────
    hdr(9, "Kafka Streaming — 50 Pakistani Sales Events")
    import pk_kafka_producer
    import pk_kafka_consumer
    pk_kafka_producer.stream_events(n=50, delay_s=0.02)
    pk_kafka_consumer.consume_and_sink()

    # ── 10: Governance & Security ─────────────────────────────────
    hdr(10, "Data Governance, Masking, RBAC & Time-Travel")
    import governance
    governance.run_demo()

    # ── 11: Snowpark ──────────────────────────────────────────────
    hdr(11, "Snowpark Analytics + ML Sales Forecast")
    import snowpark_simulation
    snowpark_simulation.run_demo()

    elapsed = time.time() - t0
    print(f"\n{'═'*62}")
    print(f"  ✅  Pipeline complete in {elapsed:.1f}s")
    print(f"  Local DB  : {os.path.join(THIS, 'local_db', 'pk_ecommerce.db')}")
    print(f"  Local S3  : {os.path.join(THIS, 'local_s3')}")
    print(f"  Stream log: {os.path.join(THIS, 'local_s3', 'streaming', 'pk-sales-data.jsonl')}")
    print(f"{'═'*62}\n")


if __name__ == "__main__":
    main()
