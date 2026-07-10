"""
09_kafka_streaming/pk_kafka_consumer.py
========================================
Consumes from the local queue (or Kafka topic) and sinks into SQLite
— simulating the Snowflake Kafka Connector (SF_connect.properties).

Mirrors:
  - SF_connect.properties  →  topic-to-table mapping, buffer size
  - connect-standalone.properties → key/value converter setup

Pakistani context: inserts pk-sales-data events into SALES_DATA table
"""

import os
import json
import sqlite3
import time
from datetime import datetime

BASE       = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH    = os.path.join(BASE, "local_db", "pk_ecommerce.db")
STREAM_OUT = os.path.join(BASE, "local_s3", "streaming")
TOPIC_NAME = "pk-sales-data"

# Mirrors SF_connect.properties buffer settings
BUFFER_COUNT   = 100
BUFFER_FLUSH_S = 10


def setup_sink_table(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS SALES_DATA (
            EVENT_ID        TEXT PRIMARY KEY,
            TRANSACTION_TS  TEXT,
            CUSTOMER_NAME   TEXT,
            CUSTOMER_CNIC   TEXT,
            CUSTOMER_PHONE  TEXT,
            GENDER          TEXT,
            CITY            TEXT,
            PRODUCT_NAME    TEXT,
            PRODUCT_CATEGORY TEXT,
            QUANTITY        INTEGER,
            UNIT_PRICE_PKR  REAL,
            GST_RATE        REAL,
            AMOUNT_PKR      REAL,
            PAYMENT_METHOD  TEXT,
            COURIER         TEXT,
            IS_COD          INTEGER,
            INGESTED_AT     TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()


def consume_and_sink(max_records: int = None):
    """
    Read events from local topic file and sink into SALES_DATA.
    Simulates Kafka → Snowflake connector with buffer-based micro-batching.
    """
    topic_file = os.path.join(STREAM_OUT, f"{TOPIC_NAME}.jsonl")
    if not os.path.exists(topic_file):
        print(f"  [WARN] No data at {topic_file}. Run pk_kafka_producer.py first.")
        return

    conn = sqlite3.connect(DB_PATH)
    setup_sink_table(conn)

    print(f"\n📥  Kafka Consumer → Snowflake Sink (pk-sales-data → SALES_DATA)\n" + "─" * 55)
    print(f"  Topic     : {TOPIC_NAME}")
    print(f"  Buffer    : {BUFFER_COUNT} records")
    print(f"  Sink table: SALES_DATA\n")

    buffer = []
    total  = 0

    with open(topic_file, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            event = json.loads(line)

            buffer.append((
                event.get("event_id"),
                event.get("transaction_ts"),
                event.get("customer_name"),
                event.get("customer_cnic"),
                event.get("customer_phone"),
                event.get("gender"),
                event.get("city"),
                event.get("product_name"),
                event.get("product_category"),
                event.get("quantity"),
                event.get("unit_price_pkr"),
                event.get("gst_rate"),
                event.get("amount_pkr"),
                event.get("payment_method"),
                event.get("courier"),
                1 if event.get("is_cod") else 0,
            ))

            # Flush when buffer reaches BUFFER_COUNT
            if len(buffer) >= BUFFER_COUNT:
                flush_buffer(conn, buffer)
                total += len(buffer)
                buffer = []

            if max_records and total >= max_records:
                break

    # Flush remaining
    if buffer:
        flush_buffer(conn, buffer)
        total += len(buffer)

    conn.close()
    print(f"\n✅  Consumed {total} events → SALES_DATA table")

    # Show summary
    conn2 = sqlite3.connect(DB_PATH)
    rows = conn2.execute("""
        SELECT CITY, count(*) as ORDERS, round(sum(AMOUNT_PKR), 0) as TOTAL_PKR
        FROM SALES_DATA
        GROUP BY CITY
        ORDER BY TOTAL_PKR DESC
        LIMIT 10
    """).fetchall()
    conn2.close()
    print("\n── Sales by City (from stream) ──")
    for r in rows:
        print(f"  {r[0]:<15}  {r[1]:>5} orders   {r[2]:>12,.0f} PKR")


def flush_buffer(conn: sqlite3.Connection, buffer: list):
    conn.executemany("""
        INSERT OR REPLACE INTO SALES_DATA (
            EVENT_ID, TRANSACTION_TS, CUSTOMER_NAME, CUSTOMER_CNIC,
            CUSTOMER_PHONE, GENDER, CITY, PRODUCT_NAME, PRODUCT_CATEGORY,
            QUANTITY, UNIT_PRICE_PKR, GST_RATE, AMOUNT_PKR,
            PAYMENT_METHOD, COURIER, IS_COD
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, buffer)
    conn.commit()
    print(f"  [FLUSH] {len(buffer)} records committed to SALES_DATA @ {datetime.utcnow().isoformat()[:19]}")


if __name__ == "__main__":
    consume_and_sink()
