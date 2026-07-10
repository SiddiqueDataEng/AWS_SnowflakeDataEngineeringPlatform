"""
05_tasks_scheduling/tasks.py
==============================
Simulates Snowflake Tasks & Query Scheduling locally.

Demonstrates:
  - Standalone scheduled task (daily sales aggregation)
  - Dependent task chain (summary → shipmode rollup)
  - Task history logging
  - CRON-style scheduling simulation (runs immediately for demo)

Pakistani context: aggregates daily PKR sales by city, province, courier
"""

import sqlite3
import os
import json
from datetime import datetime, timedelta

BASE      = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH   = os.path.join(BASE, "local_db", "pk_ecommerce.db")
LOG_FILE  = os.path.join(BASE, "logs", "task_history.json")


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def log_task(name: str, status: str, rows_affected: int, duration_ms: float):
    history = []
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE) as f:
            history = json.load(f)
    history.append({
        "TASK_NAME":      name,
        "STATUS":         status,
        "ROWS_AFFECTED":  rows_affected,
        "DURATION_MS":    round(duration_ms, 2),
        "SCHEDULED_TIME": datetime.utcnow().isoformat(),
    })
    with open(LOG_FILE, "w") as f:
        json.dump(history, f, indent=2)


def setup_task_tables(conn: sqlite3.Connection):
    conn.executescript("""
        -- Mirrors: DAILY_AGGREGATED_SUMMARY in create_tasks.sql
        CREATE TABLE IF NOT EXISTS DAILY_SALES_SUMMARY (
            SUM_QTY           REAL,
            TOTAL_BASE_PRICE  REAL,
            TOTAL_DISC_PRICE  REAL,
            TOTAL_CHARGE_PKR  REAL,
            ORDER_COUNT       INTEGER,
            SHIP_DATE         TEXT,
            SHIP_MODE         TEXT,
            COURIER           TEXT,
            PROVINCE          TEXT
        );

        -- Mirrors: ORDERS_BY_SHIPMODE
        CREATE TABLE IF NOT EXISTS ORDERS_BY_SHIPMODE (
            TOTAL_ORDERS     INTEGER,
            TOTAL_DISCOUNT   REAL,
            SHIP_DATE        TEXT,
            SHIP_MODE        TEXT,
            COURIER          TEXT
        );

        -- Task history (mirrors information_schema.task_history)
        CREATE TABLE IF NOT EXISTS TASK_HISTORY (
            TASK_NAME       TEXT,
            STATUS          TEXT,
            ROWS_AFFECTED   INTEGER,
            DURATION_MS     REAL,
            SCHEDULED_TIME  TEXT
        );
    """)
    conn.commit()


def task_daily_sales_summary(conn: sqlite3.Connection, ship_date: str = None):
    """
    Standalone Task: TSK_DAILY_SALES_SUMMARY
    Schedule: CRON 0 8 * * * UTC (runs daily at 08:00 UTC)

    Mirrors create_tasks.sql → inserts into DAILY_AGGREGATED_SUMMARY
    Pakistani version: includes courier + province breakdown
    """
    start = datetime.utcnow()
    if not ship_date:
        ship_date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

    conn.execute("DELETE FROM DAILY_SALES_SUMMARY WHERE SHIP_DATE = ?", (ship_date,))
    conn.execute("""
        INSERT INTO DAILY_SALES_SUMMARY
            (SUM_QTY, TOTAL_BASE_PRICE, TOTAL_DISC_PRICE, TOTAL_CHARGE_PKR,
             ORDER_COUNT, SHIP_DATE, SHIP_MODE, COURIER, PROVINCE)
        SELECT
            sum(li.L_QUANTITY)                                           as SUM_QTY,
            sum(li.L_EXTENDEDPRICE)                                      as TOTAL_BASE_PRICE,
            sum(li.L_EXTENDEDPRICE * (1 - li.L_DISCOUNT))               as TOTAL_DISC_PRICE,
            sum(li.L_EXTENDEDPRICE * (1 - li.L_DISCOUNT) * (1 + li.L_TAX)) as TOTAL_CHARGE_PKR,
            count(*)                                                     as ORDER_COUNT,
            li.L_SHIPDATE                                                as SHIP_DATE,
            li.L_SHIPMODE,
            li.L_COURIER,
            s.S_PROVINCE
        FROM LINEITEM li
        JOIN SUPPLIER s ON li.L_SUPPKEY = s.S_SUPPKEY
        WHERE li.L_SHIPDATE = ?
        GROUP BY li.L_SHIPDATE, li.L_SHIPMODE, li.L_COURIER, s.S_PROVINCE
    """, (ship_date,))
    conn.commit()

    rows = conn.execute(
        "SELECT count(1) as cnt FROM DAILY_SALES_SUMMARY WHERE SHIP_DATE = ?", (ship_date,)
    ).fetchone()["cnt"]
    dur = (datetime.utcnow() - start).total_seconds() * 1000
    log_task("TSK_DAILY_SALES_SUMMARY", "SUCCEEDED", rows, dur)
    print(f"  [TASK] TSK_DAILY_SALES_SUMMARY  date={ship_date}  rows={rows}  {dur:.1f}ms")
    return rows


def task_orders_by_shipmode(conn: sqlite3.Connection, ship_date: str):
    """
    Dependent Task: TSK_ORDERS_BY_SHIPMODE
    Runs AFTER TSK_DAILY_SALES_SUMMARY (mirrors create_tasks.sql)
    """
    start = datetime.utcnow()
    conn.execute("DELETE FROM ORDERS_BY_SHIPMODE WHERE SHIP_DATE = ?", (ship_date,))
    conn.execute("""
        INSERT INTO ORDERS_BY_SHIPMODE (TOTAL_ORDERS, TOTAL_DISCOUNT, SHIP_DATE, SHIP_MODE, COURIER)
        SELECT
            sum(ORDER_COUNT)      as TOTAL_ORDERS,
            round(sum(TOTAL_DISC_PRICE), 0) as TOTAL_DISCOUNT,
            SHIP_DATE,
            SHIP_MODE,
            COURIER
        FROM DAILY_SALES_SUMMARY
        WHERE SHIP_DATE = ?
        GROUP BY SHIP_DATE, SHIP_MODE, COURIER
    """, (ship_date,))
    conn.commit()

    rows = conn.execute(
        "SELECT count(1) as cnt FROM ORDERS_BY_SHIPMODE WHERE SHIP_DATE = ?", (ship_date,)
    ).fetchone()["cnt"]
    dur = (datetime.utcnow() - start).total_seconds() * 1000
    log_task("TSK_ORDERS_BY_SHIPMODE", "SUCCEEDED", rows, dur)
    print(f"  [TASK] TSK_ORDERS_BY_SHIPMODE   date={ship_date}  rows={rows}  {dur:.1f}ms")
    return rows


def run_task_chain(date_range: list[str]):
    """Run the full dependent task chain for a list of dates."""
    conn = get_conn()
    setup_task_tables(conn)

    print("\n⏰  Task Scheduler — Pakistani Sales Aggregation Pipeline\n" + "─" * 52)
    for ship_date in date_range:
        print(f"\n  ── Processing {ship_date} ──")
        r1 = task_daily_sales_summary(conn, ship_date)
        if r1 > 0:
            task_orders_by_shipmode(conn, ship_date)
        else:
            log_task("TSK_ORDERS_BY_SHIPMODE", "SKIPPED_NO_DATA", 0, 0)
            print(f"  [SKIP] TSK_ORDERS_BY_SHIPMODE — no data for {ship_date}")

    # Print summary
    print("\n── Task History (last 10 runs) ──")
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE) as f:
            history = json.load(f)
        for h in history[-10:]:
            print(f"  {h['SCHEDULED_TIME'][:19]}  {h['TASK_NAME']:<30}  {h['STATUS']}  rows={h['ROWS_AFFECTED']}")

    # Print aggregation sample
    print("\n── DAILY_SALES_SUMMARY sample (top 5 by revenue) ──")
    rows = conn.execute("""
        SELECT SHIP_DATE, SHIP_MODE, COURIER, PROVINCE,
               round(TOTAL_CHARGE_PKR, 0) as REVENUE_PKR, ORDER_COUNT
        FROM DAILY_SALES_SUMMARY
        ORDER BY TOTAL_CHARGE_PKR DESC
        LIMIT 5
    """).fetchall()
    for r in rows:
        print(f"  {dict(r)}")

    conn.close()
    print("\n✅  Task chain complete.")


if __name__ == "__main__":
    # Run for a sample date range
    dates = [
        "2024-03-15", "2024-03-16", "2024-03-17",
        "2024-06-01", "2024-09-20",
    ]
    run_task_chain(dates)
