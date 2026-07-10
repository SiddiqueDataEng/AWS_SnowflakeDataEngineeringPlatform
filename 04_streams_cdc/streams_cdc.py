"""
04_streams_cdc/streams_cdc.py
==============================
Locally simulates Snowflake Streams (CDC) and Tasks using SQLite triggers.

Demonstrates:
  - Standard (delta) stream  → captures INSERT / UPDATE / DELETE
  - Append-only stream       → captures INSERT only
  - Transactional stream     → routes by member_type (like streams-in-transactions.sql)
  - Change tracking via timestamp snapshots
  - Task simulation (scheduled MERGE into production tables)
"""

import sqlite3
import os
import time
from datetime import datetime

BASE    = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH = os.path.join(BASE, "local_db", "pk_ecommerce.db")


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def setup_stream_tables(conn: sqlite3.Connection):
    """Create stream-support tables that mirror Snowflake stream concepts."""

    # Raw staging table for membership/subscription data (Pakistani context)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS SUBSCRIBERS_RAW (
            ID           INTEGER PRIMARY KEY,
            NAME         TEXT NOT NULL,
            PHONE        TEXT,
            CITY         TEXT,
            PLAN_TYPE    TEXT NOT NULL,    -- 'prepaid' or 'postpaid'
            MONTHLY_FEE  REAL DEFAULT 0,
            CREATED_AT   TEXT DEFAULT (datetime('now'))
        );

        -- Production tables — split by plan type (like streams-in-transactions.sql)
        CREATE TABLE IF NOT EXISTS SUBSCRIBERS_PREPAID_PROD (
            ID          INTEGER PRIMARY KEY,
            NAME        TEXT,
            PHONE       TEXT,
            CITY        TEXT,
            MONTHLY_FEE REAL
        );

        CREATE TABLE IF NOT EXISTS SUBSCRIBERS_POSTPAID_PROD (
            ID          INTEGER PRIMARY KEY,
            NAME        TEXT,
            PHONE       TEXT,
            CITY        TEXT,
            MONTHLY_FEE REAL
        );

        -- Stream table: simulates Snowflake standard stream (delta)
        CREATE TABLE IF NOT EXISTS SUBSCRIBERS_STD_STREAM (
            STREAM_ID       INTEGER PRIMARY KEY AUTOINCREMENT,
            ID              INTEGER,
            NAME            TEXT,
            PHONE           TEXT,
            CITY            TEXT,
            PLAN_TYPE       TEXT,
            MONTHLY_FEE     REAL,
            METADATA_ACTION TEXT,          -- 'INSERT' | 'UPDATE' | 'DELETE'
            CAPTURED_AT     TEXT DEFAULT (datetime('now'))
        );

        -- Append-only stream: only INSERTs
        CREATE TABLE IF NOT EXISTS SUBSCRIBERS_APPEND_STREAM (
            STREAM_ID       INTEGER PRIMARY KEY AUTOINCREMENT,
            ID              INTEGER,
            NAME            TEXT,
            PHONE           TEXT,
            CITY            TEXT,
            PLAN_TYPE       TEXT,
            MONTHLY_FEE     REAL,
            CAPTURED_AT     TEXT DEFAULT (datetime('now'))
        );

        -- Change tracking snapshot table
        CREATE TABLE IF NOT EXISTS CHANGE_TRACKING_SNAPSHOTS (
            SNAPSHOT_ID  INTEGER PRIMARY KEY AUTOINCREMENT,
            TABLE_NAME   TEXT,
            SNAPSHOT_TS  TEXT,
            ROW_COUNT    INTEGER
        );
    """)

    # Triggers to populate streams (simulate Snowflake stream capture)
    conn.executescript("""
        DROP TRIGGER IF EXISTS trg_subscribers_insert;
        CREATE TRIGGER trg_subscribers_insert
        AFTER INSERT ON SUBSCRIBERS_RAW
        BEGIN
            INSERT INTO SUBSCRIBERS_STD_STREAM (ID, NAME, PHONE, CITY, PLAN_TYPE, MONTHLY_FEE, METADATA_ACTION)
            VALUES (NEW.ID, NEW.NAME, NEW.PHONE, NEW.CITY, NEW.PLAN_TYPE, NEW.MONTHLY_FEE, 'INSERT');

            INSERT INTO SUBSCRIBERS_APPEND_STREAM (ID, NAME, PHONE, CITY, PLAN_TYPE, MONTHLY_FEE)
            VALUES (NEW.ID, NEW.NAME, NEW.PHONE, NEW.CITY, NEW.PLAN_TYPE, NEW.MONTHLY_FEE);
        END;

        DROP TRIGGER IF EXISTS trg_subscribers_update;
        CREATE TRIGGER trg_subscribers_update
        AFTER UPDATE ON SUBSCRIBERS_RAW
        BEGIN
            -- Delta stream captures both pre- and post-image (simulated as UPDATE)
            INSERT INTO SUBSCRIBERS_STD_STREAM (ID, NAME, PHONE, CITY, PLAN_TYPE, MONTHLY_FEE, METADATA_ACTION)
            VALUES (NEW.ID, NEW.NAME, NEW.PHONE, NEW.CITY, NEW.PLAN_TYPE, NEW.MONTHLY_FEE, 'UPDATE');
        END;

        DROP TRIGGER IF EXISTS trg_subscribers_delete;
        CREATE TRIGGER trg_subscribers_delete
        AFTER DELETE ON SUBSCRIBERS_RAW
        BEGIN
            INSERT INTO SUBSCRIBERS_STD_STREAM (ID, NAME, PHONE, CITY, PLAN_TYPE, MONTHLY_FEE, METADATA_ACTION)
            VALUES (OLD.ID, OLD.NAME, OLD.PHONE, OLD.CITY, OLD.PLAN_TYPE, OLD.MONTHLY_FEE, 'DELETE');
        END;
    """)
    conn.commit()
    print("  [SETUP] Stream tables and triggers created.")


def insert_subscribers(conn: sqlite3.Connection):
    """Simulate inserting Pakistani telecom subscribers."""
    subscribers = [
        (1, "Ahmed Khan",      "0300-1234567", "Karachi",   "prepaid",  200),
        (2, "Fatima Malik",    "0321-7654321", "Lahore",    "postpaid", 800),
        (3, "Usman Chaudhry",  "0333-1111111", "Islamabad", "prepaid",  200),
        (4, "Ayesha Qureshi",  "0311-2222222", "Rawalpindi","postpaid", 1200),
        (5, "Bilal Ahmed",     "0345-3333333", "Peshawar",  "prepaid",  200),
        (6, "Zainab Hussain",  "0302-4444444", "Multan",    "postpaid", 600),
        (7, "Tariq Mirza",     "0312-5555555", "Faisalabad","prepaid",  150),
        (8, "Sara Baig",       "0346-6666666", "Sialkot",   "postpaid", 900),
    ]
    conn.executemany(
        "INSERT OR IGNORE INTO SUBSCRIBERS_RAW (ID, NAME, PHONE, CITY, PLAN_TYPE, MONTHLY_FEE) VALUES (?,?,?,?,?,?)",
        subscribers,
    )
    conn.commit()
    print(f"  [INSERT] {len(subscribers)} subscribers inserted → stream captured")


def read_stream(conn: sqlite3.Connection, stream_name: str, action_filter: str = None):
    """Read pending stream records — simulate SELECT * FROM <stream>"""
    if action_filter:
        rows = conn.execute(
            f"SELECT * FROM {stream_name} WHERE METADATA_ACTION = ?", (action_filter,)
        ).fetchall()
    else:
        rows = conn.execute(f"SELECT * FROM {stream_name}").fetchall()
    print(f"\n  [STREAM] {stream_name}  ({len(rows)} records{'  filter='+action_filter if action_filter else ''}):")
    for r in rows:
        print(f"    {dict(r)}")
    return rows


def consume_stream_transactional(conn: sqlite3.Connection):
    """
    Simulate a Snowflake transaction that consumes stream into two prod tables.
    Mirrors streams-in-transactions.sql — routes by PLAN_TYPE.
    """
    print("\n  [TASK] Consuming stream in transaction → PREPAID / POSTPAID prod tables")
    try:
        conn.execute("BEGIN")

        # Insert prepaid subscribers
        conn.execute("""
            INSERT OR REPLACE INTO SUBSCRIBERS_PREPAID_PROD (ID, NAME, PHONE, CITY, MONTHLY_FEE)
            SELECT ID, NAME, PHONE, CITY, MONTHLY_FEE
            FROM SUBSCRIBERS_STD_STREAM
            WHERE METADATA_ACTION = 'INSERT' AND PLAN_TYPE = 'prepaid'
        """)

        # Insert postpaid subscribers
        conn.execute("""
            INSERT OR REPLACE INTO SUBSCRIBERS_POSTPAID_PROD (ID, NAME, PHONE, CITY, MONTHLY_FEE)
            SELECT ID, NAME, PHONE, CITY, MONTHLY_FEE
            FROM SUBSCRIBERS_STD_STREAM
            WHERE METADATA_ACTION = 'INSERT' AND PLAN_TYPE = 'postpaid'
        """)

        # Clear consumed records from stream (advance the stream offset)
        conn.execute("DELETE FROM SUBSCRIBERS_STD_STREAM WHERE METADATA_ACTION = 'INSERT'")
        conn.execute("COMMIT")
        print("  [COMMIT] Transaction committed — stream offset advanced")
    except Exception as e:
        conn.execute("ROLLBACK")
        print(f"  [ROLLBACK] {e}")
        raise


def update_subscriber_fee(conn: sqlite3.Connection, sub_id: int, new_fee: float):
    """Simulate an UPDATE — delta stream captures the change."""
    conn.execute(
        "UPDATE SUBSCRIBERS_RAW SET MONTHLY_FEE = ? WHERE ID = ?",
        (new_fee, sub_id),
    )
    conn.commit()
    print(f"  [UPDATE] Subscriber {sub_id} fee → {new_fee} PKR  (delta stream captured)")


def merge_updates_to_prod(conn: sqlite3.Connection, prod_table: str, plan_type: str):
    """
    Simulate MERGE statement — apply stream UPDATEs to production table.
    Mirrors the MERGE in standard-delta-streams.sql.
    """
    rows = conn.execute(
        "SELECT * FROM SUBSCRIBERS_STD_STREAM WHERE METADATA_ACTION = 'UPDATE' AND PLAN_TYPE = ?",
        (plan_type,),
    ).fetchall()

    for r in rows:
        existing = conn.execute(
            f"SELECT 1 FROM {prod_table} WHERE ID = ?", (r["ID"],)
        ).fetchone()
        if existing:
            conn.execute(
                f"UPDATE {prod_table} SET NAME=?, PHONE=?, CITY=?, MONTHLY_FEE=? WHERE ID=?",
                (r["NAME"], r["PHONE"], r["CITY"], r["MONTHLY_FEE"], r["ID"]),
            )
        else:
            conn.execute(
                f"INSERT INTO {prod_table} (ID, NAME, PHONE, CITY, MONTHLY_FEE) VALUES (?,?,?,?,?)",
                (r["ID"], r["NAME"], r["PHONE"], r["CITY"], r["MONTHLY_FEE"]),
            )

    conn.execute(
        "DELETE FROM SUBSCRIBERS_STD_STREAM WHERE METADATA_ACTION = 'UPDATE' AND PLAN_TYPE = ?",
        (plan_type,),
    )
    conn.commit()
    print(f"  [MERGE] {len(rows)} updates merged into {prod_table}")


def check_prod_tables(conn: sqlite3.Connection):
    for tbl in ["SUBSCRIBERS_PREPAID_PROD", "SUBSCRIBERS_POSTPAID_PROD"]:
        rows = conn.execute(f"SELECT * FROM {tbl}").fetchall()
        print(f"\n  [{tbl}]  {len(rows)} rows:")
        for r in rows:
            print(f"    {dict(r)}")


def run_demo():
    print("\n📡  Streams & CDC Demo (Pakistani Telecom Subscribers)\n" + "─" * 50)
    conn = get_conn()

    setup_stream_tables(conn)
    insert_subscribers(conn)

    print("\n── Step 1: Read standard delta stream (all actions) ──")
    read_stream(conn, "SUBSCRIBERS_STD_STREAM")

    print("\n── Step 2: Consume stream transactionally → route to prod tables ──")
    consume_stream_transactional(conn)

    print("\n── Step 3: Update subscriber fees (simulate CDC) ──")
    update_subscriber_fee(conn, 2, 1000)   # Fatima's postpaid plan upgraded
    update_subscriber_fee(conn, 4, 1500)   # Ayesha upgraded

    print("\n── Step 4: Check delta stream for updates ──")
    read_stream(conn, "SUBSCRIBERS_STD_STREAM", "UPDATE")

    print("\n── Step 5: Merge updates into production tables ──")
    merge_updates_to_prod(conn, "SUBSCRIBERS_POSTPAID_PROD", "postpaid")

    print("\n── Step 6: Verify production tables ──")
    check_prod_tables(conn)

    conn.close()
    print("\n✅  CDC demo complete.")


if __name__ == "__main__":
    run_demo()
