"""
03_data_loading/setup_db.py
===========================
Locally simulates Snowflake using SQLite.
Creates the pk_ecommerce.db and loads all CSV data generated
by the Pakistani data generator.

Snowflake concepts demonstrated:
  - Database / schema creation
  - Table DDL with clustering hints (comment)
  - COPY INTO simulation (bulk CSV load)
  - File format handling (CSV)
  - Load history tracking
"""

import sqlite3
import csv
import os
import json
from datetime import datetime

BASE       = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH    = os.path.join(BASE, "local_db", "pk_ecommerce.db")
S3_BASE    = os.path.join(BASE, "local_s3", "pk_ecommerce_dev")
LOG_FILE   = os.path.join(BASE, "logs", "load_history.json")

DDL = {
    "CUSTOMER": """
        CREATE TABLE IF NOT EXISTS CUSTOMER (
            C_CUSTKEY     INTEGER PRIMARY KEY,
            C_NAME        TEXT NOT NULL,
            C_CNIC        TEXT UNIQUE,
            C_PHONE       TEXT,
            C_GENDER      TEXT,
            C_CITY        TEXT,
            C_PROVINCE    TEXT,
            C_ADDRESS     TEXT,
            C_ACCTBAL     REAL,
            C_COMMENT     TEXT
        )
    """,
    "SUPPLIER": """
        CREATE TABLE IF NOT EXISTS SUPPLIER (
            S_SUPPKEY     INTEGER PRIMARY KEY,
            S_NAME        TEXT NOT NULL,
            S_ADDRESS     TEXT,
            S_CITY        TEXT,
            S_PROVINCE    TEXT,
            S_PHONE       TEXT,
            S_ACCTBAL     REAL,
            S_COMMENT     TEXT
        )
    """,
    "PART": """
        CREATE TABLE IF NOT EXISTS PART (
            P_PARTKEY     INTEGER PRIMARY KEY,
            P_NAME        TEXT NOT NULL,
            P_CATEGORY    TEXT,
            P_BRAND       TEXT,
            P_TYPE        TEXT,
            P_SIZE        INTEGER,
            P_RETAILPRICE REAL,
            P_COMMENT     TEXT
        )
    """,
    "ORDERS": """
        CREATE TABLE IF NOT EXISTS ORDERS (
            O_ORDERKEY      INTEGER PRIMARY KEY,
            O_CUSTKEY       INTEGER REFERENCES CUSTOMER(C_CUSTKEY),
            O_ORDERSTATUS   TEXT,
            O_TOTALPRICE    REAL,
            O_ORDERDATE     TEXT,
            O_ORDERPRIORITY TEXT,
            O_CLERK         TEXT,
            O_SHIPPRIORITY  INTEGER,
            O_COMMENT       TEXT
        )
    """,
    "LINEITEM": """
        CREATE TABLE IF NOT EXISTS LINEITEM (
            L_ORDERKEY      INTEGER REFERENCES ORDERS(O_ORDERKEY),
            L_PARTKEY       INTEGER REFERENCES PART(P_PARTKEY),
            L_SUPPKEY       INTEGER REFERENCES SUPPLIER(S_SUPPKEY),
            L_LINENUMBER    INTEGER,
            L_QUANTITY      REAL,
            L_EXTENDEDPRICE REAL,
            L_DISCOUNT      REAL,
            L_TAX           REAL,
            L_RETURNFLAG    TEXT,
            L_LINESTATUS    TEXT,
            L_SHIPDATE      TEXT,
            L_COMMITDATE    TEXT,
            L_RECEIPTDATE   TEXT,
            L_SHIPINSTRUCT  TEXT,
            L_SHIPMODE      TEXT,
            L_COURIER       TEXT,
            L_COMMENT       TEXT,
            PRIMARY KEY (L_ORDERKEY, L_LINENUMBER)
        )
    """,
    "LOAD_HISTORY": """
        CREATE TABLE IF NOT EXISTS LOAD_HISTORY (
            TABLE_NAME      TEXT,
            FILE_PATH       TEXT,
            ROW_COUNT       INTEGER,
            STATUS          TEXT,
            LOAD_TIME       TEXT
        )
    """,
}

CSV_MAP = {
    "CUSTOMER": os.path.join(S3_BASE, "customers", "customers.csv"),
    "SUPPLIER": os.path.join(S3_BASE, "suppliers", "suppliers.csv"),
    "PART":     os.path.join(S3_BASE, "parts",     "parts.csv"),
    "ORDERS":   os.path.join(S3_BASE, "orders",    "orders.csv"),
    "LINEITEM": os.path.join(S3_BASE, "lineitems", "lineitems.csv"),
}


def load_history_append(entry: dict):
    history = []
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE) as f:
            history = json.load(f)
    history.append(entry)
    with open(LOG_FILE, "w") as f:
        json.dump(history, f, indent=2)


def copy_csv_into_table(conn: sqlite3.Connection, table: str, csv_path: str):
    """Simulate Snowflake COPY INTO <table> FROM @stage/file.csv"""
    if not os.path.exists(csv_path):
        print(f"  [SKIP] {csv_path} not found — run data generator first.")
        return 0

    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        return 0

    cols   = list(rows[0].keys())
    ph     = ", ".join(["?"] * len(cols))
    sql    = f"INSERT OR REPLACE INTO {table} ({', '.join(cols)}) VALUES ({ph})"
    values = [tuple(r[c] for c in cols) for r in rows]

    conn.executemany(sql, values)
    conn.commit()

    entry = {
        "TABLE_NAME": table,
        "FILE_PATH":  csv_path,
        "ROW_COUNT":  len(rows),
        "STATUS":     "LOADED",
        "LOAD_TIME":  datetime.utcnow().isoformat(),
    }
    conn.execute(
        "INSERT INTO LOAD_HISTORY VALUES (?,?,?,?,?)",
        list(entry.values()),
    )
    conn.commit()
    load_history_append(entry)
    print(f"  [COPY] {table:12s}  {len(rows):>6} rows loaded from {os.path.basename(csv_path)}")
    return len(rows)


def setup():
    print("\n🗄️  Setting up local Snowflake simulation (SQLite)")
    print(f"   DB path: {DB_PATH}\n")

    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)

    # Create all tables (simulate database + schema creation)
    for name, ddl in DDL.items():
        conn.execute(ddl)
    conn.commit()
    print("  [DDL]  All tables created (pk_ecommerce_db.pk_ecommerce_liv)")

    # COPY INTO simulation — load order matters for FK constraints
    total = 0
    for table in ["CUSTOMER", "SUPPLIER", "PART", "ORDERS", "LINEITEM"]:
        total += copy_csv_into_table(conn, table, CSV_MAP[table])

    conn.close()
    print(f"\n✅  Setup complete — {total} total rows loaded into {DB_PATH}")


if __name__ == "__main__":
    setup()
