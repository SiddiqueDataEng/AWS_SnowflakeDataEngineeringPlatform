"""
10_governance_security/governance.py
======================================
Local simulation of Section 12: Data Governance & Security.

Demonstrates (for Pakistani ecommerce platform):
  1. Column-level masking policies (CNIC, phone masking for non-admin roles)
  2. Row-level access policies (province-based data access)
  3. Role-Based Access Control (RBAC)
  4. Time-travel (snapshot-based undo/restore using SQLite)
  5. Data retention policy simulation
"""

import sqlite3
import os
import json
from datetime import datetime, timedelta

BASE    = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH = os.path.join(BASE, "local_db", "pk_ecommerce.db")

CURRENT_ROLE = "sysadmin"   # simulate USE ROLE


# ── RBAC ──────────────────────────────────────────────────────────────────────

ROLES = {
    "accountadmin": {
        "can_view_cnic":     True,
        "can_view_phone":    True,
        "can_view_all_rows": True,
        "provinces":         None,   # no restriction
    },
    "sysadmin": {
        "can_view_cnic":     True,
        "can_view_phone":    True,
        "can_view_all_rows": True,
        "provinces":         None,
    },
    "reporting_intern": {
        "can_view_cnic":     False,  # masked
        "can_view_phone":    False,  # masked
        "can_view_all_rows": True,
        "provinces":         None,
    },
    "punjab_regional_admin": {
        "can_view_cnic":     True,
        "can_view_phone":    True,
        "can_view_all_rows": False,  # row-level restriction
        "provinces":         ["Punjab"],
    },
    "sindh_regional_admin": {
        "can_view_cnic":     True,
        "can_view_phone":    True,
        "can_view_all_rows": False,
        "provinces":         ["Sindh"],
    },
}


def set_role(role: str):
    global CURRENT_ROLE
    if role not in ROLES:
        raise ValueError(f"Role '{role}' not found.")
    CURRENT_ROLE = role
    print(f"  [ROLE] USE ROLE {role}")


# ── Column-level masking ──────────────────────────────────────────────────────

def mask_cnic(cnic: str, role: str) -> str:
    """
    Mirrors: masking_policies.sql
    CREATE MASKING POLICY mask_cnic AS (val TEXT) RETURNS TEXT ->
      CASE WHEN current_role() IN ('reporting_intern') THEN '***-*******-*' ELSE val END;
    """
    if not ROLES[role]["can_view_cnic"]:
        return "***-*******-*"
    return cnic


def mask_phone(phone: str, role: str) -> str:
    if not ROLES[role]["can_view_phone"]:
        return "03**-*******"
    return phone


# ── Row-level access policy ───────────────────────────────────────────────────

def apply_row_policy(rows: list[dict], role: str) -> list[dict]:
    """
    Mirrors: row-level-policy.sql
    CREATE ROW ACCESS POLICY province_access AS (province_filter VARCHAR)
    RETURNS BOOLEAN ->
      CURRENT_ROLE() = 'ACCOUNTADMIN'
      OR EXISTS (SELECT 1 FROM access_management WHERE province=province_filter AND role=CURRENT_ROLE())
    """
    allowed = ROLES[role]["provinces"]
    if allowed is None:
        return rows   # no restriction for this role
    return [r for r in rows if r.get("C_PROVINCE") in allowed]


# ── Masked query simulation ───────────────────────────────────────────────────

def query_customers(conn: sqlite3.Connection, role: str, limit: int = 5) -> list[dict]:
    """SELECT with masking + row policy applied."""
    rows = conn.execute(
        "SELECT C_CUSTKEY, C_NAME, C_CNIC, C_PHONE, C_CITY, C_PROVINCE, C_ACCTBAL "
        "FROM CUSTOMER LIMIT 50"
    ).fetchall()

    result = [dict(r) for r in rows]
    # Apply row-level policy
    result = apply_row_policy(result, role)
    # Apply column masking
    for r in result:
        r["C_CNIC"]  = mask_cnic(r["C_CNIC"], role)
        r["C_PHONE"] = mask_phone(r["C_PHONE"], role)

    return result[:limit]


# ── Time-travel (snapshot-based restore) ─────────────────────────────────────

SNAPSHOTS: dict[str, list[dict]] = {}


def take_snapshot(conn: sqlite3.Connection, table: str) -> str:
    """
    Simulate: SET ts = CURRENT_TIMESTAMP()
    Captures a timestamped snapshot for time-travel.
    """
    rows = conn.execute(f"SELECT * FROM {table}").fetchall()
    ts   = datetime.utcnow().isoformat()
    SNAPSHOTS[f"{table}:{ts}"] = [dict(r) for r in rows]
    print(f"  [SNAPSHOT] {table} @ {ts[:19]}  ({len(rows)} rows captured)")
    return ts


def restore_from_snapshot(conn: sqlite3.Connection, table: str, ts: str):
    """
    Simulate: SELECT * FROM <table> AT(TIMESTAMP => ts)
    and:      CREATE TABLE <table>_restored CLONE <table> AT(TIMESTAMP => ts)
    """
    key = f"{table}:{ts}"
    if key not in SNAPSHOTS:
        # Find closest snapshot
        candidates = [k for k in SNAPSHOTS if k.startswith(f"{table}:")]
        if not candidates:
            print(f"  [ERROR] No snapshot found for {table}")
            return
        key = sorted(candidates)[-1]
        print(f"  [TIME-TRAVEL] Using closest snapshot: {key}")

    rows = SNAPSHOTS[key]
    if not rows:
        print(f"  [TIME-TRAVEL] Snapshot is empty.")
        return

    cols = list(rows[0].keys())
    ph   = ", ".join(["?"] * len(cols))
    conn.execute(f"DROP TABLE IF EXISTS {table}_RESTORED")
    conn.execute(f"CREATE TABLE {table}_RESTORED AS SELECT * FROM {table} WHERE 0")
    conn.executemany(
        f"INSERT INTO {table}_RESTORED ({', '.join(cols)}) VALUES ({ph})",
        [tuple(r[c] for c in cols) for r in rows],
    )
    conn.commit()
    print(f"  [RESTORE] {table}_RESTORED created with {len(rows)} rows from snapshot @ {key[len(table)+1:len(table)+20]}")


# ── Demo runner ───────────────────────────────────────────────────────────────

def run_demo():
    print("\n🔒  Data Governance & Security Demo (Pakistan Ecommerce)\n" + "─" * 55)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # 1. RBAC + masking demo
    print("\n── Column-Level Masking by Role ──")
    for role in ["accountadmin", "reporting_intern", "punjab_regional_admin"]:
        set_role(role)
        rows = query_customers(conn, role, limit=3)
        print(f"\n  Role={role}  ({len(rows)} rows visible):")
        for r in rows:
            print(f"    {r['C_NAME']:<25} CNIC={r['C_CNIC']}  Phone={r['C_PHONE']}  Province={r['C_PROVINCE']}")

    # 2. Row-level policy demo
    print("\n── Row-Level Access Policy ──")
    for role in ["sysadmin", "sindh_regional_admin", "punjab_regional_admin"]:
        rows = query_customers(conn, role, limit=5)
        provinces = list({r["C_PROVINCE"] for r in rows})
        print(f"  Role={role:<25} sees provinces: {provinces}")

    # 3. Time-travel demo
    print("\n── Time-Travel Demo ──")
    ts = take_snapshot(conn, "CUSTOMER")

    # Make a destructive change
    conn.execute("DELETE FROM CUSTOMER WHERE C_PROVINCE = 'Balochistan'")
    conn.commit()
    after_count = conn.execute("SELECT count(1) FROM CUSTOMER").fetchone()[0]
    print(f"  After DELETE: CUSTOMER has {after_count} rows")

    # Restore
    restore_from_snapshot(conn, "CUSTOMER", ts)
    restored = conn.execute("SELECT count(1) FROM CUSTOMER_RESTORED").fetchone()[0]
    print(f"  CUSTOMER_RESTORED has {restored} rows (Balochistan rows recovered)")

    # Optionally swap back
    conn.execute("INSERT OR IGNORE INTO CUSTOMER SELECT * FROM CUSTOMER_RESTORED")
    conn.commit()
    final = conn.execute("SELECT count(1) FROM CUSTOMER").fetchone()[0]
    print(f"  After restore-merge: CUSTOMER has {final} rows")

    # 4. Data retention info
    print("\n── Data Retention Policy (reference) ──")
    print("  ALTER TABLE LINEITEM SET data_retention_time_in_days = 30;")
    print("  ALTER TABLE ORDERS   SET data_retention_time_in_days = 30;")
    print("  (Snowflake time-travel window = 30 days for production tables)")

    conn.close()
    print("\n✅  Governance & Security demo complete.")


if __name__ == "__main__":
    run_demo()
