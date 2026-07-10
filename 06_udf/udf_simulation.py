"""
06_udf/udf_simulation.py
=========================
Simulates Snowflake UDFs (Scalar, Tabular, JavaScript-style) locally.

Pakistani context UDFs:
  - convert_pkr_to_usd(amount, rate)         → scalar: currency conversion
  - get_gst_amount(price, gst_rate)           → scalar: Pakistan GST calc
  - sales_by_supplier(ship_date, supp_key)    → scalar: total sales per supplier
  - sales_detail_by_supplier(ship_date, key)  → tabular: return table
  - categorize_order_value(amount)            → scalar: PKR bracket labeling
  - ip_range_generator(prefix, start, end)    → tabular: JS-style UDTF simulation
"""

import sqlite3
import os
from typing import Generator

BASE    = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH = os.path.join(BASE, "local_db", "pk_ecommerce.db")


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ── Scalar UDFs ───────────────────────────────────────────────────────────────

def convert_pkr_to_usd(amount_pkr: float, exchange_rate: float = 278.0) -> float:
    """
    Scalar UDF: Convert PKR to USD.
    Equivalent to:
        CREATE FUNCTION convert_pkr_to_usd(amount FLOAT, rate FLOAT)
        RETURNS FLOAT AS $$ SELECT amount / rate $$;
    """
    return round(amount_pkr / exchange_rate, 4)


def get_gst_amount(price_pkr: float, gst_rate: float) -> float:
    """
    Scalar UDF: Calculate Pakistan GST on a given price.
    GST rates in Pakistan: 0%, 5%, 17% (General Sales Tax Act)
    """
    return round(price_pkr * gst_rate, 2)


def categorize_order_value(amount_pkr: float) -> str:
    """
    Scalar UDF: Categorize order value into PKR brackets.
    Equivalent to a JavaScript UDF returning a string label.
    """
    if amount_pkr < 5_000:
        return "Small (< 5K PKR)"
    elif amount_pkr < 50_000:
        return "Medium (5K–50K PKR)"
    elif amount_pkr < 200_000:
        return "Large (50K–200K PKR)"
    else:
        return "Enterprise (200K+ PKR)"


def sales_qty_by_supplier(conn: sqlite3.Connection, ship_date: str, supp_key: int) -> float:
    """
    Scalar UDF: Total quantity shipped by a supplier on a given date.
    Mirrors sql_udf.sql → sales_qty_by_supplier(ship_date, supplier_key)
    """
    row = conn.execute(
        """
        SELECT SUM(L_QUANTITY) as total_qty
        FROM LINEITEM
        WHERE L_SHIPDATE = ? AND L_SUPPKEY = ?
        """,
        (ship_date, supp_key),
    ).fetchone()
    return float(row["total_qty"] or 0)


# ── Tabular UDFs (UDTF) ───────────────────────────────────────────────────────

def sales_detail_by_supplier(
    conn: sqlite3.Connection, ship_date: str, supp_key: int
) -> list[dict]:
    """
    Tabular UDF: Returns a table of line items for a supplier on a given date.
    Mirrors the tabular UDF in sql_udf.sql.

    Equivalent to:
        CREATE FUNCTION sales_detail_by_supplier(...)
        RETURNS TABLE (supp_key INT, qty REAL, price REAL, courier TEXT)
        AS $$ SELECT ... $$;

    Call with: SELECT * FROM TABLE(sales_detail_by_supplier('2024-03-15', 1))
    """
    rows = conn.execute(
        """
        SELECT
            li.L_SUPPKEY          as SUPP_KEY,
            sum(li.L_QUANTITY)    as QTY_SOLD,
            sum(li.L_EXTENDEDPRICE * (1 - li.L_DISCOUNT)) as NET_REVENUE_PKR,
            li.L_COURIER          as COURIER,
            s.S_NAME              as SUPPLIER_NAME,
            s.S_CITY              as SUPPLIER_CITY
        FROM LINEITEM li
        JOIN SUPPLIER s ON li.L_SUPPKEY = s.S_SUPPKEY
        WHERE li.L_SHIPDATE = ? AND li.L_SUPPKEY = ?
        GROUP BY li.L_SUPPKEY, li.L_COURIER, s.S_NAME, s.S_CITY
        HAVING QTY_SOLD > 0
        """,
        (ship_date, supp_key),
    ).fetchall()
    return [dict(r) for r in rows]


def ip_range_generator(prefix: str, range_start: int, range_end: int) -> Generator[str, None, None]:
    """
    Tabular UDTF: Generate IP addresses in a range.
    Simulates the JavaScript UDTF from javascript_udf.sql.

    SELECT * FROM TABLE(range_to_values('10.10.1', 1, 10));
    """
    for i in range(range_start, range_end + 1):
        yield f"{prefix}.{i}"


# ── UDF Security role simulation ──────────────────────────────────────────────

ROLE_PERMISSIONS = {
    "sysadmin":       {"can_create_udf": True,  "can_view_ddl": True},
    "udf_developer":  {"can_create_udf": False,  "can_view_ddl": False},  # limited
    "reporting_user": {"can_create_udf": False,  "can_view_ddl": False},
}


def check_udf_permission(role: str, action: str) -> bool:
    perms = ROLE_PERMISSIONS.get(role, {})
    return perms.get(action, False)


# ── Demo runner ───────────────────────────────────────────────────────────────

def run_demo():
    print("\n🔧  UDF Simulation — Pakistani Ecommerce Platform\n" + "─" * 48)
    conn = get_conn()

    # Scalar UDFs
    print("\n── Scalar UDFs ──")
    print(f"  convert_pkr_to_usd(85000)        → ${convert_pkr_to_usd(85_000):.2f} USD")
    print(f"  convert_pkr_to_usd(3500)         → ${convert_pkr_to_usd(3_500):.2f} USD")
    print(f"  get_gst_amount(85000, 0.17)      → {get_gst_amount(85_000, 0.17):,.2f} PKR")
    print(f"  get_gst_amount(3500, 0.05)       → {get_gst_amount(3_500, 0.05):,.2f} PKR")
    print(f"  categorize_order_value(4500)     → {categorize_order_value(4_500)}")
    print(f"  categorize_order_value(75000)    → {categorize_order_value(75_000)}")
    print(f"  categorize_order_value(350000)   → {categorize_order_value(350_000)}")

    # Fetch a real ship_date from the data
    row = conn.execute("SELECT L_SHIPDATE, L_SUPPKEY FROM LINEITEM LIMIT 1").fetchone()
    if row:
        sd, sk = row["L_SHIPDATE"], row["L_SUPPKEY"]
        qty = sales_qty_by_supplier(conn, sd, sk)
        print(f"\n── Scalar UDF: sales_qty_by_supplier('{sd}', {sk}) → {qty} units")

        # Tabular UDF
        print(f"\n── Tabular UDF: sales_detail_by_supplier('{sd}', {sk}) ──")
        rows = sales_detail_by_supplier(conn, sd, sk)
        if rows:
            for r in rows:
                print(f"  {r}")
        else:
            print("  (no data for this date/supplier)")
    else:
        print("  (load data first with setup_db.py)")

    # UDTF IP generator
    print("\n── JavaScript-style UDTF: ip_range_generator('192.168.1', 1, 5) ──")
    for ip in ip_range_generator("192.168.1", 1, 5):
        print(f"  {ip}")

    # Role-based access check
    print("\n── UDF Security (RBAC) ──")
    for role in ["sysadmin", "udf_developer", "reporting_user"]:
        can_create = check_udf_permission(role, "can_create_udf")
        can_ddl    = check_udf_permission(role, "can_view_ddl")
        print(f"  Role={role:<20} create_udf={can_create}  view_ddl={can_ddl}")

    conn.close()
    print("\n✅  UDF demo complete.")


if __name__ == "__main__":
    run_demo()
