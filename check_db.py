import sqlite3, os
db = os.path.join(os.path.dirname(__file__), "local_db", "pk_ecommerce.db")
if not os.path.exists(db):
    print("DB not found — run run_pipeline.py first")
    exit(1)
conn = sqlite3.connect(db)
tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
print(f"DB: {db}")
for t in tables:
    c = conn.execute(f"SELECT count(1) FROM {t[0]}").fetchone()[0]
    print(f"  {t[0]:<35} {c:>6} rows")
conn.close()
print("OK")
