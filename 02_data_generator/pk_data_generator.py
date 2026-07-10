"""
Pakistani Contextual Data Generator
====================================
Simulates a Pakistani ecommerce/retail order dataset.
Domain: Online retail covering mobile phones, electronics, clothing,
        food, agriculture, and textiles — all priced in PKR.

Entities: Customers (Pakistani names, CNIC IDs, cities by province),
          Suppliers (10 major Pakistani business hubs),
          Parts/Products (real Pakistani retail items),
          Orders, Lineitems (with local courier services & GST tax)
"""

import random
import csv
import json
import os
from datetime import datetime, timedelta
from faker import Faker

fake = Faker("en_US")
random.seed(42)

# ── Pakistani master data ──────────────────────────────────────────────────────

PK_CITIES = [
    ("Karachi", "Sindh"), ("Lahore", "Punjab"), ("Islamabad", "ICT"),
    ("Rawalpindi", "Punjab"), ("Peshawar", "KPK"), ("Quetta", "Balochistan"),
    ("Multan", "Punjab"), ("Faisalabad", "Punjab"), ("Hyderabad", "Sindh"),
    ("Sialkot", "Punjab"), ("Gujranwala", "Punjab"), ("Abbottabad", "KPK"),
    ("Sukkur", "Sindh"), ("Larkana", "Sindh"), ("Mardan", "KPK"),
]

PK_MALE_NAMES = [
    "Ahmed", "Muhammad", "Ali", "Hassan", "Usman", "Bilal", "Tariq", "Imran",
    "Zubair", "Asif", "Shahid", "Naveed", "Faisal", "Kamran", "Rizwan",
    "Waqas", "Adnan", "Khalid", "Sohail", "Irfan", "Umer", "Salman",
    "Danish", "Hamid", "Jawad", "Kashif", "Majid", "Naeem", "Omer", "Pervaiz",
]
PK_FEMALE_NAMES = [
    "Fatima", "Ayesha", "Zainab", "Mariam", "Sara", "Nadia", "Sana", "Hina",
    "Rabia", "Amna", "Sobia", "Uzma", "Sadia", "Farah", "Noor", "Mehwish",
    "Anum", "Bushra", "Iram", "Kiran", "Lubna", "Maryam", "Nighat", "Parveen",
]
PK_SURNAMES = [
    "Khan", "Malik", "Ahmed", "Chaudhry", "Butt", "Sheikh", "Qureshi", "Ansari",
    "Siddiqui", "Hussain", "Mirza", "Baig", "Rana", "Bhatti", "Hashmi",
    "Abbasi", "Rajput", "Gilani", "Niazi", "Afridi", "Wazir", "Baloch",
    "Mengal", "Bugti", "Soomro", "Leghari", "Talpur", "Bhutto",
]

PRODUCT_CATALOG = {
    "Mobile Phones": [
        ("Samsung Galaxy A54", 85000), ("Xiaomi Redmi Note 12", 45000),
        ("Vivo V25", 55000), ("Oppo Reno 8", 65000), ("iPhone 13", 250000),
        ("Tecno Camon 19", 35000), ("Infinix Note 12", 38000),
    ],
    "Electronics": [
        ("LG LED TV 43 inch", 75000), ("Dawlance AC 1.5 Ton", 95000),
        ("Haier Refrigerator 12 Cu Ft", 65000), ("Kenwood Microwave 30L", 18000),
        ("Dell Laptop Core i5", 120000), ("Orient Split AC 1 Ton", 80000),
    ],
    "Clothing": [
        ("Gul Ahmed Lawn 3PC", 3500), ("Khaadi Kurta Embroidered", 4500),
        ("J. Slim Fit Jeans", 3200), ("Bonanza Shalwar Kameez", 2800),
        ("Al-Karam Stitched Suit", 5000), ("Sapphire Winter Shawl", 2200),
    ],
    "Food & Grocery": [
        ("Basmati Rice 5kg", 1200), ("Sunflower Cooking Oil 5L", 1800),
        ("National Masala Variety Pack", 450), ("Tapal Danedar 900g", 700),
        ("Nestle Everyday Milk Powder 1kg", 600), ("Olper Milk 1L x12", 1400),
    ],
    "Agriculture": [
        ("Wheat Seeds 50kg Bag", 8500), ("DAP Fertilizer 50kg", 12000),
        ("Water Pump Motor 1HP", 22000), ("Mini Hand Tractor", 180000),
        ("Pesticide Spray 5L", 3500), ("Drip Irrigation Kit", 45000),
    ],
    "Textiles": [
        ("Cotton Yarn 1kg Cone", 1500), ("Silk Fabric 5m Roll", 7500),
        ("Denim Fabric 10m", 9000), ("Polyester Thread Set", 800),
        ("Hand Embroidery Kit", 2500), ("Wool Shawl 2m", 3800),
    ],
}

SUPPLIERS = [
    (1, "Karachi Traders Pvt Ltd",         "Karachi",     "Sindh"),
    (2, "Lahore Wholesale Hub",             "Lahore",      "Punjab"),
    (3, "Islamabad Tech Imports",           "Islamabad",   "ICT"),
    (4, "Multan Cotton & Co",               "Multan",      "Punjab"),
    (5, "Peshawar Agricultural Supplies",   "Peshawar",    "KPK"),
    (6, "Quetta Dry Fruits & Trading",      "Quetta",      "Balochistan"),
    (7, "Faisalabad Textile Mills",         "Faisalabad",  "Punjab"),
    (8, "Sialkot Sports & Goods",           "Sialkot",     "Punjab"),
    (9, "Hyderabad Electronics Hub",        "Hyderabad",   "Sindh"),
    (10, "Gujranwala Steel & Hardware",     "Gujranwala",  "Punjab"),
]

COURIER_SERVICES   = ["TCS", "Leopards Courier", "M&P Express", "Pakistan Post", "Trax", "Swyft"]
SHIP_MODES         = ["AIR", "ROAD", "RAIL", "SEA"]
SHIP_INSTRUCTIONS  = ["DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN", "FRAGILE HANDLE CAREFULLY"]
ORDER_PRIORITIES   = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-LOW", "5-NOT SPECIFIED"]
ORDER_STATUSES     = ["P", "O", "F"]   # Processing / Open / Fulfilled
GST_RATES          = [0.0, 0.05, 0.17]  # Pakistan GST rates


# ── Helpers ────────────────────────────────────────────────────────────────────

def random_date(start="2023-01-01", end="2024-12-31") -> datetime:
    s = datetime.strptime(start, "%Y-%m-%d")
    e = datetime.strptime(end, "%Y-%m-%d")
    return s + timedelta(days=random.randint(0, (e - s).days))


def gen_cnic() -> str:
    """Pakistani CNIC: DDDDD-DDDDDDD-D  (Divisional code + serial + check digit)"""
    return f"{random.randint(10000, 54000)}-{random.randint(1000000, 9999999)}-{random.randint(1, 9)}"


def gen_pk_phone() -> str:
    prefix = random.choice([
        "0300", "0301", "0302", "0303", "0311", "0312", "0313",
        "0321", "0322", "0331", "0332", "0333", "0345", "0346",
    ])
    return f"{prefix}-{random.randint(1000000, 9999999)}"


def gen_name():
    gender = random.choice(["M", "F"])
    first  = random.choice(PK_MALE_NAMES if gender == "M" else PK_FEMALE_NAMES)
    return f"{first} {random.choice(PK_SURNAMES)}", gender


# ── Entity generators ─────────────────────────────────────────────────────────

def generate_customers(n: int = 500) -> list[dict]:
    rows = []
    for i in range(1, n + 1):
        name, gender = gen_name()
        city, province = random.choice(PK_CITIES)
        rows.append({
            "C_CUSTKEY":  i,
            "C_NAME":     name,
            "C_CNIC":     gen_cnic(),
            "C_PHONE":    gen_pk_phone(),
            "C_GENDER":   gender,
            "C_CITY":     city,
            "C_PROVINCE": province,
            "C_ADDRESS":  f"H#{random.randint(1, 999)}, Street {random.randint(1, 50)}, {city}",
            "C_ACCTBAL":  round(random.uniform(0, 500_000), 2),
            "C_COMMENT":  fake.sentence(nb_words=6),
        })
    return rows


def generate_suppliers() -> list[dict]:
    return [
        {
            "S_SUPPKEY":  s[0],
            "S_NAME":     s[1],
            "S_ADDRESS":  f"Plot {random.randint(1, 200)}, Industrial Area, {s[2]}",
            "S_CITY":     s[2],
            "S_PROVINCE": s[3],
            "S_PHONE":    gen_pk_phone(),
            "S_ACCTBAL":  round(random.uniform(50_000, 5_000_000), 2),
            "S_COMMENT":  fake.sentence(nb_words=8),
        }
        for s in SUPPLIERS
    ]


def generate_parts() -> list[dict]:
    parts, pk = [], 1
    for category, products in PRODUCT_CATALOG.items():
        for name, base_price in products:
            for _ in range(5):   # 5 SKU variants per product
                parts.append({
                    "P_PARTKEY":    pk,
                    "P_NAME":       name,
                    "P_CATEGORY":   category,
                    "P_BRAND":      random.choice(["Brand-A", "Brand-B", "Brand-C", "Brand-D", "Brand-E"]),
                    "P_TYPE":       random.choice(["Standard", "Premium", "Economy", "Local", "Imported"]),
                    "P_SIZE":       random.randint(1, 50),
                    "P_RETAILPRICE": round(base_price * random.uniform(0.9, 1.1), 2),
                    "P_COMMENT":    fake.sentence(nb_words=5),
                })
                pk += 1
    return parts


def generate_orders(customers: list[dict], n: int = 2000) -> list[dict]:
    orders = []
    for i in range(1, n + 1):
        cust = random.choice(customers)
        orders.append({
            "O_ORDERKEY":      i,
            "O_CUSTKEY":       cust["C_CUSTKEY"],
            "O_ORDERSTATUS":   random.choice(ORDER_STATUSES),
            "O_TOTALPRICE":    0,          # filled after lineitems
            "O_ORDERDATE":     random_date().strftime("%Y-%m-%d"),
            "O_ORDERPRIORITY": random.choice(ORDER_PRIORITIES),
            "O_CLERK":         f"Clerk#{random.randint(1, 50):04d}",
            "O_SHIPPRIORITY":  random.randint(0, 3),
            "O_COMMENT":       fake.sentence(nb_words=7),
        })
    return orders


def generate_lineitems(orders: list[dict], parts: list[dict], suppliers: list[dict]) -> list[dict]:
    lineitems = []
    totals: dict[int, float] = {}

    for order in orders:
        order_dt = datetime.strptime(order["O_ORDERDATE"], "%Y-%m-%d")
        for line in range(1, random.randint(1, 6)):
            part  = random.choice(parts)
            supp  = random.choice(suppliers)
            qty   = random.randint(1, 20)
            price = part["P_RETAILPRICE"]
            disc  = round(random.choice([0, 0.02, 0.05, 0.07, 0.10, 0.15]), 2)
            tax   = round(random.choice(GST_RATES), 2)
            ext   = round(qty * price, 2)

            ship_date    = order_dt + timedelta(days=random.randint(1, 30))
            commit_date  = order_dt + timedelta(days=random.randint(3, 25))
            receipt_date = ship_date + timedelta(days=random.randint(1, 7))

            lineitems.append({
                "L_ORDERKEY":     order["O_ORDERKEY"],
                "L_PARTKEY":      part["P_PARTKEY"],
                "L_SUPPKEY":      supp["S_SUPPKEY"],
                "L_LINENUMBER":   line,
                "L_QUANTITY":     qty,
                "L_EXTENDEDPRICE": ext,
                "L_DISCOUNT":     disc,
                "L_TAX":          tax,
                "L_RETURNFLAG":   random.choice(["N", "R", "A"]),
                "L_LINESTATUS":   random.choice(["O", "F"]),
                "L_SHIPDATE":     ship_date.strftime("%Y-%m-%d"),
                "L_COMMITDATE":   commit_date.strftime("%Y-%m-%d"),
                "L_RECEIPTDATE":  receipt_date.strftime("%Y-%m-%d"),
                "L_SHIPINSTRUCT": random.choice(SHIP_INSTRUCTIONS),
                "L_SHIPMODE":     random.choice(SHIP_MODES),
                "L_COURIER":      random.choice(COURIER_SERVICES),
                "L_COMMENT":      fake.sentence(nb_words=5),
            })
            totals[order["O_ORDERKEY"]] = (
                totals.get(order["O_ORDERKEY"], 0)
                + ext * (1 - disc) * (1 + tax)
            )

    for o in orders:
        o["O_TOTALPRICE"] = round(totals.get(o["O_ORDERKEY"], 0), 2)

    return lineitems


# ── I/O helpers ───────────────────────────────────────────────────────────────

def save_csv(data: list[dict], path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=data[0].keys())
        w.writeheader()
        w.writerows(data)
    print(f"  [CSV]  {len(data):>6} rows  →  {path}")


def save_json(data: list[dict], path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  [JSON] {len(data):>6} records → {path}")


# ── Main ──────────────────────────────────────────────────────────────────────

def generate_all(output_base: str) -> dict:
    print("\n🇵🇰  Pakistani Contextual Data Generator\n" + "─" * 44)

    customers  = generate_customers(500)
    suppliers  = generate_suppliers()
    parts      = generate_parts()
    orders     = generate_orders(customers, 2000)
    lineitems  = generate_lineitems(orders, parts, suppliers)

    s3 = os.path.join(output_base, "local_s3", "pk_ecommerce_dev")
    save_csv(customers, os.path.join(s3, "customers", "customers.csv"))
    save_csv(suppliers, os.path.join(s3, "suppliers", "suppliers.csv"))
    save_csv(parts,     os.path.join(s3, "parts",     "parts.csv"))
    save_csv(orders,    os.path.join(s3, "orders",    "orders.csv"))
    save_csv(lineitems, os.path.join(s3, "lineitems", "lineitems.csv"))

    # JSON sample for streaming / snowpipe simulation
    save_json(lineitems[:200], os.path.join(s3, "lineitems", "lineitems_sample.json"))
    save_json(orders[:100],    os.path.join(s3, "orders",    "orders_sample.json"))

    print(f"\n✅  Generation complete — {len(lineitems)} lineitems across {len(orders)} orders")
    return dict(customers=customers, suppliers=suppliers,
                parts=parts, orders=orders, lineitems=lineitems)


if __name__ == "__main__":
    BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    generate_all(BASE)
