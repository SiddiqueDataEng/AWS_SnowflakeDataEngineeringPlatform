"""
09_kafka_streaming/pk_kafka_producer.py
========================================
Local simulation of Section 11 Kafka streaming with Pakistani data.

Replaces:
  - fake_data.py  (de_DE Faker → Pakistani contextual data)
  - producer.py   (generic numbers → real Pakistani sales events)

Modes:
  1. Local queue mode (default, no Kafka required) — writes to local_s3/streaming/
  2. Kafka mode — set USE_KAFKA=True, needs kafka-python installed

Pakistani context:
  - Customers: Pakistani names, CNICs, phone numbers
  - Products: Mobile phones, clothing, electronics (PKR prices)
  - Cities: Karachi, Lahore, Islamabad, etc.
  - Payment: Easypaisa, JazzCash, Bank Transfer, COD
"""

import os
import sys
import json
import random
import time
from datetime import datetime, timedelta
from faker import Faker

BASE    = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
STREAM_OUT = os.path.join(BASE, "local_s3", "streaming")
os.makedirs(STREAM_OUT, exist_ok=True)

fake = Faker("en_US")
random.seed(None)   # real random for streaming simulation

# ── Pakistani master data ─────────────────────────────────────────────────────

PK_MALE   = ["Ahmed", "Muhammad", "Ali", "Hassan", "Usman", "Bilal", "Tariq", "Imran",
              "Rizwan", "Faisal", "Kamran", "Waqas", "Naveed", "Sohail", "Irfan"]
PK_FEMALE = ["Fatima", "Ayesha", "Zainab", "Sara", "Nadia", "Sana", "Hina",
              "Rabia", "Amna", "Sobia", "Farah", "Noor", "Maryam", "Kiran"]
PK_SNAMES = ["Khan", "Malik", "Ahmed", "Chaudhry", "Butt", "Sheikh", "Qureshi",
              "Hussain", "Mirza", "Baig", "Rana", "Bhatti", "Hashmi", "Abbasi"]
PK_CITIES = ["Karachi", "Lahore", "Islamabad", "Rawalpindi", "Peshawar",
              "Quetta", "Multan", "Faisalabad", "Hyderabad", "Sialkot"]
PRODUCTS  = [
    ("Samsung Galaxy A54", 85000, "Mobile Phones"),
    ("Xiaomi Redmi Note 12", 45000, "Mobile Phones"),
    ("Gul Ahmed Lawn 3PC", 3500, "Clothing"),
    ("Basmati Rice 5kg", 1200, "Food & Grocery"),
    ("LG LED TV 43 inch", 75000, "Electronics"),
    ("Khaadi Kurta", 4500, "Clothing"),
    ("Dawlance AC 1.5 Ton", 95000, "Electronics"),
    ("National Masala Pack", 450, "Food & Grocery"),
    ("Wheat Seeds 50kg", 8500, "Agriculture"),
    ("Vivo V25", 55000, "Mobile Phones"),
]
PAYMENT_METHODS = ["Easypaisa", "JazzCash", "HBL Bank Transfer",
                   "MCB Bank Transfer", "COD", "UBL Internet Banking"]
COURIERS  = ["TCS", "Leopards Courier", "M&P Express", "Pakistan Post", "Trax"]


def gen_cnic() -> str:
    return f"{random.randint(10000,54000)}-{random.randint(1000000,9999999)}-{random.randint(1,9)}"


def gen_phone() -> str:
    prefix = random.choice(["0300","0301","0311","0321","0333","0345","0346"])
    return f"{prefix}-{random.randint(1000000, 9999999)}"


def gen_event() -> dict:
    """Generate one Pakistani ecommerce transaction event."""
    gender = random.choice(["M", "F"])
    name   = f"{random.choice(PK_MALE if gender=='M' else PK_FEMALE)} {random.choice(PK_SNAMES)}"
    product_name, base_price, category = random.choice(PRODUCTS)
    qty    = random.randint(1, 5)
    price  = round(base_price * random.uniform(0.95, 1.05), 2)
    gst    = round(random.choice([0, 0.05, 0.17]), 2)
    amount = round(qty * price * (1 + gst), 2)

    return {
        "event_id":       f"EVT-{random.randint(100000, 999999)}",
        "transaction_ts": datetime.utcnow().isoformat(),
        "customer_name":  name,
        "customer_cnic":  gen_cnic(),
        "customer_phone": gen_phone(),
        "gender":         gender,
        "city":           random.choice(PK_CITIES),
        "product_name":   product_name,
        "product_category": category,
        "quantity":       qty,
        "unit_price_pkr": price,
        "gst_rate":       gst,
        "amount_pkr":     amount,
        "payment_method": random.choice(PAYMENT_METHODS),
        "courier":        random.choice(COURIERS),
        "is_cod":         random.choice([True, False]),
    }


# ── Local queue mode (no Kafka dependency) ────────────────────────────────────

class LocalQueueProducer:
    """Writes events to a local JSONL file — simulates Kafka topic."""

    def __init__(self, topic: str):
        self.topic    = topic
        self.out_file = os.path.join(STREAM_OUT, f"{topic}.jsonl")
        self._buffer  = []
        print(f"  [PRODUCER] Topic={topic}  Output={self.out_file}")

    def send(self, value: dict):
        self._buffer.append(value)
        with open(self.out_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(value, ensure_ascii=False) + "\n")

    def flush(self):
        print(f"  [FLUSH] {len(self._buffer)} events written to {self.topic}")
        self._buffer = []


# ── Kafka mode (optional) ─────────────────────────────────────────────────────

USE_KAFKA    = False   # flip to True if Kafka is running
TOPIC_NAME   = "pk-sales-data"
KAFKA_BROKER = "localhost:9092"


def get_producer():
    if USE_KAFKA:
        try:
            from kafka import KafkaProducer
            return KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda x: json.dumps(x).encode("utf-8"),
            )
        except ImportError:
            print("  [WARN] kafka-python not installed, falling back to local queue")
    return LocalQueueProducer(TOPIC_NAME)


# ── Main streaming loop ───────────────────────────────────────────────────────

def stream_events(n: int = 50, delay_s: float = 0.1):
    """
    Produce n Pakistani sales events.
    Mirrors fake_data.py loop with Pakistani context.
    """
    print(f"\n📡  Pakistani Sales Stream Producer\n" + "─" * 44)
    print(f"  Topic : {TOPIC_NAME}")
    print(f"  Events: {n}  delay: {delay_s}s each\n")

    producer = get_producer()

    for i in range(1, n + 1):
        event = gen_event()
        if isinstance(producer, LocalQueueProducer):
            producer.send(event)
        else:
            producer.send(TOPIC_NAME, value=json.dumps(event))

        if i % 10 == 0 or i == 1:
            print(f"  [{i:>3}/{n}] {event['customer_name']:<25} "
                  f"{event['city']:<12} {event['amount_pkr']:>10.2f} PKR  "
                  f"via {event['courier']}")
        time.sleep(delay_s)

    if isinstance(producer, LocalQueueProducer):
        producer.flush()
    else:
        producer.flush()

    print(f"\n✅  Streamed {n} events to topic '{TOPIC_NAME}'")


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 50
    stream_events(n, delay_s=0.05)
