import argparse
import random
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline import config

PRODUCTS = {"Tomato Seeds": 150, "Organic Fertilizer": 320, "Drip Irrigation Kit": 1250,
            "Cucumber Seeds": 120, "Pesticide Spray": 210}
CITIES = ["Cairo", "Giza", "Alexandria", "Mansoura", "Tanta"]
NAMES = ["Ahmed Ali", "Mona Hassan", "Omar Khaled", "Sara Adel", "Youssef Samir"]

parser = argparse.ArgumentParser()
parser.add_argument("--new", type=int, default=20)
parser.add_argument("--update", type=int, default=0)
args = parser.parse_args()

config.CLIENT_DB_PATH.parent.mkdir(exist_ok=True)
conn = sqlite3.connect(config.CLIENT_DB_PATH)
conn.execute("""
CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY, customer TEXT, product TEXT, qty INTEGER,
    price REAL, city TEXT, created_at TEXT, updated_at TEXT
)""")

now = datetime.now()
stamp = now.strftime("%Y-%m-%d %H:%M:%S.%f")
count = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]

for i in range(args.new):
    product = random.choice(list(PRODUCTS))
    conn.execute("INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (
        f"DB-{count + i + 1:05d}", random.choice(NAMES), product, random.randint(1, 20),
        PRODUCTS[product], random.choice(CITIES),
        (now - timedelta(days=random.randint(0, 60))).strftime("%Y-%m-%d %H:%M:%S"), stamp,
    ))

ids = [r[0] for r in conn.execute("SELECT order_id FROM orders ORDER BY RANDOM() LIMIT ?", (args.update,))]
for order_id in ids:
    conn.execute("UPDATE orders SET qty = qty + 1, updated_at = ? WHERE order_id = ?", (stamp, order_id))

conn.commit()
total = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
print(f"Added {args.new}, updated {len(ids)} {ids}. Total orders now: {total}")
