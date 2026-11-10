import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline import config

df = pd.DataFrame({
    "order_no": ["EM-101", "EM-102", "EM-103"],
    "customer": ["Karim Fathy", "Hana Mostafa", "Youssef Samir"],
    "product_name": ["Organic Fertilizer", "Tomato Seeds", "Drip Irrigation Kit"],
    "amount": [5, 12, 2],
    "price": [320, 150, 1250],
    "city": ["Ismailia", "Assiut", "Cairo"],
    "date": ["2026-10-16", "2026-10-16", "2026-10-17"],
})
path = config.EMAIL_INBOX / "orders_branch2.xlsx"
df.to_excel(path, index=False)
print(f"Created {path}")
