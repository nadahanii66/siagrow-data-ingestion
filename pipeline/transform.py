import hashlib
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation

import pandas as pd

from pipeline import config

log = logging.getLogger("transform")

COLUMNS = [
    "record_id", "source", "source_id", "sender", "customer_name", "product",
    "quantity", "unit_price", "total_amount", "city", "order_date",
    "received_at", "version", "row_hash", "ingested_at", "run_id",
]

RENAME = {
    "order_id": "source_id", "order_no": "source_id", "id": "source_id",
    "customer": "customer_name", "client": "customer_name", "name": "customer_name",
    "item": "product", "product_name": "product",
    "qty": "quantity", "amount": "quantity",
    "price": "unit_price", "unit price": "unit_price",
    "location": "city",
    "date": "order_date", "created_at": "order_date",
    "updated_at": "received_at",
    "total": "source_total", "total_amount": "source_total",
}

REQUIRED = ["source_id", "product", "quantity", "unit_price", "order_date"]
CENT = Decimal("0.01")


class SchemaError(Exception):
    pass


@dataclass
class Result:
    clean: pd.DataFrame
    rejected: pd.DataFrame
    duplicates: int


def normalize_columns(df):
    df = df.copy()
    df.columns = [" ".join(str(c).strip().lower().split()) for c in df.columns]
    df = df.rename(columns=RENAME)

    if df.columns.duplicated().any():
        merged = {}
        for name in dict.fromkeys(df.columns):
            block = df.loc[:, df.columns == name]
            if block.shape[1] == 1:
                merged[name] = block.iloc[:, 0]
                continue
            filled = block.notna().sum(axis=1)
            conflicts = (filled > 1) & (block.astype(str).nunique(axis=1) > 1)
            if conflicts.any():
                log.warning("Column mapping conflict on '%s' in %d row(s); keeping the first value", name, int(conflicts.sum()))
            merged[name] = block.bfill(axis=1).iloc[:, 0]
        df = pd.DataFrame(merged)
    return df


def check_schema(df, source):
    missing = [c for c in REQUIRED if c not in df.columns]
    if missing:
        raise SchemaError(f"{source}: missing required column(s) {missing}. Found {list(df.columns)}")


def clean_text(series):
    return series.astype("string").str.split().str.join(" ").replace("", pd.NA)


def to_money(value):
    try:
        if pd.isna(value):
            return None
        return Decimal(str(value).strip().replace(",", "")).quantize(CENT, ROUND_HALF_UP)
    except (InvalidOperation, ValueError, TypeError):
        return None


def to_utc(series):
    parsed = pd.to_datetime(series, errors="coerce", format="mixed")
    if parsed.dt.tz is None:
        parsed = parsed.dt.tz_localize(config.SOURCE_TIMEZONE, ambiguous="NaT", nonexistent="shift_forward")
    return parsed.dt.tz_convert("UTC").dt.tz_localize(None)


def make_id(source, source_id):
    return hashlib.md5(f"{source}|{source_id}".encode()).hexdigest()


def make_hash(row):
    parts = [row.customer_name, row.product, row.quantity, row.unit_price, row.city, row.order_date]
    return hashlib.md5("|".join(str(p) for p in parts).encode()).hexdigest()


def standardize(df, source, run_id="", received_at=None):
    if df is None or df.empty:
        return Result(pd.DataFrame(columns=COLUMNS), pd.DataFrame(), 0)

    df = normalize_columns(df)
    check_schema(df, source)

    now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    for col in COLUMNS + ["source_total"]:
        if col not in df.columns:
            df[col] = None
    raw = df.copy()

    df["source"] = source
    df["source_id"] = clean_text(df["source_id"]).str.upper()
    df["customer_name"] = clean_text(df["customer_name"]).str.title().fillna("Unknown")
    df["product"] = clean_text(df["product"]).str.title()
    df["city"] = clean_text(df["city"]).str.title().fillna("Unknown")
    df["sender"] = clean_text(df["sender"]).fillna(source)

    qty = pd.to_numeric(df["quantity"], errors="coerce")
    df["unit_price"] = df["unit_price"].map(to_money)
    source_total = df["source_total"].map(to_money)

    df["order_date"] = to_utc(df["order_date"])
    df["received_at"] = to_utc(df["received_at"]).fillna(received_at or now)

    reasons = pd.Series([[] for _ in range(len(df))], index=df.index)

    def flag(mask, reason):
        for i in df.index[mask.fillna(False).astype(bool)]:
            reasons[i].append(reason)

    flag(df["source_id"].isna(), "missing_source_id")
    flag(df["product"].isna(), "missing_product")
    flag(qty.isna(), "invalid_quantity")
    flag(qty.notna() & (qty <= 0), "non_positive_quantity")
    flag(qty.notna() & (qty % 1 != 0), "non_integer_quantity")
    flag(df["unit_price"].isna(), "invalid_price")
    flag(df["unit_price"].map(lambda p: p is not None and p < 0), "negative_price")
    flag(df["order_date"].isna(), "invalid_order_date")
    flag(df["order_date"] > pd.Timestamp(now) + pd.Timedelta(days=1), "future_order_date")
    flag(df["order_date"] < pd.Timestamp("2000-01-01"), "order_date_too_old")

    ok_numbers = qty.notna() & df["unit_price"].notna() & (qty % 1 == 0)
    df["quantity"] = qty.where(ok_numbers, 0).astype("int64")
    df["total_amount"] = [
        (Decimal(int(q)) * p).quantize(CENT, ROUND_HALF_UP) if good else None
        for q, p, good in zip(df["quantity"], df["unit_price"], ok_numbers)
    ]
    mismatch = pd.Series(
        [s is not None and t is not None and abs(s - t) > CENT for s, t in zip(source_total, df["total_amount"])],
        index=df.index,
    )
    flag(mismatch, "total_mismatch")

    bad = reasons.map(len) > 0
    rejected = raw[bad].copy()
    rejected["reason"] = reasons[bad].map(";".join)
    df = df[~bad].copy()

    df["record_id"] = [make_id(source, s) for s in df["source_id"]]
    df["version"] = df["received_at"]
    df = df.sort_values(["record_id", "version"])
    before = len(df)
    df = df.drop_duplicates(subset="record_id", keep="last")
    duplicates = before - len(df)

    df["row_hash"] = [make_hash(r) for r in df.itertuples()]
    df["ingested_at"] = now
    df["run_id"] = run_id
    df["order_date"] = df["order_date"].dt.floor("s")
    df["received_at"] = df["received_at"].dt.floor("s")

    for col in ["record_id", "source", "source_id", "sender", "customer_name", "product", "city"]:
        df[col] = df[col].astype(str)

    if len(rejected):
        log.warning("[%s] %d row(s) rejected: %s", source, len(rejected), rejected["reason"].value_counts().to_dict())
    if duplicates:
        log.info("[%s] %d duplicate row(s) removed inside the batch", source, duplicates)
    return Result(df[COLUMNS].reset_index(drop=True), rejected.reset_index(drop=True), duplicates)
