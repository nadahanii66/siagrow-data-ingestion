import json
import logging

import clickhouse_connect
from clickhouse_connect.driver.exceptions import OperationalError
from tenacity import before_sleep_log, retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from pipeline import config
from pipeline.transform import COLUMNS

log = logging.getLogger("loader")

RETRY = dict(
    retry=retry_if_exception_type((OperationalError, ConnectionError, TimeoutError)),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    before_sleep=before_sleep_log(log, logging.WARNING),
    reraise=True,
)


@retry(**RETRY)
def get_client(user=None, password=None, database=None):
    if user is None:
        user, password = config.pipeline_credentials()
    client = clickhouse_connect.get_client(
        host=config.CLICKHOUSE_HOST,
        port=config.CLICKHOUSE_PORT,
        username=user,
        password=password,
        database=database or config.CLICKHOUSE_DATABASE,
        secure=config.CLICKHOUSE_SECURE,
        connect_timeout=10,
        send_receive_timeout=120,
    )
    client.command("SELECT 1")
    return client


def existing_hashes(client, source, record_ids):
    found = {}
    ids = list(record_ids)
    for start in range(0, len(ids), 1000):
        result = client.query(
            "SELECT record_id, argMax(row_hash, version) FROM orders "
            "WHERE source = {source:String} AND has({ids:Array(String)}, record_id) GROUP BY record_id",
            parameters={"source": source, "ids": ids[start:start + 1000]},
        )
        found.update(dict(result.result_rows))
    return found


def drop_unchanged(client, df):
    if df.empty:
        return df, 0
    known = existing_hashes(client, df["source"].iat[0], df["record_id"])
    unchanged = df["record_id"].map(known).eq(df["row_hash"])
    return df[~unchanged], int(unchanged.sum())


@retry(**RETRY)
def insert_batch(client, table, df, columns):
    client.insert_df(table, df[columns], column_names=columns)


def load_orders(client, df):
    for start in range(0, len(df), config.BATCH_SIZE):
        insert_batch(client, "orders", df.iloc[start:start + config.BATCH_SIZE], COLUMNS)
    return len(df)


def save_rejected(client, rejected, source, run_id):
    if rejected.empty:
        return 0
    rows = rejected.drop(columns="reason").astype(object).where(rejected.notna(), None)
    out = rejected[["reason"]].copy()
    out["run_id"] = run_id
    out["source"] = source
    out["source_id"] = rejected.get("source_id", "").astype(str)
    out["raw_row"] = [json.dumps(r, default=str, ensure_ascii=False) for r in rows.to_dict("records")]
    insert_batch(client, "rejected_rows", out, ["run_id", "source", "source_id", "reason", "raw_row"])
    return len(out)
