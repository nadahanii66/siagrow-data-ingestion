import json
import logging
import os
import sqlite3

import pandas as pd

from pipeline import config

log = logging.getLogger("db_source")
START = "1970-01-01 00:00:00"


def load_state():
    if config.STATE_FILE.exists():
        with open(config.STATE_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_state(state):
    config.STATE_FILE.parent.mkdir(exist_ok=True)
    tmp = config.STATE_FILE.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
    os.replace(tmp, config.STATE_FILE)


def fetch_from_db(chunksize=None):
    last = load_state().get("db_last_updated_at", START)
    if not config.CLIENT_DB_PATH.exists():
        raise FileNotFoundError(f"Client database not found at {config.CLIENT_DB_PATH}")

    conn = sqlite3.connect(f"file:{config.CLIENT_DB_PATH}?mode=ro", uri=True, timeout=10)
    try:
        chunks = pd.read_sql(
            "SELECT * FROM orders WHERE updated_at >= ? ORDER BY updated_at",
            conn,
            params=(last,),
            chunksize=chunksize or config.BATCH_SIZE,
        )
        for chunk in chunks:
            chunk["sender"] = "client_database"
            yield chunk, chunk["updated_at"].max()
    finally:
        conn.close()


def save_watermark(value):
    state = load_state()
    state["db_last_updated_at"] = value
    save_state(state)
