import hashlib
import logging
import shutil
from datetime import datetime, timezone

import pandas as pd

from pipeline import config

log = logging.getLogger("email_source")
SUPPORTED = (".csv", ".xlsx", ".xls")


def inbox_files():
    config.EMAIL_INBOX.mkdir(parents=True, exist_ok=True)
    return sorted(p for p in config.EMAIL_INBOX.iterdir() if p.is_file() and p.suffix.lower() in SUPPORTED)


def file_hash(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_attachment(path):
    if path.suffix.lower() == ".csv":
        df = pd.read_csv(path, dtype=str, encoding="utf-8-sig")
    else:
        df = pd.read_excel(path, dtype=str)
    df = df.dropna(how="all")
    df["sender"] = "client@example.com"
    return df


def received_time(path):
    return datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).replace(tzinfo=None, microsecond=0)


def move_to(path, folder):
    target = config.EMAIL_INBOX / folder
    target.mkdir(exist_ok=True)
    destination = target / path.name
    if destination.exists():
        destination = target / f"{path.stem}_{datetime.now():%Y%m%d%H%M%S}{path.suffix}"
    shutil.move(str(path), destination)
    return destination
