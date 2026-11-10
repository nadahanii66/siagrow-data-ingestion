import hashlib
import json
import logging
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path

import pandas as pd
import yaml

from pipeline import config
from pipeline.alerts import send_alert_email
from pipeline.loader import get_client
from pipeline.monitor import RunTracker, send_alert
from pipeline.schema_check import check_schema, has_errors, load_contract
from run_pipeline import process, report_issues

log = logging.getLogger("ingest")

RAW_DIR = config.ROOT / "data" / "raw"
QUEUE_DIR = config.ROOT / "data" / "retry_queue"
DEAD_DIR = QUEUE_DIR / "dead"
RETRY_MINUTES = [5, 15, 60]


class IngestError(Exception):
    def __init__(self, status, detail):
        super().__init__(detail)
        self.status = status
        self.detail = detail


def utc_now():
    return datetime.now(timezone.utc)


def load_customers():
    with open(config.ROOT / "config" / "customers.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)["customers"]


def save_raw(customer_id, filename, content):
    now = utc_now()
    folder = RAW_DIR / customer_id / now.strftime("%Y-%m-%d")
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{now:%H%M%S_%f}_{Path(filename).name}"
    path.write_bytes(content)
    return path


def read_file(filename, content):
    if filename.lower().endswith(".csv"):
        df = pd.read_csv(BytesIO(content), dtype=str, encoding="utf-8-sig")
    else:
        df = pd.read_excel(BytesIO(content), dtype=str)
    return df.dropna(how="all")


def enqueue(customer_id, filename, raw_path, reason, attempts=0):
    QUEUE_DIR.mkdir(parents=True, exist_ok=True)
    item = {
        "customer_id": customer_id,
        "filename": filename,
        "raw_path": str(raw_path),
        "attempts": attempts,
        "next_retry_at": (utc_now() + timedelta(minutes=RETRY_MINUTES[min(attempts, len(RETRY_MINUTES) - 1)])).isoformat(),
        "last_error": str(reason)[:500],
    }
    path = QUEUE_DIR / f"{Path(raw_path).stem}.json"
    path.write_text(json.dumps(item, indent=2), encoding="utf-8")
    log.warning("[%s] %s queued for retry at %s", customer_id, filename, item["next_retry_at"])
    return path


def ingest(customer_id, info, filename, content, raw_path, queue_on_failure=True):
    source = f"api_{customer_id}"
    try:
        client = get_client()
    except Exception as error:
        log.critical("Cannot connect to ClickHouse: %s", error)
        if not queue_on_failure:
            raise IngestError(503, str(error))
        enqueue(customer_id, filename, raw_path, error)
        send_alert(f"Siagrow API cannot reach ClickHouse: {error}. File {filename} from {customer_id} is saved at {raw_path} and queued for automatic retry")
        raise IngestError(503, "The service is temporarily unavailable. Your file is saved and will be processed automatically.")

    digest = hashlib.sha256(content).hexdigest()
    seen = client.command("SELECT count() FROM processed_files WHERE file_hash = {h:String}", parameters={"h": digest})
    if int(seen):
        raise IngestError(409, "This exact file was already uploaded before")

    try:
        df = read_file(filename, content)
    except Exception as error:
        send_alert_email(f"ERROR in {filename}",
                         f"Source: {source}\nFile: {filename}\n\nThe file could not be read: {error}\n\nThe original is saved at {raw_path}")
        raise IngestError(400, f"The file could not be read: {error}")
    df["sender"] = customer_id

    issues = [i for i in check_schema(df, load_contract(info.get("contract", "orders"))) if i.level != "info"]
    if issues:
        report_issues(source, filename, issues, f"The file was NOT loaded. The original is saved at {raw_path}")
    if has_errors(issues):
        raise IngestError(422, {"message": "File rejected", "issues": [vars(i) for i in issues]})

    with RunTracker(client, source) as tracker:
        process(client, tracker, df, source)
    if tracker.status == "failed":
        if queue_on_failure:
            enqueue(customer_id, filename, raw_path, " | ".join(tracker.errors))
        raise IngestError(500, "Loading failed. The file is saved and will be retried automatically.")

    client.insert("processed_files", [[filename, digest, len(df), tracker.run_id]],
                  column_names=["file_name", "file_hash", "rows", "run_id"])
    return {
        "status": "loaded",
        "customer": customer_id,
        "file": filename,
        "run_id": tracker.run_id,
        **tracker.stats,
        "warnings": [vars(i) for i in issues],
    }


def retry_pending():
    if not QUEUE_DIR.exists():
        return {"retried": 0, "loaded": 0, "still_waiting": 0, "gave_up": 0}
    customers = load_customers()
    summary = {"retried": 0, "loaded": 0, "still_waiting": 0, "gave_up": 0}

    for path in sorted(QUEUE_DIR.glob("*.json")):
        item = json.loads(path.read_text(encoding="utf-8"))
        if datetime.fromisoformat(item["next_retry_at"]) > utc_now():
            summary["still_waiting"] += 1
            continue

        summary["retried"] += 1
        attempts = item["attempts"] + 1
        path.unlink()
        try:
            content = Path(item["raw_path"]).read_bytes()
            ingest(item["customer_id"], customers[item["customer_id"]], item["filename"], content, item["raw_path"], queue_on_failure=False)
            summary["loaded"] += 1
            log.info("[%s] %s loaded on retry %d", item["customer_id"], item["filename"], attempts)
        except IngestError as error:
            if error.status not in (500, 503):
                log.warning("[%s] %s finished on retry with status %s", item["customer_id"], item["filename"], error.status)
                continue
            queued = QUEUE_DIR / path.name
            if attempts >= len(RETRY_MINUTES):
                queued.unlink(missing_ok=True)
                DEAD_DIR.mkdir(parents=True, exist_ok=True)
                item.update(attempts=attempts, last_error=str(error.detail))
                (DEAD_DIR / path.name).write_text(json.dumps(item, indent=2), encoding="utf-8")
                send_alert_email(f"GAVE UP on {item['filename']}",
                                 f"Customer: {item['customer_id']}\nFile: {item['filename']}\n\nThe file failed {attempts} retries and needs manual review.\nThe original is saved at {item['raw_path']}")
                summary["gave_up"] += 1
            else:
                enqueue(item["customer_id"], item["filename"], item["raw_path"], error.detail, attempts)
    return summary