import logging
import time
import uuid
from datetime import datetime, timezone

import httpx

from pipeline import config

log = logging.getLogger("monitor")


def utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


class RunTracker:
    def __init__(self, client, source, run_id=None):
        self.client = client
        self.source = source
        self.run_id = run_id or str(uuid.uuid4())
        self.stats = dict(rows_read=0, rows_loaded=0, rows_rejected=0, rows_duplicate=0, rows_unchanged=0, files_failed=0)
        self.errors = []

    def add(self, **counts):
        for key, value in counts.items():
            self.stats[key] += int(value)

    def warn(self, message):
        self.errors.append(message)
        log.error("[%s] %s", self.source, message)

    def __enter__(self):
        self.started = utc_now()
        self.clock = time.monotonic()
        log.info("[%s] run %s started", self.source, self.run_id[:8])
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc is not None:
            log.exception("[%s] run failed", self.source, exc_info=(exc_type, exc, tb))
            self.errors.append(f"{exc_type.__name__}: {exc}")
            status = "failed"
        elif self.errors:
            status = "partial"
        else:
            status = "success"

        duration = round(time.monotonic() - self.clock, 2)
        log.info("[%s] %s in %ss %s", self.source, status, duration, self.stats)
        try:
            self.client.insert(
                "pipeline_runs",
                [[self.run_id, self.source, self.started, utc_now(), status, *self.stats.values(), duration, " | ".join(self.errors)[:2000]]],
                column_names=["run_id", "source", "started_at", "finished_at", "status", *self.stats.keys(), "duration_sec", "error"],
            )
        except Exception:
            log.exception("Could not write the run record to ClickHouse")
        if status == "failed":
            send_alert(f"Siagrow pipeline [{self.source}] {status}: {' | '.join(self.errors)[:500]}")
        self.status = status
        return True


def send_alert(text):
    from pipeline.alerts import send_alert_email

    send_alert_email(text.split(":")[0], text)
    if not config.ALERT_WEBHOOK_URL:
        return
    try:
        httpx.post(config.ALERT_WEBHOOK_URL, json={"text": text}, timeout=10)
    except Exception:
        log.exception("Alert could not be sent")


CHECKS = {
    "duplicate record_id in orders_clean": "SELECT count() - uniqExact(record_id) FROM orders_clean",
    "orders with total != quantity * price": "SELECT countIf(total_amount != quantity * unit_price) FROM orders_clean",
    "orders with non-positive quantity": "SELECT countIf(quantity = 0) FROM orders_clean",
    "orders dated in the future": "SELECT countIf(order_date > now() + INTERVAL 1 DAY) FROM orders_clean",
}


def run_quality_checks(client):
    failed = 0
    for name, sql in CHECKS.items():
        value = int(client.command(sql))
        if value:
            failed += 1
            log.error("Quality check failed: %s = %d", name, value)
        else:
            log.info("Quality check passed: %s", name)
    return failed