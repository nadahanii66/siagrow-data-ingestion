import logging
import sys

from pipeline.alerts import send_alert_email
from pipeline.loader import drop_unchanged, get_client, load_orders, save_rejected
from pipeline.logger import setup_logging
from pipeline.monitor import RunTracker, run_quality_checks, send_alert
from pipeline.schema_check import check_schema, has_errors, load_contract
from pipeline.sources import db_source, email_source
from pipeline.transform import SchemaError, standardize

log = logging.getLogger("pipeline")


def process(client, tracker, raw, source, received_at=None):
    result = standardize(raw, source, run_id=tracker.run_id, received_at=received_at)
    new_rows, unchanged = drop_unchanged(client, result.clean)
    loaded = load_orders(client, new_rows)
    rejected = save_rejected(client, result.rejected, source, tracker.run_id)
    tracker.add(rows_read=len(raw), rows_loaded=loaded, rows_rejected=rejected,
                rows_duplicate=result.duplicates, rows_unchanged=unchanged)


def report_issues(source, file_name, issues, blocked_note="The file was NOT loaded and was moved to the failed folder."):
    blocked = has_errors(issues)
    level = "ERROR" if blocked else "WARNING"
    lines = [f"Source: {source}", f"File: {file_name}", ""]
    lines += [f"[{issue.level.upper()}] {issue.message}" for issue in issues]
    lines += ["", blocked_note if blocked else "The file was loaded. Please review the warnings above."]
    log.warning("[%s] %s: %s", source, file_name, [issue.code for issue in issues])
    send_alert_email(f"{level} in {file_name}", "\n".join(lines))


def run_database(client):
    with RunTracker(client, "database") as tracker:
        for chunk, watermark in db_source.fetch_from_db():
            process(client, tracker, chunk, "database")
            db_source.save_watermark(watermark)
    return tracker


def run_email(client):
    contract = load_contract("orders")
    with RunTracker(client, "email") as tracker:
        for path in email_source.inbox_files():
            digest = email_source.file_hash(path)
            seen = client.command(
                "SELECT count() FROM processed_files WHERE file_hash = {h:String}", parameters={"h": digest}
            )
            if int(seen):
                log.warning("[email] %s was already loaded before (same content), skipping", path.name)
                email_source.move_to(path, "duplicates")
                continue
            try:
                raw = email_source.read_attachment(path)
                issues = [i for i in check_schema(raw, contract) if i.level != "info"]
                if issues:
                    report_issues("email", path.name, issues)
                if has_errors(issues):
                    tracker.add(files_failed=1)
                    tracker.warn(f"{path.name}: {', '.join(i.code for i in issues)}")
                    email_source.move_to(path, "failed")
                    continue
                process(client, tracker, raw, "email", received_at=email_source.received_time(path))
            except (SchemaError, ValueError, OSError) as error:
                tracker.add(files_failed=1)
                tracker.warn(f"{path.name}: {error}")
                send_alert_email(f"ERROR in {path.name}", f"Source: email\nFile: {path.name}\n\nThe file could not be read: {error}\n\nIt was moved to the failed folder.")
                email_source.move_to(path, "failed")
                continue
            client.insert("processed_files", [[path.name, digest, len(raw), tracker.run_id]],
                          column_names=["file_name", "file_hash", "rows", "run_id"])
            email_source.move_to(path, "processed")
            log.info("[email] %s loaded", path.name)
    return tracker


def main():
    setup_logging()
    try:
        client = get_client()
    except Exception as error:
        log.critical("Cannot connect to ClickHouse: %s", error)
        send_alert(f"Siagrow pipeline cannot connect to ClickHouse: {error}")
        return 1

    trackers = [run_database(client), run_email(client)]
    failed_checks = run_quality_checks(client)

    summary = client.query_df(
        "SELECT source, count() AS orders, sum(total_amount) AS revenue FROM orders_clean GROUP BY source ORDER BY source"
    )
    log.info("Totals in ClickHouse:\n%s", summary.to_string(index=False))

    ok = all(t.status == "success" for t in trackers) and failed_checks == 0
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())