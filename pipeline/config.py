import os
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")


def required(name):
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required setting {name}. Copy .env.example to .env and fill it in.")
    return value


CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_SECURE = os.getenv("CLICKHOUSE_SECURE", "false").lower() == "true"
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "siagrow")

CLIENT_DB_PATH = ROOT / os.getenv("CLIENT_DB_PATH", "data/client_db.sqlite")
EMAIL_INBOX = ROOT / os.getenv("EMAIL_INBOX", "sample_data/email_inbox")
STATE_FILE = ROOT / "data" / "state.json"
LOG_DIR = ROOT / "logs"

SOURCE_TIMEZONE = os.getenv("SOURCE_TIMEZONE", "Africa/Cairo")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5000"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL", "")


def pipeline_credentials():
    return required("CLICKHOUSE_USER"), required("CLICKHOUSE_PASSWORD")


def admin_credentials():
    return required("CLICKHOUSE_ADMIN_USER"), required("CLICKHOUSE_ADMIN_PASSWORD")


def powerbi_credentials():
    return required("POWERBI_USER"), required("POWERBI_PASSWORD")
