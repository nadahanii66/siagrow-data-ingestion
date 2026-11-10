import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline import config
from pipeline.loader import get_client

TABLES = ["orders_daily", "orders_clean", "orders", "rejected_rows", "pipeline_runs", "processed_files"]


def reset(client):
    for table in TABLES:
        client.command(f"DROP TABLE IF EXISTS siagrow.{table}")
    config.STATE_FILE.unlink(missing_ok=True)
    done = config.EMAIL_INBOX / "processed"
    if done.exists():
        for f in done.iterdir():
            shutil.move(str(f), config.EMAIL_INBOX / f.name)
    print("Old tables dropped, state reset, processed emails moved back to the inbox")


def create_users(client):
    db = config.CLICKHOUSE_DATABASE
    user, password = config.pipeline_credentials()
    client.command(f"CREATE USER IF NOT EXISTS {user} IDENTIFIED WITH sha256_password BY %(p)s", parameters={"p": password})
    client.command(f"GRANT SELECT, INSERT ON {db}.* TO {user}")

    bi_user, bi_password = config.powerbi_credentials()
    client.command(f"CREATE USER IF NOT EXISTS {bi_user} IDENTIFIED WITH sha256_password BY %(p)s", parameters={"p": bi_password})
    client.command(f"GRANT SELECT ON {db}.* TO {bi_user}")
    print(f"Users ready: {user} (SELECT, INSERT) and {bi_user} (SELECT only)")


admin_user, admin_password = config.admin_credentials()
client = get_client(admin_user, admin_password, database="default")
client.command(f"CREATE DATABASE IF NOT EXISTS {config.CLICKHOUSE_DATABASE}")

if "--reset" in sys.argv:
    reset(client)

sql = (config.ROOT / "sql" / "create_tables.sql").read_text(encoding="utf-8")
for statement in sql.split(";"):
    if statement.strip():
        client.command(statement)

create_users(client)
print("Tables:", client.command(f"SHOW TABLES FROM {config.CLICKHOUSE_DATABASE}"))
