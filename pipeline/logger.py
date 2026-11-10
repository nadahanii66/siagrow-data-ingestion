import logging
from logging.handlers import RotatingFileHandler

from pipeline import config

FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"


def setup_logging():
    config.LOG_DIR.mkdir(exist_ok=True)
    root = logging.getLogger()
    if root.handlers:
        return
    root.setLevel(config.LOG_LEVEL)

    console = logging.StreamHandler()
    console.setFormatter(logging.Formatter(FORMAT, "%H:%M:%S"))
    root.addHandler(console)

    file = RotatingFileHandler(config.LOG_DIR / "pipeline.log", maxBytes=5_000_000, backupCount=5, encoding="utf-8")
    file.setFormatter(logging.Formatter(FORMAT))
    root.addHandler(file)

    for noisy in ("urllib3", "clickhouse_connect", "apscheduler.executors"):
        logging.getLogger(noisy).setLevel(logging.ERROR)
