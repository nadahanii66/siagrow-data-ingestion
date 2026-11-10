import logging
import os
import subprocess
import sys
from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler

from pipeline.logger import setup_logging

setup_logging()
log = logging.getLogger("scheduler")
MINUTES = int(os.getenv("PIPELINE_INTERVAL_MINUTES", "1"))


def run():
    result = subprocess.run([sys.executable, "run_pipeline.py"])
    if result.returncode != 0:
        log.error("Pipeline run finished with problems (exit code %s), check logs/pipeline.log", result.returncode)


if __name__ == "__main__":
    scheduler = BlockingScheduler()
    scheduler.add_job(run, "interval", minutes=MINUTES, max_instances=1, coalesce=True,
                      misfire_grace_time=60, next_run_time=datetime.now())
    log.info("Scheduler started, running every %s minute(s). Press Ctrl+C to stop.", MINUTES)
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Scheduler stopped")
