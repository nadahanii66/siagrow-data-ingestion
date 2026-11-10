import hashlib
import hmac
import logging
import os
from pathlib import Path

from fastapi import Depends, FastAPI, File, Header, HTTPException, UploadFile

from api.ingest import IngestError, ingest, load_customers, retry_pending, save_raw
from pipeline.loader import get_client
from pipeline.logger import setup_logging
from run_pipeline import run_database, run_email

setup_logging()
log = logging.getLogger("api")
app = FastAPI(title="Siagrow Ingestion API")

MAX_BYTES = 10 * 1024 * 1024
ALLOWED = {".csv", ".xlsx", ".xls"}
FETCHERS = {"database": run_database, "email": run_email}


def current_customer(x_api_key: str | None = Header(default=None)):
    if not x_api_key:
        raise HTTPException(status_code=401, detail="Missing API key. Send it in the X-API-Key header")
    digest = hashlib.sha256(x_api_key.encode()).hexdigest()
    for customer_id, info in load_customers().items():
        if hmac.compare_digest(digest, info["api_key_sha256"]):
            return customer_id, info
    log.warning("Rejected a request with an invalid API key")
    raise HTTPException(status_code=401, detail="Invalid API key")


def require_admin(x_admin_key: str | None = Header(default=None)):
    expected = os.getenv("ADMIN_API_KEY", "")
    if not expected or not x_admin_key or not hmac.compare_digest(x_admin_key, expected):
        raise HTTPException(status_code=401, detail="Invalid admin key")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/upload")
async def upload(file: UploadFile = File(...), customer=Depends(current_customer)):
    customer_id, info = customer
    if Path(file.filename).suffix.lower() not in ALLOWED:
        raise HTTPException(status_code=400, detail=f"Only {sorted(ALLOWED)} files are accepted")
    content = await file.read()
    if len(content) > MAX_BYTES:
        raise HTTPException(status_code=413, detail="The file is larger than 10 MB")

    raw_path = save_raw(customer_id, file.filename, content)
    log.info("[api_%s] received %s, saved to %s", customer_id, file.filename, raw_path)
    try:
        return ingest(customer_id, info, file.filename, content, raw_path)
    except IngestError as error:
        raise HTTPException(status_code=error.status, detail=error.detail)


@app.get("/sources", dependencies=[Depends(require_admin)])
def sources():
    return {"sources": sorted(FETCHERS)}


@app.post("/sources/{name}/fetch", dependencies=[Depends(require_admin)])
def fetch(name: str):
    if name not in FETCHERS:
        raise HTTPException(status_code=404, detail=f"Unknown source '{name}'. Available: {sorted(FETCHERS)}")
    try:
        client = get_client()
    except Exception as error:
        raise HTTPException(status_code=503, detail=f"ClickHouse is not reachable: {error}")
    tracker = FETCHERS[name](client)
    return {"source": name, "status": tracker.status, "run_id": tracker.run_id, **tracker.stats, "errors": tracker.errors}


@app.post("/retry-pending", dependencies=[Depends(require_admin)])
def retry():
    return retry_pending()