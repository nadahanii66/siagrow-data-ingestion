# Siagrow Data Ingestion & Alerting

A data ingestion layer that receives client data from several sources, keeps every original file safe, checks it against a data contract, sends an email alarm when something is wrong, and loads clean data into ClickHouse for the dashboards.

## The Flow

```
Client API upload ─┐
Email attachments ─┼─> Save original ─> Contract check ─┬─> 🔴 error   ─> not loaded + alarm email
Client database   ─┘   (data/raw)       (contracts/)    ├─> 🟡 warning ─> loaded + alarm email
                                                        └─> ✅ clean   ─> clean, deduplicate, load ─> ClickHouse ─> Dashboard
```

## What It Does

| Requirement | How I implemented it |
|---|---|
| Multiple sources | One processing core (`api/ingest.py`) used by every entry point: clients push files with `POST /upload`, and the API pulls from the client database (incremental load) and email attachments with `POST /sources/{name}/fetch`. A new source is one connector function and one line in `FETCHERS` |
| Safe storage | Every uploaded file is saved untouched in `data/raw/<customer>/<date>/` before any processing, so nothing is ever lost. Rejected email files go to `failed/` instead of being deleted |
| Alarm | Each source is checked against a YAML data contract. Empty files, missing required columns, unexpected extra columns and changed column types trigger an email alarm with the exact problem and file |
| Security | API key per customer, stored only as a SHA-256 hash and compared with `hmac.compare_digest`. File type and size limits. All secrets in `.env` (git-ignored). Gmail App Password over TLS. Least-privilege ClickHouse users |
| Retry | ClickHouse connection and inserts retry 5 times with exponential backoff. Alarm emails retry 3 times. If ClickHouse is still down, the client gets `503`, the file is saved and put in `data/retry_queue/`, and the team gets an alarm. The scheduler re-processes the queue automatically after 5, 15 and 60 minutes. After 3 failed retries the file moves to `data/retry_queue/dead/` and a "GAVE UP" email is sent |
| Expansion | New customer = new key + three lines in `config/customers.yaml`. New file layout = new contract file. No code changes |
| Transfer | The API and ClickHouse both run in Docker and every server-specific value lives in `.env`, so moving to another server is copying the project, filling `.env`, running `docker compose up -d --build` and `python scripts/create_tables.py` |

## Alarm Rules

| Situation | Level | Action |
|---|---|---|
| Empty file | Error | Not loaded, alarm email |
| Unreadable file | Error | Not loaded, alarm email |
| Required column missing | Error | Not loaded, alarm email |
| Extra column | Warning | Loaded, alarm email |
| Column type changed | Warning | Loaded (bad rows rejected), alarm email |
| Database unreachable after retries | Critical | Client gets 503, file queued for automatic retry, alarm email with the saved file path |
| File still failing after 3 retries | Critical | Moved to `dead/`, "GAVE UP" email for manual review |

## API

Open `http://127.0.0.1:8000/docs` after starting it.

| Endpoint | Key | Description |
|---|---|---|
| `GET /health` | None | Service health check |
| `POST /upload` | `X-API-Key` (customer) | Upload a CSV or Excel file |
| `GET /sources` | `X-Admin-Key` | List the sources the API can fetch from |
| `POST /sources/{name}/fetch` | `X-Admin-Key` | Fetch new data from `database` or `email` now |
| `POST /retry-pending` | `X-Admin-Key` | Re-process queued files that are due now |

| Response | Meaning |
|---|---|
| `200` | File loaded, with row counts and any warnings |
| `400` | Wrong file type or unreadable file |
| `401` | Missing or invalid API key or admin key |
| `404` | Unknown source name |
| `409` | The exact same file was already uploaded |
| `413` | File larger than 10 MB |
| `422` | File rejected by the contract, with the list of issues |
| `503` | ClickHouse unavailable, file saved, queued for retry and team alerted |

## Project Structure

```
siagrow-v2/
├── api/
│   ├── main.py              API endpoints and authentication
│   └── ingest.py            Shared ingestion core and retry queue
├── config/customers.yaml    Customers and hashed API keys
├── contracts/orders.yaml    Data contract
├── pipeline/
│   ├── schema_check.py      Contract check
│   ├── alerts.py            Alarm emails
│   ├── transform.py         Cleaning and validation
│   ├── loader.py            ClickHouse loading with retries
│   ├── monitor.py           Run tracking and quality checks
│   └── sources/             Database and email connectors
├── scripts/
│   ├── create_api_key.py
│   ├── create_tables.py
│   └── seed_client_db.py
├── run_pipeline.py          Batch run for database and email sources
├── scheduler.py             Runs the pipeline and the retry queue on a schedule
├── Dockerfile
└── docker-compose.yml       ClickHouse and the API
```

## How to Run

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
```

Fill in `.env` (ClickHouse users, Gmail sender with an App Password, alarm recipients, `ADMIN_API_KEY`), then:

```bash
docker compose up -d --build
python scripts/create_tables.py
python scripts/create_api_key.py acme
python scripts/seed_client_db.py --new 40
python scheduler.py
```

`docker compose up -d --build` starts ClickHouse and the API together. `scheduler.py` runs the batch pipeline and the retry queue every minute. Put the printed key hash in `config/customers.yaml` and keep the key itself for the client.

To run the API outside Docker during development, stop the `siagrow-api` container and run `uvicorn api.main:app --reload`.

Run the tests with `pytest`.

## Next Steps

- Move customers, contracts and job history into MySQL and give each customer a separate ClickHouse database and read-only user
- Store the raw files in S3-compatible storage (MinIO locally, S3 in the cloud)
- WhatsApp and IMAP connectors on the same processing core
