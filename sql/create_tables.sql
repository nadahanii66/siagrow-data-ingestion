CREATE TABLE IF NOT EXISTS siagrow.orders
(
    record_id      String,
    source         LowCardinality(String),
    source_id      String,
    sender         String,
    customer_name  String,
    product        LowCardinality(String),
    quantity       UInt32,
    unit_price     Decimal(12, 2),
    total_amount   Decimal(14, 2),
    city           LowCardinality(String),
    order_date     DateTime('UTC'),
    received_at    DateTime('UTC'),
    version        DateTime64(3, 'UTC'),
    row_hash       String,
    ingested_at    DateTime('UTC'),
    run_id         String
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (source, record_id);

CREATE TABLE IF NOT EXISTS siagrow.rejected_rows
(
    rejected_at  DateTime('UTC') DEFAULT now(),
    run_id       String,
    source       LowCardinality(String),
    source_id    String,
    reason       String,
    raw_row      String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(rejected_at)
ORDER BY (source, rejected_at);

CREATE TABLE IF NOT EXISTS siagrow.pipeline_runs
(
    run_id          String,
    source          LowCardinality(String),
    started_at      DateTime('UTC'),
    finished_at     DateTime('UTC'),
    status          LowCardinality(String),
    rows_read       UInt32,
    rows_loaded     UInt32,
    rows_rejected   UInt32,
    rows_duplicate  UInt32,
    rows_unchanged  UInt32,
    files_failed    UInt32,
    duration_sec    Float64,
    error           String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(started_at)
ORDER BY (source, started_at);

CREATE TABLE IF NOT EXISTS siagrow.processed_files
(
    file_hash     String,
    file_name     String,
    rows          UInt32,
    run_id        String,
    processed_at  DateTime('UTC') DEFAULT now()
)
ENGINE = ReplacingMergeTree(processed_at)
ORDER BY file_hash;

CREATE VIEW IF NOT EXISTS siagrow.orders_clean AS
SELECT
    record_id, source, source_id, sender, customer_name, product, quantity,
    unit_price, total_amount, city,
    toTimeZone(order_date, 'Africa/Cairo') AS order_date,
    toDate(toTimeZone(order_date, 'Africa/Cairo')) AS order_day,
    toTimeZone(received_at, 'Africa/Cairo') AS received_at,
    ingested_at
FROM siagrow.orders FINAL;

CREATE VIEW IF NOT EXISTS siagrow.orders_daily AS
SELECT
    order_day,
    source,
    product,
    city,
    count() AS orders,
    sum(quantity) AS items_sold,
    sum(total_amount) AS revenue
FROM siagrow.orders_clean
GROUP BY order_day, source, product, city;
