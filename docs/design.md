# Production Data Pipeline — Design Doc

## Summary
This project implements a production-style batch pipeline on AWS that ingests raw event data from S3, transforms it with AWS Glue (Spark) into curated Parquet, enforces data quality checks, emits metrics artifacts, supports idempotent reruns and late-arriving data, and sends alerts on failures and missing upstream data.

Key outcomes:
- Idempotent, partition-scoped processing by `ingestion_date`
- Data quality checks with fail-fast behavior to protect curated data
- Metrics artifacts written to S3 per partition run
- Alerts via SNS for Glue job failures (EventBridge rule)
- Backfill support via a date-range driver that runs one date per job invocation


## Goals and Non-Goals

### Goals
- Provide a clear raw → curated batch pipeline that is safe to rerun
- Partition outputs by `ingestion_date` for efficient queries and reprocessing
- Add data quality checks with explicit thresholds and failure behavior
- Produce operational artifacts (metrics) to support monitoring and debugging
- Add alerts for job failures and missing upstream data
- Simulate a backfill and late-arriving data

### Non-Goals (for this project)
- Real-time streaming ingestion (Kinesis/Kafka)
- Slowly changing dimensions / advanced modeling
- Full Infrastructure-as-Code (Terraform/CDK)
- Transactional lakehouse (Delta/Iceberg/Hudi) — discussed as an alternative


## Architecture

### Data Flow (conceptual)
1. **Raw landing (S3):**
   - `s3://vishalb-personal-prod-data-pipeline/raw/source_system=app/ingestion_date=YYYY-MM-DD/events.json`
2. **Glue job (Spark):**
   - Reads one `ingestion_date` at a time
   - Parses schema + timestamps
   - Runs data quality checks
   - Writes metrics to S3
   - If DQ passes: purges and rewrites only the target curated partition
3. **Curated storage (S3 Parquet):**
   - `s3://vishalb-personal-prod-data-pipeline/curated/events/ingestion_date=YYYY-MM-DD/`
4. **Query layer:**
   - Glue Data Catalog + Athena external table
5. **Alerting:**
   - EventBridge rule watches Glue job state changes (FAILED) → SNS email
   - Optional sensor Lambda checks for missing raw data → SNS email
6. **Backfills:**
   - Local driver script invokes Glue job once per date and waits for completion


## Storage Layout

### Raw (immutable landing)
- `raw/source_system=app/ingestion_date=YYYY-MM-DD/events.json`

Rationale:
- Immutable raw enables debugging, replay, and backfills.
- Partitioning by ingestion_date supports operational day-by-day processing.

### Curated (analytics-ready)
- `curated/events/ingestion_date=YYYY-MM-DD/` in Parquet

Rationale:
- Parquet reduces scan costs and improves query performance in Athena.
- Partitioning enables reprocessing only affected dates and faster queries.

### Metrics
- `metrics/ingestion_date=YYYY-MM-DD/` (DQ report output)

Rationale:
- Metrics provide run-level observability (counts, null rates, dup rates).
- Metrics are used for audits, alerting, and debugging unexpected changes.


## Idempotency Strategy

### Approach
- The Glue job is parameterized by `--ingestion_date`.
- The job reads only `raw/.../ingestion_date=<date>/`.
- On success, it purges and rewrites only:
  - `curated/events/ingestion_date=<date>/`

### Why this works
- Re-running the same date produces the same partition output (deterministic for that input).
- Other partitions are untouched, preventing accidental deletion or rewrites.
- Late-arriving data is handled by re-running the affected date only.

### Alternatives Considered
1. **Full overwrite of curated dataset**
   - Simple but dangerous: reruns can wipe unrelated partitions.
2. **Glue Job Bookmarks**
   - Useful for incremental ingestion, but can conflict with explicit backfills and deterministic reruns.
3. **Atomic partition swap**
   - Write to temp path then swap/rename; more robust but more complex in S3.


## Data Quality Checks

### Implemented checks (per ingestion_date)
- `total_rows > 0`
- `event_id` not null
- `event_type` not null
- `event_type` within allowed values: `login, view_item, add_to_cart, purchase`
- `user_id` null rate ≤ 3%
- duplicate `event_id` rate ≤ 2% (extra duplicate rows / total rows)

### Failure behavior
- DQ report is emitted.
- If any hard check fails, the job fails before purging/writing curated data.

Rationale:
- Prevents bad data from replacing good curated partitions (fail-safe design).


## Alerting and Monitoring

### Glue job failure alerts
- EventBridge rule:
  - Source: `aws.glue`
  - Detail type: `Glue Job State Change`
  - Filter: specific job name + `state=FAILED`
- Target: SNS topic → email notifications

Rationale:
- Event-driven alerts are lower latency and less brittle than relying on CloudWatch metric availability.

### Missing data alerts
- Sensor Lambda checks whether the expected raw object exists for a given date.
- If missing, publishes an SNS alert and fails.

Rationale:
- Missing upstream data is a common silent failure mode; detect early before processing.


## Backfill Design

### Approach
- Keep processing job single-date for isolation.
- Backfill script iterates over date range:
  - Starts Glue run
  - Polls until completion
  - Stops on first failure

Rationale:
- Simplifies retries: rerun only failed dates.
- Prevents concurrency limits from causing failures.
- Creates clear audit trail per date.


## Tradeoffs and Future Improvements

### Tradeoffs
- Metrics output is written as a Spark text output (folder + part file) instead of a single named JSON object.
  - Simpler implementation; could be refined with a rename/copy step.
- Raw updates were simulated by overwriting the raw file for a date.
  - In production, raw would usually be append-only with separate arrivals and a compaction step.
- Orchestration is currently local/manual.
  - Step Functions or Airflow would improve scheduling, retries, and observability.

### Next Steps
- Add Step Functions to orchestrate: sensor → Glue job → success notification.
- Tighten IAM permissions (bucket-scoped least privilege).
- Add schema evolution strategy (data contracts / validation against expected schema versions).
- Add stronger anomaly detection:
  - row count deltas vs rolling baseline
  - per-event-type distribution checks
- Consider Iceberg/Delta for transactional upserts and time travel at scale.


## How to Run (Developer Notes)

### Generate data
- `python data/sample_raw/generate_events.py`
- `python data/sample_raw/add_late_events.py` (optional late arrivals)

### Upload raw
- Upload NDJSON to:
  - `raw/source_system=app/ingestion_date=YYYY-MM-DD/events.json`

### Run Glue for a date
- Job argument: `--ingestion_date=YYYY-MM-DD`

### Backfill
- `python3 scripts/run_backfill.py 2025-01-01 2025-01-03`

### Query in Athena
- `SELECT ingestion_date, COUNT(*) FROM prod_data_pipeline.curated_events GROUP BY ingestion_date;`
