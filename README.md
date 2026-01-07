# prod-data-pipeline

Production-style batch pipeline project : raw → curated with idempotency, data quality checks, alerts, and backfills.


## Phase 1 – Baseline Pipeline

- Generated synthetic event data with intentional quality issues
- Ingested raw JSON data into S3 with date-based partitions
- Processed raw data into curated Parquet using AWS Glue (Spark)
- Registered curated dataset in Glue Data Catalog
- Queried results using Athena
