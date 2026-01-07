CREATE DATABASE IF NOT EXISTS prod_data_pipeline;

CREATE EXTERNAL TABLE IF NOT EXISTS prod_data_pipeline.curated_events (
  event_id string,
  user_id string,
  event_type string,
  event_ts timestamp,
  amount double,
  processing_ts timestamp
)
PARTITIONED BY (ingestion_date string)
STORED AS PARQUET
LOCATION 's3://vishalb-personal-prod-data-pipeline1/curated/events/';

MSCK REPAIR TABLE prod_data_pipeline.curated_events;
