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


SELECT ingestion_date, COUNT(*) AS n
FROM prod_data_pipeline.curated_events
GROUP BY ingestion_date
ORDER BY ingestion_date;


SELECT
    SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END) AS missing_user_id,
    SUM(CASE WHEN event_id IS NULL THEN 1 ELSE 0 END) AS missing_event_id,
    COUNT(*) AS total_rows
FROM prod_data_pipeline.curated_events;



SELECT event_id, COUNT(*) as c
FROM prod_data_pipeline.curated_events
GROUP BY event_id
HAVING COUNT(*) > 1
ORDER BY c DESC
LIMIT 20;






