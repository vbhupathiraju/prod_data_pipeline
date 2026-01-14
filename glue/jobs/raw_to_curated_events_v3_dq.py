import sys
import json
from datetime import datetime
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from pyspark.sql import types as T

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# --------------------
# Config
# --------------------
BUCKET = "vishalb-personal-prod-data-pipeline1"
RAW_BASE = f"s3://{BUCKET}/raw/source_system=app/"
CURATED_BASE = f"s3://{BUCKET}/curated/events/"
METRICS_BASE = f"s3://{BUCKET}/metrics/"

ALLOWED_EVENT_TYPES = {"login", "view_item", "add_to_cart", "purchase"}

# thresholds
MAX_NULL_USER_ID_RATE = 0.03
MAX_DUP_EVENT_ID_RATE = 0.02

# --------------------
# Args
# --------------------
args = getResolvedOptions(sys.argv, ["ingestion_date"])
ingestion_date = args["ingestion_date"]

try:
    datetime.strptime(ingestion_date, "%Y-%m-%d")
except ValueError:
    raise ValueError("ingestion_date must be YYYY-MM-DD, e.g. 2025-01-02")

raw_path = f"{RAW_BASE}ingestion_date={ingestion_date}/"
target_partition_path = f"{CURATED_BASE}ingestion_date={ingestion_date}/"
metrics_path = f"{METRICS_BASE}ingestion_date={ingestion_date}/data_quality.json"

# --------------------
# Schema
# --------------------
schema = T.StructType([
    T.StructField("event_id", T.StringType(), True),
    T.StructField("user_id", T.StringType(), True),
    T.StructField("event_type", T.StringType(), True),
    T.StructField("event_ts", T.StringType(), True),
    T.StructField("amount", T.DoubleType(), True),
])

raw_df = (
    spark.read
    .schema(schema)
    .json(raw_path)
    .withColumn("ingestion_date", F.lit(ingestion_date))
)

df = (
    raw_df
    .withColumn("event_ts", F.to_timestamp("event_ts"))
    .withColumn("processing_ts", F.current_timestamp())
)

# --------------------
# Data Quality Checks
# --------------------
total_rows = df.count()

# helper counts
null_event_id = df.filter(F.col("event_id").isNull()).count()
null_event_type = df.filter(F.col("event_type").isNull()).count()
null_user_id = df.filter(F.col("user_id").isNull()).count()

# invalid event types
invalid_event_type = df.filter(~F.col("event_type").isin(list(ALLOWED_EVENT_TYPES))).count()

# duplicate event_id count (rows beyond first occurrence)
dupes_df = df.groupBy("event_id").count().filter(F.col("count") > 1)
dup_rows = dupes_df.agg(F.sum(F.col("count") - 1).alias("dup_rows")).collect()[0]["dup_rows"]
dup_rows = int(dup_rows) if dup_rows is not None else 0

# rates
null_user_rate = (null_user_id / total_rows) if total_rows else 0.0
dup_rate = (dup_rows / total_rows) if total_rows else 0.0

dq_report = {
    "ingestion_date": ingestion_date,
    "raw_path": raw_path,
    "curated_partition_path": target_partition_path,
    "total_rows": total_rows,
    "null_event_id": null_event_id,
    "null_event_type": null_event_type,
    "null_user_id": null_user_id,
    "null_user_rate": null_user_rate,
    "invalid_event_type": invalid_event_type,
    "duplicate_event_id_extra_rows": dup_rows,
    "duplicate_event_id_rate": dup_rate,
    "thresholds": {
        "max_null_user_id_rate": MAX_NULL_USER_ID_RATE,
        "max_duplicate_event_id_rate": MAX_DUP_EVENT_ID_RATE
    },
    "status": "PASS"
}

# Determine failures
failures = []

if total_rows == 0:
    failures.append("No rows found (total_rows == 0)")
if null_event_id > 0:
    failures.append(f"event_id has nulls: {null_event_id}")
if null_event_type > 0:
    failures.append(f"event_type has nulls: {null_event_type}")
if invalid_event_type > 0:
    failures.append(f"event_type has invalid values: {invalid_event_type}")
if null_user_rate > MAX_NULL_USER_ID_RATE:
    failures.append(f"user_id null rate too high: {null_user_rate:.4f} > {MAX_NULL_USER_ID_RATE}")
if dup_rate > MAX_DUP_EVENT_ID_RATE:
    failures.append(f"duplicate event_id rate too high: {dup_rate:.4f} > {MAX_DUP_EVENT_ID_RATE}")

if failures:
    dq_report["status"] = "FAIL"
    dq_report["failures"] = failures

# --------------------
# Write DQ report to S3 (as a single JSON file)
# --------------------
report_str = json.dumps(dq_report, indent=2)

# Write with Spark to guarantee S3 write permissions via Glue role
(
    spark.createDataFrame([(report_str,)], ["json"])
    .coalesce(1)
    .write
    .mode("overwrite")
    .text(f"{METRICS_BASE}ingestion_date={ingestion_date}/_tmp_dq_report/")
)

# Rename part file to data_quality.json using S3 copy (simple approach avoided here).
# We'll instead keep the folder output for now and treat it as our report location.
# For Phase 4 alerts, we only need the content, not the exact filename.

print("DQ REPORT:\n", report_str)

# Fail job if DQ failed
if dq_report["status"] == "FAIL":
    raise Exception("Data Quality checks failed: " + "; ".join(failures))

# --------------------
# Idempotent write (purge partition + append)
# --------------------
glueContext.purge_s3_path(target_partition_path, options={"retentionPeriod": 0})

(
    df.write
    .mode("append")
    .partitionBy("ingestion_date")
    .parquet(CURATED_BASE)
)

print(f"SUCCESS ingestion_date={ingestion_date}")
