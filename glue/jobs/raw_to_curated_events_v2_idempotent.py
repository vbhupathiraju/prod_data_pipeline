import sys
from datetime import datetime
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from pyspark.sql import types as T

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Config
BUCKET = "vishalb-personal-prod-data-pipeline1"
RAW_BASE = f"s3://{BUCKET}/raw/source_system=app/"
CURATED_BASE = f"s3://{BUCKET}/curated/events/"

# Job args
# We require ingestion_date for idempotent runs.
# Example: --ingestion_date=2025-01-02
args = getResolvedOptions(sys.argv, ["ingestion_date"])
ingestion_date = args["ingestion_date"]

# Validate format early (fail fast)
try:
    datetime.strptime(ingestion_date, "%Y-%m-%d")
except ValueError:
    raise ValueError("ingestion_date must be YYYY-MM-DD, e.g. 2025-01-01")

raw_path = f"{RAW_BASE}ingestion_date={ingestion_date}/"

# Schema (enforced)
schema = T.StructType([
    T.StructField("event_id", T.StringType(), True),
    T.StructField("user_id", T.StringType(), True),
    T.StructField("event_type", T.StringType(), True),
    T.StructField("event_ts", T.StringType(), True),
    T.StructField("amount", T.DoubleType(), True),
])

# Read only ONE date partition
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
# Idempotent write strategy:
# Purge the target partition path, then rewrite only that partition.
# --------------------
target_partition_path = f"{CURATED_BASE}ingestion_date={ingestion_date}/"

# Purge existing partition output (safe reruns)
# This avoids "overwrite everything" and makes reruns deterministic.
glueContext.purge_s3_path(target_partition_path, options={"retentionPeriod": 0})

# Write ONLY this partition to its own folder
(
    df.write
    .mode("overwrite")
    .parquet(target_partition_path)
)

print(f"Processed ingestion_date={ingestion_date}")
print("Read:", raw_path)
print("Wrote partition:", target_partition_path)
