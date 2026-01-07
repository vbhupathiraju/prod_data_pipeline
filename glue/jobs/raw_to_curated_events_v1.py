import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from pyspark.sql import types as T

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# ---- Config ----
RAW_BASE = "s3://vishalb-personal-prod-data-pipeline1/raw/source_system=app/"
CURATED_BASE = "s3://vishalb-personal-prod-data-pipeline1/curated/events/"

# ---- Schema (enforced) ----
schema = T.StructType([
    T.StructField("event_id", T.StringType(), True),
    T.StructField("user_id", T.StringType(), True),
    T.StructField("event_type", T.StringType(), True),
    T.StructField("event_ts", T.StringType(), True),
    T.StructField("amount", T.DoubleType(), True),
])

# Read newline-delimited JSON recursively under RAW_BASE
raw_df = (
    spark.read
    .schema(schema)
    .option("recursiveFileLookup", "true")
    .json(RAW_BASE)
)

# Derive ingestion_date from file path: .../ingestion_date=YYYY-MM-DD/...
raw_df = raw_df.withColumn(
    "ingestion_date",
    F.regexp_extract(F.input_file_name(), r"ingestion_date=(\d{4}-\d{2}-\d{2})", 1)
)

# Parse event_ts to timestamp
df = (
    raw_df
    .withColumn("event_ts", F.to_timestamp("event_ts"))
    .withColumn("processing_ts", F.current_timestamp())
)

# Basic filter: drop rows where ingestion_date couldn't be parsed (expected to be none)
df = df.filter(F.col("ingestion_date") != "")

# Write Parquet partitioned by ingestion_date
(
    df.write
    .mode("overwrite")  # OK for now, will update later
    .partitionBy("ingestion_date")
    .parquet(CURATED_BASE)
)

print("Wrote curated parquet to:", CURATED_BASE)
