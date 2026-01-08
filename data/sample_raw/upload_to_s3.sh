#!/usr/bin/env bash
set -euo pipefail

BUCKET="${1:?Usage: ./upload_to_s3.sh <bucket-name>}"

aws s3 cp out/events_2025-01-01.json \
  "s3://$BUCKET/raw/source_system=app/ingestion_date=2025-01-01/events.json"

aws s3 cp out/events_2025-01-02.json \
  "s3://$BUCKET/raw/source_system=app/ingestion_date=2025-01-02/events.json"

aws s3 cp out/events_2025-01-03.json \
  "s3://$BUCKET/raw/source_system=app/ingestion_date=2025-01-03/events.json"

echo "Uploaded raw files to s3://$BUCKET/raw/source_system=app/"
