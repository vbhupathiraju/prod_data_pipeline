import sys
import subprocess
import json
import time
from datetime import datetime, timedelta

GLUE_JOB_NAME = "raw_to_curated_events_v3_dq"
REGION = "us-east-1"
POLL_SECONDS = 20

def daterange(start, end):
    curr = start
    while curr <= end:
        yield curr
        curr += timedelta(days=1)

def aws_json(cmd):
    """Run an AWS CLI command and return parsed JSON."""
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip())
    return json.loads(result.stdout)

def start_glue_job(date_str):
    print(f"\nStarting Glue job for {date_str}")

    arguments_json = json.dumps({
        "--ingestion_date": date_str
    })

    cmd = [
        "aws", "glue", "start-job-run",
        "--job-name", GLUE_JOB_NAME,
        "--arguments", arguments_json,
        "--region", REGION,
    ]

    out = aws_json(cmd)
    job_run_id = out["JobRunId"]
    print(f"Started: {job_run_id}")
    return job_run_id


def get_run_state(job_run_id):
    cmd = [
        "aws", "glue", "get-job-run",
        "--job-name", GLUE_JOB_NAME,
        "--run-id", job_run_id,
        "--region", REGION,
    ]
    out = aws_json(cmd)
    return out["JobRun"]["JobRunState"]

def wait_for_completion(job_run_id, date_str):
    while True:
        state = get_run_state(job_run_id)
        print(f"[{date_str}] {job_run_id} state={state}")
        if state in ("SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT"):
            return state
        time.sleep(POLL_SECONDS)

def main():
    if len(sys.argv) != 3:
        print("Usage: python run_backfill.py <start_date> <end_date>")
        sys.exit(1)

    start = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
    end = datetime.strptime(sys.argv[2], "%Y-%m-%d").date()

    for d in daterange(start, end):
        date_str = d.isoformat()

        # Retry-start logic: if concurrency exceeded, wait a bit and retry
        while True:
            try:
                job_run_id = start_glue_job(date_str)
                break
            except RuntimeError as e:
                if "ConcurrentRunsExceededException" in str(e):
                    print(f"[{date_str}] Concurrency limit hit. Waiting {POLL_SECONDS}s and retrying...")
                    time.sleep(POLL_SECONDS)
                    continue
                raise

        final_state = wait_for_completion(job_run_id, date_str)
        if final_state != "SUCCEEDED":
            raise RuntimeError(f"Backfill failed for {date_str}. Final state: {final_state}")

    print("\nBackfill complete: all dates SUCCEEDED")

if __name__ == "__main__":
    main()
