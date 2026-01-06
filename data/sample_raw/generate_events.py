import json
import os
import random
import uuid
from datetime import datetime, timedelta, timezone

EVENT_TYPES = ["login", "view_item", "add_to_cart", "purchase"]

def iso_ts(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def generate_day_events(
    ingestion_date: str,
    n_events: int = 1000,
    missing_user_rate: float = 0.02,
    duplicate_event_rate: float = 0.01,
    seed: int = 42,
):
    """
    Returns a list of event dicts for one ingestion date.
    Includes intentional data issues:
      - missing user_id
      - duplicate event_id
    """
    random.seed(hash((seed, ingestion_date)) % (2**32))

    # Base timestamp window for that day in UTC
    day_start = datetime.fromisoformat(ingestion_date).replace(tzinfo=timezone.utc)
    day_end = day_start + timedelta(days=1)

    events = []
    event_ids = []

    for _ in range(n_events):
        event_type = random.choices(
            EVENT_TYPES,
            weights=[0.25, 0.45, 0.20, 0.10],  # more views than purchases
            k=1
        )[0]

        # Random time within the day
        seconds_into_day = random.randint(0, 86399)
        event_ts = day_start + timedelta(seconds=seconds_into_day)

        event_id = str(uuid.uuid4())
        user_id = str(random.randint(1, 500))  # 500 users

        # Intentionally make some user_id missing
        if random.random() < missing_user_rate:
            user_id = None

        # amount only for purchase
        amount = None
        if event_type == "purchase":
            amount = round(random.uniform(5, 200), 2)

        evt = {
            "event_id": event_id,
            "user_id": user_id,
            "event_type": event_type,
            "event_ts": iso_ts(event_ts),
            "amount": amount,
        }
        events.append(evt)
        event_ids.append(event_id)

    # Inject duplicates by copying some events but reusing event_id
    n_dupes = max(1, int(n_events * duplicate_event_rate))
    for _ in range(n_dupes):
        idx = random.randint(0, len(events) - 1)
        dupe = dict(events[idx])  # copy
        # Keep same event_id, tweak timestamp slightly so it looks real
        original_ts = datetime.fromisoformat(dupe["event_ts"].replace("Z", "+00:00"))
        dupe["event_ts"] = iso_ts(original_ts + timedelta(seconds=random.randint(1, 120)))
        events.append(dupe)

    return events

def write_ndjson(events, out_path: str):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        for evt in events:
            f.write(json.dumps(evt) + "\n")

def main():
    # 3 days of data
    dates = ["2025-01-01", "2025-01-02", "2025-01-03"]
    out_root = os.path.join(os.path.dirname(__file__), "out")

    for d in dates:
        events = generate_day_events(d, n_events=1000)
        out_file = os.path.join(out_root, f"events_{d}.json")
        write_ndjson(events, out_file)
        print(f"Wrote {len(events)} events to {out_file}")

if __name__ == "__main__":
    main()
