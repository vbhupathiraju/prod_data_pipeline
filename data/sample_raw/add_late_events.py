import json
import random
import uuid
from datetime import datetime, timedelta, timezone

def iso_ts(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def main():
    random.seed(123)

    # Late-arriving events that still belong to 2025-01-02
    day = datetime(2025, 1, 2, tzinfo=timezone.utc)
    out_path = "data/sample_raw/out/late_events_2025-01-02.json"

    events = []
    for _ in range(50):  # add 50 late events
        event_type = random.choice(["view_item", "add_to_cart", "purchase"])
        seconds_into_day = random.randint(0, 86399)
        event_ts = day + timedelta(seconds=seconds_into_day)

        evt = {
            "event_id": str(uuid.uuid4()),
            "user_id": str(random.randint(1, 500)),
            "event_type": event_type,
            "event_ts": iso_ts(event_ts),
            "amount": round(random.uniform(5, 200), 2) if event_type == "purchase" else None,
        }
        events.append(evt)

    with open(out_path, "w", encoding="utf-8") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")

    print(f"Wrote {len(events)} late events to {out_path}")

if __name__ == "__main__":
    main()
