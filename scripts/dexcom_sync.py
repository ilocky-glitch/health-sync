"""
dexcom_sync.py
Pulls CGM data from Dexcom Share (via pydexcom) and computes
daily metrics, then upserts into Notion.

Database targeted:
  🩸 CGM Daily Metrics → NOTION_DB_CGM

Env vars required:
  DEXCOM_USERNAME, DEXCOM_PASSWORD
  DEXCOM_REGION  (us | ous | jp)  — set "ous" for Singapore
  NOTION_TOKEN
  NOTION_DB_CGM

Units: All glucose values stored in mmol/L
"""

import os, json, datetime, statistics
from pydexcom import Dexcom
import requests

# ── Config ────────────────────────────────────────────────────────────────────
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DB_CGM       = os.environ["NOTION_DB_CGM"]

DEXCOM_USER   = os.environ["DEXCOM_USERNAME"]
DEXCOM_PASS   = os.environ["DEXCOM_PASSWORD"]
DEXCOM_REGION = os.environ.get("DEXCOM_REGION", "ous")  # Singapore → ous

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}

# Glucose thresholds (mmol/L)
RANGE_LOW  = 3.9
RANGE_HIGH = 10.0
OVERNIGHT_START_HOUR = 0   # midnight
OVERNIGHT_END_HOUR   = 6   # 6am

# ── Helpers ───────────────────────────────────────────────────────────────────
def mg_to_mmol(mg: float) -> float:
    return round(mg / 18.018, 2)

def notion_query(db_id: str, filter_payload: dict) -> list:
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    r = requests.post(url, headers=NOTION_HEADERS, json={"filter": filter_payload})
    r.raise_for_status()
    return r.json().get("results", [])

def notion_create(db_id: str, props: dict):
    url = "https://api.notion.com/v1/pages"
    body = {"parent": {"database_id": db_id}, "properties": props}
    r = requests.post(url, headers=NOTION_HEADERS, json=body)
    r.raise_for_status()

def notion_update(page_id: str, props: dict):
    url = f"https://api.notion.com/v1/pages/{page_id}"
    r = requests.patch(url, headers=NOTION_HEADERS, json={"properties": props})
    r.raise_for_status()

def upsert(db_id: str, title_prop: str, title_value: str, props: dict):
    existing = notion_query(db_id, {
        "property": title_prop,
        "title": {"equals": title_value}
    })
    if existing:
        notion_update(existing[0]["id"], props)
        print(f"  Updated: {title_value}")
    else:
        notion_create(db_id, props)
        print(f"  Created: {title_value}")

def title_prop(v: str) -> dict:
    return {"title": [{"text": {"content": str(v)}}]}

def number_prop(v) -> dict:
    return {"number": round(float(v), 2) if v is not None else None}

def select_prop(v: str) -> dict:
    return {"select": {"name": v} if v else None}

def text_prop(v: str) -> dict:
    return {"rich_text": [{"text": {"content": str(v)[:2000]}}]}

# ── CGM Processing ────────────────────────────────────────────────────────────
def fetch_day_readings(client: Dexcom, target_dt: datetime.date) -> list:
    """
    Fetch all 5-min EGV readings for a calendar day.
    pydexcom returns last N minutes; we fetch 1440 min = 24h window.
    Returns list of dicts: {time: datetime, mmol: float}
    """
    # pydexcom get_glucose_readings takes minutes (max 1440)
    readings_raw = client.get_glucose_readings(minutes=1440, max_count=288)

    day_readings = []
    for r in readings_raw:
        # r.time is a datetime object
        if r.time.date() == target_dt:
            mmol = mg_to_mmol(r.value) if r.value else None
            if mmol:
                day_readings.append({"time": r.time, "mmol": mmol})

    return sorted(day_readings, key=lambda x: x["time"])

def compute_metrics(readings: list, target_dt: datetime.date) -> dict:
    """Compute all CGM metrics from a day's readings."""
    if not readings:
        return {}

    values = [r["mmol"] for r in readings]
    n = len(values)

    mean_glucose = round(statistics.mean(values), 2)
    glucose_sd   = round(statistics.stdev(values), 2) if n > 1 else 0.0
    daily_min    = round(min(values), 2)
    daily_max    = round(max(values), 2)

    in_range  = sum(1 for v in values if RANGE_LOW <= v <= RANGE_HIGH)
    below     = sum(1 for v in values if v < RANGE_LOW)
    above     = sum(1 for v in values if v > RANGE_HIGH)

    tir = round(in_range / n * 100, 1)
    tbr = round(below    / n * 100, 1)
    tar = round(above    / n * 100, 1)

    # Count events (consecutive readings, not individual points)
    high_events = _count_events(readings, lambda v: v > RANGE_HIGH)
    low_events  = _count_events(readings, lambda v: v < RANGE_LOW)

    # Overnight mean
    overnight = [
        r["mmol"] for r in readings
        if OVERNIGHT_START_HOUR <= r["time"].hour < OVERNIGHT_END_HOUR
    ]
    overnight_mean = round(statistics.mean(overnight), 2) if overnight else None

    # Glucose variability label
    if glucose_sd < 2.0:
        variability = "Stable"
    elif glucose_sd < 3.5:
        variability = "Moderate"
    else:
        variability = "High"

    return {
        "mean":           mean_glucose,
        "sd":             glucose_sd,
        "tir":            tir,
        "tbr":            tbr,
        "tar":            tar,
        "min":            daily_min,
        "max":            daily_max,
        "overnight_mean": overnight_mean,
        "high_events":    high_events,
        "low_events":     low_events,
        "variability":    variability,
    }

def _count_events(readings: list, condition) -> int:
    """Count distinct episodes where condition is continuously true."""
    count = 0
    in_event = False
    for r in readings:
        if condition(r["mmol"]):
            if not in_event:
                count += 1
                in_event = True
        else:
            in_event = False
    return count

def find_workout_glucose(readings: list, workout_start: datetime.datetime | None) -> tuple:
    """Find pre and post workout glucose readings."""
    if not readings or not workout_start:
        return None, None

    # Pre: closest reading within 30 min before workout
    pre_window = [
        r for r in readings
        if -30 <= (r["time"] - workout_start).total_seconds() / 60 <= 5
    ]
    pre = min(pre_window, key=lambda r: abs((r["time"] - workout_start).total_seconds()), default=None)

    # Post: reading closest to 60 min after workout start
    post_target = workout_start + datetime.timedelta(minutes=60)
    post_window = [
        r for r in readings
        if 30 <= (r["time"] - workout_start).total_seconds() / 60 <= 90
    ]
    post = min(post_window, key=lambda r: abs((r["time"] - post_target).total_seconds()), default=None)

    return (round(pre["mmol"], 2) if pre else None,
            round(post["mmol"], 2) if post else None)

# ── Main sync ─────────────────────────────────────────────────────────────────
def main():
    target_str  = os.environ.get(
        "SYNC_DATE",
        (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
    )
    target_date = datetime.date.fromisoformat(target_str)
    print(f"🔄 Dexcom sync starting for {target_str}")
    print(f"  Region: {DEXCOM_REGION}")

    # Connect
    client = Dexcom(
        username=DEXCOM_USER,
        password=DEXCOM_PASS,
        ous=(DEXCOM_REGION == "ous")
    )
    print("  ✓ Connected to Dexcom Share")

    # Fetch readings
    readings = fetch_day_readings(client, target_date)
    print(f"  Readings found: {len(readings)}")

    if not readings:
        print("  No readings for this date, skipping.")
        return

    metrics = compute_metrics(readings, target_date)

    # Optionally correlate with workout time from env (set by GitHub Actions if
    # you want to pass workout start time from garmin_sync output)
    workout_start_str = os.environ.get("WORKOUT_START_UTC")
    workout_start = None
    if workout_start_str:
        try:
            workout_start = datetime.datetime.fromisoformat(workout_start_str)
        except:
            pass

    pre_wkt, post_wkt = find_workout_glucose(readings, workout_start)

    props = {
        "Date":                 title_prop(target_str),
        "Mean Glucose":         number_prop(metrics.get("mean")),
        "Glucose SD":           number_prop(metrics.get("sd")),
        "Time in Range %":      number_prop(metrics.get("tir")),
        "Time Below Range %":   number_prop(metrics.get("tbr")),
        "Time Above Range %":   number_prop(metrics.get("tar")),
        "Overnight Mean":       number_prop(metrics.get("overnight_mean")),
        "Pre-Workout Glucose":  number_prop(pre_wkt),
        "Post-Workout Glucose": number_prop(post_wkt),
        "High Events":          number_prop(metrics.get("high_events")),
        "Low Events":           number_prop(metrics.get("low_events")),
        "Daily Min":            number_prop(metrics.get("min")),
        "Daily Max":            number_prop(metrics.get("max")),
        "Glucose Variability":  select_prop(metrics.get("variability")),
    }

    upsert(DB_CGM, "Date", target_str, props)
    print(f"\n  Mean: {metrics.get('mean')} mmol/L | TIR: {metrics.get('tir')}% | SD: {metrics.get('sd')}")
    print("\n✅ Dexcom sync complete.")

if __name__ == "__main__":
    main()
