"""
dexcom_sync.py
Pulls CGM data from Dexcom Share (via pydexcom) and computes
daily metrics, then upserts into Notion.

Env vars required:
  DEXCOM_USERNAME, DEXCOM_PASSWORD
  DEXCOM_REGION  (us | ous | jp)
  NOTION_TOKEN
  NOTION_DB_CGM
"""

import os, json, datetime, statistics
from pydexcom import Dexcom
import requests

NOTION_TOKEN  = os.environ["NOTION_TOKEN"]
DB_CGM        = os.environ["NOTION_DB_CGM"]
DEXCOM_USER   = os.environ["DEXCOM_USERNAME"]
DEXCOM_PASS   = os.environ["DEXCOM_PASSWORD"]
DEXCOM_REGION = os.environ.get("DEXCOM_REGION", "ous")

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}

RANGE_LOW  = 3.9
RANGE_HIGH = 10.0
OVERNIGHT_START_HOUR = 0
OVERNIGHT_END_HOUR   = 6

def mg_to_mmol(mg):
    return round(mg / 18.018, 2)

def notion_query(db_id, filter_payload):
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    r = requests.post(url, headers=NOTION_HEADERS, json={"filter": filter_payload})
    r.raise_for_status()
    return r.json().get("results", [])

def notion_create(db_id, props):
    r = requests.post("https://api.notion.com/v1/pages", headers=NOTION_HEADERS,
                      json={"parent": {"database_id": db_id}, "properties": props})
    r.raise_for_status()

def notion_update(page_id, props):
    r = requests.patch(f"https://api.notion.com/v1/pages/{page_id}",
                       headers=NOTION_HEADERS, json={"properties": props})
    r.raise_for_status()

def upsert(db_id, title_prop, title_value, props):
    existing = notion_query(db_id, {"property": title_prop, "title": {"equals": title_value}})
    if existing:
        notion_update(existing[0]["id"], props)
        print(f"  Updated: {title_value}")
    else:
        notion_create(db_id, props)
        print(f"  Created: {title_value}")

def tp(v):  return {"title": [{"text": {"content": str(v)}}]}
def np(v):  return {"number": round(float(v), 2) if v is not None else None}
def sp(v):  return {"select": {"name": v} if v else None}

def connect_dexcom():
    """Try new pydexcom API first, fall back to legacy if needed."""
    try:
        client = Dexcom(username=DEXCOM_USER, password=DEXCOM_PASS,
                        region=DEXCOM_REGION)
        print("  Connected to Dexcom (new API)")
        return client
    except TypeError:
        pass
    try:
        client = Dexcom(DEXCOM_USER, DEXCOM_PASS,
                        ous=(DEXCOM_REGION == "ous"))
        print("  Connected to Dexcom (legacy API)")
        return client
    except Exception as e:
        raise RuntimeError(f"Dexcom connection failed: {e}")

def fetch_day_readings(client, target_dt):
    readings_raw = client.get_glucose_readings(minutes=1440, max_count=288)
    day = []
    for r in readings_raw:
        if r.time.date() == target_dt and r.value:
            day.append({"time": r.time, "mmol": mg_to_mmol(r.value)})
    return sorted(day, key=lambda x: x["time"])

def _count_events(readings, condition):
    count, in_event = 0, False
    for r in readings:
        if condition(r["mmol"]):
            if not in_event:
                count += 1
                in_event = True
        else:
            in_event = False
    return count

def compute_metrics(readings):
    if not readings:
        return {}
    values = [r["mmol"] for r in readings]
    n = len(values)
    mean_g   = round(statistics.mean(values), 2)
    sd_g     = round(statistics.stdev(values), 2) if n > 1 else 0.0
    tir = round(sum(1 for v in values if RANGE_LOW <= v <= RANGE_HIGH) / n * 100, 1)
    tbr = round(sum(1 for v in values if v < RANGE_LOW)  / n * 100, 1)
    tar = round(sum(1 for v in values if v > RANGE_HIGH) / n * 100, 1)
    overnight = [r["mmol"] for r in readings
                 if OVERNIGHT_START_HOUR <= r["time"].hour < OVERNIGHT_END_HOUR]
    variability = "Stable" if sd_g < 2.0 else ("Moderate" if sd_g < 3.5 else "High")
    return {
        "mean": mean_g, "sd": sd_g,
        "tir": tir, "tbr": tbr, "tar": tar,
        "min": round(min(values), 2), "max": round(max(values), 2),
        "overnight_mean": round(statistics.mean(overnight), 2) if overnight else None,
        "high_events": _count_events(readings, lambda v: v > RANGE_HIGH),
        "low_events":  _count_events(readings, lambda v: v < RANGE_LOW),
        "variability": variability,
    }

def main():
    target_str  = os.environ.get(
        "SYNC_DATE",
        (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
    )
    target_date = datetime.date.fromisoformat(target_str)
    print(f"Dexcom sync starting for {target_str}")
    print(f"  Region: {DEXCOM_REGION}")

    client   = connect_dexcom()
    readings = fetch_day_readings(client, target_date)
    print(f"  Readings found: {len(readings)}")

    if not readings:
        print("  No readings for this date, skipping.")
        return

    metrics = compute_metrics(readings)

    props = {
        "Date":                 tp(target_str),
        "Mean Glucose":         np(metrics.get("mean")),
        "Glucose SD":           np(metrics.get("sd")),
        "Time in Range %":      np(metrics.get("tir")),
        "Time Below Range %":   np(metrics.get("tbr")),
        "Time Above Range %":   np(metrics.get("tar")),
        "Overnight Mean":       np(metrics.get("overnight_mean")),
        "Pre-Workout Glucose":  np(None),
        "Post-Workout Glucose": np(None),
        "High Events":          np(metrics.get("high_events")),
        "Low Events":           np(metrics.get("low_events")),
        "Daily Min":            np(metrics.get("min")),
        "Daily Max":            np(metrics.get("max")),
        "Glucose Variability":  sp(metrics.get("variability")),
    }

    upsert(DB_CGM, "Date", target_str, props)
    print(f"  Mean: {metrics.get('mean')} mmol/L | TIR: {metrics.get('tir')}% | SD: {metrics.get('sd')}")
    print("\nDexcom sync complete.")

if __name__ == "__main__":
    main()
