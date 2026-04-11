"""
garmin_sync.py
Pulls daily metrics + running activities from Garmin Connect
and upserts into Notion databases.

Databases targeted:
  🏃 Garmin Daily Metrics   → NOTION_DB_GARMIN
  ⚡ Running Performance Log → NOTION_DB_RUNNING

Env vars required:
  GARMIN_EMAIL, GARMIN_PASSWORD
  NOTION_TOKEN
  NOTION_DB_GARMIN
  NOTION_DB_RUNNING
"""

import os, json, math, datetime, time
import requests
from garminconnect import Garmin

# ── Config ────────────────────────────────────────────────────────────────────
NOTION_TOKEN    = os.environ["NOTION_TOKEN"]
DB_GARMIN       = os.environ["NOTION_DB_GARMIN"]
DB_RUNNING      = os.environ["NOTION_DB_RUNNING"]
GARMIN_EMAIL    = os.environ["GARMIN_EMAIL"]
GARMIN_PASSWORD = os.environ["GARMIN_PASSWORD"]

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}

# Walk pace threshold: laps slower than this (min/km) are excluded from run metrics
WALK_THRESHOLD_MIN_KM = 7.5

# ── Helpers ───────────────────────────────────────────────────────────────────
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
    """Create or update a Notion page matched by title."""
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

def title_prop(value: str) -> dict:
    return {"title": [{"text": {"content": str(value)}}]}

def number_prop(value) -> dict:
    return {"number": round(float(value), 2) if value is not None else None}

def select_prop(value: str) -> dict:
    return {"select": {"name": value} if value else None}

def text_prop(value: str) -> dict:
    return {"rich_text": [{"text": {"content": str(value)[:2000]}}]}

def safe(d: dict, *keys, default=None):
    for k in keys:
        if not isinstance(d, dict):
            return default
        d = d.get(k, default)
        if d is None:
            return default
    return d

def ms_to_min_per_km(ms: float) -> float | None:
    """Convert m/s pace to min/km."""
    if not ms or ms <= 0:
        return None
    return round(1000 / ms / 60, 3)

def sec_to_min(s) -> float | None:
    if s is None:
        return None
    return round(float(s) / 60, 2)

# ── Garmin connection ─────────────────────────────────────────────────────────
def connect_garmin() -> Garmin:
    print("Connecting to Garmin Connect…")
    client = Garmin(GARMIN_EMAIL, GARMIN_PASSWORD)
    client.login()
    print("  ✓ Connected")
    return client

# ── Daily metrics sync ────────────────────────────────────────────────────────
def sync_daily(client: Garmin, date_str: str):
    print(f"\n── Daily metrics for {date_str} ──")

    stats      = client.get_stats(date_str) or {}
    sleep      = client.get_sleep_data(date_str) or {}
    hrv        = client.get_hrv_data(date_str) or {}
    readiness  = client.get_training_readiness(date_str) or {}
    bb         = client.get_body_battery(date_str, date_str) or []

    # ── Sleep
    sleep_summary = safe(sleep, "dailySleepDTO") or {}
    sleep_score   = safe(sleep_summary, "sleepScores", "overall", "value")
    sleep_dur     = safe(sleep_summary, "sleepTimeSeconds")
    deep_pct      = None
    rem_pct       = None
    if sleep_dur and sleep_dur > 0:
        deep_sec = safe(sleep_summary, "deepSleepSeconds") or 0
        rem_sec  = safe(sleep_summary, "remSleepSeconds")  or 0
        deep_pct = round(deep_sec / sleep_dur * 100, 1)
        rem_pct  = round(rem_sec  / sleep_dur * 100, 1)
    sleep_dur_h   = round(float(sleep_dur) / 3600, 2) if sleep_dur else None

    # ── HRV
    hrv_summary  = safe(hrv, "hrvSummary") or {}
    hrv_last     = safe(hrv_summary, "lastNight")
    hrv_5day     = safe(hrv_summary, "lastNight5MinHigh")  # 5-day rolling
    hrv_status   = safe(hrv_summary, "status") or ""
    # Normalise status strings
    hrv_status_map = {"BALANCED": "Balanced", "UNBALANCED": "Unbalanced", "POOR": "Poor"}
    hrv_status_clean = hrv_status_map.get(hrv_status.upper(), None)

    # ── Body Battery
    bb_low, bb_high = None, None
    if bb:
        values = [v.get("charged") for d in bb for v in (d.get("bodyBatteryValuesDescriptors") or []) if v.get("charged") is not None]
        if values:
            bb_low  = min(values)
            bb_high = max(values)

    # ── Training
    readiness_score = safe(readiness, "score")
    vo2             = safe(stats, "maxMetValue")
    training_load   = safe(stats, "acuteTrainingLoad") or safe(stats, "trainingLoadBalance", "acuteLoad")
    resting_hr      = safe(stats, "restingHeartRate")
    steps           = safe(stats, "totalSteps")
    active_cal      = safe(stats, "activeKilocalories")

    props = {
        "Date":               title_prop(date_str),
        "Resting HR":         number_prop(resting_hr),
        "HRV Last Night":     number_prop(hrv_last),
        "HRV 5-Day Avg":      number_prop(hrv_5day),
        "HRV Status":         select_prop(hrv_status_clean),
        "Body Battery Low":   number_prop(bb_low),
        "Body Battery High":  number_prop(bb_high),
        "Training Readiness": number_prop(readiness_score),
        "Sleep Score":        number_prop(sleep_score),
        "Sleep Duration":     number_prop(sleep_dur_h),
        "Deep Sleep %":       number_prop(deep_pct),
        "REM Sleep %":        number_prop(rem_pct),
        "VO2 Max":            number_prop(vo2),
        "Training Load":      number_prop(training_load),
        "Steps":              number_prop(steps),
        "Active Calories":    number_prop(active_cal),
    }

    upsert(DB_GARMIN, "Date", date_str, props)

# ── Running / activity sync ───────────────────────────────────────────────────
ACTIVITY_TYPE_MAP = {
    "running":          "Run",
    "trail_running":    "Run",
    "treadmill_running":"Run",
    "cycling":          "Cycle",
    "strength_training":"Strength",
    "hiit":             "HIIT",
    "cardio":           "HIIT",
    "resort_skiing":    "Rest",
}

def classify_activity(type_key: str, name: str) -> str:
    name_lower = (name or "").lower()
    if "hyrox" in name_lower:
        return "HYROX"
    if "tempo" in name_lower:
        return "Tempo"
    if "interval" in name_lower or "fartlek" in name_lower:
        return "Intervals"
    if "zone 2" in name_lower or "easy" in name_lower or "recovery" in name_lower:
        return "Zone 2"
    return ACTIVITY_TYPE_MAP.get((type_key or "").lower(), "Run")

def compute_run_only_metrics(laps: list) -> dict:
    """Filter out walk laps (pace > WALK_THRESHOLD) and compute run-only stats."""
    run_laps = []
    walk_time = 0.0

    for lap in laps:
        duration = float(lap.get("duration") or lap.get("elapsedDuration") or 0)
        speed    = float(lap.get("averageSpeed") or 0)
        if speed <= 0:
            walk_time += duration
            continue
        pace = 1000 / speed / 60  # min/km
        if pace > WALK_THRESHOLD_MIN_KM:
            walk_time += duration
        else:
            run_laps.append({
                "duration": duration,
                "speed":    speed,
                "hr":       lap.get("averageHR") or lap.get("averageHeartRate"),
                "distance": float(lap.get("distance") or 0),
            })

    if not run_laps:
        return {"walk_min": round(walk_time / 60, 2), "run_pace": None, "run_hr": None}

    total_dist = sum(l["distance"] for l in run_laps)
    total_time = sum(l["duration"] for l in run_laps)

    run_pace = ms_to_min_per_km(total_dist / total_time) if total_time > 0 else None

    hr_vals = [l["hr"] for l in run_laps if l["hr"]]
    run_hr  = round(sum(hr_vals) / len(hr_vals), 1) if hr_vals else None

    return {
        "walk_min":  round(walk_time / 60, 2),
        "run_pace":  run_pace,
        "run_hr":    run_hr,
    }

def extract_hr_zones(details: dict) -> dict:
    """Extract zone time percentages from activity details."""
    zones = {}
    hr_zones = safe(details, "heartRateZones") or []
    if not hr_zones:
        return zones
    total = sum(float(z.get("secsInZone") or 0) for z in hr_zones)
    if total <= 0:
        return zones
    for i, z in enumerate(hr_zones[:5], 1):
        secs = float(z.get("secsInZone") or 0)
        zones[f"Zone {i} %"] = round(secs / total * 100, 1)
    return zones

def extract_splits(laps: list) -> str:
    """Return a compact JSON string of per-km splits."""
    splits = []
    for i, lap in enumerate(laps, 1):
        speed = float(lap.get("averageSpeed") or 0)
        splits.append({
            "km":      i,
            "pace":    ms_to_min_per_km(speed),
            "hr":      lap.get("averageHR") or lap.get("averageHeartRate"),
            "cadence": lap.get("averageRunCadence") or lap.get("averageCadence"),
            "gct_ms":  lap.get("averageGroundContactTime"),
        })
    return json.dumps(splits)

def sync_activities(client: Garmin, date_str: str):
    print(f"\n── Activities for {date_str} ──")

    activities = client.get_activities_by_date(date_str, date_str) or []
    if not activities:
        print("  No activities found.")
        return

    for act in activities:
        act_id   = act.get("activityId")
        act_name = act.get("activityName") or f"Activity {act_id}"
        type_key = safe(act, "activityType", "typeKey") or ""
        act_type = classify_activity(type_key, act_name)

        print(f"  Processing: {act_name} [{act_type}]")

        # Detailed data
        try:
            details = client.get_activity(act_id) or {}
            time.sleep(0.5)  # rate limit
        except Exception as e:
            print(f"    ⚠ Could not fetch details: {e}")
            details = {}

        try:
            laps = client.get_activity_splits(act_id) or []
            if isinstance(laps, dict):
                laps = laps.get("lapDTOs") or laps.get("laps") or []
            time.sleep(0.5)
        except Exception as e:
            print(f"    ⚠ Could not fetch splits: {e}")
            laps = []

        # Core stats
        distance_km   = round(float(act.get("distance") or 0) / 1000, 3)
        moving_time   = sec_to_min(act.get("movingDuration") or act.get("duration"))
        elapsed_time  = sec_to_min(act.get("elapsedDuration") or act.get("duration"))
        avg_hr        = act.get("averageHR")
        max_hr        = act.get("maxHR")
        avg_speed     = float(act.get("averageSpeed") or 0)
        best_speed    = float(act.get("maxSpeed") or 0)
        avg_cadence   = act.get("averageRunningCadenceInStepsPerMinute") or act.get("averageBikingCadenceInRevPerMinute")
        elevation     = act.get("elevationGain")
        te_aerobic    = act.get("aerobicTrainingEffect")
        te_anaerobic  = act.get("anaerobicTrainingEffect")
        perf_cond     = safe(details, "summaryDTO", "trainingEffect") or act.get("performanceCondition")

        # Running dynamics from details
        dyn = safe(details, "summaryDTO") or {}
        avg_gct       = dyn.get("avgGroundContactTime")
        avg_osc       = dyn.get("avgVerticalOscillation")
        avg_stride    = dyn.get("avgStrideLength")
        avg_power     = dyn.get("avgPower")

        # HR zones
        zone_data = extract_hr_zones(details)

        # Splits
        splits_json = extract_splits(laps) if laps else "[]"

        # Walk-filtered run metrics
        run_metrics = compute_run_only_metrics(laps) if laps else {}

        props = {
            "Activity":                    title_prop(act_name),
            "Date":                        text_prop(date_str),
            "Activity Type":               select_prop(act_type),
            "Total Distance km":           number_prop(distance_km),
            "Moving Time min":             number_prop(moving_time),
            "Elapsed Time min":            number_prop(elapsed_time),
            "Avg Pace min/km":             number_prop(ms_to_min_per_km(avg_speed)),
            "Best Pace min/km":            number_prop(ms_to_min_per_km(best_speed)),
            "Avg HR":                      number_prop(avg_hr),
            "Max HR":                      number_prop(max_hr),
            "Avg Cadence spm":             number_prop(avg_cadence),
            "Avg GCT ms":                  number_prop(avg_gct),
            "Avg Vertical Oscillation cm": number_prop(avg_osc),
            "Avg Stride Length m":         number_prop(avg_stride),
            "Avg Power W":                 number_prop(avg_power),
            "Elevation Gain m":            number_prop(elevation),
            "Splits JSON":                 text_prop(splits_json),
            "Walk Time Excluded min":      number_prop(run_metrics.get("walk_min")),
            "Run Only Avg Pace min/km":    number_prop(run_metrics.get("run_pace")),
            "Run Only Avg HR":             number_prop(run_metrics.get("run_hr")),
            "Training Effect Aerobic":     number_prop(te_aerobic),
            "Training Effect Anaerobic":   number_prop(te_anaerobic),
            "Performance Condition":       number_prop(perf_cond),
            "Garmin Activity ID":          text_prop(str(act_id)),
        }

        # Merge zone percentages
        for z in range(1, 6):
            props[f"Zone {z} %"] = number_prop(zone_data.get(f"Zone {z} %"))

        upsert(DB_RUNNING, "Activity", act_name, props)

# ── Entry point ───────────────────────────────────────────────────────────────
def main():
    # Default: yesterday (so data is complete after midnight sync)
    target_date = os.environ.get(
        "SYNC_DATE",
        (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
    )
    print(f"🔄 Garmin sync starting for {target_date}")

    client = connect_garmin()
    sync_daily(client, target_date)
    sync_activities(client, target_date)

    print("\n✅ Garmin sync complete.")

if __name__ == "__main__":
    main()
