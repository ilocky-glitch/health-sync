"""
garmin_sync.py
Direct HTTP calls to Garmin Connect API using bearer token.
No garminconnect library — bypasses all auth issues entirely.

Env vars required:
  GARMIN_OAUTH_TOKEN  (JSON token generated from Mac)
  NOTION_TOKEN
  NOTION_DB_GARMIN
  NOTION_DB_RUNNING
"""

import os, json, datetime, time
import requests

NOTION_TOKEN       = os.environ["NOTION_TOKEN"]
DB_GARMIN          = os.environ["NOTION_DB_GARMIN"]
DB_RUNNING         = os.environ["NOTION_DB_RUNNING"]
GARMIN_OAUTH_TOKEN = os.environ["GARMIN_OAUTH_TOKEN"]

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}

WALK_THRESHOLD_MIN_KM = 7.5
BASE = "https://connect.garmin.com"

# ── Garmin API ────────────────────────────────────────────────────────────────
def get_access_token():
    return json.loads(GARMIN_OAUTH_TOKEN)["access_token"]

def garmin_headers(token):
    return {
        "Authorization": f"Bearer {token}",
        "Di-Backend": "connectapi.garmin.com",
        "X-app-ver": "4.70.2.0",
        "NK": "NT",
        "Content-Type": "application/json",
    }

def garmin_get(path, token, params=None):
    url = f"{BASE}{path}"
    r = requests.get(url, headers=garmin_headers(token), params=params, timeout=30)
    if r.status_code == 200:
        try:
            return r.json()
        except Exception:
            return {}
    print(f"  Warning: GET {path} → {r.status_code}: {r.text[:200]}")
    return {}

def get_display_name(token):
    data = garmin_get("/userprofile-service/socialProfile", token)
    return data.get("displayName") or data.get("userName") or "user"

# ── Notion helpers ────────────────────────────────────────────────────────────
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

def upsert(db_id, title_prop_name, title_value, props):
    existing = notion_query(db_id, {"property": title_prop_name,
                                     "title": {"equals": title_value}})
    if existing:
        notion_update(existing[0]["id"], props)
        print(f"  Updated: {title_value}")
    else:
        notion_create(db_id, props)
        print(f"  Created: {title_value}")

def tp(v):  return {"title": [{"text": {"content": str(v)}}]}
def np(v):  return {"number": round(float(v), 2) if v is not None else None}
def sp(v):  return {"select": {"name": v} if v else None}
def txp(v): return {"rich_text": [{"text": {"content": str(v)[:2000]}}]}

def safe(d, *keys, default=None):
    for k in keys:
        if not isinstance(d, dict): return default
        d = d.get(k, default)
        if d is None: return default
    return d

def ms_to_pace(ms):
    if not ms or ms <= 0: return None
    return round(1000 / ms / 60, 3)

def sec_to_min(s):
    return round(float(s) / 60, 2) if s is not None else None

# ── Daily metrics ─────────────────────────────────────────────────────────────
def sync_daily(token, display_name, date_str):
    print(f"\n-- Daily metrics for {date_str} --")

    stats = garmin_get(
        f"/usersummary-service/usersummary/daily/{display_name}",
        token, {"calendarDate": date_str}
    )
    sleep = garmin_get(
        f"/wellness-service/wellness/dailySleepData/{display_name}",
        token, {"date": date_str, "nonSleepBufferMinutes": 60}
    )
    hrv = garmin_get(f"/hrv-service/hrv/{date_str}", token)
    readiness = garmin_get(
        f"/metrics-service/metrics/trainingreadiness/{date_str}", token)
    bb_data = garmin_get(
        "/wellness-service/wellness/bodyBattery/reports/daily",
        token, {"startDate": date_str, "endDate": date_str}
    )

    # Sleep
    sleep_summary = safe(sleep, "dailySleepDTO") or {}
    sleep_score   = safe(sleep_summary, "sleepScores", "overall", "value")
    sleep_dur     = safe(sleep_summary, "sleepTimeSeconds")
    deep_pct = rem_pct = None
    if sleep_dur and sleep_dur > 0:
        deep_pct = round((safe(sleep_summary, "deepSleepSeconds") or 0) / sleep_dur * 100, 1)
        rem_pct  = round((safe(sleep_summary, "remSleepSeconds") or 0) / sleep_dur * 100, 1)
    sleep_dur_h = round(float(sleep_dur) / 3600, 2) if sleep_dur else None

    # HRV
    hrv_summary = safe(hrv, "hrvSummary") or {}
    hrv_status  = {"BALANCED": "Balanced", "UNBALANCED": "Unbalanced", "POOR": "Poor"}.get(
        (safe(hrv_summary, "status") or "").upper(), None)

    # Body battery
    bb_vals = []
    if isinstance(bb_data, list):
        for d in bb_data:
            for v in (d.get("bodyBatteryValuesDescriptors") or []):
                if v.get("charged") is not None:
                    bb_vals.append(v["charged"])

    # Readiness
    readiness_score = None
    if isinstance(readiness, list) and readiness:
        readiness_score = readiness[0].get("score")
    elif isinstance(readiness, dict):
        readiness_score = readiness.get("score")

    props = {
        "Date":               tp(date_str),
        "Resting HR":         np(safe(stats, "restingHeartRate")),
        "HRV Last Night":     np(safe(hrv_summary, "lastNight")),
        "HRV 5-Day Avg":      np(safe(hrv_summary, "lastNight5MinHigh")),
        "HRV Status":         sp(hrv_status),
        "Body Battery Low":   np(min(bb_vals) if bb_vals else None),
        "Body Battery High":  np(max(bb_vals) if bb_vals else None),
        "Training Readiness": np(readiness_score),
        "Sleep Score":        np(sleep_score),
        "Sleep Duration":     np(sleep_dur_h),
        "Deep Sleep %":       np(deep_pct),
        "REM Sleep %":        np(rem_pct),
        "VO2 Max":            np(safe(stats, "maxMetValue")),
        "Training Load":      np(safe(stats, "acuteTrainingLoad")),
        "Steps":              np(safe(stats, "totalSteps")),
        "Active Calories":    np(safe(stats, "activeKilocalories")),
    }
    upsert(DB_GARMIN, "Date", date_str, props)
    print(f"  Steps: {safe(stats,'totalSteps')} | RHR: {safe(stats,'restingHeartRate')} | Sleep: {sleep_score}")

# ── Activities ────────────────────────────────────────────────────────────────
TYPE_MAP = {
    "running": "Run", "trail_running": "Run", "treadmill_running": "Run",
    "cycling": "Cycle", "strength_training": "Strength",
    "hiit": "HIIT", "cardio": "HIIT"
}

def classify(type_key, name):
    n = (name or "").lower()
    if "hyrox" in n:    return "HYROX"
    if "tempo" in n:    return "Tempo"
    if "interval" in n: return "Intervals"
    if "zone 2" in n or "easy" in n: return "Zone 2"
    return TYPE_MAP.get((type_key or "").lower(), "Run")

def walk_filter(laps):
    run, walk_time = [], 0.0
    for lap in laps:
        dur   = float(lap.get("duration") or lap.get("elapsedDuration") or 0)
        speed = float(lap.get("averageSpeed") or 0)
        if speed <= 0 or 1000 / speed / 60 > WALK_THRESHOLD_MIN_KM:
            walk_time += dur
        else:
            run.append({"dur": dur, "speed": speed,
                        "hr": lap.get("averageHR") or lap.get("averageHeartRate"),
                        "dist": float(lap.get("distance") or 0)})
    if not run:
        return round(walk_time / 60, 2), None, None
    td = sum(l["dist"] for l in run)
    tt = sum(l["dur"] for l in run)
    hrs = [l["hr"] for l in run if l["hr"]]
    return (round(walk_time / 60, 2),
            ms_to_pace(td / tt) if tt else None,
            round(sum(hrs) / len(hrs), 1) if hrs else None)

def hr_zones(details):
    zones = {}
    hz = safe(details, "heartRateZones") or []
    total = sum(float(z.get("secsInZone") or 0) for z in hz)
    if total > 0:
        for i, z in enumerate(hz[:5], 1):
            zones[f"Zone {i} %"] = round(float(z.get("secsInZone") or 0) / total * 100, 1)
    return zones

def splits_json(laps):
    return json.dumps([{
        "km": i + 1,
        "pace": ms_to_pace(float(l.get("averageSpeed") or 0)),
        "hr": l.get("averageHR") or l.get("averageHeartRate"),
        "cadence": l.get("averageRunCadence") or l.get("averageCadence"),
        "gct_ms": l.get("averageGroundContactTime")
    } for i, l in enumerate(laps)])

def sync_activities(token, date_str):
    print(f"\n-- Activities for {date_str} --")

    activities = garmin_get(
        "/activitylist-service/activities/search/activities",
        token,
        {"startDate": date_str, "endDate": date_str, "start": 0, "limit": 10}
    )

    if not activities or not isinstance(activities, list):
        print("  No activities found.")
        return

    for act in activities:
        act_id   = act.get("activityId")
        act_name = act.get("activityName") or f"Activity {act_id}"
        type_key = safe(act, "activityType", "typeKey") or ""
        act_type = classify(type_key, act_name)
        print(f"  Processing: {act_name} [{act_type}]")

        details  = garmin_get(f"/activity-service/activity/{act_id}", token)
        time.sleep(0.3)

        laps_raw = garmin_get(f"/activity-service/activity/{act_id}/splits", token)
        laps = []
        if isinstance(laps_raw, dict):
            laps = laps_raw.get("lapDTOs") or laps_raw.get("laps") or []
        time.sleep(0.3)

        walk_min, run_pace, run_hr = walk_filter(laps) if laps else (None, None, None)
        zones = hr_zones(details)
        dyn   = safe(details, "summaryDTO") or {}

        props = {
            "Activity":                    tp(act_name),
            "Date":                        txp(date_str),
            "Activity Type":               sp(act_type),
            "Total Distance km":           np(round(float(act.get("distance") or 0) / 1000, 3)),
            "Moving Time min":             np(sec_to_min(act.get("movingDuration") or act.get("duration"))),
            "Elapsed Time min":            np(sec_to_min(act.get("elapsedDuration") or act.get("duration"))),
            "Avg Pace min/km":             np(ms_to_pace(float(act.get("averageSpeed") or 0))),
            "Best Pace min/km":            np(ms_to_pace(float(act.get("maxSpeed") or 0))),
            "Avg HR":                      np(act.get("averageHR")),
            "Max HR":                      np(act.get("maxHR")),
            "Avg Cadence spm":             np(act.get("averageRunningCadenceInStepsPerMinute")),
            "Avg GCT ms":                  np(dyn.get("avgGroundContactTime")),
            "Avg Vertical Oscillation cm": np(dyn.get("avgVerticalOscillation")),
            "Avg Stride Length m":         np(dyn.get("avgStrideLength")),
            "Avg Power W":                 np(dyn.get("avgPower")),
            "Elevation Gain m":            np(act.get("elevationGain")),
            "Splits JSON":                 txp(splits_json(laps) if laps else "[]"),
            "Walk Time Excluded min":      np(walk_min),
            "Run Only Avg Pace min/km":    np(run_pace),
            "Run Only Avg HR":             np(run_hr),
            "Training Effect Aerobic":     np(act.get("aerobicTrainingEffect")),
            "Training Effect Anaerobic":   np(act.get("anaerobicTrainingEffect")),
            "Garmin Activity ID":          txp(str(act_id)),
        }
        for z in range(1, 6):
            props[f"Zone {z} %"] = np(zones.get(f"Zone {z} %"))

        upsert(DB_RUNNING, "Activity", act_name, props)

# ── Entry point ───────────────────────────────────────────────────────────────
def main():
    target_date = os.environ.get(
        "SYNC_DATE",
        (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
    )
    print(f"Garmin sync starting for {target_date}")

    token = get_access_token()
    print(f"  Token loaded OK")

    display_name = get_display_name(token)
    print(f"  Display name: {display_name}")

    sync_daily(token, display_name, target_date)
    sync_activities(token, target_date)
    print("\nGarmin sync complete.")

if __name__ == "__main__":
    main()
