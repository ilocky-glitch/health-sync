"""
strength_sync.py
Cross-references the Strength Session Log with CGM and Garmin Daily Metrics,
computes % of 1RM using the Epley formula, generates an exercise summary,
and upserts enriched data back into the Strength Session Log.

Also updates the 1RM Reference table with the latest estimated 1RM from
each session's top sets.

Env vars required:
  NOTION_TOKEN
  NOTION_DB_STRENGTH   (🏋️ Strength Session Log)
  NOTION_DB_CGM        (🩸 CGM Daily Metrics)
  NOTION_DB_GARMIN     (🏃 Garmin Daily Metrics)
  NOTION_DB_1RM        (💪 1RM Reference)
"""

import os, json, re, datetime
import requests

NOTION_TOKEN     = os.environ["NOTION_TOKEN"]
DB_STRENGTH      = os.environ["NOTION_DB_STRENGTH"]
DB_CGM           = os.environ["NOTION_DB_CGM"]
DB_GARMIN        = os.environ["NOTION_DB_GARMIN"]
DB_1RM           = os.environ["NOTION_DB_1RM"]

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}

# ── Notion helpers ────────────────────────────────────────────────────────────
def query(db_id, filter_payload=None, sorts=None):
    url  = f"https://api.notion.com/v1/databases/{db_id}/query"
    body = {"page_size": 50}
    if filter_payload: body["filter"] = filter_payload
    if sorts:          body["sorts"]  = sorts
    r = requests.post(url, headers=NOTION_HEADERS, json=body)
    r.raise_for_status()
    return r.json().get("results", [])

def update_page(page_id, props):
    url = f"https://api.notion.com/v1/pages/{page_id}"
    r   = requests.patch(url, headers=NOTION_HEADERS, json={"properties": props})
    r.raise_for_status()

def create_page(db_id, props):
    url  = "https://api.notion.com/v1/pages"
    body = {"parent": {"database_id": db_id}, "properties": props}
    r    = requests.post(url, headers=NOTION_HEADERS, json=body)
    r.raise_for_status()

def prop(page, name, ptype):
    p = page.get("properties", {}).get(name)
    if not p: return None
    if ptype == "title":   return p.get("title", [{}])[0].get("text", {}).get("content")
    if ptype == "number":  return p.get("number")
    if ptype == "select":  return p.get("select", {}).get("name") if p.get("select") else None
    if ptype == "text":    return p.get("rich_text", [{}])[0].get("text", {}).get("content") if p.get("rich_text") else None
    if ptype == "date":    return p.get("date", {}).get("start") if p.get("date") else None
    return None

def tp(v):  return {"title": [{"text": {"content": str(v)}}]}
def np(v):  return {"number": round(float(v), 1) if v is not None else None}
def sp(v):  return {"select": {"name": v} if v else None}
def txp(v): return {"rich_text": [{"text": {"content": str(v)[:2000]}}]}

# ── 1RM calculation (Epley formula) ──────────────────────────────────────────
def epley_1rm(weight_kg: float, reps: int) -> float:
    """Estimate 1RM using the Epley formula: weight × (1 + reps/30)"""
    if reps == 1:
        return weight_kg
    return round(weight_kg * (1 + reps / 30), 1)

def pct_of_1rm(weight_kg: float, one_rm: float) -> float | None:
    if not one_rm or one_rm <= 0:
        return None
    return round(weight_kg / one_rm * 100, 1)

# ── Parse exercise blocks into a readable summary ─────────────────────────────
def parse_block_summary(block_text: str) -> str:
    """
    Extract key exercises and sets from a block text string.
    Expects format like: 'Squat 3x5 @ 120kg\nRDL 3x8 @ 80kg'
    Returns a compact one-line summary.
    """
    if not block_text:
        return ""
    lines = [l.strip() for l in block_text.strip().split("\n") if l.strip()]
    # Take first word of each line (exercise name) + weight if present
    summaries = []
    for line in lines[:4]:  # cap at 4 exercises per block
        # Try to extract weight
        weight_match = re.search(r'@\s*(\d+(?:\.\d+)?)\s*kg', line, re.IGNORECASE)
        # Try to extract sets x reps
        sets_match = re.search(r'(\d+)\s*[xX×]\s*(\d+)', line)
        # Get exercise name (first meaningful word(s))
        name = re.split(r'\s+\d', line)[0].strip()[:25]
        if weight_match and sets_match:
            summaries.append(f"{name} {sets_match.group(1)}×{sets_match.group(2)}@{weight_match.group(1)}kg")
        elif sets_match:
            summaries.append(f"{name} {sets_match.group(1)}×{sets_match.group(2)}")
        else:
            summaries.append(name)
    return " | ".join(summaries)

def build_exercise_summary(session_page) -> str:
    """Build a full session exercise summary from all blocks."""
    blocks = {
        "A": prop(session_page, "A Block — Power", "text"),
        "B": prop(session_page, "B Block — Strength", "text"),
        "C": prop(session_page, "C Block — Accessories", "text"),
        "D": prop(session_page, "D Block — Abs", "text"),
        "E": prop(session_page, "E Block — Burner", "text"),
    }
    parts = []
    for label, text in blocks.items():
        summary = parse_block_summary(text)
        if summary:
            parts.append(f"[{label}] {summary}")
    return " · ".join(parts) if parts else "No exercise data"

# ── CGM lookup ────────────────────────────────────────────────────────────────
def get_cgm_for_date(date_str: str) -> dict:
    results = query(DB_CGM, {
        "property": "Date",
        "title": {"equals": date_str}
    })
    if not results:
        return {}
    p = results[0]
    return {
        "pre":         prop(p, "Pre-Workout Glucose", "number"),
        "post":        prop(p, "Post-Workout Glucose", "number"),
        "variability": prop(p, "Glucose Variability", "select"),
        "mean":        prop(p, "Mean Glucose", "number"),
        "tir":         prop(p, "Time in Range %", "number"),
    }

def get_garmin_for_date(date_str: str) -> dict:
    results = query(DB_GARMIN, {
        "property": "Date",
        "title": {"equals": date_str}
    })
    if not results:
        return {}
    p = results[0]
    return {
        "hrv":       prop(p, "HRV Last Night", "number"),
        "readiness": prop(p, "Training Readiness", "number"),
        "avg_hr":    prop(p, "Resting HR", "number"),
    }

# ── 1RM reference lookup & update ────────────────────────────────────────────
def get_1rm_table() -> dict:
    """Returns {lift_name: {page_id, current_1rm}} from the 1RM Reference db."""
    results = query(DB_1RM)
    table = {}
    for p in results:
        lift = prop(p, "Lift", "title")
        if lift:
            table[lift.lower()] = {
                "page_id":    p["id"],
                "current_1rm": prop(p, "Current 1RM kg", "number"),
            }
    return table

def update_1rm_reference(lift_name: str, estimated_1rm: float,
                          top_set_kg: float, date_str: str, table: dict):
    """Update the 1RM Reference table with the latest estimated 1RM."""
    key = lift_name.lower()
    if key not in table:
        return  # lift not in reference table, skip

    entry       = table[key]
    current_1rm = entry.get("current_1rm")

    props = {
        "Estimated from Top Set": np(estimated_1rm),
        "Last Tested": txp(date_str),
    }

    # Only update Current 1RM if the estimate exceeds current known 1RM
    if current_1rm is None or estimated_1rm > current_1rm:
        props["Previous 1RM kg"] = np(current_1rm)
        props["Current 1RM kg"]  = np(estimated_1rm)
        if current_1rm:
            props["Change kg"] = np(estimated_1rm - current_1rm)
        print(f"    📈 New estimated 1RM for {lift_name}: {estimated_1rm}kg (was {current_1rm}kg)")

    update_page(entry["page_id"], props)

# ── Main sync ─────────────────────────────────────────────────────────────────
def main():
    target_str  = os.environ.get(
        "SYNC_DATE",
        (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
    )
    print(f"🔄 Strength sync starting for {target_str}")

    # Load 1RM reference table once
    one_rm_table = get_1rm_table()
    print(f"  Loaded {len(one_rm_table)} lifts from 1RM Reference")

    # Find strength sessions for the target date
    sessions = query(DB_STRENGTH, {
        "property": "Date",
        "date": {"equals": target_str}
    })

    if not sessions:
        print(f"  No strength sessions found for {target_str}")
        return

    # Load CGM and Garmin data for the day
    cgm    = get_cgm_for_date(target_str)
    garmin = get_garmin_for_date(target_str)

    print(f"  Found {len(sessions)} session(s)")
    if cgm:
        print(f"  CGM: pre={cgm.get('pre')} post={cgm.get('post')} TIR={cgm.get('tir')}%")
    if garmin:
        print(f"  Garmin: HRV={garmin.get('hrv')} Readiness={garmin.get('readiness')}")

    for session in sessions:
        session_name = prop(session, "Session", "title") or "Unknown Session"
        print(f"\n  Processing: {session_name}")

        # Build exercise summary
        exercise_summary = build_exercise_summary(session)
        print(f"    Summary: {exercise_summary[:80]}…")

        # Get main lift data from existing properties
        lift1_name   = prop(session, "Main Lift 1 Exercise", "select")
        lift1_weight = prop(session, "Main Lift 1 Top Set kg", "number")
        lift1_reps   = prop(session, "Main Lift 1 Reps", "number")

        lift2_name   = prop(session, "Main Lift 2 Exercise", "select")
        lift2_weight = prop(session, "Main Lift 2 Top Set kg", "number")
        lift2_reps   = prop(session, "Main Lift 2 Reps", "number")

        props = {
            "Exercise Summary":   txp(exercise_summary),
            "Pre-Session Glucose":  np(cgm.get("pre")),
            "Post-Session Glucose": np(cgm.get("post")),
            "Glucose Variability":  sp(cgm.get("variability")),
            "Session HRV":          np(garmin.get("hrv")),
            "Session Readiness":    np(garmin.get("readiness")),
            "CGM Date":             txp(target_str),
        }

        # Compute 1RM estimates and % of 1RM for Lift 1
        if lift1_weight and lift1_reps:
            est_1rm_1 = epley_1rm(lift1_weight, int(lift1_reps))
            props["Main Lift 1 1RM kg"] = np(est_1rm_1)

            # Get stored 1RM for % calculation
            stored_1rm_1 = None
            if lift1_name:
                entry = one_rm_table.get(lift1_name.lower(), {})
                stored_1rm_1 = entry.get("current_1rm")
                update_1rm_reference(lift1_name, est_1rm_1, lift1_weight,
                                      target_str, one_rm_table)

            pct_1 = pct_of_1rm(lift1_weight, stored_1rm_1 or est_1rm_1)
            props["Main Lift 1 % of 1RM"] = np(pct_1)
            print(f"    {lift1_name}: {lift1_weight}kg × {int(lift1_reps)} reps "
                  f"→ Est. 1RM {est_1rm_1}kg ({pct_1}% of 1RM)")

        # Compute 1RM estimates and % of 1RM for Lift 2
        if lift2_weight and lift2_reps:
            est_1rm_2 = epley_1rm(lift2_weight, int(lift2_reps))
            props["Main Lift 2 1RM kg"] = np(est_1rm_2)

            stored_1rm_2 = None
            if lift2_name:
                entry = one_rm_table.get(lift2_name.lower(), {})
                stored_1rm_2 = entry.get("current_1rm")
                update_1rm_reference(lift2_name, est_1rm_2, lift2_weight,
                                      target_str, one_rm_table)

            pct_2 = pct_of_1rm(lift2_weight, stored_1rm_2 or est_1rm_2)
            props["Main Lift 2 % of 1RM"] = np(pct_2)
            print(f"    {lift2_name}: {lift2_weight}kg × {int(lift2_reps)} reps "
                  f"→ Est. 1RM {est_1rm_2}kg ({pct_2}% of 1RM)")

        update_page(session["id"], props)
        print(f"    ✓ Updated session")

    print("\n✅ Strength sync complete.")

if __name__ == "__main__":
    main()
