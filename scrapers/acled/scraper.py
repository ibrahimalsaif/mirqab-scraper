"""
scrapers/acled/scraper.py

Fetches ACLED conflict event data from the Al Jazeera Datawrapper embed
and upserts into the Supabase `acled_events` table.

Public API
----------
run(dry_run=False) -> None
"""

from __future__ import annotations

import io
import logging
import os
import re
import sys
from typing import Optional

import requests

logger = logging.getLogger(__name__)

DATAWRAPPER_CHART_ID = "ld3tV"
DATAWRAPPER_EMBED_URL = f"https://datawrapper.dwcdn.net/{DATAWRAPPER_CHART_ID}/"


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

def _get_latest_version() -> Optional[int]:
    """Fetch the Datawrapper embed page to extract the latest data version number."""
    try:
        r = requests.get(DATAWRAPPER_EMBED_URL, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        # Version appears in URLs like /ld3tV/42/data.csv
        match = re.search(rf"{DATAWRAPPER_CHART_ID}/(\d+)/", r.text)
        if match:
            return int(match.group(1))
    except Exception as e:
        logger.warning("Could not determine latest version: %s", e)
    return None


def _fetch_csv() -> list[dict]:
    """Download the ACLED CSV from Datawrapper and return as list of dicts."""
    import pandas as pd

    version = _get_latest_version()
    if version:
        url = f"https://datawrapper.dwcdn.net/{DATAWRAPPER_CHART_ID}/{version}/data.csv"
        logger.info("Fetching ACLED data v%d from Datawrapper", version)
    else:
        url = f"https://datawrapper.dwcdn.net/{DATAWRAPPER_CHART_ID}/42/data.csv"
        logger.warning("Using fallback version URL: %s", url)

    r = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()

    df = pd.read_csv(io.StringIO(r.content.decode("utf-8")))
    logger.info("Downloaded %d rows from ACLED", len(df))
    return df.to_dict(orient="records")


# ---------------------------------------------------------------------------
# Normalize
# ---------------------------------------------------------------------------

def _normalize(raw_rows: list[dict]) -> list[dict]:
    """Map raw CSV rows to acled_events schema."""
    rows = []
    for r in raw_rows:
        event_id = str(r.get("event_id_cnty") or "").strip()
        if not event_id:
            continue

        event_date = str(r.get("event_date") or "").strip() or None
        fatalities = r.get("fatalities")
        try:
            fatalities = int(fatalities) if fatalities is not None and str(fatalities) not in ("", "nan") else 0
        except (ValueError, TypeError):
            fatalities = 0

        def _str(val) -> Optional[str]:
            s = str(val).strip() if val is not None else ""
            return None if s in ("", "nan", "NaN", "None") else s

        rows.append({
            "event_id":       event_id,
            "event_date":     event_date,
            "country":        _str(r.get("country")),
            "admin1":         _str(r.get("admin1")),
            "admin2":         _str(r.get("admin2")),
            "admin3":         _str(r.get("admin3")),
            "location":       _str(r.get("location")),
            "latitude":       float(r["latitude"]) if r.get("latitude") not in (None, "") else None,
            "longitude":      float(r["longitude"]) if r.get("longitude") not in (None, "") else None,
            "event_type":     _str(r.get("event_type")),
            "sub_event_type": _str(r.get("sub_event_type")),
            "actor1":         _str(r.get("actor1")),
            "actor2":         _str(r.get("actor2")),
            "notes":          _str(r.get("notes")),
            "fatalities":     fatalities,
            "source":         _str(r.get("source")),
        })

    return rows


# ---------------------------------------------------------------------------
# Upsert
# ---------------------------------------------------------------------------

def _upsert(client, rows: list[dict]) -> tuple[int, int]:
    if not rows:
        return 0, 0

    # Get existing event_ids
    existing = client.table("acled_events").select("event_id").execute()
    existing_ids = {r["event_id"] for r in existing.data}

    client.table("acled_events").upsert(
        rows,
        on_conflict="event_id",
        ignore_duplicates=False,
    ).execute()

    inserted = sum(1 for r in rows if r["event_id"] not in existing_ids)
    updated  = sum(1 for r in rows if r["event_id"] in existing_ids)
    return inserted, updated


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run(dry_run: bool = False) -> None:
    """Fetch ACLED data and upsert into Supabase (or print JSON if dry_run)."""
    import json

    if not dry_run:
        from supabase import create_client
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_KEY")
        if not url or not key:
            logger.error("SUPABASE_URL and SUPABASE_KEY must be set in .env")
            sys.exit(1)
        client = create_client(url, key)

    raw = _fetch_csv()
    rows = _normalize(raw)

    if not rows:
        logger.warning("No rows to insert")
        return

    if dry_run:
        print(json.dumps(rows[:5], indent=2, default=str))
        print(f"\n... {len(rows)} total rows")
        return

    try:
        inserted, updated = _upsert(client, rows)
        logger.info("SUMMARY  inserted=%d  updated=%d  total=%d", inserted, updated, len(rows))
    except Exception as exc:
        logger.error("Upsert failed: %s", exc)
