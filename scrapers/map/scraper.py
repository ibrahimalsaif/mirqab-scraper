"""
scrapers/map/scraper.py

Fetches strike/attack data from the Flourish story embed (story 3606069)
and upserts into the Supabase `strikes` table.

Public API
----------
run(dry_run=False) -> None
"""

from __future__ import annotations

import json
import logging
import os
import sys
from typing import Optional

logger = logging.getLogger(__name__)

FLOURISH_URL = "https://flo.uri.sh/story/3606069/embed"

# ---------------------------------------------------------------------------
# Flourish scraper
# ---------------------------------------------------------------------------

def _scrape_flourish() -> list[dict]:
    """Launch a headless browser, extract points data from Flourish template frame."""
    from playwright.sync_api import sync_playwright

    points = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        try:
            page = browser.new_page()
            logger.info("Navigating to %s", FLOURISH_URL)
            page.goto(FLOURISH_URL, wait_until="networkidle", timeout=60_000)
            page.wait_for_timeout(5_000)

            template_frame = next(
                (f for f in page.frames if "template" in f.url), None
            )
            if not template_frame:
                logger.error("Could not find Flourish template frame")
                return []

            raw = template_frame.evaluate(
                "() => JSON.stringify(window.template?.data?.points)"
            )
            col_names_raw = template_frame.evaluate(
                "() => JSON.stringify(window.template?.data?.points?.column_names)"
            )

            if not raw:
                logger.error("No points data found in Flourish template")
                return []

            points = json.loads(raw)
            col_names = json.loads(col_names_raw) if col_names_raw else {}
            logger.info("Extracted %d points from Flourish", len(points))

            # Store column names for normalization
            points = _normalize_points(points, col_names)

        finally:
            browser.close()

    return points


def _parse_date_col(date_str: str) -> Optional[str]:
    """Parse Flourish date column names to 'YYYY-MM-DD'.

    Handles:
      '28 Feb'          → '2026-02-28'
      '1 Mar'           → '2026-03-01'
      '28 Feb - 17 Mar' → '2026-02-28'  (use range start)
      'Feb 28'          → '2026-02-28'  (fallback)
      '2026-03-01'      → '2026-03-01'  (already ISO)
    """
    from datetime import datetime
    if not date_str:
        return None
    s = date_str.strip()
    # Skip date ranges (e.g. "28 Feb - 17 Mar") — these are cumulative totals, not single dates
    if " - " in s:
        return None
    # Try day-first formats (Flourish default)
    for fmt in ["%d %b", "%d %B", "%d %b %Y", "%d %B %Y"]:
        try:
            dt = datetime.strptime(s, fmt)
            year = dt.year if dt.year != 1900 else 2026
            return f"{year}-{dt.month:02d}-{dt.day:02d}"
        except ValueError:
            continue
    # Fallback: month-first formats
    for fmt in ["%b %d", "%B %d", "%b %d, %Y", "%B %d, %Y"]:
        try:
            dt = datetime.strptime(s, fmt)
            year = dt.year if dt.year != 1900 else 2026
            return f"{year}-{dt.month:02d}-{dt.day:02d}"
        except ValueError:
            continue
    # Already ISO
    try:
        datetime.strptime(s, "%Y-%m-%d")
        return s
    except ValueError:
        pass
    logger.warning("Could not parse date column: %r", date_str)
    return None


def _normalize_points(raw_points: list[dict], col_names: dict) -> list[dict]:
    """Expand each Flourish point into one row per date, matching the strikes table schema."""
    metadata_cols: list[str] = col_names.get("metadata", [])
    value_cols: list[str] = col_names.get("value", [])

    rows = []
    for pt in raw_points:
        lat = pt.get("lat")
        lon = pt.get("lon")
        strike_type = pt.get("color", "")
        metadata: list = pt.get("metadata", [])
        value: list = pt.get("value", [])

        if lat is None or lon is None:
            continue

        # Map metadata list → dict using column names
        meta_dict = {}
        for i, col in enumerate(metadata_cols):
            meta_dict[col] = metadata[i] if i < len(metadata) else None

        # Normalize strike type
        if "iran" in strike_type.lower():
            stype = "iran"
        elif "us" in strike_type.lower() or "israel" in strike_type.lower():
            stype = "us_israel"
        else:
            stype = strike_type.lower().replace(" ", "_")

        # Expand into one row per date (value_cols are date labels)
        for i, date_col in enumerate(value_cols):
            count = value[i] if i < len(value) else None
            if not count:
                continue  # skip zero or null counts

            event_date = _parse_date_col(date_col)
            if not event_date:
                continue

            country = meta_dict.get("Country") or None
            rows.append({
                "strike_type": stype,
                "latitude": float(lat),
                "longitude": float(lon),
                "location": meta_dict.get("Location"),
                "country": country,
                "event_date": event_date,
                "total": int(count) if isinstance(count, float) and count.is_integer() else count,
                "properties": {**meta_dict, "date_label": date_col},
            })

    # Deduplicate by (strike_type, lat, lon, event_date) — sum totals on collision
    seen: dict[tuple, dict] = {}
    for row in rows:
        key = (row["strike_type"], row["latitude"], row["longitude"], row["event_date"])
        if key in seen:
            seen[key]["total"] = (seen[key]["total"] or 0) + (row["total"] or 0)
        else:
            seen[key] = row

    return list(seen.values())


def _upsert(client, rows: list[dict]) -> tuple[int, int]:
    if not rows:
        return 0, 0

    strike_types = {r["strike_type"] for r in rows}
    already_present: set[tuple] = set()

    for st in strike_types:
        result = (
            client.table("strikes")
            .select("strike_type, latitude, longitude, event_date")
            .eq("strike_type", st)
            .execute()
        )
        for rec in result.data:
            already_present.add((rec["strike_type"], rec["latitude"], rec["longitude"], rec["event_date"]))

    client.table("strikes").upsert(
        rows,
        on_conflict="strike_type,latitude,longitude,event_date",
        ignore_duplicates=False,
    ).execute()

    inserted = updated = 0
    for row in rows:
        key = (row["strike_type"], row["latitude"], row["longitude"], row["event_date"])
        if key in already_present:
            updated += 1
        else:
            inserted += 1

    return inserted, updated


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run(dry_run: bool = False) -> None:
    """Fetch Flourish map data and upsert into Supabase (or print JSON if dry_run)."""
    if not dry_run:
        from supabase import create_client
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_KEY")
        if not url or not key:
            logger.error("SUPABASE_URL and SUPABASE_KEY must be set in .env")
            sys.exit(1)
        client = create_client(url, key)

    rows = _scrape_flourish()
    if not rows:
        logger.warning("No rows extracted from Flourish")
        return

    if dry_run:
        print(json.dumps(rows, indent=2, default=str))
        return

    try:
        inserted, updated = _upsert(client, rows)
        logger.info("SUMMARY  inserted=%d  updated=%d  total=%d",
                    inserted, updated, len(rows))
    except Exception as exc:
        logger.error("Upsert failed: %s", exc)


# ---------------------------------------------------------------------------
# OLD ArcGIS scraper (commented out — kept for reference)
# ---------------------------------------------------------------------------

# import requests
#
# ENDPOINTS = [
#     {
#         "strike_type": "iran",
#         "url": (
#             "https://services-eu1.arcgis.com/cOhMqNf3ihcdtO7J/arcgis/rest/services"
#             "/IranianAttack2026/FeatureServer/0/query"
#             "?where=1%3D1&outFields=*&returnGeometry=true&f=geojson"
#         ),
#     },
#     {
#         "strike_type": "missile",
#         "url": (
#             "https://services-eu1.arcgis.com/cOhMqNf3ihcdtO7J/arcgis/rest/services"
#             "/Reported_Missile_Tests/FeatureServer/0/query"
#             "?where=1%3D1&outFields=*&returnGeometry=true&f=geojson"
#         ),
#     },
#     {
#         "strike_type": "uav",
#         "url": (
#             "https://services-eu1.arcgis.com/cOhMqNf3ihcdtO7J/arcgis/rest/services"
#             "/IRAN_UAV/FeatureServer/0/query"
#             "?where=1%3D1&outFields=*&returnGeometry=true&f=geojson"
#         ),
#     },
#     {
#         "strike_type": "us_israel",
#         "url": (
#             "https://services-eu1.arcgis.com/cOhMqNf3ihcdtO7J/arcgis/rest/services"
#             "/IDF_US_Strikes_2026/FeatureServer/0/query"
#             "?where=1%3D1&outFields=*&returnGeometry=true&f=geojson"
#         ),
#     },
# ]
#
# DATE_FORMATS = [
#     "%Y-%m-%d",
#     "%d-%b-%y",
#     "%d %B %Y",
#     "%B %d, %Y",
#     "%d/%m/%Y",
# ]
#
# REQUEST_TIMEOUT = 30
#
# def _parse_date(raw):
#     if not raw:
#         return None
#     raw = raw.strip()
#     for fmt in DATE_FORMATS:
#         try:
#             return datetime.strptime(raw, fmt).date().isoformat()
#         except ValueError:
#             continue
#     return None
#
# def _coords(feature):
#     geom = feature.get("geometry") or {}
#     props = feature.get("properties") or {}
#     if geom.get("type") == "Point":
#         coords = geom.get("coordinates", [])
#         if len(coords) >= 2:
#             return float(coords[1]), float(coords[0])
#     lat = props.get("latitude") or props.get("Latitude")
#     lon = props.get("longitude") or props.get("Longitude")
#     if lat is not None and lon is not None:
#         return float(lat), float(lon)
#     return None, None
#
# def _normalize_arcgis(feature, strike_type):
#     props = feature.get("properties") or {}
#     lat, lon = _coords(feature)
#     if lat is None or lon is None:
#         return None
#     raw_date = props.get("Date") or props.get("LastReport") or props.get("date")
#     event_date = _parse_date(raw_date)
#     location = (
#         props.get("TargeteSite") or props.get("TargetedSite")
#         or props.get("Location") or props.get("System") or props.get("Event")
#     )
#     country = props.get("Country") or props.get("country")
#     return {
#         "strike_type": strike_type,
#         "latitude": lat,
#         "longitude": lon,
#         "location": location,
#         "country": country,
#         "event_date": event_date,
#         "total": 1,
#         "properties": {k: v for k, v in props.items() if k != "OBJECTID"},
#     }
#
# def _fetch_arcgis(url):
#     response = requests.get(url, timeout=REQUEST_TIMEOUT)
#     response.raise_for_status()
#     return response.json().get("features") or []
#
# def run_arcgis(dry_run=False):
#     """Original ArcGIS runner."""
#     ...
