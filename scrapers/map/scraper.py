"""
scrapers/arcgis.py

Fetches strike/attack data from 4 ArcGIS GeoJSON endpoints and upserts
into the Supabase `strikes` table.

Public API
----------
run(dry_run=False) -> None
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime
from typing import Optional

import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

ENDPOINTS = [
    {
        "strike_type": "iran",
        "url": (
            "https://services-eu1.arcgis.com/cOhMqNf3ihcdtO7J/arcgis/rest/services"
            "/IranianAttack2026/FeatureServer/0/query"
            "?where=1%3D1&outFields=*&returnGeometry=true&f=geojson"
        ),
    },
    {
        "strike_type": "missile",
        "url": (
            "https://services-eu1.arcgis.com/cOhMqNf3ihcdtO7J/arcgis/rest/services"
            "/Reported_Missile_Tests/FeatureServer/0/query"
            "?where=1%3D1&outFields=*&returnGeometry=true&f=geojson"
        ),
    },
    {
        "strike_type": "uav",
        "url": (
            "https://services-eu1.arcgis.com/cOhMqNf3ihcdtO7J/arcgis/rest/services"
            "/IRAN_UAV/FeatureServer/0/query"
            "?where=1%3D1&outFields=*&returnGeometry=true&f=geojson"
        ),
    },
    {
        "strike_type": "us_israel",
        "url": (
            "https://services-eu1.arcgis.com/cOhMqNf3ihcdtO7J/arcgis/rest/services"
            "/IDF_US_Strikes_2026/FeatureServer/0/query"
            "?where=1%3D1&outFields=*&returnGeometry=true&f=geojson"
        ),
    },
]

DATE_FORMATS = [
    "%Y-%m-%d",      # "2026-02-28"
    "%d-%b-%y",      # "21-Jul-25"
    "%d %B %Y",      # "27 January 2026"
    "%B %d, %Y",     # "January 27, 2026"
    "%d/%m/%Y",      # "27/01/2026"
]

REQUEST_TIMEOUT = 30


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_date(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    raw = raw.strip()
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _coords(feature: dict) -> tuple[Optional[float], Optional[float]]:
    geom = feature.get("geometry") or {}
    props = feature.get("properties") or {}

    if geom.get("type") == "Point":
        coords = geom.get("coordinates", [])
        if len(coords) >= 2:
            return float(coords[1]), float(coords[0])  # GeoJSON is [lon, lat]

    lat = props.get("latitude") or props.get("Latitude")
    lon = props.get("longitude") or props.get("Longitude")
    if lat is not None and lon is not None:
        return float(lat), float(lon)

    return None, None


def _normalize(feature: dict, strike_type: str) -> Optional[dict]:
    props = feature.get("properties") or {}

    lat, lon = _coords(feature)
    if lat is None or lon is None:
        return None

    raw_date = props.get("Date") or props.get("LastReport") or props.get("date")
    event_date = _parse_date(raw_date)

    location = (
        props.get("TargeteSite")
        or props.get("TargetedSite")
        or props.get("Location")
        or props.get("System")
        or props.get("Event")
    )

    country = props.get("Country") or props.get("country")

    return {
        "strike_type": strike_type,
        "latitude": lat,
        "longitude": lon,
        "location": location,
        "country": country,
        "event_date": event_date,
        "total": 1,
        "properties": {k: v for k, v in props.items() if k != "OBJECTID"},
    }


def _fetch(url: str) -> list[dict]:
    response = requests.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json().get("features") or []


def _upsert(client, rows: list[dict]) -> tuple[int, int]:
    if not rows:
        return 0, 0

    strike_types = {r["strike_type"] for r in rows}
    already_present: set[tuple] = set()
    for st in strike_types:
        dates = [r["event_date"] for r in rows if r["strike_type"] == st and r["event_date"]]
        if not dates:
            continue
        result = (
            client.table("strikes")
            .select("strike_type, latitude, longitude, event_date")
            .eq("strike_type", st)
            .in_("event_date", dates)
            .execute()
        )
        for rec in result.data:
            already_present.add(
                (rec["strike_type"], rec["latitude"], rec["longitude"], rec["event_date"])
            )

    client.table("strikes").upsert(
        rows,
        on_conflict="strike_type,latitude,longitude,event_date",
        ignore_duplicates=False,
    ).execute()

    inserted, updated = 0, 0
    for row in rows:
        if row["event_date"] is None:
            inserted += 1
            continue
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
    """Fetch all ArcGIS endpoints and upsert into Supabase (or print JSON if dry_run)."""
    if not dry_run:
        import os
        from supabase import create_client
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_KEY")
        if not url or not key:
            logger.error("SUPABASE_URL and SUPABASE_KEY must be set in .env")
            sys.exit(1)
        client = create_client(url, key)

    all_rows: dict[str, list[dict]] = {}
    total_inserted = total_updated = total_skipped = 0

    for endpoint in ENDPOINTS:
        strike_type = endpoint["strike_type"]
        url = endpoint["url"]

        logger.info("[%s] Fetching ...", strike_type)
        try:
            features = _fetch(url)
        except requests.RequestException as exc:
            logger.error("[%s] Fetch failed: %s", strike_type, exc)
            continue

        logger.info("[%s] %d features received", strike_type, len(features))

        rows, skipped = [], 0
        for feature in features:
            row = _normalize(feature, strike_type)
            if row is None:
                skipped += 1
            else:
                rows.append(row)

        if skipped:
            logger.info("[%s] %d features skipped (no coordinates)", strike_type, skipped)

        if not rows:
            total_skipped += skipped
            continue

        if dry_run:
            all_rows[strike_type] = rows
            total_skipped += skipped
            logger.info("[%s] %d rows normalized (dry run)", strike_type, len(rows))
        else:
            try:
                inserted, updated = _upsert(client, rows)
            except Exception as exc:
                logger.error("[%s] Upsert failed: %s", strike_type, exc)
                total_skipped += len(rows) + skipped
                continue

            logger.info("[%s] inserted=%d updated=%d skipped=%d", strike_type, inserted, updated, skipped)
            total_inserted += inserted
            total_updated += updated
            total_skipped += skipped

    if dry_run:
        print(json.dumps(all_rows, indent=2, default=str))
    else:
        logger.info("SUMMARY  inserted=%d  updated=%d  skipped=%d",
                    total_inserted, total_updated, total_skipped)
