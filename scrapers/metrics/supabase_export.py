"""
Upload parsed Power BI datasets to Supabase.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any

from . import config

logger = logging.getLogger(__name__)


def _infer_table(records: list[dict], label: str) -> str:
    cols = frozenset(records[0].keys()) if records else frozenset()

    if "fatalities" in cols and "injured" in cols:
        return "casualties_by_location"
    if "barrage_from_iran" in cols or "barrage_from_lebanon" in cols:
        return "barrages_by_date"
    if "displaced_persons" in cols:
        return "displaced_by_city"
    if "civilian_fatalities" in cols:
        return "civilian_casualties_by_date"
    if "missile_quantity" in cols or "uav_quantity" in cols:
        return "missiles_uavs_by_country"
    if "central_district" in cols or "haifa_district" in cols:
        return "incidents_by_district"
    if "quantity" in cols and "country" in cols:
        return "attacks_by_country"

    if cols == frozenset(["target", "value"]):
        targets = [str(r.get("target", "")).lower() for r in records]
        if any("us " in t or "military base" in t for t in targets):
            return "us_strikes_by_target"
        return "strikes_by_target"

    if cols == frozenset(["location", "value"]):
        locations = [str(r.get("location", "")).lower() for r in records]
        if any("iran" in loc for loc in locations):
            return "israeli_strikes_in_iran"
        return "strikes_by_location"

    name = label.lower()
    name = re.sub(r"[^a-z0-9]+", "_", name)
    return name.strip("_") or "powerbi_data"


def _sanitize_columns(records: list[dict]) -> list[dict]:
    if not records:
        return records
    all_keys: list[str] = list(dict.fromkeys(k for row in records for k in row))
    col_map: dict[str, str] = {}
    for i, key in enumerate(all_keys):
        col = key.lower()
        col = re.sub(r"[^a-z0-9]+", "_", col).strip("_")
        col_map[key] = col if col else f"col_{i}"
    return [{col_map[k]: v for k, v in row.items()} for row in records]


def _get_client():
    from supabase import create_client
    url = config.SUPABASE_URL
    key = config.SUPABASE_KEY
    if not url or not key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_KEY must be set to use Supabase export.")
    return create_client(url, key)


def upload_dataset(
    records: list[dict[str, Any]],
    label: str,
    add_timestamp: bool = True,
) -> int:
    if not records:
        logger.warning("Empty dataset '%s' — nothing to upload", label)
        return 0

    client = _get_client()
    records = _sanitize_columns(records)
    table = _infer_table(records, label)

    if add_timestamp:
        ts = datetime.now(timezone.utc).isoformat()
        records = [{**row, "scraped_at": ts} for row in records]

    chunk_size = 500
    total = 0
    for i in range(0, len(records), chunk_size):
        chunk = records[i: i + chunk_size]
        client.table(table).upsert(chunk).execute()
        total += len(chunk)
        logger.info("Upserted %d rows into '%s' (%d/%d)", len(chunk), table, total, len(records))

    return total


def truncate_all(named_datasets: list[tuple[str, list[dict[str, Any]]]]) -> None:
    client = _get_client()
    seen: set[str] = set()
    for label, records in named_datasets:
        sanitized = _sanitize_columns(records)
        table = _infer_table(sanitized, label)
        if table not in seen:
            client.table(table).delete().neq("id", 0).execute()
            logger.info("Truncated table '%s'", table)
            seen.add(table)


def upload_all(
    named_datasets: list[tuple[str, list[dict[str, Any]]]],
    add_timestamp: bool = True,
) -> dict[str, int]:
    results: dict[str, int] = {}
    for label, records in named_datasets:
        sanitized = _sanitize_columns(records)
        table = _infer_table(sanitized, label)
        try:
            count = upload_dataset(records, label, add_timestamp=add_timestamp)
            results[table] = count
        except Exception as exc:
            logger.error("Failed to upload dataset '%s': %s", label, exc)
            results[table] = 0
    return results
