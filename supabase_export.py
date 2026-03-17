"""
Upload parsed Power BI datasets to Supabase.

Each dataset is upserted into a table whose name is derived from the dataset
label (spaces/dashes replaced with underscores, lowercased).

Requirements
------------
- pip install supabase
- SUPABASE_URL and SUPABASE_KEY env vars (or set in config.py)

The tables must already exist in Supabase with columns that match the dataset
fields.  A ``scraped_at`` column (timestamptz) is appended automatically if
your table has one.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any

import config

logger = logging.getLogger(__name__)


def _infer_table(records: list[dict], label: str) -> str:
    """
    Infer the target Supabase table from the sanitized column names.
    This is order-independent — it doesn't rely on the numeric prefix of the label.
    For the two ambiguous column sets (target+value, location+value) it falls
    back to inspecting the actual row values.
    """
    cols = frozenset(records[0].keys()) if records else frozenset()

    # Unique column signatures — unambiguous matches
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

    # Ambiguous: both target+value tables share the same columns
    if cols == frozenset(["target", "value"]):
        targets = [str(r.get("target", "")).lower() for r in records]
        if any("us " in t or "military base" in t for t in targets):
            return "us_strikes_by_target"
        return "strikes_by_target"

    # Ambiguous: both location+value tables share the same columns
    if cols == frozenset(["location", "value"]):
        locations = [str(r.get("location", "")).lower() for r in records]
        if any("iran" in loc for loc in locations):
            return "israeli_strikes_in_iran"
        return "strikes_by_location"

    # Final fallback: derive from label
    name = label.lower()
    name = re.sub(r"[^a-z0-9]+", "_", name)
    return name.strip("_") or "powerbi_data"


def _sanitize_columns(records: list[dict]) -> list[dict]:
    """Rename keys so they are valid snake_case Postgres column names.

    Non-ASCII characters (e.g. Hebrew) are dropped; if the result is empty
    a positional fallback ``col_0``, ``col_1`` … is used instead.
    """
    if not records:
        return records

    # Collect all keys across every row (some rows may have extra columns)
    all_keys: list[str] = list(dict.fromkeys(k for row in records for k in row))
    col_map: dict[str, str] = {}
    for i, key in enumerate(all_keys):
        col = key.lower()
        col = re.sub(r"[^a-z0-9]+", "_", col)
        col = col.strip("_")
        col_map[key] = col if col else f"col_{i}"

    return [{col_map[k]: v for k, v in row.items()} for row in records]


def _get_client():
    """Return a Supabase client, or raise if credentials are missing."""
    from supabase import create_client  # imported lazily so the rest of the app
                                        # works even without the package installed

    url = config.SUPABASE_URL
    key = config.SUPABASE_KEY
    if not url or not key:
        raise RuntimeError(
            "SUPABASE_URL and SUPABASE_KEY must be set to use Supabase export."
        )
    return create_client(url, key)


def upload_dataset(
    records: list[dict[str, Any]],
    label: str,
    add_timestamp: bool = True,
) -> int:
    """
    Upsert *records* into the Supabase table derived from *label*.

    Parameters
    ----------
    records : list[dict]
        Row-oriented data as returned by :mod:`parser`.
    label : str
        Dataset label used to derive the target table name.
    add_timestamp : bool
        If True, a ``scraped_at`` field (UTC ISO-8601) is added to every row.

    Returns
    -------
    int
        Number of rows upserted.
    """
    if not records:
        logger.warning("Empty dataset '%s' — nothing to upload", label)
        return 0

    client = _get_client()
    records = _sanitize_columns(records)
    table = _infer_table(records, label)

    if add_timestamp:
        ts = datetime.now(timezone.utc).isoformat()
        records = [{**row, "scraped_at": ts} for row in records]

    # Supabase Python SDK upserts in batches; chunk to stay under request limits
    chunk_size = 500
    total = 0
    for i in range(0, len(records), chunk_size):
        chunk = records[i : i + chunk_size]
        client.table(table).upsert(chunk).execute()
        total += len(chunk)
        logger.info("Upserted %d rows into '%s' (%d/%d)", len(chunk), table, total, len(records))

    return total


def truncate_all(named_datasets: list[tuple[str, list[dict[str, Any]]]]) -> None:
    """Truncate every target table before a fresh insert."""
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
    """
    Upload multiple named datasets to Supabase.

    Returns
    -------
    dict[str, int]
        Mapping of table name → rows upserted.
    """
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
