"""
Export parsed Power BI datasets to CSV and/or Excel files.
"""

from __future__ import annotations

import logging
import os
import re
from typing import Any

import pandas as pd

from . import config

logger = logging.getLogger(__name__)


def _sanitize_filename(name: str) -> str:
    name = re.sub(r"[^\w\s-]", "", name)
    name = re.sub(r"[\s]+", "_", name.strip())
    return name[:120] or "dataset"


def _ensure_output_dir() -> str:
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    return config.OUTPUT_DIR


def export_dataset(
    records: list[dict[str, Any]],
    name: str = "dataset",
    fmt: str | None = None,
) -> list[str]:
    fmt = (fmt or config.EXPORT_FORMAT).lower()
    if fmt not in ("csv", "excel", "both"):
        logger.warning("Unknown EXPORT_FORMAT '%s' — defaulting to 'both'", fmt)
        fmt = "both"

    out_dir = _ensure_output_dir()
    safe_name = _sanitize_filename(name)
    df = pd.DataFrame(records)

    if df.empty:
        logger.warning("Empty dataset '%s' — nothing to export", name)
        return []

    written: list[str] = []

    if fmt in ("csv", "both"):
        path = os.path.join(out_dir, f"{safe_name}.csv")
        df.to_csv(path, index=False)
        written.append(path)
        logger.info("Saved CSV  → %s (%d rows)", path, len(df))

    if fmt in ("excel", "both"):
        path = os.path.join(out_dir, f"{safe_name}.xlsx")
        df.to_excel(path, index=False, engine="openpyxl")
        written.append(path)
        logger.info("Saved XLSX → %s (%d rows)", path, len(df))

    return written


def export_all(
    named_datasets: list[tuple[str, list[dict[str, Any]]]],
    fmt: str | None = None,
) -> list[str]:
    all_paths: list[str] = []
    for name, records in named_datasets:
        all_paths.extend(export_dataset(records, name=name, fmt=fmt))
    return all_paths
