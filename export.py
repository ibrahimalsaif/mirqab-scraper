"""
Export parsed Power BI datasets to CSV and/or Excel files.
"""

from __future__ import annotations

import logging
import os
import re
from typing import Any

import pandas as pd

import config

logger = logging.getLogger(__name__)


def _sanitize_filename(name: str) -> str:
    """Turn an arbitrary string into a safe, readable filename stem."""
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
    """
    Write a single dataset to disk.

    Parameters
    ----------
    records : list[dict]
        Row-oriented data (as returned by :mod:`parser`).
    name : str
        Base name for the output file(s).
    fmt : str, optional
        ``"csv"``, ``"excel"``, or ``"both"``.  Falls back to
        ``config.EXPORT_FORMAT``.

    Returns
    -------
    list[str]
        Paths of files written.
    """
    fmt = (fmt or config.EXPORT_FORMAT).lower()
    out_dir = _ensure_output_dir()
    safe_name = _sanitize_filename(name)

    df = pd.DataFrame(records)
    if df.empty:
        logger.warning("Empty dataset '%s' — nothing to export", name)
        return []

    written: list[str] = []

    if fmt in ("csv", "both"):
        csv_path = os.path.join(out_dir, f"{safe_name}.csv")
        df.to_csv(csv_path, index=False)
        written.append(csv_path)
        logger.info("Saved CSV  → %s (%d rows)", csv_path, len(df))

    if fmt in ("excel", "both"):
        xlsx_path = os.path.join(out_dir, f"{safe_name}.xlsx")
        df.to_excel(xlsx_path, index=False, engine="openpyxl")
        written.append(xlsx_path)
        logger.info("Saved XLSX → %s (%d rows)", xlsx_path, len(df))

    return written


def export_all(
    named_datasets: list[tuple[str, list[dict[str, Any]]]],
    fmt: str | None = None,
) -> list[str]:
    """
    Export multiple named datasets.

    Parameters
    ----------
    named_datasets : list of (name, records) tuples
    fmt : str, optional
        Override for export format.

    Returns
    -------
    list[str]
        All file paths written.
    """
    all_paths: list[str] = []
    for name, records in named_datasets:
        paths = export_dataset(records, name=name, fmt=fmt)
        all_paths.extend(paths)
    return all_paths
