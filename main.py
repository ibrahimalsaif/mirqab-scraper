#!/usr/bin/env python3
"""
Power BI Public Dashboard Scraper — entry point.

Usage:
    python main.py                          # uses URL from config.py
    POWERBI_URL="https://..." python main.py  # override via env var
"""

from __future__ import annotations

import logging
import re
import sys
from urllib.parse import urlparse

import config
from powerbi_scraper import scrape
from parser import try_parse
from export import export_all

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("main")


def _label_for_exchange(
    index: int,
    request_payload: dict | None,
    fallback_url: str,
) -> str:
    """Build a dataset filename from the request's Select columns."""
    if request_payload:
        try:
            queries = request_payload.get("queries", [])
            selects = (
                queries[0]
                .get("Query", {})
                .get("Commands", [{}])[0]
                .get("SemanticQueryDataShapeCommand", {})
                .get("Query", {})
                .get("Select", [])
            )
            names = []
            for s in selects:
                raw = s.get("Name", "")
                part = raw.rsplit(".", 1)[-1].strip()
                part = re.sub(r"[^\w\s-]", "", part).strip()
                if part:
                    names.append(part)
            if names:
                return f"{index:02d}_{'_'.join(names[:3])}"
        except Exception:
            pass

    path = urlparse(fallback_url).path.rstrip("/").split("/")
    meaningful = [seg for seg in path if seg and seg not in ("public", "reports")]
    suffix = meaningful[-1] if meaningful else "query"
    return f"{index:02d}_{suffix}"


def main() -> None:
    url = config.DASHBOARD_URL
    if "PASTE_YOUR_REPORT_TOKEN_HERE" in url:
        logger.error(
            "No dashboard URL configured.  Set POWERBI_URL as an env var "
            "or edit config.py before running."
        )
        sys.exit(1)

    logger.info("Target URL: %s", url)

    # ── 1. Scrape ───────────────────────────────────────────────────
    logger.info("Starting scraper …")
    result = scrape(url)
    logger.info("Scraper finished — %d exchange(s) captured", len(result.exchanges))

    if not result.exchanges:
        logger.warning("No Power BI data exchanges were intercepted. "
                       "Double-check the URL and network patterns in config.py.")
        sys.exit(0)

    # ── 2. Parse ────────────────────────────────────────────────────
    named_datasets: list[tuple[str, list[dict]]] = []

    for idx, exchange in enumerate(result.exchanges):
        datasets = try_parse(exchange.response_body, exchange.request_payload)
        for ds_idx, records in enumerate(datasets):
            label = _label_for_exchange(idx, exchange.request_payload, exchange.url)
            if len(datasets) > 1:
                label += f"_part{ds_idx}"
            named_datasets.append((label, records))

    logger.info("Parsed %d dataset(s) from %d exchange(s)",
                len(named_datasets), len(result.exchanges))

    if not named_datasets:
        logger.warning("Responses were captured but none contained parseable data.")
        sys.exit(0)

    # ── 3. Export ───────────────────────────────────────────────────
    paths = export_all(named_datasets)

    # ── 4. Summary ──────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  SCRAPE SUMMARY")
    print("=" * 60)
    print(f"  Exchanges captured : {len(result.exchanges)}")
    print(f"  Datasets parsed    : {len(named_datasets)}")
    print(f"  Files written      : {len(paths)}")
    for p in paths:
        print(f"    → {p}")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
