"""
main.py — Mirqab scraper orchestrator

Usage:
    python main.py map              # fetch ArcGIS endpoints → Supabase strikes table
    python main.py map --dry-run    # fetch & print JSON, no Supabase needed
    python main.py metrics          # scrape Power BI dashboard → Supabase tables
    python main.py all              # run both

Environment variables (set in .env):
    SUPABASE_URL=
    SUPABASE_KEY=
    POWERBI_URL=        # optional — defaults to the built-in dashboard URL
    SAVE_FILES=false    # set to true to also write CSV/Excel to output/
    EXPORT_FORMAT=both  # csv | excel | both
"""

from __future__ import annotations

import logging
import re
import sys
from urllib.parse import urlparse

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("main")

VALID_COMMANDS = ("map", "metrics", "all")


def _usage() -> None:
    print(__doc__)
    sys.exit(1)


# ---------------------------------------------------------------------------
# Map runner (ArcGIS)
# ---------------------------------------------------------------------------

def run_map(dry_run: bool = False) -> None:
    from scrapers.map.scraper import run
    logger.info("=== Map ingest %s===", "(dry run) " if dry_run else "")
    run(dry_run=dry_run)


# ---------------------------------------------------------------------------
# Metrics runner (Power BI)
# ---------------------------------------------------------------------------

def _label_for_exchange(index: int, request_payload: dict | None, fallback_url: str) -> str:
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


def run_metrics() -> None:
    from scrapers.metrics.scraper import scrape
    from scrapers.metrics.parser import try_parse
    from scrapers.metrics.export import export_all
    from scrapers.metrics.supabase_export import upload_all, truncate_all
    from scrapers.metrics import config

    logger.info("=== Metrics scrape ===")

    if "PASTE_YOUR_REPORT_TOKEN_HERE" in config.DASHBOARD_URL:
        logger.error("No dashboard URL configured. Set POWERBI_URL in .env.")
        sys.exit(1)

    logger.info("Target URL: %s", config.DASHBOARD_URL)

    # 1. Scrape
    result = scrape(config.DASHBOARD_URL)
    logger.info("Scraper finished — %d exchange(s) captured", len(result.exchanges))

    if not result.exchanges:
        logger.warning("No Power BI data exchanges intercepted.")
        sys.exit(0)

    # 2. Parse
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
        logger.warning("Responses captured but none contained parseable data.")
        sys.exit(0)

    # 3. Export to files
    paths: list[str] = []
    if config.SAVE_FILES:
        paths = export_all(named_datasets)
    else:
        logger.info("File export disabled (SAVE_FILES=false)")

    # 4. Supabase upload
    supabase_results: dict = {}
    if config.SUPABASE_URL and config.SUPABASE_KEY:
        logger.info("Uploading datasets to Supabase …")
        truncate_all(named_datasets)
        supabase_results = upload_all(named_datasets)
    elif not config.SAVE_FILES:
        logger.error("SUPABASE_URL/KEY must be set when SAVE_FILES=false.")
        sys.exit(1)
    else:
        logger.info("Supabase credentials not set — skipping upload.")

    # 5. Summary
    print("\n" + "=" * 60)
    print("  METRICS SUMMARY")
    print("=" * 60)
    print(f"  Exchanges captured : {len(result.exchanges)}")
    print(f"  Datasets parsed    : {len(named_datasets)}")
    if paths:
        print(f"  Files written      : {len(paths)}")
        for p in paths:
            print(f"    → {p}")
    print(f"  Supabase tables    : {len(supabase_results)}")
    for tbl, cnt in supabase_results.items():
        print(f"    → {tbl}: {cnt} rows" if cnt else f"    → {tbl}: FAILED")
    print("=" * 60 + "\n")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    args = sys.argv[1:]
    if not args:
        _usage()

    command = args[0].lower()
    dry_run = "--dry-run" in args

    if command not in VALID_COMMANDS:
        logger.error("Unknown command '%s'. Choose from: %s", command, ", ".join(VALID_COMMANDS))
        _usage()

    if command in ("map", "all"):
        run_map(dry_run=dry_run)

    if command in ("metrics", "all"):
        run_metrics()


if __name__ == "__main__":
    main()
