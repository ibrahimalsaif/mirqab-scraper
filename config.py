"""
Configuration for the Power BI dashboard scraper.

Set DASHBOARD_URL to the public Power BI report URL you want to scrape.
Public URLs typically look like:
  https://app.powerbi.com/view?r=<encoded_token>
"""

import os

# ── Target dashboard ────────────────────────────────────────────────
DASHBOARD_URL = os.getenv(
    "POWERBI_URL",
    "https://app.powerbi.com/view?r=eyJrIjoiYmI3MzIzMTAtMTdjNC00NTY1LWFiM2YtNjI0NTk5MWEyYTI5IiwidCI6IjgwOGNmNGIzLTFhOTYtNDEzZi1iMDZiLTlkZTZjOThmNTQ2OSJ9",
)

# ── Network interception filters ────────────────────────────────────
# Substrings that identify Power BI data API calls worth capturing.
INTERCEPT_PATTERNS = [
    "querydata",
    "public/reports",
]

# ── Browser / timing settings ───────────────────────────────────────
HEADLESS = True
PAGE_LOAD_TIMEOUT_MS = 60_000
# Extra wait (ms) after load to let late-firing visuals settle.
EXTRA_SETTLE_MS = 5_000

# ── Output settings ─────────────────────────────────────────────────
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
# Supported: "csv", "excel", or "both"
EXPORT_FORMAT = "both"
