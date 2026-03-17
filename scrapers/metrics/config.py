"""
Configuration for the Power BI dashboard scraper.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── Target dashboard ────────────────────────────────────────────────
DASHBOARD_URL = os.getenv(
    "POWERBI_URL",
    "https://app.powerbi.com/view?r=eyJrIjoiYmI3MzIzMTAtMTdjNC00NTY1LWFiM2YtNjI0NTk5MWEyYTI5IiwidCI6IjgwOGNmNGIzLTFhOTYtNDEzZi1iMDZiLTlkZTZjOThmNTQ2OSJ9",
)

# ── Network interception filters ────────────────────────────────────
INTERCEPT_PATTERNS = [
    "querydata",
    "public/reports",
]

# ── Browser / timing settings ───────────────────────────────────────
HEADLESS = True
PAGE_LOAD_TIMEOUT_MS = 60_000
EXTRA_SETTLE_MS = 5_000

# ── Output settings ─────────────────────────────────────────────────
SAVE_FILES = os.getenv("SAVE_FILES", "false").lower() == "true"
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "output")
EXPORT_FORMAT = os.getenv("EXPORT_FORMAT", "both")

# ── Supabase settings ────────────────────────────────────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
