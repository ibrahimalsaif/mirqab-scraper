# Mirqab — Power BI Public Dashboard Scraper

Scrapes data from any **public** Power BI dashboard by intercepting the API calls the dashboard makes under the hood, parsing the deeply-nested JSON responses, and exporting clean CSV/Excel files.

## How it works

1. A headless Chromium browser (via Playwright) loads the dashboard.
2. Every network response whose URL contains `querydata` or `public/reports` is captured.
3. The nested Power BI JSON (results → result → data → dsr → DS → PH → DM0) is parsed, handling Power BI's bitmask-based data compression.
4. Each parsed dataset is exported to `output/` as CSV and/or Excel.

## Setup

```bash
# 1. Create & activate a virtual environment (recommended)
python -m venv .venv
source .venv/bin/activate   # macOS / Linux
# .venv\Scripts\activate    # Windows

# 2. Install Python dependencies
pip install -r requirements.txt

# 3. Install Playwright's Chromium browser
playwright install chromium
```

## Configuration

Open `config.py` and set `DASHBOARD_URL` to the public Power BI URL you want to scrape, or pass it as an environment variable:

```bash
export POWERBI_URL="https://app.powerbi.com/view?r=YOUR_TOKEN_HERE"
```

Other settings in `config.py`:

| Setting | Default | Description |
|---------|---------|-------------|
| `HEADLESS` | `True` | Run browser without a visible window |
| `PAGE_LOAD_TIMEOUT_MS` | `60000` | Max time to wait for page load |
| `EXTRA_SETTLE_MS` | `5000` | Extra wait after load for late visuals |
| `OUTPUT_DIR` | `./output` | Where exported files are saved |
| `EXPORT_FORMAT` | `"both"` | `"csv"`, `"excel"`, or `"both"` |

## Usage

```bash
python main.py
```

Output files land in the `output/` folder.

## Project Structure

```
├── main.py               # Entry point — orchestrates scrape → parse → export
├── powerbi_scraper.py    # Playwright browser automation & request interception
├── parser.py             # Parses Power BI's nested JSON into flat rows
├── export.py             # Exports parsed data to CSV / Excel via pandas
├── config.py             # Dashboard URL, timing, and output settings
├── requirements.txt      # Python dependencies
└── output/               # Generated CSV and Excel files (git-ignored)
```

## Notes

- Only **public** (unauthenticated) dashboards are supported.
- Each visual on a Power BI dashboard fires its own API call, so a single page typically yields multiple datasets.
- Power BI compresses repeated cell values using a bitmask (`"R"` key). The parser handles this automatically by filling forward from the previous row.
- If the dashboard has multiple tabs/pages, the scraper will attempt to click through them to capture all data.
