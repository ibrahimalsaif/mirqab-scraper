# strikes-ingest

Fetches strike/attack data from 4 ArcGIS GeoJSON endpoints and upserts into Supabase.

| `strike_type` | Source |
|---|---|
| `iran`      | IranianAttack2026 |
| `us_israel` | IDF_US_Strikes_2026 |
| `missile`   | Reported_Missile_Tests |
| `uav`       | IRAN_UAV |

---

## 1. Create the Supabase table

Open the **SQL Editor** in your Supabase project and run the contents of `schema.sql`.

---

## 2. Configure credentials

Create a `.env` file in this directory (never commit it):

```
SUPABASE_URL=https://xxxx.supabase.co
SUPABASE_KEY=your-service-role-or-anon-key
```

---

## 3. Install dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

---

## 4. Run manually

```bash
python main.py
```

Sample output:

```
[iran] Fetching https://services-eu1.arcgis.com/...
  42 features received
  inserted=40  updated=2  skipped=0

[missile] Fetching https://services-eu1.arcgis.com/...
  ...

========================================
SUMMARY  inserted=110  updated=5  skipped=0
========================================
```

---

## 5. Schedule as a daily cron job

### macOS / Linux — crontab

```bash
crontab -e
```

Add (adjust paths to match your setup):

```cron
# Run strikes-ingest every day at 06:00
0 6 * * * /path/to/strikes-ingest/.venv/bin/python /path/to/strikes-ingest/main.py >> /path/to/strikes-ingest/cron.log 2>&1
```

### macOS — launchd (recommended over crontab on macOS)

Create `~/Library/LaunchAgents/com.mirqab.strikes-ingest.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.mirqab.strikes-ingest</string>
  <key>ProgramArguments</key>
  <array>
    <string>/path/to/strikes-ingest/.venv/bin/python</string>
    <string>/path/to/strikes-ingest/main.py</string>
  </array>
  <key>StartCalendarInterval</key>
  <dict>
    <key>Hour</key>   <integer>6</integer>
    <key>Minute</key> <integer>0</integer>
  </dict>
  <key>StandardOutPath</key>
  <string>/path/to/strikes-ingest/cron.log</string>
  <key>StandardErrorPath</key>
  <string>/path/to/strikes-ingest/cron.log</string>
</dict>
</plist>
```

Load it:

```bash
launchctl load ~/Library/LaunchAgents/com.mirqab.strikes-ingest.plist
```

### Linux — systemd timer

`/etc/systemd/system/strikes-ingest.service`:
```ini
[Unit]
Description=Strikes ingest

[Service]
Type=oneshot
WorkingDirectory=/path/to/strikes-ingest
ExecStart=/path/to/strikes-ingest/.venv/bin/python main.py
EnvironmentFile=/path/to/strikes-ingest/.env
```

`/etc/systemd/system/strikes-ingest.timer`:
```ini
[Unit]
Description=Run strikes-ingest daily

[Timer]
OnCalendar=*-*-* 06:00:00
Persistent=true

[Install]
WantedBy=timers.target
```

```bash
systemctl enable --now strikes-ingest.timer
```
