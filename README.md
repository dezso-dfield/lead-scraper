# Lead Scraper

A self-hosted lead generation tool with a browser-based dashboard. Search for businesses by niche and location, collect emails and phone numbers, manage campaigns across multiple projects, send mass emails, and track call outcomes — all from one place.

![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![License](https://img.shields.io/badge/License-MIT-green)
![Platform](https://img.shields.io/badge/Platform-macOS%20%7C%20Linux%20%7C%20Windows-lightgrey)

---

## Features

- **Multi-source scraping** — DuckDuckGo, Bing, Google Search, Google Maps, Europages, and country-specific directories
- **Smart query expansion** — uses autocomplete suggestions to find the most relevant queries automatically
- **Email & phone extraction** — pulls contact info from websites, JSON-LD, schema.org, and page text
- **Web UI** — full-featured dashboard at `http://localhost:7337`
- **Projects** — separate lead databases per campaign
- **Mass email campaigns** — SMTP with merge tags, configurable delay, real-time progress
- **Calling mode** — step through leads one by one, log outcomes (Answered, Callback, Voicemail, etc.)
- **Activity history** — per-lead timeline of calls and emails
- **Import / Export** — CSV, Excel, and JSON in both directions
- **Settings UI** — SMTP config, send delays, per-project `.env` overrides
- **TUI** — terminal interface for headless servers

---

## Quick Start

### macOS / Linux

```bash
git clone https://github.com/dezso-dfield/lead-scraper.git
cd lead-scraper
chmod +x setup.sh
./setup.sh
```

The setup script will:
1. Check for Python 3.10+
2. Create a virtual environment
3. Install all dependencies
4. Optionally install Playwright for Google Maps scraping
5. Launch the web UI automatically

After setup, use `./start.sh` to launch.

### Windows

```bat
git clone https://github.com/dezso-dfield/lead-scraper.git
cd lead-scraper
setup.bat
```

After setup, use `start.bat` to launch.

### Manual install

```bash
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
pip install -e .
python -m scraper
```

---

## Usage

### Web UI (recommended)

```bash
./start.sh
# Opens http://localhost:7337
```

### Terminal TUI

```bash
./start.sh --tui
```

### CLI (headless)

```bash
./start.sh "web design agency" -l "London"
./start.sh "event organizer" -l "Berlin" --limit 200
```

---

## Web UI Guide

### Scraping leads

1. Click **New Scrape** in the header
2. Enter a niche (e.g. `wedding photographer`) and location (e.g. `New York`)
3. Choose sources and result limit
4. Click **Start** — results stream in real time

### Managing leads

| Action | How |
|--------|-----|
| Filter | Search bar, status dropdown, email/phone toggles |
| Sort | Click any column header |
| Edit status | Click the status badge in a row |
| Add notes | Double-click a row to open detail view |
| Delete | Click the ✕ button on a row, or bulk-delete via checkboxes |

### Mass email campaign

1. Select leads using the checkboxes (or **Select All**)
2. Click **Send Email** in the selection bar
3. Write your subject and body — use merge tags:
   - `{{company_name}}`, `{{first_name}}`, `{{website}}`, `{{city}}`, `{{niche}}`
4. Click **Send Campaign**
5. Progress streams in real time; sent leads are auto-marked as *Contacted*

> SMTP must be configured in **Settings → SMTP** before sending.

### Calling mode

1. Select leads with phone numbers
2. Click **Start Calling** in the selection bar
3. Step through each lead — click an outcome button to log the call:
   - **Answered**, **Interested**, **Callback**, **Voicemail**, **No Answer**, **Not Interested**
4. *Answered* and *Interested* auto-qualify the lead
5. Click the phone number to copy it to your clipboard

### Activity History

Click **History** in the header to see a timeline of all calls and emails, filterable by type.

### Projects

Click the project name in the header to switch projects or create a new one. Each project has its own isolated leads database and settings.

---

## Settings

Open via the gear icon in the header.

### SMTP tab

| Field | Description |
|-------|-------------|
| Host | SMTP server hostname (e.g. `smtp.gmail.com`) |
| Port | Usually `587` (STARTTLS) or `465` (SSL) |
| SSL | Enable for port 465 |
| STARTTLS | Enable for port 587 |
| Username | Your email address or SMTP login |
| Password | App password or SMTP password |
| From Name | Display name for outgoing emails |
| From Email | Sender address |

### Sending tab

| Field | Description |
|-------|-------------|
| Min delay | Minimum seconds between emails (default: 3) |
| Max delay | Maximum seconds between emails (default: 8) |
| Daily limit | Maximum emails to send per campaign run |

### Environment tab

Edit raw `.env` variables for global or per-project overrides. Settings are loaded in this order (later overrides earlier):

```
defaults → global settings.json → global .env → project settings.json → project .env → OS env vars
```

---

## Data storage

All data is stored locally in `~/.scraper/`:

```
~/.scraper/
├── leads.db              # Default project database
├── settings.json         # Global settings
├── .env                  # Global env overrides
└── projects/
    ├── projects.json     # Project registry
    └── {project-id}/
        ├── leads.db      # Per-project database
        ├── settings.json
        └── .env
```

---

## Updating

### macOS / Linux

```bash
./update.sh
```

### Windows

```bat
update.bat
```

The update script:
- Checks for new commits on GitHub
- Shows the changelog before applying
- Stashes local changes if needed
- Reinstalls dependencies if `requirements.txt` or `pyproject.toml` changed

---

## Requirements

- Python 3.10+
- Git (for updates)
- Optional: Playwright (for Google Maps scraping, adds ~300 MB Chromium)

---

## License

MIT — see [LICENSE](LICENSE)
