# Minecraft Mod Download Tracker

Tracks Minecraft mod download data from Modrinth and CurseForge, stores snapshots in SQLite, and exports CSV/JSON/PNG analytics.

## Requirements

- Python 3.10+ (3.11+ recommended)
- `pip` (included with normal Python installs)

Python packages used by this project:

- `cloudscraper`
- `matplotlib`
- `requests`
- `beautifulsoup4`

## 1) Install Python

### Windows
1. Go to https://www.python.org/downloads/
2. Install the latest Python 3 release.
3. In the installer, enable **Add python.exe to PATH**.
4. Verify:

```powershell
python --version
pip --version
```

### macOS
```bash
brew install python
python3 --version
pip3 --version
```

### Linux (Debian/Ubuntu)
```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip
python3 --version
pip3 --version
```

## 2) Open this project directory

```powershell
cd C:\src\minecraft\mods\modDownloadTracker
```

## 3) Create and activate a virtual environment

### Windows (PowerShell)
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### macOS / Linux
```bash
python3 -m venv .venv
source .venv/bin/activate
```

## 4) Install dependencies

```bash
pip install cloudscraper matplotlib requests beautifulsoup4
```

## 5) Run the tracker

From the project root:

```bash
python mod_download_tracker.py
```

On macOS/Linux, if `python` is not available:

```bash
python3 mod_download_tracker.py
```

Use same-day snapshot attribution (override default prior-day offset):

```bash
python mod_download_tracker.py 0
```

## Output

After a successful run:

- SQLite database: `mod_downloads.sqlite3`
- Analytics folder: `tracker_output/`
  - CSV exports (daily totals and breakdowns)
  - `summary.json`
  - Chart PNG files in `tracker_output/charts/`

## Optional: edit tracker configuration

The tracker configuration is in `tracker_config.json`. You can adjust:

- tracked projects
- output directory
- spike detection thresholds
- HTTP timeout/retry settings

### Where to set the mods to track

The list of mods is defined in `projects` inside `tracker_config.json`.

Simplest form (recommended):

```json
"projects": [
  "hearthguard",
  "another-mod-slug"
]
```

Each string slug auto-populates:
- `name` (defaults to slug)
- `modrinth.id`
- `curseforge.slug`
- `curseforge.base_url`

Advanced form (optional) for custom naming/overrides:

```json
"projects": [
  {
    "slug": "hearthguard",
    "name": "HearthGuard"
  }
]
```

Global platform switches are also in `tracker_config.json`:
- `enable_modrinth`
- `enable_curseforge`
