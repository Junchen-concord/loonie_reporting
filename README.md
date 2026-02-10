# loonie_reporting
Repo for data analysis used for Loonie reporting

## Quickstart (venv + install)

### Create + activate venv

```bash
cd /Users/starsrain/2025_concord/py_loonie_perf_reporting
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
```

### Install dependencies

```bash
pip install -r requirements.txt
```

## Run the Streamlit MVP dashboard (localhost)

```bash
streamlit run python/dashboard_kpis.py
```

Then open the URL Streamlit prints (typically `http://localhost:8501`).

## Data refresh pattern (modeled after `jcx_lending_guide`)

This repo is set up so the dashboard can read a **refreshed KPI feed** from:

- `data/refresh/kpi_metrics.csv`

### Generate a sample refresh output (no DB needed)

```bash
python -m loonie_reporting.refresh_kpis --sample
# (CloneLending_guide-style entrypoint)
python python/refresh_kpis.py --sample
```

### Refresh from SQL Server (for cron later)

1. Create a `.env` file at repo root with:

```
DB_SERVER=your_server
DB_USERNAME=your_username   # (or DB_USER=your_username)
DB_PASSWORD=your_password
ODBC_DRIVER_VERSION=ODBC Driver 18 for SQL Server
```

2. Update the query template in `sql/kpi_metrics.sql`.

3. Run:

```bash
python -m loonie_reporting.refresh_kpis
# (CloneLending_guide-style entrypoint)
python python/refresh_kpis.py
```

### Cron hook (optional)

There is a cron-friendly wrapper at `scripts/refresh_kpis_cron.sh` that writes logs to `./logs/`.

## CloneLending_guide-style entrypoints (optional)

- Dashboard:

```bash
streamlit run python/dashboard_kpis.py
```
