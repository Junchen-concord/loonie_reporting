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

## KPI orchestration (Accept Count first slice)

Use the orchestrator to:
- execute Accept Count stored procedure and save raw outputs
- backfill Accept Count daily history from DB (Option A)
- write normalized history + serving files for dashboard integration

```bash
python python/run_all.py --backfill-days 90
```

Optional explicit windows:

```bash
python python/run_all.py --backfill-days 90 --windows 7,30,60
```

Retention and archival controls:

```bash
python python/run_all.py --backfill-days 90 --history-retention-days 365 --history-archive-dir data/archive
```

Outputs:
- `data/refresh/accept_count_active_providers.csv`
- `data/refresh/accept_count_by_provider.csv`
- `data/refresh/kpi_history.csv`
- `data/refresh/kpi_serving_metrics.csv`

Notes:
- `kpi_history.csv` stores daily facts (`window_days=1`).
- `kpi_serving_metrics.csv` stores dashboard rows for configured windows (`7/30/60` by default).
- Old history rows are archived monthly under `data/archive/` and active history is retained by `--history-retention-days` (default 365).

Window calculation rules:
- For `value_type=count`, window value is a rolling sum over the last N days.
- For non-count metrics, window value is a rolling average over the last N days.
- Threshold calculation can exclude weekdays from config (e.g., `exclude_weekdays: [sun]`).
- Sundays remain in raw history; weekday filters are applied in threshold evaluation.

Serving diagnostics columns:
- `lower_threshold`, `upper_threshold`, `pct_change`, `seasonal_zscore`
- `signal_count`, `signals`
- `rolling_points_used`, `seasonal_points_used`, `weekday_filter_applied`

## CloneLending_guide-style entrypoints (optional)

- Dashboard:

```bash
streamlit run python/dashboard_kpis.py
```
