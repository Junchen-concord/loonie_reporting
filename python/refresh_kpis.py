from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

REPO_ROOT = Path(__file__).resolve().parents[1]
SQL_FILE = REPO_ROOT / "sql" / "kpi_metrics.sql"
OUTPUT_DIR = REPO_ROOT / "data" / "refresh"
OUTPUT_CSV = OUTPUT_DIR / "kpi_metrics.csv"
RAW_OUTPUT_CSV = OUTPUT_DIR / "kpi_daily_metrics.csv"

def _configure_odbc_ini_for_homebrew_macos() -> None:
    """
    If Homebrew unixODBC is installed on macOS, ensure Driver Manager env vars
    point at its config so pyodbc can discover registered drivers.
    """
    if os.name != "posix":
        return
    try:
        if os.uname().sysname.lower() != "darwin":
            return
    except Exception:
        return

    hb_etc = Path("/opt/homebrew/etc")
    alt_etc = Path("/usr/local/etc")
    etc_dir = hb_etc if hb_etc.exists() else alt_etc
    odbcinst = etc_dir / "odbcinst.ini"
    if not odbcinst.exists():
        return

    # unixODBC + iODBC typically honor these.
    os.environ.setdefault("ODBCSYSINI", str(etc_dir))
    os.environ.setdefault("ODBCINSTINI", "odbcinst.ini")

    # Some stacks also rely on ODBCINI for data sources; harmless to set if missing.
    os.environ.setdefault("ODBCINI", str(etc_dir / "odbc.ini"))


# Configure early so environments (conda/pyenv) can locate driver registry.
_configure_odbc_ini_for_homebrew_macos()


import pyodbc  # noqa: E402

try:
    # Optional: if config exists, use it for alert thresholds.
    from scripts.controller import get_threshold_value, get_thresholds  # type: ignore
except Exception:  # pragma: no cover
    get_threshold_value = None  # type: ignore
    get_thresholds = None  # type: ignore


def _yield_result_sets(cursor: pyodbc.Cursor):
    """Iterate over result sets produced by a multi-statement script."""
    while True:
        if cursor.description:
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            yield pd.DataFrame.from_records(rows, columns=columns)
        if not cursor.nextset():
            break


def get_db_connection(database: str) -> pyodbc.Connection:
    load_dotenv()
    _configure_odbc_ini_for_homebrew_macos()
    server = os.getenv("DB_SERVER", "")
    # Support both naming conventions:
    # - CloneLending_guide uses DB_USERNAME
    # - Your current .env uses DB_USER
    username = os.getenv("DB_USERNAME", "") or os.getenv("DB_USER", "")
    password = os.getenv("DB_PASSWORD", "")
    driver = os.getenv("ODBC_DRIVER_VERSION", "ODBC Driver 18 for SQL Server")

    if not server or not username or not password:
        raise ValueError(
            "Database credentials are not configured. Set DB_SERVER, DB_USERNAME (or DB_USER), and DB_PASSWORD in your environment or .env."
        )

    conn_str = (
        f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={username};PWD={password};"
        "TrustServerCertificate=yes"
    )
    LOGGER.info("Connecting to %s", database)
    return pyodbc.connect(conn_str, autocommit=True)


def fetch_kpi_metrics(sql_path: Path = SQL_FILE, database: str = "LMSMaster") -> pd.DataFrame:
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    sql_script = sql_path.read_text(encoding="utf-8")
    with get_db_connection(database=database) as conn:
        cur = conn.cursor()
        LOGGER.info("Executing %s", sql_path)
        cur.execute(sql_script)
        result_sets = list(_yield_result_sets(cur))
        if not result_sets:
            raise RuntimeError("SQL script produced no result sets.")
        # Your `sql/kpi_metrics.sql` ends with the rollup SELECT, so the last set is the one we want.
        return result_sets[-1]


def _looks_like_kpi_feed(df: pd.DataFrame) -> bool:
    """True if SQL already returns KPI rows for the dashboard."""
    cols = set(df.columns)
    return {"Metric", "Value", "Alert"}.issubset(cols) and ("GroupName" in cols or "Group" in cols)


def _format_int(x) -> str:
    try:
        return f"{int(x):,}"
    except Exception:
        return str(x)


def _format_pct_from_decimal(x) -> str:
    """Assumes x is a decimal (e.g., 0.2037) and formats as 20.37%."""
    try:
        return f"{float(x) * 100.0:.2f}%"
    except Exception:
        return str(x)


def _to_kpi_feed(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert a 'wide' daily metrics table (e.g., ActivityDate/Seen/Scored/Accepted/...)
    into the row-based KPI feed consumed by the Streamlit dashboard.
    """
    if df.empty:
        return pd.DataFrame(columns=["GroupName", "Metric", "Value", "Alert", "Link", "UpdatedAt"])

    cols = set(df.columns)
    if "ActivityDate" in cols:
        tmp = df.copy()
        tmp["ActivityDate"] = pd.to_datetime(tmp["ActivityDate"], errors="coerce")
        tmp = tmp.sort_values(by="ActivityDate")
        latest = tmp.tail(1).iloc[0]
        updated_at = latest.get("ActivityDate")
    else:
        latest = df.iloc[0]
        updated_at = None

    def alert_for_count(v) -> str:
        try:
            n = float(v)
            if n <= 0:
                return "Red"
            return "Green"
        except Exception:
            return "Yellow"

    def alert_for_accepted(v) -> str:
        """
        Accept Count thresholding (mirrors NotazoSystemAlerts logic at a high level):
        - Red if Accepted <= lower_threshold
        - Green otherwise
        - Fall back to >0 logic if thresholds aren't available
        """
        try:
            n = float(v)
        except Exception:
            return "Yellow"

        if callable(get_threshold_value):
            lower = get_threshold_value("AcceptCount", "lower_threshold", default=None)
            try:
                if lower is not None and n <= float(lower):
                    return "Red"
            except Exception:
                # If config value is malformed, ignore and fall back.
                pass

        return "Red" if n <= 0 else "Green"

    rows: list[dict] = []

    # Start with the three KPIs you can reliably track now.
    for col in ("Seen", "Scored", "Accepted"):
        if col in cols:
            alert = alert_for_accepted(latest.get(col)) if col == "Accepted" else alert_for_count(latest.get(col))
            rows.append(
                {
                    "GroupName": "Sales",
                    "Metric": col,
                    "Value": _format_int(latest.get(col)),
                    "Alert": alert,
                    "Link": None,
                    "UpdatedAt": updated_at,
                }
            )

    # Optional extra rates if your query includes them (kept for future expansion).
    rate_cols = [
        ("ScoringRate", "Scoring Rate"),
        ("AcceptRate", "Accept Rate"),
        ("ConvRate", "Conversion Rate"),
        ("BidRate", "Bid Rate"),
        ("WinRate", "Win Rate"),
    ]
    for col, label in rate_cols:
        if col in cols:
            rows.append(
                {
                    "GroupName": "Sales",
                    "Metric": label,
                    "Value": _format_pct_from_decimal(latest.get(col)),
                    "Alert": "Green",
                    "Link": None,
                    "UpdatedAt": updated_at,
                }
            )

    return pd.DataFrame(rows)

def _aggregate_daily_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate a multi-day daily metrics table into a single summary row.
    - Sums: Seen, Scored, Accepted, Originated, Bids, LoansFunded
    - Averages: BidRate, WinRate, ScoringRate, AcceptRate, ConvRate, ScoringCost, BidCost
    """
    if df.empty:
        return pd.DataFrame()

    sums = ["Seen", "Scored", "Accepted", "Originated", "Bids", "LoansFunded"]

    out: dict[str, float | str | int] = {}
    avg_df = df.copy()

    def _weekday_name_to_idx(name: str) -> int | None:
        mapping = {
            "mon": 0,
            "monday": 0,
            "tue": 1,
            "tues": 1,
            "tuesday": 1,
            "wed": 2,
            "wednesday": 2,
            "thu": 3,
            "thur": 3,
            "thurs": 3,
            "thursday": 3,
            "fri": 4,
            "friday": 4,
            "sat": 5,
            "saturday": 5,
            "sun": 6,
            "sunday": 6,
        }
        return mapping.get(str(name).strip().lower())

    if "ActivityDate" in df.columns:
        dates = pd.to_datetime(df["ActivityDate"], errors="coerce").dropna()
        if not dates.empty:
            out["ActivityStart"] = dates.min().date().isoformat()
            out["ActivityEnd"] = dates.max().date().isoformat()
            out["Days"] = int(dates.nunique())

        # Shared config-driven weekday exclusions for legacy averages.
        excluded_weekdays: set[int] = set()
        if callable(get_thresholds):
            try:
                th = get_thresholds("AcceptCount") or {}
                dyn = th.get("dynamic") or {}
                raw_days = dyn.get("exclude_weekdays") or []
                if isinstance(raw_days, list):
                    for d in raw_days:
                        if isinstance(d, int) and 0 <= d <= 6:
                            excluded_weekdays.add(d)
                        else:
                            idx = _weekday_name_to_idx(str(d))
                            if idx is not None:
                                excluded_weekdays.add(idx)
            except Exception:
                excluded_weekdays = set()

        if excluded_weekdays:
            avg_df = avg_df.copy()
            avg_df["ActivityDate"] = pd.to_datetime(avg_df["ActivityDate"], errors="coerce")
            avg_df = avg_df[avg_df["ActivityDate"].notna()]
            avg_df = avg_df[~avg_df["ActivityDate"].dt.dayofweek.isin(excluded_weekdays)]
            if avg_df.empty:
                # Fall back to full set so output is never blank.
                avg_df = df.copy()

    for col in sums:
        if col in df.columns:
            out[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).sum()

    # Rate metrics should be computed as weighted ratios from aggregated counts,
    # not simple mean of daily rates.
    def _sum_col(name: str, frame: pd.DataFrame) -> float:
        if name not in frame.columns:
            return 0.0
        return float(pd.to_numeric(frame[name], errors="coerce").fillna(0).sum())

    def _ratio(numerator: float, denominator: float) -> float | None:
        if denominator == 0:
            return None
        return numerator / denominator

    seen_sum = _sum_col("Seen", avg_df)
    scored_sum = _sum_col("Scored", avg_df)
    accepted_sum = _sum_col("Accepted", avg_df)
    originated_sum = _sum_col("Originated", avg_df)
    bids_sum = _sum_col("Bids", avg_df)

    out["AcceptRate"] = _ratio(accepted_sum, seen_sum)
    out["ScoringRate"] = _ratio(scored_sum, seen_sum)
    out["BidRate"] = _ratio(bids_sum, scored_sum)
    out["WinRate"] = _ratio(accepted_sum, bids_sum)
    out["ConvRate"] = _ratio(originated_sum, accepted_sum)

    # Keep cost fields as simple mean over filtered days for now.
    for col in ("ScoringCost", "BidCost"):
        if col in avg_df.columns:
            out[col] = pd.to_numeric(avg_df[col], errors="coerce").mean()

    return pd.DataFrame([out])

def sample_kpi_metrics() -> pd.DataFrame:
    # Keep consistent with Streamlit dashboard expected columns
    return pd.DataFrame(
        [
            {
                "GroupName": "Sales",
                "Metric": "Accept Count",
                "Value": "1,284",
                "Alert": "Green",
                "Link": "https://example.com/accept-count",
            },
            {
                "GroupName": "Sales",
                "Metric": "Conversion Rate",
                "Value": "9.2%",
                "Alert": "Green",
                "Link": "https://example.com/conversion-rate",
            },
            {
                "GroupName": "Performance",
                "Metric": "ACH Return Rate",
                "Value": "2.4%",
                "Alert": "Red",
                "Link": "https://reports.speedyloan.com/single/?appid=f9a3184e-f3e8-4fab-94fa-373a0f869114&obj=36bfd39d-16a2-4b83-88a1-0a5fdb7f7f72&theme=sense&bookmark=06cc43dc-cb1b-4466-b0a3-d60246ef27cf&opt=ctxmenu,currsel",
            },
            {
                "GroupName": "Performance",
                "Metric": "First Payment Default - FA%",
                "Value": "6.1%",
                "Alert": "Yellow",
                "Link": "https://example.com/fpd-fa",
            },
            {
                "GroupName": "Performance",
                "Metric": "Avg Payin",
                "Value": "1.18",
                "Alert": "Red",
                "Link": "https://example.com/payin",
            },
        ]
    )


def write_kpis(df: pd.DataFrame, output_csv: Path = OUTPUT_CSV) -> Path:
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_csv, index=False)
    LOGGER.info("Wrote %s (%d rows)", output_csv, len(df))
    return output_csv


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh KPI metrics into a local CSV for the dashboard.")
    parser.add_argument("--database", default="LMSMaster", help="SQL Server database name")
    parser.add_argument("--sql", default=str(SQL_FILE), help="Path to SQL file")
    parser.add_argument("--output", default=str(OUTPUT_CSV), help="Path to output CSV")
    parser.add_argument("--raw-output", default=str(RAW_OUTPUT_CSV), help="Path to raw output CSV (debugging/validation)")
    parser.add_argument("--sample", action="store_true", help="Write sample KPIs instead of querying SQL")
    args = parser.parse_args()

    if args.sample:
        kpi_df = sample_kpi_metrics()
    else:
        raw_df = fetch_kpi_metrics(sql_path=Path(args.sql), database=args.database)

        # Always keep a local copy of the raw query result for validation/debugging.
        Path(args.raw_output).parent.mkdir(parents=True, exist_ok=True)
        raw_df.to_csv(Path(args.raw_output), index=False)
        LOGGER.info("Wrote %s (%d rows)", args.raw_output, len(raw_df))

        if _looks_like_kpi_feed(raw_df):
            kpi_df = raw_df.copy()
        else:
            # For the current MVP, write an aggregated daily summary to kpi_metrics.csv.
            kpi_df = _aggregate_daily_metrics(raw_df)

    write_kpis(kpi_df, output_csv=Path(args.output))


if __name__ == "__main__":
    main()


