from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

import pandas as pd
import pyodbc
from dotenv import load_dotenv

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

REPO_ROOT = Path(__file__).resolve().parents[1]
SQL_FILE = REPO_ROOT / "sql" / "kpi_metrics.sql"
OUTPUT_DIR = REPO_ROOT / "data" / "refresh"
OUTPUT_CSV = OUTPUT_DIR / "kpi_metrics.csv"


def get_db_connection(database: str) -> pyodbc.Connection:
    load_dotenv()
    server = os.getenv("DB_SERVER", "")
    username = os.getenv("DB_USERNAME", "")
    password = os.getenv("DB_PASSWORD", "")
    driver = os.getenv("ODBC_DRIVER_VERSION", "ODBC Driver 18 for SQL Server")

    if not server or not username or not password:
        raise ValueError(
            "Database credentials are not configured. Set DB_SERVER, DB_USERNAME, and DB_PASSWORD in your environment or .env."
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
        rows = cur.fetchall()
        cols = [c[0] for c in (cur.description or [])]
        return pd.DataFrame.from_records(rows, columns=cols)


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
    parser.add_argument("--sample", action="store_true", help="Write sample KPIs instead of querying SQL")
    args = parser.parse_args()

    if args.sample:
        df = sample_kpi_metrics()
    else:
        df = fetch_kpi_metrics(sql_path=Path(args.sql), database=args.database)

    write_kpis(df, output_csv=Path(args.output))


if __name__ == "__main__":
    main()


