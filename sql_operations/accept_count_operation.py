from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from DatabaseConnections.ConnectToLMSMaster import ConnectToLMSMaster
from scripts.logging_utils import setup_logger

LOGGER = setup_logger(__name__, "accept_count_operation")

REPO_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = REPO_ROOT / "data" / "refresh"

STORED_PROCEDURE = "EXEC USP_SystemAlert_AcceptCountProcedure ?, ?"


def fetch_accept_count_result_sets(date_range: int = 3, time_range: int = 4) -> list[pd.DataFrame]:
    dbconnector = ConnectToLMSMaster(database="LMSMaster")
    return dbconnector.callStoredProcedure(STORED_PROCEDURE, date_range, time_range)


def run_accept_count(date_range: int = 3, time_range: int = 4) -> list[Path]:
    """
    Execute AcceptCount stored procedure and persist result sets as CSV files.
    Returns written file paths.
    """
    result_sets = fetch_accept_count_result_sets(date_range=date_range, time_range=time_range)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    outputs = [
        OUTPUT_DIR / "accept_count_active_providers.csv",
        OUTPUT_DIR / "accept_count_by_provider.csv",
    ]

    written: list[Path] = []
    for idx, df in enumerate(result_sets):
        if idx < len(outputs):
            target = outputs[idx]
        else:
            target = OUTPUT_DIR / f"accept_count_result_set_{idx + 1}.csv"
        df.to_csv(target, index=False)
        LOGGER.info("Wrote %s (%d rows)", target, len(df))
        written.append(target)

    if not written:
        raise RuntimeError("AcceptCount stored procedure returned no result sets.")

    return written


def summarize_accept_count_from_proc(date_range: int = 3, time_range: int = 4) -> dict:
    """
    Return a normalized metric dict from procedure output.
    AcceptCount is the sum of ApplicationCount across providers.
    """
    result_sets = fetch_accept_count_result_sets(date_range=date_range, time_range=time_range)
    if len(result_sets) < 2 or result_sets[1].empty:
        raise RuntimeError("AcceptCount procedure did not return provider-level application counts.")

    by_provider = result_sets[1]
    if "ApplicationCount" not in by_provider.columns:
        raise RuntimeError("Expected ApplicationCount column in AcceptCount procedure result.")

    accept_count = int(pd.to_numeric(by_provider["ApplicationCount"], errors="coerce").fillna(0).sum())
    return {
        "accept_count": accept_count,
        "source": "accept_count_proc",
    }


def backfill_accept_count_daily_from_db(days: int = 90) -> pd.DataFrame:
    """
    Option A (preferred): backfill from DB source-of-truth.
    Builds daily accepted counts for last N days (excluding today).
    """
    query = """
    SELECT
        CAST(A.ApplicationDate AS date) AS as_of_date,
        COUNT(DISTINCT A.APPGUID) AS AcceptCount
    FROM dbo.Application AS A
    WHERE A.ApplicationStatus IN ('A', 'P')
      AND A.ApplicationDate >= DATEADD(DAY, -?, CAST(GETDATE() AS date))
      AND A.ApplicationDate < CAST(GETDATE() AS date)
    GROUP BY CAST(A.ApplicationDate AS date)
    ORDER BY CAST(A.ApplicationDate AS date);
    """
    dbconnector = ConnectToLMSMaster(database="LMSMaster")
    df = dbconnector.callQuery(query, days)
    if df.empty:
        return df
    df["as_of_date"] = pd.to_datetime(df["as_of_date"], errors="coerce").dt.date.astype(str)
    df["AcceptCount"] = pd.to_numeric(df["AcceptCount"], errors="coerce").fillna(0).astype(int)
    return df


def main() -> None:
    parser = argparse.ArgumentParser(description="Run AcceptCount stored procedure and save result sets to CSV.")
    parser.add_argument("--date-range", type=int, default=3, help="Active provider lookback in days")
    parser.add_argument("--time-range", type=int, default=4, help="Accepted count lookback in hours")
    args = parser.parse_args()

    run_accept_count(date_range=args.date_range, time_range=args.time_range)


if __name__ == "__main__":
    main()

