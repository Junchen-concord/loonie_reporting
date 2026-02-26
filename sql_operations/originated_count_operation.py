from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import pandas as pd

from DatabaseConnections.ConnectToLMSMaster import ConnectToLMSMaster
from scripts.logging_utils import setup_logger

LOGGER = setup_logger(__name__, "originated_count_operation")

REPO_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = REPO_ROOT / "data" / "refresh"

STORED_PROCEDURE = "EXEC USP_SystemAlert_ConversionRateProcedure ?, ?, ?"


def _find_column(columns: list[str], candidates: list[str]) -> str | None:
    lower_map = {str(c).strip().lower(): c for c in columns}
    for item in candidates:
        key = item.strip().lower()
        if key in lower_map:
            return str(lower_map[key])
    return None


def _originated_cols(df: pd.DataFrame) -> list[str]:
    cols: list[str] = []
    for c in df.columns:
        name = str(c).strip().lower()
        if "originated loans" in name:
            cols.append(str(c))
    return cols


def fetch_originated_count_result_sets(start_num: int = 1, end_num: int = 1, days: int = 90) -> list[pd.DataFrame]:
    dbconnector = ConnectToLMSMaster(database="LMSMaster")
    return dbconnector.callStoredProcedure(STORED_PROCEDURE, start_num, end_num, days)


def run_originated_count(start_num: int = 1, end_num: int = 1, days: int = 90) -> list[Path]:
    """
    Execute ConversionRate stored procedure and persist all result sets.
    Returns written file paths.
    """
    result_sets = fetch_originated_count_result_sets(start_num=start_num, end_num=end_num, days=days)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    outputs = [
        OUTPUT_DIR / "conversion_rate_originated_totals.csv",
        OUTPUT_DIR / "conversion_rate_accepted_totals.csv",
        OUTPUT_DIR / "conversion_rate_start_date.csv",
        OUTPUT_DIR / "conversion_rate_end_date.csv",
        OUTPUT_DIR / "conversion_rate_originated_daily_by_custtype.csv",
        OUTPUT_DIR / "conversion_rate_accepted_daily_by_custtype.csv",
    ]

    written: list[Path] = []
    for idx, df in enumerate(result_sets):
        target = outputs[idx] if idx < len(outputs) else OUTPUT_DIR / f"conversion_rate_result_set_{idx + 1}.csv"
        df.to_csv(target, index=False)
        LOGGER.info("Wrote %s (%d rows)", target, len(df))
        written.append(target)

    if not written:
        raise RuntimeError("ConversionRate stored procedure returned no result sets.")
    return written


def summarize_originated_count_from_proc(start_num: int = 1, end_num: int = 1, days: int = 90) -> dict:
    """
    Return total Originated Count from proc output for the requested range.
    Sums NEW/RTG/RTO originated columns from a totals result set.
    """
    result_sets = fetch_originated_count_result_sets(start_num=start_num, end_num=end_num, days=days)
    if not result_sets:
        raise RuntimeError("ConversionRate procedure returned no result sets.")

    for df in result_sets:
        if df.empty:
            continue
        cols = _originated_cols(df)
        date_col = _find_column(list(df.columns), ["ApplicationDate"])
        if cols and date_col is None:
            total = pd.Series(0.0, index=df.index)
            for c in cols:
                total = total + pd.to_numeric(df[c], errors="coerce").fillna(0.0)
            originated_count = int(float(total.sum()))
            return {"originated_count": originated_count, "source": "originated_count_proc"}

    raise RuntimeError("Could not find originated totals result set in ConversionRate procedure output.")


def backfill_originated_count_daily_from_proc(days: int = 90) -> pd.DataFrame:
    """
    Build daily OriginatedCount history from proc daily result set.
    OriginatedCount is NEW + RTG + RTO originated counts per day.
    """
    result_sets = fetch_originated_count_result_sets(start_num=max(days, 1), end_num=1, days=max(days, 1))
    if not result_sets:
        return pd.DataFrame()

    daily_df: pd.DataFrame | None = None
    for df in result_sets:
        if df.empty:
            continue
        cols = _originated_cols(df)
        date_col = _find_column(list(df.columns), ["ApplicationDate"])
        if cols and date_col is not None:
            daily_df = df.copy()
            break

    if daily_df is None or daily_df.empty:
        return pd.DataFrame()

    date_col = _find_column(list(daily_df.columns), ["ApplicationDate"])
    if date_col is None:
        return pd.DataFrame()

    originated_cols = _originated_cols(daily_df)
    if not originated_cols:
        return pd.DataFrame()

    daily_df["as_of_date"] = pd.to_datetime(daily_df[date_col], errors="coerce").dt.date
    daily_df = daily_df[daily_df["as_of_date"].notna()].copy()
    if daily_df.empty:
        return pd.DataFrame()

    total_series = pd.Series(0.0, index=daily_df.index)
    for c in originated_cols:
        total_series = total_series + pd.to_numeric(daily_df[c], errors="coerce").fillna(0.0)
    daily_df["OriginatedCount"] = total_series

    out = (
        daily_df.groupby("as_of_date", as_index=False)["OriginatedCount"]
        .sum()
        .sort_values("as_of_date")
        .reset_index(drop=True)
    )
    out["as_of_date"] = pd.to_datetime(out["as_of_date"], errors="coerce").dt.date
    today = date.today()
    out = out[out["as_of_date"] < today].copy()
    out["as_of_date"] = out["as_of_date"].astype(str)
    out["OriginatedCount"] = pd.to_numeric(out["OriginatedCount"], errors="coerce").fillna(0).astype(int)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run ConversionRate stored procedure and save result sets (OriginatedCount extraction)."
    )
    parser.add_argument("--start-num", type=int, default=1, help="Range start in days-back for proc totals")
    parser.add_argument("--end-num", type=int, default=1, help="Range end in days-back for proc totals")
    parser.add_argument("--days", type=int, default=90, help="Daily lookback window used by the proc")
    args = parser.parse_args()

    run_originated_count(start_num=args.start_num, end_num=args.end_num, days=args.days)


if __name__ == "__main__":
    main()
