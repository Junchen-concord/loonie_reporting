from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from sql_operations.accept_count_operation import (
    backfill_accept_count_daily_from_db,
    run_accept_count,
    summarize_accept_count_from_proc,
)
from sql_operations.originated_count_operation import (
    backfill_originated_count_daily_from_proc,
    run_originated_count,
    summarize_originated_count_from_proc,
)
from sql_operations.normalize import (
    append_history_rows,
    apply_history_retention,
    build_windowed_serving_snapshot,
    make_kpi_row,
)
from scripts.logging_utils import setup_logger

LOGGER = setup_logger(__name__, "run_all")

REFRESH_DIR = REPO_ROOT / "data" / "refresh"


def _build_accept_count_rows(
    *,
    backfill_days: int,
    date_range: int,
    time_range: int,
) -> list[dict]:
    rows: list[dict] = []

    if backfill_days > 0:
        backfill_df = backfill_accept_count_daily_from_db(days=backfill_days)
        for _, row in backfill_df.iterrows():
            rows.append(
                make_kpi_row(
                    as_of_date=row["as_of_date"],
                    section="sales",
                    metric_key="AcceptCount",
                    metric_label="Accept Count",
                    value=int(row["AcceptCount"]),
                    window_days=1,
                    value_type="count",
                    source="accept_count_backfill_db",
                )
            )
        LOGGER.info("Prepared %d backfill rows from DB.", len(backfill_df))

    proc_summary = summarize_accept_count_from_proc(date_range=date_range, time_range=time_range)
    proc_as_of = date.today() - timedelta(days=1)
    rows.append(
        make_kpi_row(
            as_of_date=proc_as_of,
            section="sales",
            metric_key="AcceptCount",
            metric_label="Accept Count",
            value=proc_summary["accept_count"],
            window_days=1,
            value_type="count",
            source=proc_summary["source"],
        )
    )
    LOGGER.info("Prepared current Accept Count snapshot for %s.", proc_as_of.isoformat())
    return rows


def _build_originated_count_rows(
    *,
    backfill_days: int,
    start_num: int,
    end_num: int,
    days: int,
) -> list[dict]:
    rows: list[dict] = []

    if backfill_days > 0:
        backfill_df = backfill_originated_count_daily_from_proc(days=backfill_days)
        for _, row in backfill_df.iterrows():
            rows.append(
                make_kpi_row(
                    as_of_date=row["as_of_date"],
                    section="sales",
                    metric_key="OriginatedCount",
                    metric_label="Originated Count",
                    value=int(row["OriginatedCount"]),
                    window_days=1,
                    value_type="count",
                    source="originated_count_backfill_proc",
                )
            )
        LOGGER.info("Prepared %d Originated Count backfill rows from proc.", len(backfill_df))

    proc_summary = summarize_originated_count_from_proc(start_num=start_num, end_num=end_num, days=days)
    proc_as_of = date.today() - timedelta(days=1)
    rows.append(
        make_kpi_row(
            as_of_date=proc_as_of,
            section="sales",
            metric_key="OriginatedCount",
            metric_label="Originated Count",
            value=proc_summary["originated_count"],
            window_days=1,
            value_type="count",
            source=proc_summary["source"],
        )
    )
    LOGGER.info("Prepared current Originated Count snapshot for %s.", proc_as_of.isoformat())
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Orchestrate KPI refresh (Accept Count first vertical slice)."
    )
    parser.add_argument("--backfill-days", type=int, default=90, help="Historical days to backfill from DB")
    parser.add_argument("--date-range", type=int, default=3, help="Stored proc active-provider lookback in days")
    parser.add_argument("--time-range", type=int, default=4, help="Stored proc accepted-count lookback in hours")
    parser.add_argument(
        "--originated-start-num",
        type=int,
        default=1,
        help="Conversion proc range start (days-back) for OriginatedCount snapshot",
    )
    parser.add_argument(
        "--originated-end-num",
        type=int,
        default=1,
        help="Conversion proc range end (days-back) for OriginatedCount snapshot",
    )
    parser.add_argument(
        "--originated-days",
        type=int,
        default=90,
        help="Conversion proc daily lookback parameter for OriginatedCount extraction",
    )
    parser.add_argument(
        "--history-output",
        type=Path,
        default=REFRESH_DIR / "kpi_history.csv",
        help="Path to normalized KPI history CSV",
    )
    parser.add_argument(
        "--serving-output",
        type=Path,
        default=REFRESH_DIR / "kpi_serving_metrics.csv",
        help="Path to normalized serving KPI snapshot CSV",
    )
    parser.add_argument(
        "--windows",
        type=str,
        default="1,7,30,60",
        help="Comma-separated rolling windows for serving snapshot (e.g. 1,7,30,60)",
    )
    parser.add_argument(
        "--history-retention-days",
        type=int,
        default=730,
        help="Days to keep in active kpi_history.csv (older rows archived)",
    )
    parser.add_argument(
        "--history-archive-dir",
        type=Path,
        default=REPO_ROOT / "data" / "archive",
        help="Directory for monthly archived history CSV files",
    )
    args = parser.parse_args()
    windows = tuple(int(x.strip()) for x in args.windows.split(",") if x.strip())

    # Step 1: Keep raw procedure outputs for traceability/debugging.
    raw_accept_files = run_accept_count(date_range=args.date_range, time_range=args.time_range)
    LOGGER.info("Wrote %d raw Accept Count output files.", len(raw_accept_files))
    raw_originated_files = run_originated_count(
        start_num=max(args.originated_start_num, 0),
        end_num=max(args.originated_end_num, 0),
        days=max(args.originated_days, 0),
    )
    LOGGER.info("Wrote %d raw Originated Count output files.", len(raw_originated_files))

    # Step 2: Normalize and store history/snapshot.
    rows = _build_accept_count_rows(
        backfill_days=max(args.backfill_days, 0),
        date_range=args.date_range,
        time_range=args.time_range,
    )
    rows.extend(
        _build_originated_count_rows(
            backfill_days=max(args.backfill_days, 0),
            start_num=max(args.originated_start_num, 0),
            end_num=max(args.originated_end_num, 0),
            days=max(args.originated_days, 0),
        )
    )
    history_df = append_history_rows(args.history_output, rows)
    history_df = apply_history_retention(
        history_df,
        args.history_output,
        retention_days=max(args.history_retention_days, 0),
        archive_dir=args.history_archive_dir,
    )
    serving_df = build_windowed_serving_snapshot(history_df, args.serving_output, windows=windows)

    LOGGER.info(
        "History rows (retained=%d, retention_days=%d) -> %s",
        len(history_df),
        max(args.history_retention_days, 0),
        args.history_output,
    )
    LOGGER.info("Serving rows: %d -> %s (windows=%s)", len(serving_df), args.serving_output, windows)


if __name__ == "__main__":
    main()

