from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Iterable

import pandas as pd

from sql_operations.thresholds import evaluate_thresholds_for_window


def _to_iso_date(value: date | datetime | str) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def make_kpi_row(
    *,
    as_of_date: date | datetime | str,
    section: str,
    metric_key: str,
    metric_label: str,
    value: float | int,
    window_days: int = 7,
    value_type: str = "count",
    source: str = "accept_count_proc",
) -> dict:
    return {
        "as_of_date": _to_iso_date(as_of_date),
        "window_days": int(window_days),
        "section": section,
        "metric_key": metric_key,
        "metric_label": metric_label,
        "value": float(value),
        "value_type": value_type,
        "source": source,
        "refreshed_at": datetime.utcnow().isoformat(timespec="seconds"),
    }


def append_history_rows(history_csv: Path, rows: Iterable[dict]) -> pd.DataFrame:
    history_csv.parent.mkdir(parents=True, exist_ok=True)
    new_df = pd.DataFrame(list(rows))
    if new_df.empty:
        return pd.DataFrame()

    if history_csv.exists():
        old_df = pd.read_csv(history_csv)
        all_df = pd.concat([old_df, new_df], ignore_index=True)
    else:
        all_df = new_df

    # Deduplicate by natural key, keep latest write
    key_cols = ["as_of_date", "window_days", "section", "metric_key"]
    all_df = all_df.drop_duplicates(subset=key_cols, keep="last")
    all_df = all_df.sort_values(by=["as_of_date", "section", "metric_key", "window_days"]).reset_index(drop=True)
    all_df.to_csv(history_csv, index=False)
    return all_df


def build_serving_snapshot(history_df: pd.DataFrame, output_csv: Path) -> pd.DataFrame:
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    if history_df.empty:
        out = pd.DataFrame(
            columns=["as_of_date", "window_days", "section", "metric_key", "metric_label", "value", "value_type", "source"]
        )
        out.to_csv(output_csv, index=False)
        return out

    # Keep latest as_of_date per (window_days, section, metric_key)
    history_df = history_df.copy()
    history_df["as_of_date_dt"] = pd.to_datetime(history_df["as_of_date"], errors="coerce")
    idx = history_df.groupby(["window_days", "section", "metric_key"])["as_of_date_dt"].idxmax()
    out = history_df.loc[idx].drop(columns=["as_of_date_dt"]).reset_index(drop=True)
    out.to_csv(output_csv, index=False)
    return out


def build_windowed_serving_snapshot(
    history_df: pd.DataFrame,
    output_csv: Path,
    windows: tuple[int, ...] = (7, 30, 60),
) -> pd.DataFrame:
    """
    Build dashboard-facing snapshot rows for rolling windows from daily history.
    Expected history grain: window_days = 1.
    """
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    if history_df.empty:
        out = pd.DataFrame(
            columns=[
                "as_of_date",
                "window_days",
                "section",
                "metric_key",
                "metric_label",
                "value",
                "value_type",
                "source",
                "status",
                "lower_threshold",
                "upper_threshold",
                "pct_change",
                "seasonal_zscore",
                "signal_count",
                "signals",
                "rolling_points_used",
                "seasonal_points_used",
                "weekday_filter_applied",
                "refreshed_at",
            ]
        )
        out.to_csv(output_csv, index=False)
        return out

    df = history_df.copy()
    df["as_of_date"] = pd.to_datetime(df["as_of_date"], errors="coerce")
    df = df[df["as_of_date"].notna()]
    df = df[df["window_days"] == 1]
    if df.empty:
        out = pd.DataFrame(
            columns=[
                "as_of_date",
                "window_days",
                "section",
                "metric_key",
                "metric_label",
                "value",
                "value_type",
                "source",
                "status",
                "lower_threshold",
                "upper_threshold",
                "pct_change",
                "seasonal_zscore",
                "signal_count",
                "signals",
                "rolling_points_used",
                "seasonal_points_used",
                "weekday_filter_applied",
                "refreshed_at",
            ]
        )
        out.to_csv(output_csv, index=False)
        return out

    out_rows: list[dict] = []
    group_cols = ["section", "metric_key"]
    for (_, _), g in df.groupby(group_cols):
        g = g.sort_values("as_of_date").reset_index(drop=True)
        latest = g.iloc[-1]
        latest_date = latest["as_of_date"]
        value_type = str(latest.get("value_type", "count")).lower()

        for window in windows:
            start_date = latest_date - pd.Timedelta(days=window - 1)
            subset = g[g["as_of_date"] >= start_date]
            if subset.empty:
                continue

            if value_type == "count":
                value = float(pd.to_numeric(subset["value"], errors="coerce").fillna(0).sum())
            else:
                value = float(pd.to_numeric(subset["value"], errors="coerce").dropna().mean())

            threshold_result = evaluate_thresholds_for_window(
                daily_df=g[["as_of_date", "value"]],
                metric_key=str(latest["metric_key"]),
                value_type=value_type,
                window_days=int(window),
            )

            out_rows.append(
                {
                    "as_of_date": latest_date.date().isoformat(),
                    "window_days": int(window),
                    "section": latest["section"],
                    "metric_key": latest["metric_key"],
                    "metric_label": latest["metric_label"],
                    "value": value,
                    "value_type": latest.get("value_type", "count"),
                    "source": "window_rollup_from_history",
                    "status": threshold_result.status,
                    "lower_threshold": threshold_result.lower_threshold,
                    "upper_threshold": threshold_result.upper_threshold,
                    "pct_change": threshold_result.pct_change,
                    "seasonal_zscore": threshold_result.seasonal_zscore,
                    "signal_count": threshold_result.signal_count,
                    "signals": threshold_result.signals,
                    "rolling_points_used": threshold_result.rolling_points_used,
                    "seasonal_points_used": threshold_result.seasonal_points_used,
                    "weekday_filter_applied": threshold_result.weekday_filter_applied,
                    "refreshed_at": datetime.utcnow().isoformat(timespec="seconds"),
                }
            )

    out = pd.DataFrame(out_rows)
    if out.empty:
        out = pd.DataFrame(
            columns=[
                "as_of_date",
                "window_days",
                "section",
                "metric_key",
                "metric_label",
                "value",
                "value_type",
                "source",
                "status",
                "lower_threshold",
                "upper_threshold",
                "pct_change",
                "seasonal_zscore",
                "signal_count",
                "signals",
                "rolling_points_used",
                "seasonal_points_used",
                "weekday_filter_applied",
                "refreshed_at",
            ]
        )
    else:
        out = out.sort_values(by=["section", "metric_key", "window_days"]).reset_index(drop=True)
    out.to_csv(output_csv, index=False)
    return out


def apply_history_retention(
    history_df: pd.DataFrame,
    history_csv: Path,
    *,
    retention_days: int,
    archive_dir: Path | None = None,
) -> pd.DataFrame:
    """
    Keep only recent rows in active history and archive older rows by month.
    """
    if history_df.empty or retention_days <= 0:
        return history_df

    df = history_df.copy()
    df["as_of_date_dt"] = pd.to_datetime(df["as_of_date"], errors="coerce")
    df = df[df["as_of_date_dt"].notna()].copy()
    if df.empty:
        return history_df

    max_date = df["as_of_date_dt"].max()
    cutoff = max_date - pd.Timedelta(days=retention_days - 1)
    keep_df = df[df["as_of_date_dt"] >= cutoff].copy()
    archive_df = df[df["as_of_date_dt"] < cutoff].copy()

    # Archive by month if requested
    if archive_dir is not None and not archive_df.empty:
        archive_dir.mkdir(parents=True, exist_ok=True)
        archive_df["year_month"] = archive_df["as_of_date_dt"].dt.strftime("%Y_%m")
        for ym, group in archive_df.groupby("year_month"):
            target = archive_dir / f"kpi_history_{ym}.csv"
            out_group = group.drop(columns=["year_month"])
            if target.exists():
                old = pd.read_csv(target)
                merged = pd.concat([old, out_group.drop(columns=["as_of_date_dt"])], ignore_index=True)
                merged = merged.drop_duplicates(
                    subset=["as_of_date", "window_days", "section", "metric_key"], keep="last"
                )
            else:
                merged = out_group.drop(columns=["as_of_date_dt"])
            merged = merged.sort_values(by=["as_of_date", "section", "metric_key", "window_days"]).reset_index(drop=True)
            merged.to_csv(target, index=False)

    keep_out = keep_df.drop(columns=["as_of_date_dt"]).copy()
    keep_out = keep_out.sort_values(by=["as_of_date", "section", "metric_key", "window_days"]).reset_index(drop=True)
    history_csv.parent.mkdir(parents=True, exist_ok=True)
    keep_out.to_csv(history_csv, index=False)
    return keep_out

