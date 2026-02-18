from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from scripts.controller import get_thresholds, threshold_mode


@dataclass
class ThresholdResult:
    status: str
    lower_threshold: float | None
    upper_threshold: float | None
    pct_change: float | None
    seasonal_zscore: float | None
    signal_count: int
    signals: str
    rolling_points_used: int
    seasonal_points_used: int
    weekday_filter_applied: bool


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        val = float(value)
        if pd.isna(val):
            return None
        return val
    except Exception:
        return None


def _direction_allows(direction: str, signal: str) -> bool:
    d = (direction or "both").strip().lower()
    if d == "both":
        return signal in {"L", "U"}
    if d == "lower_only":
        return signal == "L"
    if d == "upper_only":
        return signal == "U"
    return signal in {"L", "U"}


def _status_from_signal_count(signal_count: int, policy: dict[str, Any] | None) -> str:
    cfg = policy or {}
    yellow_at = int(cfg.get("yellow_if_signal_count_gte", 1))
    red_at = int(cfg.get("red_if_signal_count_gte", 2))
    if signal_count >= red_at:
        return "Red"
    if signal_count >= yellow_at:
        return "Yellow"
    return "Green"


def _parse_excluded_weekdays(raw: Any) -> set[int]:
    """
    Parse weekday exclusions from config.
    Supports: ["sun"], ["mon", "sun"], [0, 6], mixed forms.
    """
    if not isinstance(raw, list):
        return set()
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
    out: set[int] = set()
    for item in raw:
        if isinstance(item, int) and 0 <= item <= 6:
            out.add(item)
            continue
        key = str(item).strip().lower()
        if key in mapping:
            out.add(mapping[key])
    return out


def evaluate_thresholds_for_window(
    *,
    daily_df: pd.DataFrame,
    metric_key: str,
    value_type: str,
    window_days: int,
) -> ThresholdResult:
    """
    Evaluate threshold status on the latest date for a metric/window.
    daily_df must contain: as_of_date, value (daily grain).
    """
    if daily_df.empty:
        return ThresholdResult("Yellow", None, None, None, None, 0, "", 0, 0, False)

    df = daily_df.copy()
    df["as_of_date"] = pd.to_datetime(df["as_of_date"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df[df["as_of_date"].notna() & df["value"].notna()].sort_values("as_of_date")
    if df.empty:
        return ThresholdResult("Yellow", None, None, None, None, 0, "", 0, 0, False)

    thresholds = get_thresholds(metric_key)
    mode = threshold_mode(metric_key, default="static").lower()
    dyn_cfg = (thresholds.get("dynamic") or {}) if isinstance(thresholds, dict) else {}

    # Calendar safeguard: remove excluded weekdays from model-calculation series.
    excluded_weekdays = _parse_excluded_weekdays(dyn_cfg.get("exclude_weekdays"))
    weekday_filter_applied = bool(excluded_weekdays)
    if excluded_weekdays:
        df = df[~df["as_of_date"].dt.dayofweek.isin(excluded_weekdays)]
        if df.empty:
            return ThresholdResult("Yellow", None, None, None, None, 0, "", 0, 0, True)

    # Build x_t series for selected window (after weekday exclusion, if configured).
    if str(value_type).lower() == "count":
        x_series = df["value"].rolling(window=window_days, min_periods=1).sum()
    else:
        x_series = df["value"].rolling(window=window_days, min_periods=1).mean()
    x_series.index = df["as_of_date"]

    x_t = _safe_float(x_series.iloc[-1])
    x_prev = _safe_float(x_series.iloc[-2]) if len(x_series) >= 2 else None
    pct_change = None
    if x_t is not None and x_prev not in (None, 0):
        pct_change = (x_t - x_prev) / x_prev

    lower_threshold: float | None = None
    upper_threshold: float | None = None
    seasonal_zscore: float | None = None
    active_signals: list[str] = []
    rolling_points_used = 0
    seasonal_points_used = 0

    direction = "both"
    policy = thresholds.get("policy") if isinstance(thresholds, dict) else None
    signals_enabled = {"L", "U", "Z", "P"}

    if mode == "dynamic":
        dyn = (thresholds.get("dynamic") or {}) if isinstance(thresholds, dict) else {}
        direction = str(dyn.get("direction", "both"))
        if isinstance(dyn.get("signals_enabled"), list):
            signals_enabled = {str(s).strip().upper() for s in dyn["signals_enabled"]}

        k = _safe_float(dyn.get("k")) or 1.0
        rolling_window = int(dyn.get("window", 30))
        z_score_lim = _safe_float(dyn.get("z_score_lim")) or 2.0
        percent_drop = _safe_float(dyn.get("percent_drop")) or 0.5
        min_history_points = int(dyn.get("min_history_points", max(rolling_window, 10)))
        min_seasonal_points = int(dyn.get("min_seasonal_points", 5))

        if len(x_series) >= min_history_points:
            r_mean = x_series.rolling(window=rolling_window, min_periods=rolling_window).mean().iloc[-1]
            r_std = x_series.rolling(window=rolling_window, min_periods=rolling_window).std(ddof=0).iloc[-1]
            r_mean_f = _safe_float(r_mean)
            r_std_f = _safe_float(r_std)
            if r_mean_f is not None and r_std_f is not None:
                lower_threshold = r_mean_f - (k * r_std_f)
                upper_threshold = r_mean_f + (k * r_std_f)
                rolling_points_used = min(len(x_series), rolling_window)

        # Seasonal z-score by month-day, using prior years if available.
        latest_date = x_series.index[-1]
        md = latest_date.strftime("%m-%d")
        seasonal_hist = x_series[x_series.index.strftime("%m-%d") == md]
        seasonal_hist = seasonal_hist[seasonal_hist.index < latest_date]
        seasonal_points_used = int(len(seasonal_hist))
        if len(seasonal_hist) >= min_seasonal_points:
            s_mean = _safe_float(seasonal_hist.mean())
            s_std = _safe_float(seasonal_hist.std(ddof=0))
            if s_mean is not None and s_std not in (None, 0):
                seasonal_zscore = (x_t - s_mean) / s_std if x_t is not None else None

        if "L" in signals_enabled and _direction_allows(direction, "L") and lower_threshold is not None and x_t is not None:
            if x_t <= lower_threshold:
                active_signals.append("L")
        if "U" in signals_enabled and _direction_allows(direction, "U") and upper_threshold is not None and x_t is not None:
            if x_t >= upper_threshold:
                active_signals.append("U")
        if "Z" in signals_enabled and seasonal_zscore is not None and abs(seasonal_zscore) >= z_score_lim:
            active_signals.append("Z")
        if "P" in signals_enabled and pct_change is not None and abs(pct_change) >= percent_drop:
            active_signals.append("P")

        status = _status_from_signal_count(len(active_signals), policy)
        return ThresholdResult(
            status=status,
            lower_threshold=lower_threshold,
            upper_threshold=upper_threshold,
            pct_change=pct_change,
            seasonal_zscore=seasonal_zscore,
            signal_count=len(active_signals),
            signals="|".join(active_signals),
            rolling_points_used=rolling_points_used,
            seasonal_points_used=seasonal_points_used,
            weekday_filter_applied=weekday_filter_applied,
        )

    # Static mode fallback.
    static_cfg = (thresholds.get("static") or {}) if isinstance(thresholds, dict) else {}
    direction = str(static_cfg.get("direction", "both"))
    lower_threshold = _safe_float(static_cfg.get("lower_threshold"))
    upper_threshold = _safe_float(static_cfg.get("upper_threshold"))

    if _direction_allows(direction, "L") and lower_threshold is not None and x_t is not None and x_t <= lower_threshold:
        active_signals.append("L")
    if _direction_allows(direction, "U") and upper_threshold is not None and x_t is not None and x_t >= upper_threshold:
        active_signals.append("U")

    # If no policy is provided for static mode, use a strict policy.
    static_policy = policy or {"yellow_if_signal_count_gte": 1, "red_if_signal_count_gte": 1}
    status = _status_from_signal_count(len(active_signals), static_policy)
    return ThresholdResult(
        status=status,
        lower_threshold=lower_threshold,
        upper_threshold=upper_threshold,
        pct_change=pct_change,
        seasonal_zscore=None,
        signal_count=len(active_signals),
        signals="|".join(active_signals),
        rolling_points_used=0,
        seasonal_points_used=0,
        weekday_filter_applied=weekday_filter_applied,
    )

