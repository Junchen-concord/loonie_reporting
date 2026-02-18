from __future__ import annotations

"""
KPIs Alert Dashboard (single entrypoint).

Usage:
  streamlit run python/dashboard_kpis.py
"""

from pathlib import Path

import pandas as pd
import streamlit as st

# Streamlit requires set_page_config to be the first Streamlit call.
# In some hot-reload paths, it may already be set; avoid crashing.
try:
    st.set_page_config(page_title="KPIs Alert Dashboard", layout="wide")
except Exception:
    pass

BASE_DIR = Path(__file__).resolve().parents[1]


def _alert_icon(alert: str) -> str:
    a = (alert or "").strip().lower()
    if a == "green":
        return "🟢"
    if a == "yellow":
        return "🟡"
    if a == "red":
        return "🔴"
    return "⚪"


def _alert_rank(alert: str) -> int:
    """Lower is worse (used for sorting)."""
    a = (alert or "").strip().lower()
    if a == "red":
        return 0
    if a == "yellow":
        return 1
    if a == "green":
        return 2
    return 99


def _badge_html(alert: str) -> str:
    a = (alert or "").strip().lower()
    if a == "red":
        cls = "badge badge-red"
        label = "Red"
    elif a == "yellow":
        cls = "badge badge-yellow"
        label = "Yellow"
    elif a == "green":
        cls = "badge badge-green"
        label = "Green"
    else:
        cls = "badge"
        label = "Unknown"
    return f'<span class="{cls}">{_alert_icon(alert)}&nbsp;{label}</span>'


def _status_html(alert: str | None) -> str:
    if not alert:
        return '<span class="kpi-muted">—</span>'
    return _badge_html(alert)


def _metric_html(
    metric: str,
    indent_level: int = 0,
    is_category: bool = False,
    is_headline: bool = False,
) -> str:
    indent = "&nbsp;" * (4 * indent_level)
    if is_headline or is_category:
        cls = "kpi-headline"
    else:
        cls = "kpi-submetric"
    return f'<span class="{cls}">{indent}{_escape_html(metric)}</span>'


def _escape_html(text: str) -> str:
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def _link_html(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return '<span class="kpi-muted">—</span>'
    safe = _escape_html(u)
    return (
        f'<a href="{safe}" target="_blank" rel="noopener noreferrer" '
        f'title="Open in a new tab">Open&nbsp;<span aria-hidden="true">↗</span></a>'
    )


def _sample_metrics() -> pd.DataFrame:
    # Showcase-only data. Replace with real computed KPIs later.
    rows = [
        {
            "Group": "Sales",
            "Metric": "Apps through the door",
            "Value": "54",
            "Alert": "Green",
            "Link": None,
        },
        {
            "Group": "Sales",
            "Metric": "Accept Count",
            "Value": "18",
            "Alert": "Green",
            "Link": None,
        },
        {
            "Group": "Sales",
            "Metric": "Accept Rate",
            "Value": "33.33%",
            "Alert": "Green",
            "Link": None,
        },
        {
            "Group": "Sales",
            "Metric": "Originated Count",
            "Value": "0",
            "Alert": "Red",
            "Link": None,
        },
        {
            "Group": "Performance",
            "Metric": "ACH Return Rate",
            "Value": "17.89%",
            "Alert": "Green",
            "Link": "https://reports.speedyloan.com/single/?appid=f9a3184e-f3e8-4fab-94fa-373a0f869114&obj=36bfd39d-16a2-4b83-88a1-0a5fdb7f7f72&theme=sense&bookmark=06cc43dc-cb1b-4466-b0a3-d60246ef27cf&opt=ctxmenu,currsel",
        },
        {
            "Group": "Performance",
            "Metric": "FPDFA / AA%",
            "Value": "18.56%",
            "Alert": "Yellow",
            "Link": None,
        },
        {
            "Group": "Performance",
            "Metric": "Payin",
            "Value": "0.95",
            "Alert": "Red",
            "Link": None,
        },
    ]
    df = pd.DataFrame(rows)
    df.insert(3, "Indicator", df["Alert"].map(_alert_icon))
    return df


def _normalize_kpi_feed(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize KPI feed coming from refresh scripts so the UI uses consistent
    executive-facing labels.
    """
    out = df.copy()

    # Normalize metric labels (primarily Sales group, based on your MVP ask).
    rename_metric = {
        "Seen": "Apps through the door",
        "Accepted": "Accept Count",
    }
    if "Metric" in out.columns:
        out["Metric"] = out["Metric"].replace(rename_metric)

    return out


def _load_metrics_from_refresh_csv() -> pd.DataFrame | None:
    """
    Prefer a refreshed KPI feed if present (e.g., written by cron / refresh script).

    Expected location: data/refresh/kpi_metrics.csv
    Expected columns (minimum): GroupName|Group, Metric, Value, Alert, Link
    """
    path = BASE_DIR / "data" / "refresh" / "kpi_metrics.csv"
    if not path.exists():
        return None

    df = pd.read_csv(path)

    # Normalize naming to match the dashboard schema
    if "GroupName" in df.columns and "Group" not in df.columns:
        df = df.rename(columns={"GroupName": "Group"})

    required = {"Group", "Metric", "Value", "Alert", "Link"}
    missing = required - set(df.columns)
    if missing:
        # If refresh output is incompatible, fall back to sample rather than crashing the UI.
        return None

    # Ensure the dashboard's indicator column exists
    if "Indicator" not in df.columns:
        df.insert(3, "Indicator", df["Alert"].map(_alert_icon))

    return _normalize_kpi_feed(df)


def _load_aggregated_metrics_from_refresh_csv() -> pd.Series | None:
    """
    Load aggregated KPI metrics (single-row table) from kpi_metrics.csv.
    """
    path = BASE_DIR / "data" / "refresh" / "kpi_metrics.csv"
    if not path.exists():
        return None
    df = pd.read_csv(path)
    if df.empty:
        return None
    return df.iloc[0]


def _load_daily_metrics_from_refresh_csv() -> pd.DataFrame | None:
    path = BASE_DIR / "data" / "refresh" / "kpi_daily_metrics.csv"
    if not path.exists():
        return None
    df = pd.read_csv(path)
    if "ActivityDate" in df.columns:
        df["ActivityDate"] = pd.to_datetime(df["ActivityDate"], errors="coerce")
        df = df.sort_values(by="ActivityDate")
    return df


def _aggregate_legacy_daily_for_ui(daily_df: pd.DataFrame, exclude_sunday_for_averages: bool = True) -> pd.Series | None:
    """
    UI-side legacy aggregation from daily rows.
    Mirrors refresh_kpis aggregation behavior for display control in Legacy mode.
    """
    if daily_df is None or daily_df.empty:
        return None

    df = daily_df.copy()
    sums = ["Seen", "Scored", "Accepted", "Originated", "Bids", "LoansFunded"]
    avgs = ["BidRate", "WinRate", "ScoringRate", "AcceptRate", "ConvRate", "ScoringCost", "BidCost"]
    out: dict[str, float | str | int] = {}
    avg_df = df.copy()

    if "ActivityDate" in df.columns:
        dates = pd.to_datetime(df["ActivityDate"], errors="coerce").dropna()
        if not dates.empty:
            out["ActivityStart"] = dates.min().date().isoformat()
            out["ActivityEnd"] = dates.max().date().isoformat()
            out["Days"] = int(dates.nunique())
        if exclude_sunday_for_averages:
            avg_df["ActivityDate"] = pd.to_datetime(avg_df["ActivityDate"], errors="coerce")
            avg_df = avg_df[avg_df["ActivityDate"].notna()]
            avg_df = avg_df[avg_df["ActivityDate"].dt.dayofweek != 6]
            if avg_df.empty:
                avg_df = df.copy()

    for col in sums:
        if col in df.columns:
            out[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).sum()
    for col in avgs:
        if col in avg_df.columns:
            out[col] = pd.to_numeric(avg_df[col], errors="coerce").mean()

    if not out:
        return None
    return pd.Series(out)


def _load_accept_count_serving(window_days: int = 7) -> dict | None:
    """
    Load AcceptCount from windowed serving snapshot.
    Returns {"value": <float>, "status": <str>} or None.
    """
    path = BASE_DIR / "data" / "refresh" / "kpi_serving_metrics.csv"
    if not path.exists():
        return None

    df = pd.read_csv(path)
    if df.empty:
        return None
    required = {"metric_key", "window_days", "as_of_date", "value", "status"}
    if not required.issubset(df.columns):
        return None

    out = df.loc[df["metric_key"].astype(str) == "AcceptCount"].copy()
    if out.empty:
        return None
    out["window_days"] = pd.to_numeric(out["window_days"], errors="coerce")
    out = out.loc[out["window_days"] == window_days]
    if out.empty:
        return None

    out["as_of_date_dt"] = pd.to_datetime(out["as_of_date"], errors="coerce")
    out = out.loc[out["as_of_date_dt"].notna()].sort_values("as_of_date_dt")
    if out.empty:
        return None

    row = out.iloc[-1]
    value = _parse_numeric(row.get("value"))
    status = str(row.get("status") or "").strip().title()
    if value is None or status not in {"Red", "Yellow", "Green"}:
        return None
    return {"value": value, "status": status}


def _load_accept_count_history() -> dict | None:
    """
    Load latest daily AcceptCount from normalized history.
    Returns {"value": <float>, "status": <str>} or None.
    Status is inferred from serving status when available; fallback Yellow.
    """
    path = BASE_DIR / "data" / "refresh" / "kpi_history.csv"
    if not path.exists():
        return None
    df = pd.read_csv(path)
    if df.empty:
        return None
    required = {"metric_key", "window_days", "as_of_date", "value"}
    if not required.issubset(df.columns):
        return None

    out = df.loc[df["metric_key"].astype(str) == "AcceptCount"].copy()
    if out.empty:
        return None
    out["window_days"] = pd.to_numeric(out["window_days"], errors="coerce")
    out = out.loc[out["window_days"] == 1]
    if out.empty:
        return None
    out["as_of_date_dt"] = pd.to_datetime(out["as_of_date"], errors="coerce")
    out = out.loc[out["as_of_date_dt"].notna()].sort_values("as_of_date_dt")
    if out.empty:
        return None

    row = out.iloc[-1]
    value = _parse_numeric(row.get("value"))
    if value is None:
        return None

    # Keep color semantics from thresholded serving output when possible.
    serving = _load_accept_count_serving(window_days=7)
    status = str((serving or {}).get("status") or "Yellow")
    return {"value": value, "status": status}


def _resolve_accept_count_override(mode: str) -> dict | None:
    m = (mode or "").strip().lower()
    if m == "legacy":
        return None
    if m == "history":
        return _load_accept_count_history()
    # default to serving
    return _load_accept_count_serving(window_days=7)


def _apply_accept_count_override(rows: list[dict], accept_serving: dict | None) -> list[dict]:
    """
    Override only Accept Count row from serving snapshot.
    """
    if not accept_serving:
        return rows
    for row in rows:
        if str(row.get("Metric", "")).strip() == "Accept Count":
            row["Value"] = _format_count(accept_serving.get("value"))
            row["Alert"] = str(accept_serving.get("status") or row.get("Alert") or "Yellow")
            break
    return rows


def _load_compare_totals() -> dict[str, float]:
    """
    Load benchmark totals from kpi_compare.xlsx. Returns {metric_name: total_value}.
    Expects a first column of metric names and a column named 'Totals'.
    """
    path = BASE_DIR / "data" / "refresh" / "kpi_compare.xlsx"
    if not path.exists():
        return {}
    df = pd.read_excel(path)
    if df.empty:
        return {}
    metric_col = df.columns[0]
    if "Totals" not in df.columns:
        return {}
    totals = {}
    for _, row in df.iterrows():
        name = str(row.get(metric_col, "")).strip()
        totals[name] = _parse_numeric(row.get("Totals"))
    return totals


def _load_compare_averages() -> dict[str, float]:
    """
    Compute benchmark averages from kpi_compare.xlsx.
    Returns {metric_name: avg_value} across week columns (excludes 'Totals').
    """
    path = BASE_DIR / "data" / "refresh" / "kpi_compare.xlsx"
    if not path.exists():
        return {}
    df = pd.read_excel(path)
    if df.empty:
        return {}
    metric_col = df.columns[0]
    value_cols = [c for c in df.columns if c not in {metric_col, "Totals"}]
    averages = {}
    for _, row in df.iterrows():
        name = str(row.get(metric_col, "")).strip()
        values = []
        for col in value_cols:
            val = _parse_numeric(row.get(col))
            if val is not None:
                values.append(val)
        averages[name] = sum(values) / len(values) if values else None

    # Persist for inspection
    out_df = pd.DataFrame(
        [
            {"Metric": k, "Average": v}
            for k, v in averages.items()
        ]
    )
    out_path = BASE_DIR / "data" / "refresh" / "kpi_compare_averages.csv"
    out_df.to_csv(out_path, index=False)
    return averages


def _parse_numeric(value) -> float | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "")
    if text.endswith("%"):
        try:
            return float(text[:-1]) / 100.0
        except Exception:
            return None
    try:
        return float(text)
    except Exception:
        return None


def _compare_alert(metric_key: str, current_value, totals: dict[str, float], averages: dict[str, float]) -> str:
    """
    Compare current value to Averages benchmark.
    Rule (per request): if Average >= current → Green, else Red.
    Missing data → Yellow.
    """
    avg_value = averages.get(metric_key)
    current_num = _parse_numeric(current_value)
    if avg_value is None or current_num is None:
        return "Yellow"
    return "Green" if avg_value >= current_num else "Red"


def _format_count(value) -> str:
    try:
        return f"{int(value):,}"
    except Exception:
        return "—"


def _format_rate(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "—"
    try:
        return f"{float(value) * 100.0:.2f}%"
    except Exception:
        return str(value)


def _alert_for_count(value) -> str:
    try:
        return "Green" if float(value) > 0 else "Red"
    except Exception:
        return "Yellow"


def _build_sales_kpis(
    kpi_feed: pd.DataFrame | None,
    daily_df: pd.DataFrame | None,
    agg_row: pd.Series | None,
    totals: dict[str, float],
    averages: dict[str, float],
    accept_serving: dict | None = None,
) -> pd.DataFrame:
    # Prefer aggregated metrics (kpi_metrics.csv). Fall back to daily_df, then sample.
    if agg_row is not None:
        rows = [
            {
                "Group": "Sales",
                "Metric": "Apps through the door",
                "Value": _format_count(agg_row.get("Seen")),
                "Alert": _compare_alert("Seen", agg_row.get("Seen"), totals, averages),
                "Link": None,
                "Indent": 0,
                "IsHeadline": True,
            },
            {
                "Group": "Sales",
                "Metric": "Score",
                "Value": "",
                "Alert": None,
                "Link": None,
                "Indent": 0,
                "IsCategory": True,
            },
            {
                "Group": "Sales",
                "Metric": "Scoring Count",
                "Value": _format_count(agg_row.get("Scored")),
                "Alert": _compare_alert("Entered Scored", agg_row.get("Scored"), totals, averages),
                "Link": None,
                "Indent": 1,
            },
            {
                "Group": "Sales",
                "Metric": "Scoring Rate",
                "Value": _format_rate(agg_row.get("ScoringRate")),
                "Alert": _compare_alert("ScoringRate", agg_row.get("ScoringRate"), totals, averages),
                "Link": None,
                "Indent": 1,
            },
            {
                "Group": "Sales",
                "Metric": "Bid",
                "Value": "",
                "Alert": None,
                "Link": None,
                "Indent": 0,
                "IsCategory": True,
            },
            {
                "Group": "Sales",
                "Metric": "Bid Count",
                "Value": _format_count(agg_row.get("Bids")),
                "Alert": _compare_alert("Bids", agg_row.get("Bids"), totals, averages),
                "Link": None,
                "Indent": 1,
            },
            {
                "Group": "Sales",
                "Metric": "Bidding Rate",
                "Value": _format_rate(agg_row.get("BidRate")),
                "Alert": _compare_alert("BidRate", agg_row.get("BidRate"), totals, averages),
                "Link": None,
                "Indent": 1,
            },
            {
                "Group": "Sales",
                "Metric": "Accept",
                "Value": "",
                "Alert": None,
                "Link": None,
                "Indent": 0,
                "IsCategory": True,
            },
            {
                "Group": "Sales",
                "Metric": "Accept Count",
                "Value": _format_count(agg_row.get("Accepted")),
                "Alert": "Yellow",
                "Link": None,
                "Indent": 1,
            },
            {
                "Group": "Sales",
                "Metric": "Accept Rate",
                "Value": _format_rate(agg_row.get("AcceptRate")),
                "Alert": "Yellow",
                "Link": None,
                "Indent": 1,
            },
            {
                "Group": "Sales",
                "Metric": "Origination",
                "Value": "",
                "Alert": None,
                "Link": None,
                "Indent": 0,
                "IsCategory": True,
            },
            {
                "Group": "Sales",
                "Metric": "Originated Count",
                "Value": _format_count(agg_row.get("Originated")),
                "Alert": _compare_alert("# Originated", agg_row.get("Originated"), totals, averages),
                "Link": None,
                "Indent": 1,
            },
            {
                "Group": "Sales",
                "Metric": "Originated Rate (Conversion Rate)",
                "Value": _format_rate(agg_row.get("ConvRate")),
                "Alert": "Yellow",
                "Link": None,
                "Indent": 1,
            },
            {
                "Group": "Sales",
                "Metric": "Loans Funded",
                "Value": _format_count(agg_row.get("LoansFunded")),
                "Alert": "Yellow",
                "Link": None,
                "Indent": 0,
                "IsHeadline": True,
            },
        ]
        rows = _apply_accept_count_override(rows, accept_serving)
        df = pd.DataFrame(rows)
        df.insert(3, "Indicator", df["Alert"].map(_alert_icon))
        return df

    if daily_df is not None and not daily_df.empty:
        latest = daily_df.tail(1).iloc[0]
        rows = [
            {
                "Group": "Sales",
                "Metric": "Apps through the door",
                "Value": _format_count(latest.get("Seen")),
                "Alert": _alert_for_count(latest.get("Seen")),
                "Link": None,
                "Indent": 0,
                "IsHeadline": True,
            },
            {
                "Group": "Sales",
                "Metric": "Score",
                "Value": "",
                "Alert": None,
                "Link": None,
                "Indent": 0,
                "IsCategory": True,
            },
            {
                "Group": "Sales",
                "Metric": "Scoring Count",
                "Value": _format_count(latest.get("Scored")),
                "Alert": _alert_for_count(latest.get("Scored")),
                "Link": None,
                "Indent": 1,
            },
            {
                "Group": "Sales",
                "Metric": "Scoring Rate",
                "Value": _format_rate(latest.get("ScoringRate")),
                "Alert": _alert_for_count(latest.get("ScoringRate")),
                "Link": None,
                "Indent": 1,
            },
            {
                "Group": "Sales",
                "Metric": "Bid",
                "Value": "",
                "Alert": None,
                "Link": None,
                "Indent": 0,
                "IsCategory": True,
            },
            {
                "Group": "Sales",
                "Metric": "Bid Count",
                "Value": _format_count(latest.get("Bids")),
                "Alert": _alert_for_count(latest.get("Bids")),
                "Link": None,
                "Indent": 1,
            },
            {
                "Group": "Sales",
                "Metric": "Bidding Rate",
                "Value": _format_rate(latest.get("BidRate")),
                "Alert": _alert_for_count(latest.get("BidRate")),
                "Link": None,
                "Indent": 1,
            },
            {
                "Group": "Sales",
                "Metric": "Accept",
                "Value": "",
                "Alert": None,
                "Link": None,
                "Indent": 0,
                "IsCategory": True,
            },
            {
                "Group": "Sales",
                "Metric": "Accept Count",
                "Value": _format_count(latest.get("Accepted")),
                "Alert": "Yellow",
                "Link": None,
                "Indent": 1,
            },
            {
                "Group": "Sales",
                "Metric": "Accept Rate",
                "Value": _format_rate(latest.get("AcceptRate")),
                "Alert": "Yellow",
                "Link": None,
                "Indent": 1,
            },
            {
                "Group": "Sales",
                "Metric": "Origination",
                "Value": "",
                "Alert": None,
                "Link": None,
                "Indent": 0,
                "IsCategory": True,
            },
            {
                "Group": "Sales",
                "Metric": "Originated Count",
                "Value": _format_count(latest.get("Originated")),
                "Alert": _alert_for_count(latest.get("Originated")),
                "Link": None,
                "Indent": 1,
            },
            {
                "Group": "Sales",
                "Metric": "Originated Rate (Conversion Rate)",
                "Value": _format_rate(latest.get("ConvRate")),
                "Alert": "Yellow",
                "Link": None,
                "Indent": 1,
            },
            {
                "Group": "Sales",
                "Metric": "Loans Funded",
                "Value": _format_count(latest.get("LoansFunded")),
                "Alert": "Yellow",
                "Link": None,
                "Indent": 0,
                "IsHeadline": True,
            },
        ]
        rows = _apply_accept_count_override(rows, accept_serving)
        df = pd.DataFrame(rows)
        df.insert(3, "Indicator", df["Alert"].map(_alert_icon))
        return df

    if kpi_feed is not None and not kpi_feed.empty:
        needed = [
            "Apps through the door",
            "Scoring Count",
            "Scoring Rate",
            "Bid Count",
            "Bidding Rate",
            "Accept Count",
            "Accept Rate",
            "Originated Count",
            "Originated Rate (Conversion Rate)",
            "Loans Funded",
        ]
        rows = []
        for metric in needed:
            match = kpi_feed.loc[kpi_feed["Metric"] == metric]
            if not match.empty:
                row = match.iloc[0].to_dict()
                row["Group"] = "Sales"
                row["Indent"] = 1 if metric in {"Scoring Count", "Scoring Rate", "Bid Count", "Bidding Rate", "Accept Count", "Accept Rate", "Originated Count", "Originated Rate (Conversion Rate)"} else 0
                if metric in {"Apps through the door", "Loans Funded"}:
                    row["IsHeadline"] = True
                if metric in {"Accept Count", "Accept Rate", "Originated Rate (Conversion Rate)", "Loans Funded"}:
                    row["Alert"] = "Yellow"
                rows.append(row)
            else:
                rows.append(
                    {
                        "Group": "Sales",
                        "Metric": metric,
                        "Value": "—",
                        "Alert": "Yellow",
                        "Link": None,
                        "Indent": 1 if metric in {"Scoring Count", "Scoring Rate", "Bid Count", "Bidding Rate", "Accept Count", "Accept Rate", "Originated Count", "Originated Rate (Conversion Rate)"} else 0,
                        "IsHeadline": True if metric in {"Apps through the door", "Loans Funded"} else False,
                    }
                )
        # Insert category rows for visual grouping.
        rows.insert(1, {"Group": "Sales", "Metric": "Score", "Value": "", "Alert": None, "Link": None, "Indent": 0, "IsCategory": True})
        rows.insert(4, {"Group": "Sales", "Metric": "Bid", "Value": "", "Alert": None, "Link": None, "Indent": 0, "IsCategory": True})
        rows.insert(7, {"Group": "Sales", "Metric": "Accept", "Value": "", "Alert": None, "Link": None, "Indent": 0, "IsCategory": True})
        rows.insert(10, {"Group": "Sales", "Metric": "Origination", "Value": "", "Alert": None, "Link": None, "Indent": 0, "IsCategory": True})
        rows = _apply_accept_count_override(rows, accept_serving)
        df = pd.DataFrame(rows)
        if "Indicator" not in df.columns:
            df.insert(3, "Indicator", df["Alert"].map(_alert_icon))
        return df

    return _sample_metrics().loc[lambda d: d["Group"] == "Sales"].copy()


def _build_performance_kpis() -> pd.DataFrame:
    # Skeleton structure (placeholders only)
    rows = [
        {"Group": "Performance", "Metric": "Defaults Rate %", "Value": "—", "Alert": None, "Link": None},
        {"Group": "Performance", "Metric": "Tracking to Plan", "Value": "—", "Alert": None, "Link": None},
        {"Group": "Performance", "Metric": "Returns vs Historical Avg", "Value": "—", "Alert": None, "Link": None},
        {"Group": "Performance", "Metric": "Payin Ratio (NEW / RETURN)", "Value": "—", "Alert": None, "Link": None},
        {"Group": "Performance", "Metric": "Collections / Resets Ratio", "Value": "—", "Alert": None, "Link": None},
    ]
    df = pd.DataFrame(rows)
    df.insert(3, "Indicator", df["Alert"].map(_alert_icon))
    return df


def _build_call_center_kpis() -> pd.DataFrame:
    rows = [
        {"Group": "CallCenter", "Metric": "# Trained Agents", "Value": "—", "Alert": None, "Link": None},
        {"Group": "CallCenter", "Metric": "# Total Agents", "Value": "—", "Alert": None, "Link": None},
        {"Group": "CallCenter", "Metric": "Capacity (Accepted / Trained)", "Value": "—", "Alert": None, "Link": None},
        {"Group": "CallCenter", "Metric": "Agent Collection Ratio", "Value": "—", "Alert": None, "Link": None},
        {"Group": "CallCenter", "Metric": "Cost per Agent", "Value": "—", "Alert": None, "Link": None},
    ]
    df = pd.DataFrame(rows)
    df.insert(3, "Indicator", df["Alert"].map(_alert_icon))
    return df


def _render_kpi_table(title: str, df: pd.DataFrame) -> None:
    st.markdown(f'<div class="kpi-group">{_escape_html(title)}</div>', unsafe_allow_html=True)
    st.markdown('<div class="kpi-divider"></div>', unsafe_allow_html=True)

    h1, h2, h3, h4 = st.columns([3.2, 1.1, 1.2, 1.6])
    h1.markdown('<div class="kpi-header">Metric</div>', unsafe_allow_html=True)
    h2.markdown('<div class="kpi-header">Value</div>', unsafe_allow_html=True)
    h3.markdown('<div class="kpi-header">Status</div>', unsafe_allow_html=True)
    h4.markdown('<div class="kpi-header">Link</div>', unsafe_allow_html=True)

    for _, row in df.iterrows():
        col1, col2, col3, col4 = st.columns([3.2, 1.1, 1.2, 1.6])
        indent_level = int(row.get("Indent", 0)) if isinstance(row.get("Indent", 0), (int, float)) else 0
        # Avoid treating NaN as truthy
        is_category = True if row.get("IsCategory") is True else False
        is_headline = True if row.get("IsHeadline") is True else False
        col1.markdown(
            f'<div class="kpi-row">{_metric_html(row["Metric"], indent_level, is_category, is_headline)}</div>',
            unsafe_allow_html=True,
        )
        col2.markdown(f'<div class="kpi-row">{_escape_html(row["Value"])}</div>', unsafe_allow_html=True)
        col3.markdown(f'<div class="kpi-row">{_status_html(row.get("Alert"))}</div>', unsafe_allow_html=True)
        col4.markdown(f'<div class="kpi-row">{_link_html(str(row.get("Link") or ""))}</div>', unsafe_allow_html=True)


def main() -> None:
    st.markdown(
        """
<style>
  /* tighten overall layout a bit */
  .block-container { padding-top: 2.0rem; padding-bottom: 2.0rem; }

  /* simple status badges */
  .badge {
    display: inline-flex;
    align-items: center;
    gap: 0.25rem;
    padding: 0.18rem 0.55rem;
    border-radius: 999px;
    border: 1px solid rgba(255,255,255,0.12);
    font-size: 0.85rem;
    font-weight: 600;
    white-space: nowrap;
  }
  .badge-red { background: rgba(255, 77, 79, 0.18); border-color: rgba(255, 77, 79, 0.35); }
  .badge-yellow { background: rgba(250, 173, 20, 0.18); border-color: rgba(250, 173, 20, 0.35); }
  .badge-green { background: rgba(82, 196, 26, 0.18); border-color: rgba(82, 196, 26, 0.35); }

  /* table header row */
  .kpi-header { font-size: 0.85rem; font-weight: 700; opacity: 0.85; padding: 0.25rem 0; }
  .kpi-row { padding: 0.15rem 0; }
  .kpi-headline { font-weight: 700; font-size: 1.0rem; }
  .kpi-submetric { font-weight: 400; }
  .kpi-muted { opacity: 0.8; }
  .kpi-group { font-size: 0.95rem; font-weight: 750; margin-top: 0.75rem; }
  .kpi-divider { height: 1px; background: rgba(255,255,255,0.10); margin: 0.35rem 0 0.55rem 0; }
</style>
""",
        unsafe_allow_html=True,
    )

    st.title("KPIs Alert Dashboard")
    st.caption("MVP showcase: traffic-light alerts + deep links (links may be null).")

    source_profile = st.selectbox(
        "Data mode",
        options=[
            "Hybrid (AcceptCount new pipeline + others legacy)",
            "Legacy only (all KPIs from old pipeline)",
            "Sample demo (no CSV required)",
        ],
        index=0,
    )
    accept_source = "Serving snapshot (kpi_serving_metrics.csv, 7D)"

    kpi_feed = _load_metrics_from_refresh_csv()
    daily_df = _load_daily_metrics_from_refresh_csv()
    agg_row = _load_aggregated_metrics_from_refresh_csv()
    totals = _load_compare_totals()
    averages = _load_compare_averages()
    accept_override = None
    source_note = ""

    if source_profile == "Hybrid (AcceptCount new pipeline + others legacy)":
        with st.expander("Advanced", expanded=False):
            accept_source = st.selectbox(
                "AcceptCount source",
                options=[
                    "Serving snapshot (kpi_serving_metrics.csv, 7D)",
                    "History snapshot (kpi_history.csv, latest daily)",
                ],
                index=0,
            )
        mode = "serving" if accept_source.startswith("Serving") else "history"
        accept_override = _resolve_accept_count_override(mode)
        if accept_override is None:
            st.warning("AcceptCount hybrid source unavailable; falling back to legacy AcceptCount.")
            source_note = "Source: Hybrid mode active. AcceptCount fallback -> legacy row."
        else:
            source_note = f"Source: Hybrid mode active. AcceptCount from {mode} pipeline; other KPIs from legacy."
    elif source_profile == "Legacy only (all KPIs from old pipeline)":
        with st.expander("Advanced", expanded=False):
            legacy_exclude_sunday = st.checkbox(
                "Exclude Sundays from legacy average calculations",
                value=False,
                help="Affects average-type legacy KPIs only. Count totals remain unchanged.",
            )
        if legacy_exclude_sunday:
            ui_agg = _aggregate_legacy_daily_for_ui(daily_df, exclude_sunday_for_averages=True)
            if ui_agg is not None:
                agg_row = ui_agg
                source_note = "Source: Legacy mode active. Sundays excluded from averages (Advanced override)."
            else:
                source_note = "Source: Legacy mode active. Sunday-exclusion override unavailable; using kpi_metrics.csv."
        else:
            source_note = "Source: Legacy mode active. Values from kpi_metrics.csv."
    elif source_profile == "Sample demo (no CSV required)":
        kpi_feed = None
        daily_df = None
        agg_row = None
        totals = {}
        averages = {}
        source_note = "Source: Sample mode active. Local sample values are displayed."

    if source_note:
        st.caption(source_note)

    sales_df = _build_sales_kpis(kpi_feed, daily_df, agg_row, totals, averages, accept_serving=accept_override)
    if source_profile == "Legacy only (all KPIs from old pipeline)":
        # Legacy demo preference: Accept Rate shown as Green.
        legacy_accept_rate_mask = sales_df["Metric"].astype(str).str.strip() == "Accept Rate"
        if legacy_accept_rate_mask.any():
            sales_df.loc[legacy_accept_rate_mask, "Alert"] = "Green"
            if "Indicator" in sales_df.columns:
                sales_df.loc[legacy_accept_rate_mask, "Indicator"] = sales_df.loc[
                    legacy_accept_rate_mask, "Alert"
                ].map(_alert_icon)
    performance_df = _build_performance_kpis()
    call_center_df = _build_call_center_kpis()

    df = pd.concat([sales_df, performance_df, call_center_df], ignore_index=True)

    red_count = int((df["Alert"].str.lower() == "red").sum())
    yellow_count = int((df["Alert"].str.lower() == "yellow").sum())
    green_count = int((df["Alert"].str.lower() == "green").sum())
    # Temporary legacy pin for demo parity; other modes remain dynamic.
    metrics_tracked = 10 if source_profile == "Legacy only (all KPIs from old pipeline)" else len(df)

    c1, c2, c3, c4 = st.columns([1.2, 1, 1, 1.4])
    c1.metric("Metrics tracked", metrics_tracked)
    c2.metric("🔴 Red", red_count)
    c3.metric("🟡 Yellow", yellow_count)
    c4.metric("🟢 Green", green_count)

    level = st.selectbox(
        "Filter",
        options=["All", "Red", "Yellow", "Green"],
        index=0,
        label_visibility="collapsed",
        key="alert_filter",
    )
    if level != "All":
        df = df.loc[df["Alert"].str.lower() == level.lower()].copy()

    st.divider()

    _render_kpi_table("Sales KPIs", sales_df)
    st.divider()
    _render_kpi_table("SERVICING / PERFORMANCE KPIs", performance_df)
    st.divider()
    _render_kpi_table("CALL CENTER PERFORMANCE KPIs", call_center_df)


if __name__ == "__main__":
    main()

