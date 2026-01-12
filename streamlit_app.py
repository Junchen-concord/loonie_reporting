from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st


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
            "Metric": "Accept Count",
            "Value": "1,284",
            "Alert": "Green",
            "Link": "https://example.com/accept-count",
        },
        {
            "Group": "Sales",
            "Metric": "Conversion Rate",
            "Value": "9.2%",
            "Alert": "Green",
            "Link": "https://example.com/conversion-rate",
        },
        {
            "Group": "Performance",
            "Metric": "ACH Return Rate",
            "Value": "2.4%",
            "Alert": "Red",
            "Link": "https://reports.speedyloan.com/single/?appid=f9a3184e-f3e8-4fab-94fa-373a0f869114&obj=36bfd39d-16a2-4b83-88a1-0a5fdb7f7f72&theme=sense&bookmark=06cc43dc-cb1b-4466-b0a3-d60246ef27cf&opt=ctxmenu,currsel",
        },
        {
            "Group": "Performance",
            "Metric": "First Payment Default - FA%",
            "Value": "6.1%",
            "Alert": "Yellow",
            "Link": "https://example.com/fpd-fa",
        },
        {
            "Group": "Performance",
            "Metric": "Avg Payin",
            "Value": "1.18",
            "Alert": "Red",
            "Link": "https://example.com/payin",
        },
    ]
    df = pd.DataFrame(rows)
    df.insert(3, "Indicator", df["Alert"].map(_alert_icon))
    return df


def _load_metrics_from_refresh_csv() -> pd.DataFrame | None:
    """
    Prefer a refreshed KPI feed if present (e.g., written by cron / refresh script).

    Expected location: data/refresh/kpi_metrics.csv
    Expected columns (minimum): GroupName|Group, Metric, Value, Alert, Link
    """
    path = Path(__file__).resolve().parent / "data" / "refresh" / "kpi_metrics.csv"
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

    return df


def main() -> None:
    st.set_page_config(page_title="KPIs Alert Dashboard", layout="wide")
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
  .kpi-metric { font-weight: 650; }
  .kpi-muted { opacity: 0.8; }
  .kpi-group { font-size: 0.95rem; font-weight: 750; margin-top: 0.75rem; }
  .kpi-divider { height: 1px; background: rgba(255,255,255,0.10); margin: 0.35rem 0 0.55rem 0; }
</style>
""",
        unsafe_allow_html=True,
    )

    st.title("KPIs Alert Dashboard")
    st.caption("MVP showcase: traffic-light alerts + deep links (links may be null).")

    df = _load_metrics_from_refresh_csv() or _sample_metrics()

    red_count = int((df["Alert"].str.lower() == "red").sum())
    yellow_count = int((df["Alert"].str.lower() == "yellow").sum())
    green_count = int((df["Alert"].str.lower() == "green").sum())

    c1, c2, c3, c4 = st.columns([1.2, 1, 1, 1.4])
    c1.metric("Metrics tracked", len(df))
    c2.metric("🔴 Red", red_count)
    c3.metric("🟡 Yellow", yellow_count)
    c4.metric("🟢 Green", green_count)

    left, right = st.columns([5, 2])
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

    # Executive grouped view (Sales, Performance, ...)
    group_order = ["Sales", "Performance"]
    groups = [g for g in group_order if g in set(df.get("Group", pd.Series([], dtype=str)))] + [
        g for g in sorted(df["Group"].dropna().unique()) if g not in group_order
    ]

    for group in groups:
        gdf = df.loc[df["Group"] == group].copy()
        gdf = (
            gdf.assign(_rank=gdf["Alert"].map(_alert_rank))
            .sort_values(by=["_rank", "Metric"])
            .drop(columns=["_rank"])
        )

        st.markdown(f'<div class="kpi-group">{_escape_html(group)} metrics</div>', unsafe_allow_html=True)
        st.markdown('<div class="kpi-divider"></div>', unsafe_allow_html=True)

        h1, h2, h3, h4 = st.columns([3.2, 1.1, 1.2, 1.6])
        h1.markdown('<div class="kpi-header">Metric</div>', unsafe_allow_html=True)
        h2.markdown('<div class="kpi-header">Value</div>', unsafe_allow_html=True)
        h3.markdown('<div class="kpi-header">Status</div>', unsafe_allow_html=True)
        h4.markdown('<div class="kpi-header">Link</div>', unsafe_allow_html=True)

        for _, row in gdf.iterrows():
            col1, col2, col3, col4 = st.columns([3.2, 1.1, 1.2, 1.6])
            col1.markdown(f'<div class="kpi-row kpi-metric">{_escape_html(row["Metric"])}</div>', unsafe_allow_html=True)
            col2.markdown(f'<div class="kpi-row">{_escape_html(row["Value"])}</div>', unsafe_allow_html=True)
            col3.markdown(f'<div class="kpi-row">{_badge_html(row["Alert"])}</div>', unsafe_allow_html=True)
            col4.markdown(f'<div class="kpi-row">{_link_html(str(row.get("Link") or ""))}</div>', unsafe_allow_html=True)

    with st.expander("How this maps to the real build"):
        st.markdown(
            """
- **Metric**: KPI name (e.g., conversion rate, return rate, payin)
- **Indicator**: Traffic-light status (🟢 / 🟡 / 🔴)
- **Link**: Deep-link to definitions, Looker/Mode chart, or a runbook (can be null)
"""
        )


if __name__ == "__main__":
    main()


