from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional, Tuple

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans


@dataclass
class BandingResult:
    kmeans: KMeans
    band_boundaries: pd.DataFrame  # columns: [min, max, ColBand]


def _ensure_required_columns(df: pd.DataFrame, required: Iterable[str]) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"Missing required columns: {missing}")


def fit_kmeans_bands(
    scores: pd.Series,
    num_bands: int = 5,
    random_state: int = 0,
    higher_is_better: bool = True,
) -> BandingResult:
    """
    Fit KMeans to a score series and create ordinal bands 1..num_bands.

    Band 1 corresponds to the best scores if higher_is_better=True.
    """
    s = pd.to_numeric(scores, errors="coerce").dropna().to_numpy().reshape(-1, 1)
    if s.size == 0:
        raise ValueError("No valid numeric scores provided to fit_kmeans_bands().")

    kmeans = KMeans(n_clusters=num_bands, random_state=random_state)
    kmeans.fit(s)

    tmp = pd.DataFrame({"ClusterID": kmeans.labels_, "score": s.flatten()})
    boundaries = (
        tmp.groupby("ClusterID")["score"]
        .agg(["min", "max"])
        .sort_values(by="min", ascending=not higher_is_better)
        .reset_index(drop=True)
    )
    boundaries["ColBand"] = range(1, len(boundaries) + 1)
    return BandingResult(kmeans=kmeans, band_boundaries=boundaries)


def apply_kmeans_bands(
    scores: pd.Series,
    result: BandingResult,
) -> pd.Series:
    """Map raw scores to ordinal bands using a fitted BandingResult.

    The mapping is derived by ranking the k-means cluster centers from best to
    worst (highest to lowest), assigning band 1 to the best center.
    """
    numeric_scores = pd.to_numeric(scores, errors="coerce")
    score_values = numeric_scores.to_numpy().reshape(-1, 1)
    valid_mask = np.isfinite(numeric_scores.to_numpy())

    cluster_ids = np.full(shape=(len(numeric_scores),), fill_value=np.nan, dtype=float)
    if valid_mask.any():
        predicted = result.kmeans.predict(score_values[valid_mask])
        cluster_ids[valid_mask] = predicted

    centers = result.kmeans.cluster_centers_.flatten()
    order_desc = np.argsort(centers)[::-1]
    label_to_band = {label: band for band, label in enumerate(order_desc, start=1)}
    bands = pd.Series(
        [label_to_band.get(int(lbl), np.nan) if np.isfinite(lbl) else np.nan for lbl in cluster_ids],
        index=scores.index,
        dtype="float",
    )
    # Leave NaN for rows where score was missing; caller may drop those.
    return bands


def band_by_quantiles(
    scores: pd.Series,
    num_bands: int = 5,
    higher_is_better: bool = True,
) -> pd.Series:
    """Create ordinal bands 1..num_bands using equal-frequency quantiles."""
    q = pd.qcut(scores.rank(method="first"), q=num_bands, labels=False) + 1
    bands = q.astype(int)
    if higher_is_better:
        # Largest scores should be band 1
        rank_desc = scores.rank(method="first", ascending=False)
        bands = pd.qcut(rank_desc, q=num_bands, labels=False) + 1
        bands = bands.astype(int)
    return bands


def percentile_top_tail_bands(
    scores: pd.Series,
    num_bands: int = 5,
    top_tail: float = 0.10,
    higher_is_better: bool = True,
    best_band_is_highest: bool = True,
) -> pd.Series:
    """
    Create bands by percentiles with an explicit top tail band.

    - If higher_is_better=True: Band 1 is the top `top_tail` fraction (safest).
      The remaining 1 - top_tail are split evenly into bands 2..num_bands by quantiles.
    - If higher_is_better=False: Band 1 is the bottom `top_tail` fraction (safest),
      and the rest split upward.
    - If best_band_is_highest=True (default), the best band is mapped to `num_bands`
      so that larger band numbers indicate safer/better (e.g., Band 5 is best).
    """
    numeric = pd.to_numeric(scores, errors="coerce")
    bands = pd.Series(np.nan, index=numeric.index, dtype="float")
    valid = numeric.dropna()
    if valid.empty:
        return bands

    # Rank so that smaller rank = better (1 = best) regardless of higher_is_better
    rank = valid.rank(method="first", ascending=not higher_is_better)
    pct = rank / len(valid)  # in (0,1]

    # Top tail (best) -> band 1
    top_mask = pct <= top_tail
    bands.loc[top_mask.index[top_mask]] = 1

    # Remaining -> split into (num_bands - 1) equal-frequency bins, labeled 2..num_bands
    remainder_idx = pct.index[~top_mask]
    if len(remainder_idx) > 0 and num_bands > 1:
        rem_rank = rank.loc[remainder_idx]
        try:
            rem_labels = list(range(2, num_bands + 1))
            bands.loc[remainder_idx] = pd.qcut(rem_rank, q=len(rem_labels), labels=rem_labels).astype(int)
        except ValueError:
            # Not enough unique values; fallback to single remainder band
            bands.loc[remainder_idx] = 2
    # Optionally invert so the best band is the highest number
    if best_band_is_highest:
        inv = bands.copy()
        mask = inv.notna()
        inv.loc[mask] = (num_bands + 1 - inv.loc[mask].astype(int)).astype(float)
        bands = inv
    return bands


def band_report(
    df: pd.DataFrame,
    band_col: str,
    is_good_col: str = "IsGood",
    payin_col: str = "Payin",
) -> pd.DataFrame:
    """
    Compute banded performance metrics analogous to the example table.

    Returns columns:
    [
        'AvgPayin','GoodPayin','LowPayin','# Loans','Count Pct (%)',
        '# IsGood','IsGood Rate (%)','Cumulative Payin ⬆️'
    ]
    indexed by band ascending (1..K).
    """
    _ensure_required_columns(df, [band_col, is_good_col, payin_col])

    # Drop rows without a band (e.g., missing scores) from the report
    df_non_missing = df.loc[df[band_col].notna()].copy()
    grouped = df_non_missing.groupby(band_col, dropna=False)
    base = pd.DataFrame({
        "# Loans": grouped.size(),
        "# IsGood": grouped[is_good_col].sum(min_count=1),
        "IsGood Rate (%)": grouped[is_good_col].mean() * 100.0,
        "AvgPayin": grouped[payin_col].mean(),
    })

    # Conditional payins
    good_payins = df_non_missing.loc[df_non_missing[payin_col] > 1]
    low_payins = df_non_missing.loc[df_non_missing[payin_col] < 1]
    base["GoodPayin"] = good_payins.groupby(band_col)[payin_col].mean()
    base["LowPayin"] = low_payins.groupby(band_col)[payin_col].mean()

    # Order columns like the example
    base = base[["AvgPayin", "GoodPayin", "LowPayin", "# Loans", "# IsGood", "IsGood Rate (%)"]]
    base["Count Pct (%)"] = base["# Loans"] / base["# Loans"].sum() * 100.0

    # Reindex by band 1..K if possible
    try:
        bands_sorted = sorted(base.index.astype(int))
        base = base.reindex(bands_sorted)
    except Exception:
        pass

    # Cumulative Payin from current band through the end (⬆️ semantic)
    # Matches the reference logic using suffix means weighted by # Loans
    loans = base["# Loans"].to_numpy()
    avg_payin = base["AvgPayin"].to_numpy()
    cum_vals = []
    for i in range(len(base)):
        w = loans[i:]
        v = avg_payin[i:]
        if np.nansum(w) == 0:
            cum_vals.append(np.nan)
        else:
            cum_vals.append(np.nansum(v * w) / np.nansum(w))
    base["Cumulative Payin ⬆️"] = cum_vals

    # Final rearrangement to match the visual order
    final_cols = [
        "AvgPayin",
        "GoodPayin",
        "LowPayin",
        "# Loans",
        "Count Pct (%)",
        "# IsGood",
        "IsGood Rate (%)",
        "Cumulative Payin ⬆️",
    ]
    return base[final_cols]


def model_application_evaluation(
    df: pd.DataFrame,
    model,
    features: Iterable[str],
    scaler=None,
    training: bool = True,
    kmeans: Optional[KMeans] = None,
    num_bands: int = 5,
    score_scale: float = 1000.0,
    population_filter: Optional[pd.Series] = None,
    higher_is_better: bool = True,
) -> Tuple[KMeans, pd.DataFrame]:
    """
    Reusable evaluation with KMeans banding similar to the user's reference code.

    - If training=True, fits KMeans and returns (fitted_kmeans, table)
    - Else, uses provided kmeans to assign bands and returns (kmeans, table)
    """
    use_df = df.copy()
    if population_filter is not None:
        use_df = use_df.loc[population_filter].copy()

    X = use_df[list(features)].copy()
    if scaler is not None:
        X.loc[:, list(features)] = scaler.transform(X[list(features)])

    # Predict positive class probability if available
    if hasattr(model, "predict_proba"):
        proba = model.predict_proba(X)[:, 1]
    else:
        # Fallback to decision_function or predict
        if hasattr(model, "decision_function"):
            proba = model.decision_function(X)
        else:
            proba = model.predict(X)

    scores = pd.Series(proba, index=use_df.index) * score_scale

    if training or kmeans is None:
        banding = fit_kmeans_bands(scores, num_bands=num_bands, higher_is_better=higher_is_better)
    else:
        # Wrap provided kmeans; infer order by cluster centers
        centers_flat = kmeans.cluster_centers_.flatten()
        order = np.argsort(centers_flat)
        if higher_is_better:
            order = order[::-1]
        boundaries = pd.DataFrame({
            "min": np.nan,
            "max": np.nan,
            "ColBand": np.arange(1, len(centers_flat) + 1),
        }).take(order).reset_index(drop=True)
        banding = BandingResult(kmeans=kmeans, band_boundaries=boundaries)

    bands = apply_kmeans_bands(scores, banding)

    eval_df = use_df.copy()
    eval_df["Band"] = bands.values
    table = band_report(eval_df, band_col="Band")
    return banding.kmeans, table


def band_and_report_from_scores(
    df: pd.DataFrame,
    score_col: str,
    method: str = "kmeans",
    num_bands: int = 5,
    higher_is_better: bool = True,
) -> Tuple[pd.Series, pd.DataFrame]:
    """Convenience: create `Band` from `score_col` and compute the table."""
    if method == "kmeans":
        banding = fit_kmeans_bands(df[score_col], num_bands=num_bands, higher_is_better=higher_is_better)
        bands = apply_kmeans_bands(df[score_col], banding)
    elif method == "quantiles":
        bands = band_by_quantiles(df[score_col], num_bands=num_bands, higher_is_better=higher_is_better)
    else:
        raise ValueError("method must be 'kmeans' or 'quantiles'")

    tmp = df.copy()
    tmp["Band"] = bands.values
    table = band_report(tmp, band_col="Band")
    return bands, table


def band_and_report_from_scores_percentiles(
    df: pd.DataFrame,
    score_col: str,
    num_bands: int = 5,
    top_tail: float = 0.10,
    higher_is_better: bool = True,
    best_band_is_highest: bool = True,
) -> Tuple[pd.Series, pd.DataFrame]:
    """
    Create percentile-based bands with an explicit top tail band and report.
    Band `num_bands` is the safest by default (top 10% when higher_is_better=True).
    """
    bands = percentile_top_tail_bands(
        df[score_col],
        num_bands=num_bands,
        top_tail=top_tail,
        higher_is_better=higher_is_better,
        best_band_is_highest=best_band_is_highest,
    )
    tmp = df.copy()
    tmp["Band"] = bands.values
    table = band_report(tmp, band_col="Band")
    return bands, table

