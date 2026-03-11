"""
Regional disparity scoring for CGM/device adoption across 17 시도.
"""

from __future__ import annotations

import pandas as pd


def compute_regional_adoption_rate(
    utilization_df: pd.DataFrame,
    prevalence_df: pd.DataFrame,
) -> pd.DataFrame:
    """Compute per-region CGM adoption rate.

    Args:
        utilization_df: Must have columns [region_code, utilization_count].
            Represents CGM users (or proxy: diabetes patients using covered device).
        prevalence_df: Must have columns [region_code, eligible_population].
            Represents total eligible T1D population per region.

    Returns DataFrame with columns:
        region_code, region_name, utilization_count, eligible_population,
        adoption_rate_pct
    """
    required_util = {"region_code", "utilization_count"}
    required_prev = {"region_code", "eligible_population"}
    missing_u = required_util - set(utilization_df.columns)
    missing_p = required_prev - set(prevalence_df.columns)
    if missing_u:
        raise ValueError(f"utilization_df missing columns: {missing_u}")
    if missing_p:
        raise ValueError(f"prevalence_df missing columns: {missing_p}")

    merged = utilization_df.merge(prevalence_df, on="region_code", how="inner")
    if merged.empty:
        raise ValueError("No matching region_codes between utilization and prevalence DataFrames")

    merged["adoption_rate_pct"] = (
        merged["utilization_count"] / merged["eligible_population"] * 100
    ).round(2)
    return merged[["region_code", "utilization_count", "eligible_population", "adoption_rate_pct"]]


def score_regional_disparity(regional_df: pd.DataFrame) -> pd.DataFrame:
    """Add disparity scoring to a regional DataFrame.

    Input must have column: adoption_rate_pct
    Adds columns:
        adoption_pct_rank: 1 = highest adoption (best), 17 = lowest
        national_median_ratio: adoption_rate_pct / national_median (1.0 = at median)
        disparity_flag: True if adoption_rate < 50% of national median
    """
    if "adoption_rate_pct" not in regional_df.columns:
        raise ValueError("regional_df must have column 'adoption_rate_pct'")

    df = regional_df.copy()
    df["adoption_pct_rank"] = df["adoption_rate_pct"].rank(ascending=False, method="min").astype(int)

    national_median = df["adoption_rate_pct"].median()
    df["national_median_ratio"] = (df["adoption_rate_pct"] / national_median).round(3)
    df["disparity_flag"] = df["adoption_rate_pct"] < (national_median * 0.50)

    return df


def compute_disparity_index(regional_df: pd.DataFrame) -> float:
    """Compute a disparity index (max/min adoption ratio).

    A value of 2.1 means the best-covered region has 2.1× the adoption rate
    of the worst-covered region.
    """
    if "adoption_rate_pct" not in regional_df.columns:
        raise ValueError("regional_df must have column 'adoption_rate_pct'")
    rates = regional_df["adoption_rate_pct"].dropna()
    if rates.empty or rates.min() == 0:
        return float("inf")
    return round(rates.max() / rates.min(), 3)
