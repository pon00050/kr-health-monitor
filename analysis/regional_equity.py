"""
Regional variation analysis — Marimo app.

Ranks all 17 시도 by CGM adoption rate and quantifies disparity.
Use run_regional_variation.py runner to execute non-interactively.
"""

import marimo

__generated_with = "0.1.0"
app = marimo.App(width="wide", app_title="지역별 CGM 도입률 격차 분석")


@app.cell
def __():
    import sys
    from pathlib import Path

    _root = Path(__file__).resolve().parent.parent
    if str(_root) not in sys.path:
        sys.path.insert(0, str(_root))

    import pandas as pd
    import plotly.express as px
    from src.config import REGION_CODES
    from src.regional_scorer import compute_disparity_index, score_regional_disparity
    from src.storage import load_parquet
    return Path, REGION_CODES, compute_disparity_index, load_parquet, pd, px, score_regional_disparity, sys


@app.cell
def __(load_parquet, pd):
    try:
        regional_raw = load_parquet("hira_regional_diabetes")
    except FileNotFoundError:
        # Use synthetic data if parquet not available
        regional_raw = pd.DataFrame({
            "region_code": ["11", "21", "22", "23", "24", "25", "26", "29",
                            "31", "32", "33", "34", "35", "36", "37", "38", "39"],
            "region_name": ["서울", "부산", "대구", "인천", "광주", "대전", "울산", "세종",
                            "경기", "강원", "충북", "충남", "전북", "전남", "경북", "경남", "제주"],
            "year": [2023] * 17,
            "patient_count": [8500, 3200, 2100, 2800, 1200, 1100, 800, 200,
                              9800, 380, 620, 720, 780, 650, 900, 1500, 350],
            "claim_count": [42000, 15800, 10500, 14000, 6000, 5500, 4000, 1000,
                            49000, 1900, 3100, 3600, 3900, 3250, 4500, 7500, 1750],
            "total_cost_krw": [12500000000, 4700000000, 3100000000, 4100000000,
                               1800000000, 1650000000, 1200000000, 300000000,
                               14500000000, 570000000, 930000000, 1080000000,
                               1170000000, 975000000, 1350000000, 2250000000, 525000000],
        })
        print("Note: Using synthetic regional data — run `krh run` to fetch real data")
    regional_raw
    return (regional_raw,)


@app.cell
def __(regional_raw, score_regional_disparity):
    """Compute latest-year regional disparity scores"""
    latest_year = regional_raw["year"].max()
    df_latest = regional_raw[regional_raw["year"] == latest_year].copy()

    # Use patient_count as adoption proxy (actual CGM adoption by region unavailable)
    df_latest["adoption_rate_pct"] = (
        df_latest["patient_count"] / df_latest["patient_count"].sum() * 100
    ).round(2)

    scored = score_regional_disparity(df_latest)
    scored_sorted = scored.sort_values("adoption_pct_rank")
    scored_sorted[["region_name", "adoption_rate_pct", "national_median_ratio",
                   "adoption_pct_rank", "disparity_flag"]]
    return df_latest, latest_year, scored, scored_sorted


@app.cell
def __(compute_disparity_index, scored):
    disparity_idx = compute_disparity_index(scored)
    print(f"지역 격차 지수 (최고/최저 비율): {disparity_idx:.1f}×")
    print(f"전국 중앙값: {scored['adoption_rate_pct'].median():.1f}%")
    print(f"격차 플래그 지역 수: {scored['disparity_flag'].sum()} / 17")
    return (disparity_idx,)


@app.cell
def __(px, scored_sorted):
    """Bar chart: 17 시도 ranked by adoption rate"""
    fig = px.bar(
        scored_sorted,
        x="region_name",
        y="adoption_rate_pct",
        color="disparity_flag",
        color_discrete_map={True: "#fc8181", False: "#4299e1"},
        title=f"지역별 CGM 급여 이용률 (시도별 순위)",
        labels={
            "region_name": "시도",
            "adoption_rate_pct": "이용률 (%)",
            "disparity_flag": "격차 플래그",
        },
    )
    fig.add_hline(
        y=scored_sorted["adoption_rate_pct"].median(),
        line_dash="dash",
        annotation_text="전국 중앙값",
    )
    fig
    return (fig,)


@app.cell
def __(regional_raw, score_regional_disparity):
    """Full output table for CSV export (all years, all regions)"""
    output_rows = []
    for year in sorted(regional_raw["year"].unique()):
        yr_df = regional_raw[regional_raw["year"] == year].copy()
        yr_df["adoption_rate_pct"] = (
            yr_df["patient_count"] / yr_df["patient_count"].sum() * 100
        ).round(2)
        scored_yr = score_regional_disparity(yr_df)
        output_rows.append(scored_yr)

    import pandas as pd
    output_df = pd.concat(output_rows, ignore_index=True) if output_rows else pd.DataFrame()
    output_df
    return output_df, output_rows, pd, year


if __name__ == "__main__":
    app.run()
