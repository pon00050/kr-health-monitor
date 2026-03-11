"""
Runner for regional_variation.py — bypasses Marimo UI.
Writes 03_Analysis/regional_variation.csv directly.

Usage:
    python 03_Analysis/run_regional_variation.py
"""

import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.regional_scorer import compute_disparity_index, score_regional_disparity
from src.storage import load_parquet

OUTPUT_CSV = PROJECT_ROOT / "03_Analysis" / "regional_variation.csv"

SYNTHETIC_REGIONAL = pd.DataFrame({
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


def main() -> None:
    print("Running regional variation analysis...")

    try:
        regional_raw = load_parquet("hira_regional_diabetes")
        print(f"Loaded hira_regional_diabetes: {len(regional_raw)} rows")
    except FileNotFoundError:
        print("Note: hira_regional_diabetes.parquet not found — using synthetic data")
        regional_raw = SYNTHETIC_REGIONAL

    all_rows = []
    for year in sorted(regional_raw["year"].unique()):
        yr_df = regional_raw[regional_raw["year"] == year].copy()
        yr_df["adoption_rate_pct"] = (
            yr_df["patient_count"] / yr_df["patient_count"].sum() * 100
        ).round(2)
        scored = score_regional_disparity(yr_df)
        all_rows.append(scored)

    output_df = pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame()

    OUTPUT_CSV.parent.mkdir(exist_ok=True)
    output_df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"Written: {OUTPUT_CSV} ({len(output_df)} rows)")

    # Summary stats for latest year
    latest = output_df[output_df["year"] == output_df["year"].max()] if not output_df.empty else output_df
    if not latest.empty:
        idx = compute_disparity_index(latest)
        print(f"\n지역 격차 지수 (최고/최저): {idx:.1f}×")
        print(latest[["region_name", "adoption_rate_pct", "adoption_pct_rank", "disparity_flag"]].to_string(index=False))


if __name__ == "__main__":
    main()
