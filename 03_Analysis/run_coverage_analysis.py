"""
Runner for coverage_adequacy.py — bypasses Marimo UI.
Writes 03_Analysis/coverage_adequacy.csv directly.

Usage:
    python 03_Analysis/run_coverage_analysis.py
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.coverage_index import compute_gap_series

OUTPUT_CSV = PROJECT_ROOT / "03_Analysis" / "coverage_adequacy.csv"


def main() -> None:
    print("Running coverage adequacy analysis...")
    years = list(range(2018, 2027))
    df = compute_gap_series(years, "cgm_sensor")

    output_cols = [
        "year", "reimb_ceiling_quarterly",
        "market_price_monthly_low", "market_price_monthly_mid", "market_price_monthly_high",
        "nhis_pays_quarterly",
        "patient_pays_quarterly_low", "patient_pays_quarterly_mid", "patient_pays_quarterly_high",
        "burden_ratio_low", "burden_ratio_mid", "burden_ratio_high",
        "coverage_ratio_low", "coverage_ratio_mid", "coverage_ratio_high",
    ]
    available = [c for c in output_cols if c in df.columns]
    out = df[available]

    OUTPUT_CSV.parent.mkdir(exist_ok=True)
    out.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"Written: {OUTPUT_CSV} ({len(out)} rows)")
    print(out.to_string())


if __name__ == "__main__":
    main()
