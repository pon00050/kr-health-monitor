"""
Runner for trend_analysis.py — bypasses Marimo UI.
Writes 03_Analysis/trend_analysis.csv directly.

Usage:
    python 03_Analysis/run_trend_analysis.py
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.coverage_index import compute_gap_series

OUTPUT_CSV = PROJECT_ROOT / "03_Analysis" / "trend_analysis.csv"


def main() -> None:
    print("Running trend analysis...")
    years = list(range(2018, 2031))  # Include 2027–2030 projection years
    df = compute_gap_series(years, "cgm_sensor")

    output_cols = [
        "year", "reimb_ceiling_quarterly",
        "market_price_monthly_low", "market_price_monthly_mid", "market_price_monthly_high",
        "burden_ratio_low", "burden_ratio_mid", "burden_ratio_high",
        "coverage_ratio_low", "coverage_ratio_mid", "coverage_ratio_high",
    ]
    available = [c for c in output_cols if c in df.columns]
    out = df[available]

    OUTPUT_CSV.parent.mkdir(exist_ok=True)
    out.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"Written: {OUTPUT_CSV} ({len(out)} rows)")

    # Projection summary
    has_ratio = out["coverage_ratio_mid"].notna() if "coverage_ratio_mid" in out.columns else None
    if has_ratio is not None and has_ratio.any():
        latest_ratio = out.loc[has_ratio, "coverage_ratio_mid"].iloc[-1]
        print(f"\nLatest coverage ratio: {latest_ratio*100:.1f}%")
        print("Note: 기준금액 frozen at ₩210,000/quarter since Aug 2022")
        print("      If market prices rise, coverage ratio will worsen further")


if __name__ == "__main__":
    main()
