"""
Runner for coverage_gap.py — bypasses Marimo UI.
Writes analysis/coverage_gap.csv directly.

Usage:
    python analysis/run_coverage_gap.py
"""

import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
from pathlib import Path

import pandas as pd
from datetime import date

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.coverage import compute_coverage_adequacy_ratio, compute_gap_series, compute_quarterly_patient_burden
from src.store import load_parquet

OUTPUT_CSV = PROJECT_ROOT / "analysis" / "coverage_gap.csv"
OUTPUT_PRODUCT_CSV = PROJECT_ROOT / "analysis" / "coverage_gap_by_product.csv"


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
    out = df[available].copy()
    out["generated_at"] = date.today().isoformat()

    OUTPUT_CSV.parent.mkdir(exist_ok=True)
    out.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"Written: {OUTPUT_CSV} ({len(out)} rows)")
    print(out.to_string())

    # Product-level output: join with HIRA treatment materials
    try:
        from src.devices import CGM_M_CODE_TO_TIER
        from src.policy import MARKET_PRICES_KRW
        materials = load_parquet("hira_treatment_materials")
        materials = materials[materials["coverage_status"] != "삭제"].copy()
        materials["price_tier"] = materials["m_code"].map(CGM_M_CODE_TO_TIER)
        materials = materials.dropna(subset=["price_tier"])

        product_rows = []
        for _, product in materials.iterrows():
            tier = product["price_tier"]
            monthly = MARKET_PRICES_KRW["cgm_sensor"][tier]
            for _, gap_row in out.iterrows():
                if pd.isna(gap_row.get("reimb_ceiling_quarterly")):
                    continue
                result = compute_quarterly_patient_burden(monthly, gap_row["reimb_ceiling_quarterly"])
                product_rows.append({
                    "year": gap_row["year"],
                    "product_name": product.get("product_name", ""),
                    "m_code": product.get("m_code", ""),
                    "importer": product.get("importer", ""),
                    "manufacturer": product.get("manufacturer", ""),
                    "coverage_status": product.get("coverage_status", ""),
                    "price_tier": tier,
                    "market_price_monthly": monthly,
                    "reimb_ceiling_quarterly": gap_row["reimb_ceiling_quarterly"],
                    "nhis_pays_quarterly": result["nhis_pays"],
                    "patient_pays_quarterly": result["patient_pays"],
                    "burden_ratio": result["burden_ratio"],
                    "coverage_ratio": compute_coverage_adequacy_ratio(
                        gap_row["reimb_ceiling_quarterly"], monthly),
                })

        product_df = pd.DataFrame(product_rows)
        product_df["generated_at"] = date.today().isoformat()
        OUTPUT_PRODUCT_CSV.parent.mkdir(exist_ok=True)
        product_df.to_csv(OUTPUT_PRODUCT_CSV, index=False, encoding="utf-8")
        print(f"Written: {OUTPUT_PRODUCT_CSV} ({len(product_df)} rows)")
    except FileNotFoundError:
        print("Note: hira_treatment_materials.parquet not found — skipping product-level output")


if __name__ == "__main__":
    main()
