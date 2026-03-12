"""
Runner for coverage_trend.py — bypasses Marimo UI.
Writes analysis/coverage_trend.csv directly.

Usage:
    python analysis/run_coverage_trend.py
"""

import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from datetime import date

from src.coverage import compute_gap_series
from src.store import load_parquet

OUTPUT_CSV = PROJECT_ROOT / "analysis" / "coverage_trend.csv"


def main() -> None:
    print("Running trend analysis...")
    years = list(range(2018, 2031))  # Include 2027–2030 projection years
    df = compute_gap_series(years, "cgm_sensor")

    # Enrich with national patient counts from NHIS annual stats
    try:
        nhis = load_parquet("nhis_annual_stats")
        t1d = nhis[nhis["icd_code"] == "E10"].groupby("year")["patient_count"].sum().reset_index()
        t1d.columns = ["year", "t1d_patient_count"]
        total = nhis.groupby("year")["patient_count"].sum().reset_index()
        total.columns = ["year", "national_diabetes_patients"]
        df = df.merge(t1d, on="year", how="left").merge(total, on="year", how="left")
        print(f"Merged nhis_annual_stats: {t1d['t1d_patient_count'].notna().sum()} T1D year rows")
    except FileNotFoundError:
        df["t1d_patient_count"] = None
        df["national_diabetes_patients"] = None
        print("Note: nhis_annual_stats.parquet not found — patient count columns set to None")

    # Supplement t1d_patient_count from nhis_t1d_age_sex (fills 2021 + 2024 gaps)
    try:
        t1d_age_sex = load_parquet("nhis_t1d_age_sex")
        # Aggregate: sum non-suppressed patient_count per year
        t1d_agg = (
            t1d_age_sex[~t1d_age_sex["suppressed"]]
            .groupby("year")["patient_count"]
            .sum()
            .reset_index()
        )
        t1d_agg.columns = ["year", "t1d_count_direct"]
        # Left merge; fill only where t1d_patient_count is currently NaN
        df = df.merge(t1d_agg, on="year", how="left")
        if "t1d_patient_count" not in df.columns:
            df["t1d_patient_count"] = df["t1d_count_direct"]
        else:
            df["t1d_patient_count"] = df["t1d_patient_count"].where(
                df["t1d_patient_count"].notna(), df["t1d_count_direct"]
            )
        df = df.drop(columns=["t1d_count_direct"])
        print(f"Supplemented t1d_patient_count from nhis_t1d_age_sex: "
              f"{df['t1d_patient_count'].notna().sum()} year rows now populated")
    except FileNotFoundError:
        print("Note: nhis_t1d_age_sex.parquet not found — run pipeline/fetch_nhis.py to generate")

    # Add consumables annual aggregate (2024 only)
    # NOTE: scope is T1D + T2D (소모성재료), NOT CGM-only. transaction_count ≠ unique beneficiaries.
    # See memory/new_datasets_2026_03_12.md for full interpretive caveats.
    try:
        consumables = load_parquet("nhis_consumables_monthly")
        consumables_annual = (
            consumables.groupby("year")
            .agg(
                consumables_transactions_annual=("transaction_count", "sum"),
                consumables_payment_annual_won=("payment_won", "sum"),
            )
            .reset_index()
        )
        df = df.merge(consumables_annual, on="year", how="left")
        print(f"Merged consumables annual aggregate: "
              f"{consumables_annual['year'].tolist()} year(s) populated")
        print("  WARNING: consumables figures cover T1D + T2D (소모성재료), not CGM-only.")
        print("  transaction_count ≠ unique beneficiaries. 2024 data only.")
    except FileNotFoundError:
        df["consumables_transactions_annual"] = None
        df["consumables_payment_annual_won"] = None
        print("Note: nhis_consumables_monthly.parquet not found — run pipeline/fetch_nhis.py to generate")

    # Merge 4 — CGM utilization (unique CGM users 2020–2024)
    try:
        cgm_util = load_parquet("nhis_cgm_utilization")
        df = df.merge(cgm_util[["year", "cgm_users"]], on="year", how="left")
        print(f"Merged nhis_cgm_utilization: "
              f"{df['cgm_users'].notna().sum()} year rows with CGM user counts")
    except FileNotFoundError:
        df["cgm_users"] = None
        print("Note: nhis_cgm_utilization.parquet not found — run pipeline/fetch_nhis.py")

    # Merge 5 — Registered 요양비 beneficiary denominator (2019–2024)
    # adoption_rate_registered = cgm_users / t1d_registered  ← PREFERRED METRIC
    #   Interpretation: of eligible T1D beneficiaries, how many actually use CGM
    # adoption_rate_total = cgm_users / t1d_patient_count  ← broader denominator
    #   Includes T1D patients who are not 요양비 registered (less eligible)
    try:
        yoyangbi = load_parquet("nhis_yoyangbi_registered")
        df = df.merge(yoyangbi[["year", "t1d_registered", "t2d_registered"]], on="year", how="left")
        if "cgm_users" in df.columns and "t1d_registered" in df.columns:
            df["adoption_rate_registered"] = (
                df["cgm_users"] / df["t1d_registered"]
            ).where(df["t1d_registered"].notna() & df["cgm_users"].notna())
            df["adoption_rate_total"] = (
                df["cgm_users"] / df["t1d_patient_count"]
            ).where(df["t1d_patient_count"].notna() & df["cgm_users"].notna())
        print(f"Merged nhis_yoyangbi_registered: "
              f"{df['t1d_registered'].notna().sum()} year rows with registered beneficiary counts")
        print("  NOTE: adoption_rate_registered (cgm/t1d_registered) is the preferred metric")
    except FileNotFoundError:
        df["t1d_registered"] = None
        df["t2d_registered"] = None
        df["adoption_rate_registered"] = None
        df["adoption_rate_total"] = None
        print("Note: nhis_yoyangbi_registered.parquet not found — run pipeline/fetch_nhis.py")

    # Merge 6 — Eligibility pools from 1-year age granularity (T1D age annual, 2013–2023)
    # eligible_19plus: T1D patients age ≥ 19 → eligibility criterion under 고시 2022-170
    # eligible_15plus: T1D patients age ≥ 15 → eligibility criterion under 고시 2024-226
    # CAVEAT: pre-2018 values reflect ICD coding drift — do not compare pre-2018 to post-2020
    try:
        t1d_ann = load_parquet("nhis_t1d_age_annual")
        non_supp = t1d_ann[~t1d_ann["suppressed"]].copy()
        elig_19 = (non_supp[non_supp["age"] >= 19]
                   .groupby("year")["patients"].sum().reset_index()
                   .rename(columns={"patients": "eligible_19plus"}))
        elig_15 = (non_supp[non_supp["age"] >= 15]
                   .groupby("year")["patients"].sum().reset_index()
                   .rename(columns={"patients": "eligible_15plus"}))
        df = df.merge(elig_19, on="year", how="left").merge(elig_15, on="year", how="left")
        print(f"Merged nhis_t1d_age_annual: "
              f"{df['eligible_19plus'].notna().sum()} year rows with eligibility pool data")
        print("  NOTE: pre-2018 values reflect ICD coding drift artifact")
    except FileNotFoundError:
        df["eligible_19plus"] = None
        df["eligible_15plus"] = None
        print("Note: nhis_t1d_age_annual.parquet not found — run pipeline/fetch_nhis.py")

    output_cols = [
        "year", "reimb_ceiling_quarterly",
        "market_price_monthly_low", "market_price_monthly_mid", "market_price_monthly_high",
        "burden_ratio_low", "burden_ratio_mid", "burden_ratio_high",
        "coverage_ratio_low", "coverage_ratio_mid", "coverage_ratio_high",
        "t1d_patient_count", "national_diabetes_patients",
        "consumables_transactions_annual", "consumables_payment_annual_won",
        "cgm_users", "t1d_registered", "t2d_registered",
        "adoption_rate_registered", "adoption_rate_total",
        "eligible_19plus", "eligible_15plus",
    ]
    available = [c for c in output_cols if c in df.columns]
    out = df[available].copy()
    out["generated_at"] = date.today().isoformat()

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
