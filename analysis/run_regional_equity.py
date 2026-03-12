"""
Runner for regional_equity.py — bypasses Marimo UI.
Writes analysis/regional_equity.csv directly.

Column note:
  patient_share_pct  = regional_patients / national_total × 100
                       (each region's share of national diabetes patients)
                       DO NOT confuse with adoption_rate_registered in coverage_trend.csv,
                       which is cgm_users / t1d_registered × 100 (actual CGM uptake).

Usage:
    python analysis/run_regional_equity.py
"""

import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
from pathlib import Path

import pandas as pd
from datetime import date

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.equity import compute_disparity_index, score_regional_disparity
from src.store import load_parquet

OUTPUT_CSV = PROJECT_ROOT / "analysis" / "regional_equity.csv"

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


def _build_sigungu_equity(sigungu_df: pd.DataFrame, t2d_clinical_df: pd.DataFrame | None) -> pd.DataFrame:
    """Build 시군구-level equity rows from nhis_sigungu_t1d_t2d parquet.

    Uses the sex sheet (sheet='sex') and 전체(계) rows to get total T1D/T2D patient counts
    per 시군구 per year.

    Returns DataFrame with columns: year, 시도, 시군구, t1d_patients, t2d_patients,
    t1d_t2d_ratio, primary_care_pct (if t2d_clinical_df provided), granularity
    """
    # Use sex sheet, 전체 dimension (계 or 전체)
    sex = sigungu_df[sigungu_df["sheet"] == "sex"].copy()
    # Keep total rows: dimension '계' or '전체' means all-sex total
    total = sex[sex["dimension"].isin(["계", "전체", "합계"])].copy()
    if total.empty:
        # Fall back to summing all sex rows per group
        total = sex.copy()

    # Pivot by 구분 (당뇨병1형 / 당뇨병2형) → t1d/t2d columns
    grp = (total.groupby(["year", "시도", "시군구", "구분"])["patients"]
           .sum().reset_index())
    t1d = grp[grp["구분"].str.contains("1형", na=False)].copy().rename(
        columns={"patients": "t1d_patients"})
    t2d = grp[grp["구분"].str.contains("2형", na=False)].copy().rename(
        columns={"patients": "t2d_patients"})

    merged = t1d[["year", "시도", "시군구", "t1d_patients"]].merge(
        t2d[["year", "시도", "시군구", "t2d_patients"]],
        on=["year", "시도", "시군구"], how="outer"
    )
    merged["t1d_t2d_ratio"] = merged["t1d_patients"] / merged["t2d_patients"].replace(0, float("nan"))

    # Add primary_care_pct from T2D clinical data (의원 visits / total visits per 시군구)
    merged["primary_care_pct"] = None
    if t2d_clinical_df is not None and not t2d_clinical_df.empty:
        try:
            t2d_clin = t2d_clinical_df.copy()
            total_visits = (t2d_clin.groupby(["year", "sido", "sigungu"])["visit_count"]
                            .sum().reset_index().rename(columns={"visit_count": "total_visits"}))
            clinic_visits = (t2d_clin[t2d_clin["institution_type"] == "의원"]
                             .groupby(["year", "sido", "sigungu"])["visit_count"]
                             .sum().reset_index().rename(columns={"visit_count": "clinic_visits"}))
            pct_df = total_visits.merge(clinic_visits, on=["year", "sido", "sigungu"], how="left")
            pct_df["primary_care_pct"] = (
                pct_df["clinic_visits"].fillna(0) / pct_df["total_visits"].replace(0, float("nan"))
            )
            # Join on year + 시군구 name (시도 name may differ slightly)
            pct_df = pct_df.rename(columns={"sigungu": "시군구"})
            merged = merged.merge(pct_df[["year", "시군구", "primary_care_pct"]],
                                  on=["year", "시군구"], how="left",
                                  suffixes=("", "_new"))
            if "primary_care_pct_new" in merged.columns:
                merged["primary_care_pct"] = merged["primary_care_pct_new"].fillna(merged["primary_care_pct"])
                merged = merged.drop(columns=["primary_care_pct_new"])
        except Exception as e:
            print(f"  Warning: could not compute primary_care_pct: {e}")

    merged["granularity"] = "시군구"
    return merged


def main() -> None:
    print("Running regional variation analysis...")

    try:
        regional_raw = load_parquet("hira_regional_diabetes")
        print(f"Loaded hira_regional_diabetes: {len(regional_raw)} rows")
    except FileNotFoundError:
        print("Note: hira_regional_diabetes.parquet not found — using synthetic data")
        regional_raw = SYNTHETIC_REGIONAL

    # Enrich with glycemic severity data from NHIS checkup summary
    try:
        checkup = load_parquet("nhis_checkup_summary")
        checkup_latest = (
            checkup.sort_values("year")
            .groupby("region_code", as_index=False)
            .last()
            [["region_code", "mean_fasting_glucose", "high_glucose_rate_pct", "screened_count"]]
        )
        regional_raw = regional_raw.merge(checkup_latest, on="region_code", how="left")
        matched = regional_raw["mean_fasting_glucose"].notna().sum()
        print(f"Merged nhis_checkup_summary: {matched} region-year rows with glucose data")
    except FileNotFoundError:
        regional_raw["mean_fasting_glucose"] = None
        regional_raw["high_glucose_rate_pct"] = None
        regional_raw["screened_count"] = None
        print("Note: nhis_checkup_summary.parquet not found — glucose columns set to None")

    # ── 시도-level summary rows (backward-compatible) ─────────────────────────
    all_rows = []
    for year in sorted(regional_raw["year"].unique()):
        yr_df = regional_raw[regional_raw["year"] == year].copy()
        yr_df["patient_share_pct"] = (
            yr_df["patient_count"] / yr_df["patient_count"].sum() * 100
        ).round(2)
        yr_df["granularity"] = "시도"
        scored = score_regional_disparity(yr_df)
        all_rows.append(scored)

    sido_df = pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame()

    # ── 시군구-level rows (higher resolution, ~247 sub-regions × 3 years) ──────
    sigungu_rows = pd.DataFrame()
    try:
        sigungu_raw = load_parquet("nhis_sigungu_t1d_t2d")
        print(f"Loaded nhis_sigungu_t1d_t2d: {len(sigungu_raw)} rows")

        try:
            t2d_clinical = load_parquet("nhis_t2d_sigungu_clinical")
            print(f"Loaded nhis_t2d_sigungu_clinical: {len(t2d_clinical)} rows")
        except FileNotFoundError:
            t2d_clinical = None
            print("Note: nhis_t2d_sigungu_clinical.parquet not found — primary_care_pct will be None")

        sigungu_rows = _build_sigungu_equity(sigungu_raw, t2d_clinical)
        print(f"Built 시군구-level equity: {len(sigungu_rows)} rows "
              f"({sigungu_rows['시군구'].nunique()} unique 시군구)")
    except FileNotFoundError:
        print("Note: nhis_sigungu_t1d_t2d.parquet not found — 시군구-level rows omitted")
        print("      Run: python pipeline/fetch_nhis.py (without --skip-sigungu)")

    # ── Combine and write output ───────────────────────────────────────────────
    frames = [f for f in [sido_df, sigungu_rows] if not f.empty]
    output_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    output_df["generated_at"] = date.today().isoformat()
    OUTPUT_CSV.parent.mkdir(exist_ok=True)
    output_df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"Written: {OUTPUT_CSV} ({len(output_df)} rows)")

    # Summary stats for latest 시도-level year
    if not sido_df.empty:
        latest = sido_df[sido_df["year"] == sido_df["year"].max()]
        if not latest.empty:
            idx = compute_disparity_index(latest)
            print(f"\n시도 격차 지수 (최고/최저): {idx:.1f}×")
            cols = [c for c in ["region_name", "patient_share_pct", "share_pct_rank", "disparity_flag"]
                    if c in latest.columns]
            if cols:
                print(latest[cols].to_string(index=False))


if __name__ == "__main__":
    main()
