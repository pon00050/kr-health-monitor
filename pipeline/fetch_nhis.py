"""
NHIS statistical data extractor — file-based parsers.

All NHIS datasets are bulk file downloads, not REST APIs.

Usage:
    python pipeline/fetch_nhis.py [--checkup-csv PATH] [--ch06-dir DIR ...]
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.nhis_client import (
    parse_annual_diabetes_clinical_xlsx,
    parse_cgm_utilization_csv,
    parse_checkup_csv,
    parse_consumables_monthly_csv,
    parse_diabetes_utilization_rate_csvs,
    parse_insulin_claims_csv,
    parse_sigungu_t1d_t2d_xlsx,
    parse_t1d_age_annual_csv,
    parse_t1d_age_sex_csv,
    parse_t2d_sigungu_csv,
    parse_yearbook_ch06,
    parse_yoyangbi_registered_xlsx,
)
from src.config import DATA_SOURCE_DIR
from src.store import (
    find_annual_diabetes_info_xlsx,
    find_cgm_utilization_csv,
    find_checkup_csv,
    find_consumables_csvs,
    find_diabetes_utilization_csvs,
    find_insulin_claims_csv,
    find_sigungu_t1d_t2d_xlsx,
    find_t1d_age_annual_csv,
    find_t1d_csv,
    find_t2d_sigungu_csv,
    save_parquet,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

RAW_DIR = DATA_SOURCE_DIR

# Default yearbook ch06 paths — one Excel per year
_CH06_YEAR_DIRS = [
    (RAW_DIR / "2022_건강보험통계연보_본문", "06*.xlsx", 2022),
    (RAW_DIR / "2023 건강보험통계연보(수정)", "06*.xlsx", 2023),
    (RAW_DIR / "(본문 및 해설서)2024 건강보험통계연보" / "1. 본문", "06*.xlsx", 2024),
]

_CHECKUP_DEFAULT = find_checkup_csv(RAW_DIR)


def _head(df: pd.DataFrame, n: int | None) -> pd.DataFrame:
    """Return first n rows of df if n is set, else return df unchanged."""
    return df.head(n) if n else df


def find_ch06_paths() -> list[Path]:
    paths = []
    for d, pattern, _ in _CH06_YEAR_DIRS:
        if d.exists():
            found = list(d.glob(pattern))
            paths.extend(found)
    return paths


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract NHIS statistical data from local files")
    parser.add_argument(
        "--checkup-csv",
        type=Path,
        default=None,
        help="Path to 건강검진정보 CSV (auto-detected from Data/raw/ if not specified)",
    )
    parser.add_argument(
        "--skip-checkup",
        action="store_true",
        help="Skip checkup CSV parsing",
    )
    parser.add_argument(
        "--skip-yearbook",
        action="store_true",
        help="Skip yearbook ch06 parsing",
    )
    parser.add_argument(
        "--skip-t1d",
        action="store_true",
        help="Skip T1D age/sex patient count CSV parsing",
    )
    parser.add_argument(
        "--skip-consumables",
        action="store_true",
        help="Skip diabetes consumables monthly payment CSV parsing (all years)",
    )
    parser.add_argument(
        "--skip-cgm",
        action="store_true",
        help="Skip CGM utilization CSV parsing",
    )
    parser.add_argument(
        "--skip-yoyangbi",
        action="store_true",
        help="Skip 요양비 registered beneficiary pools XLSX parsing",
    )
    parser.add_argument(
        "--skip-t1d-annual",
        action="store_true",
        help="Skip T1D by 1-year age band annual CSV parsing",
    )
    parser.add_argument(
        "--skip-annual-clinical",
        action="store_true",
        help="Skip annual diabetes clinical (E10-E14 age-split) XLSX parsing",
    )
    parser.add_argument(
        "--skip-sigungu",
        action="store_true",
        help="Skip 시군구-level T1D+T2D XLSX parsing",
    )
    parser.add_argument(
        "--skip-t2d-sigungu",
        action="store_true",
        help="Skip T2D clinical by institution type per 시군구 CSV parsing",
    )
    parser.add_argument(
        "--skip-util-rate",
        action="store_true",
        help="Skip diabetes utilization rate CSV parsing (merged multi-file)",
    )
    parser.add_argument(
        "--skip-insulin",
        action="store_true",
        help="Skip insulin claims monthly CSV parsing",
    )
    parser.add_argument(
        "--sample",
        type=int,
        metavar="N",
        default=None,
        help="Limit each output parquet to N rows (smoke test mode)",
    )
    args = parser.parse_args()

    # ── Part A: Parse yearbook ch06 → nhis_annual_stats.parquet ──────────────
    if not args.skip_yearbook:
        ch06_paths = find_ch06_paths()
        if not ch06_paths:
            logger.warning("No ch06 yearbook files found in Data/raw/ — skipping")
        else:
            logger.info(f"Parsing {len(ch06_paths)} ch06 yearbook file(s)...")
            df_ch06 = parse_yearbook_ch06(ch06_paths)
            if not df_ch06.empty:
                df_ch06 = _head(df_ch06, args.sample)
                save_parquet(df_ch06, "nhis_annual_stats")
                logger.info(f"nhis_annual_stats.parquet: {len(df_ch06):,} rows")
                logger.info(f"ICD codes: {sorted(df_ch06['icd_code'].unique())}")
                logger.info(f"Years: {sorted(df_ch06['year'].unique())}")
            else:
                logger.warning("No data extracted from ch06 yearbooks")
                empty = pd.DataFrame(columns=["year", "icd_code", "patient_count",
                                               "visit_days", "cost_krw_thousands",
                                               "case_count", "source"])
                save_parquet(empty, "nhis_annual_stats")
    else:
        logger.info("Skipping yearbook parsing (--skip-yearbook)")

    # ── Part B: Parse checkup CSV → nhis_checkup_summary.parquet ─────────────
    if not args.skip_checkup:
        csv_path = args.checkup_csv or _CHECKUP_DEFAULT  # auto-detect if not provided
        if csv_path is None or not csv_path.exists():
            logger.warning(f"Checkup CSV not found: {csv_path}")
            logger.info("Download from data.go.kr dataset #15007122 to Data/raw/")
        else:
            logger.info(f"Parsing checkup CSV: {csv_path.name}")
            df_checkup = parse_checkup_csv(csv_path)
            if not df_checkup.empty:
                df_checkup = _head(df_checkup, args.sample)
                save_parquet(df_checkup, "nhis_checkup_summary")
                logger.info(f"nhis_checkup_summary.parquet: {len(df_checkup):,} rows")
            else:
                logger.warning("No data extracted from checkup CSV")
    else:
        logger.info("Skipping checkup parsing (--skip-checkup)")

    # ── Part C: T1D patient counts by age/sex (2021–2024) ────────────────────
    if not args.skip_t1d:
        t1d_path = find_t1d_csv(RAW_DIR)
        if t1d_path is None:
            logger.warning("T1D age/sex CSV not found in Data/raw/ — skipping")
            logger.info("Expected filename contains: 제1형 당뇨병 환자 수")
        else:
            logger.info(f"Parsing T1D age/sex CSV: {t1d_path.name}")
            df_t1d = parse_t1d_age_sex_csv(t1d_path)
            if not df_t1d.empty:
                df_t1d = _head(df_t1d, args.sample)
                save_parquet(df_t1d, "nhis_t1d_age_sex")
                logger.info(f"nhis_t1d_age_sex.parquet: {len(df_t1d):,} rows")
                logger.info(f"Years: {sorted(df_t1d['year'].dropna().unique().tolist())}")
            else:
                logger.warning("No data extracted from T1D age/sex CSV")
    else:
        logger.info("Skipping T1D age/sex parsing (--skip-t1d)")

    # ── Part D: Diabetes consumables monthly payments (all years: 2021–2024) ───
    if not args.skip_consumables:
        consumables_paths = find_consumables_csvs(RAW_DIR)
        if not consumables_paths:
            logger.warning("No consumables monthly CSVs found in Data/raw/ — skipping")
            logger.info("Expected filename(s) contain: 소모성재료")
        else:
            logger.info(f"Parsing {len(consumables_paths)} consumables CSV file(s)...")
            frames = [parse_consumables_monthly_csv(p) for p in consumables_paths]
            df_consumables = pd.concat(frames, ignore_index=True)
            if not df_consumables.empty:
                df_consumables = _head(df_consumables, args.sample)
                save_parquet(df_consumables, "nhis_consumables_monthly")
                logger.info(f"nhis_consumables_monthly.parquet: {len(df_consumables):,} rows")
                logger.info(f"Years: {sorted(df_consumables['year'].dropna().unique().tolist())}")
            else:
                logger.warning("No data extracted from consumables CSVs")
    else:
        logger.info("Skipping consumables parsing (--skip-consumables)")

    # ── Part E: CGM utilization (unique CGM users 2020–2024) ──────────────────
    if not args.skip_cgm:
        cgm_path = find_cgm_utilization_csv(RAW_DIR)
        if cgm_path is None:
            logger.warning("CGM utilization CSV not found in Data/raw/ — skipping")
            logger.info("Expected filename contains: 연속혈당측정")
        else:
            logger.info(f"Parsing CGM utilization CSV: {cgm_path.name}")
            df_cgm = parse_cgm_utilization_csv(cgm_path)
            if not df_cgm.empty:
                df_cgm = _head(df_cgm, args.sample)
                save_parquet(df_cgm, "nhis_cgm_utilization")
                logger.info(f"nhis_cgm_utilization.parquet: {len(df_cgm):,} rows")
                logger.info(f"Years: {sorted(df_cgm['year'].dropna().unique().tolist())}")
            else:
                logger.warning("No data extracted from CGM utilization CSV")
    else:
        logger.info("Skipping CGM utilization parsing (--skip-cgm)")

    # ── Parts F + H share the same XLSX file — find once ─────────────────────
    ann_path = find_annual_diabetes_info_xlsx(RAW_DIR)

    # ── Part F: Registered beneficiary pools (Sheet 2) ────────────────────────
    if not args.skip_yoyangbi:
        if ann_path is None:
            logger.warning("Annual diabetes info XLSX not found in Data/raw/ — skipping")
            logger.info("Expected filename contains: 연도별 당뇨병 진료정보")
        else:
            logger.info(f"Parsing 요양비 registered XLSX (Sheet 2): {ann_path.name}")
            df_reg = parse_yoyangbi_registered_xlsx(ann_path)
            if not df_reg.empty:
                df_reg = _head(df_reg, args.sample)
                save_parquet(df_reg, "nhis_yoyangbi_registered")
                logger.info(f"nhis_yoyangbi_registered.parquet: {len(df_reg):,} rows")
            else:
                logger.warning("No data extracted from 요양비 registered XLSX")
    else:
        logger.info("Skipping 요양비 registered parsing (--skip-yoyangbi)")

    # ── Part G: T1D by 1-year age band (2013–2023) ────────────────────────────
    if not args.skip_t1d_annual:
        t1d_ann_path = find_t1d_age_annual_csv(RAW_DIR)
        if t1d_ann_path is None:
            logger.warning("T1D age annual CSV not found in Data/raw/ — skipping")
            logger.info("Expected filename contains: 1형 당뇨병 연도별 연령별")
        else:
            logger.info(f"Parsing T1D age annual CSV: {t1d_ann_path.name}")
            df_t1d_ann = parse_t1d_age_annual_csv(t1d_ann_path)
            if not df_t1d_ann.empty:
                df_t1d_ann = _head(df_t1d_ann, args.sample)
                save_parquet(df_t1d_ann, "nhis_t1d_age_annual")
                logger.info(f"nhis_t1d_age_annual.parquet: {len(df_t1d_ann):,} rows")
                logger.info(f"Years: {sorted(df_t1d_ann['year'].dropna().unique().tolist())}")
            else:
                logger.warning("No data extracted from T1D age annual CSV")
    else:
        logger.info("Skipping T1D age annual parsing (--skip-t1d-annual)")

    # ── Part H: Annual diabetes clinical E10-E14 age-split (Sheet 1) ──────────
    if not args.skip_annual_clinical:
        if ann_path is None:
            logger.warning("Annual diabetes info XLSX not found — skipping clinical sheet")
        else:
            logger.info(f"Parsing annual diabetes clinical XLSX (Sheet 1): {ann_path.name}")
            df_clin = parse_annual_diabetes_clinical_xlsx(ann_path)
            if not df_clin.empty:
                df_clin = _head(df_clin, args.sample)
                save_parquet(df_clin, "nhis_e10_age_split")
                logger.info(f"nhis_e10_age_split.parquet: {len(df_clin):,} rows")
            else:
                logger.warning("No data extracted from annual diabetes clinical XLSX")
    else:
        logger.info("Skipping annual clinical parsing (--skip-annual-clinical)")

    # ── Part I: 시군구-level T1D+T2D (sex + age sheets) ───────────────────────
    if not args.skip_sigungu:
        sigungu_path = find_sigungu_t1d_t2d_xlsx(RAW_DIR)
        if sigungu_path is None:
            logger.warning("시군구 T1D+T2D XLSX not found in Data/raw/ — skipping")
            logger.info("Expected filename contains: 시군구 and 당뇨병")
        else:
            logger.info(f"Parsing 시군구 T1D+T2D XLSX: {sigungu_path.name}")
            df_sig = parse_sigungu_t1d_t2d_xlsx(sigungu_path)
            if not df_sig.empty:
                df_sig = _head(df_sig, args.sample)
                save_parquet(df_sig, "nhis_sigungu_t1d_t2d")
                logger.info(f"nhis_sigungu_t1d_t2d.parquet: {len(df_sig):,} rows")
                logger.info(f"Years: {sorted(df_sig['year'].dropna().unique().tolist())}")
            else:
                logger.warning("No data extracted from 시군구 T1D+T2D XLSX")
    else:
        logger.info("Skipping 시군구 T1D+T2D parsing (--skip-sigungu)")

    # ── Part J: T2D clinical by institution type per 시군구 (2021–2023) ────────
    if not args.skip_t2d_sigungu:
        t2d_sig_path = find_t2d_sigungu_csv(RAW_DIR)
        if t2d_sig_path is None:
            logger.warning("T2D 시군구 clinical CSV not found in Data/raw/ — skipping")
            logger.info("Expected filename contains: 2형 당뇨병 and 시군구")
        else:
            logger.info(f"Parsing T2D 시군구 clinical CSV: {t2d_sig_path.name}")
            df_t2d_sig = parse_t2d_sigungu_csv(t2d_sig_path)
            if not df_t2d_sig.empty:
                df_t2d_sig = _head(df_t2d_sig, args.sample)
                save_parquet(df_t2d_sig, "nhis_t2d_sigungu_clinical")
                logger.info(f"nhis_t2d_sigungu_clinical.parquet: {len(df_t2d_sig):,} rows")
                logger.info(f"Years: {sorted(df_t2d_sig['year'].dropna().unique().tolist())}")
            else:
                logger.warning("No data extracted from T2D 시군구 clinical CSV")
    else:
        logger.info("Skipping T2D 시군구 clinical parsing (--skip-t2d-sigungu)")

    # ── Part K: Diabetes utilization rate (merged multi-file, 2002–2024) ─────
    if not args.skip_util_rate:
        util_paths = find_diabetes_utilization_csvs(RAW_DIR)
        if not util_paths:
            logger.warning("No 당뇨병의료이용률 CSVs found in Data/raw/ — skipping")
            logger.info("Expected filenames contain: 당뇨병의료이용률")
        else:
            logger.info(f"Parsing {len(util_paths)} 당뇨병의료이용률 CSV file(s)...")
            df_util = parse_diabetes_utilization_rate_csvs(util_paths)
            if not df_util.empty:
                df_util = _head(df_util, args.sample)
                save_parquet(df_util, "nhis_diabetes_utilization_rate")
                logger.info(f"nhis_diabetes_utilization_rate.parquet: {len(df_util):,} rows")
                years = sorted(df_util["year"].dropna().unique().tolist())
                logger.info(f"Years: {years[0]}–{years[-1]} ({len(years)} distinct)")
            else:
                logger.warning("No data extracted from 당뇨병의료이용률 CSVs")
    else:
        logger.info("Skipping diabetes utilization rate parsing (--skip-util-rate)")

    # ── Part L: Insulin claims monthly (2016–2023) ────────────────────────────
    if not args.skip_insulin:
        ins_path = find_insulin_claims_csv(RAW_DIR)
        if ins_path is None:
            logger.warning("인슐린 주사 청구 CSV not found in Data/raw/ — skipping")
            logger.info("Expected filename contains: 인슐린 주사")
        else:
            logger.info(f"Parsing insulin claims CSV: {ins_path.name}")
            df_ins = parse_insulin_claims_csv(ins_path)
            if not df_ins.empty:
                df_ins = _head(df_ins, args.sample)
                save_parquet(df_ins, "nhis_insulin_claims_monthly")
                logger.info(f"nhis_insulin_claims_monthly.parquet: {len(df_ins):,} rows")
                logger.info(f"Years: {sorted(df_ins['year'].dropna().unique().tolist())}")
            else:
                logger.warning("No data extracted from insulin claims CSV")
    else:
        logger.info("Skipping insulin claims parsing (--skip-insulin)")

    logger.info("Done. Run `krh status` to verify outputs.")


if __name__ == "__main__":
    main()
