"""
Transform: join all extracted parquets and derive coverage analytics.

Usage:
    python pipeline/build_master.py
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import REGION_CODES
from src.policy import MARKET_PRICES_KRW, NHIS_REIMB_HISTORY
from src.coverage import (
    compute_coverage_adequacy_index,
    compute_quarterly_patient_burden,
    get_reimb_ceiling,
)
from src.equity import score_regional_disparity
from src.store import load_parquet, save_parquet

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def _load_or_empty(name: str, columns: list[str]) -> pd.DataFrame:
    try:
        return load_parquet(name)
    except FileNotFoundError:
        logger.warning(f"{name}.parquet not found — using empty DataFrame")
        return pd.DataFrame(columns=columns)


def main() -> None:
    logger.info("Loading input parquets...")

    regional = _load_or_empty(
        "hira_regional_diabetes",
        ["year", "region_code", "region_name", "patient_count", "visit_days",
         "cost_krw_thousands", "icd_scope", "source"],
    )
    mfds = _load_or_empty(
        "mfds_device_prices",
        ["device_name", "manufacturer", "approved_date", "device_category",
         "market_price_low", "market_price_mid", "market_price_high"],
    )
    nhis = _load_or_empty(
        "nhis_annual_stats",
        ["year", "icd_code", "patient_count", "visit_days",
         "cost_krw_thousands", "case_count", "source"],
    )

    if regional.empty:
        logger.warning("No regional data — coverage_master will be built from hardcoded constants only")

    # Build coverage rows from hardcoded 기준금액 + market price ranges
    rows = []

    # Use CGM as primary device category
    device_category = "cgm_sensor"
    prices = MARKET_PRICES_KRW[device_category]

    # If we have regional data, produce one row per (year, region, tier)
    if not regional.empty:
        for _, reg_row in regional.iterrows():
            year = reg_row["year"]
            region_code = str(reg_row["region_code"])
            as_of = f"{year}-12-31"

            try:
                ceiling = get_reimb_ceiling(device_category, as_of)
            except ValueError:
                ceiling = None

            for tier in ("low", "mid", "high"):
                monthly = prices[tier]
                burden = None
                if ceiling is not None:
                    burden = compute_quarterly_patient_burden(monthly, ceiling)

                rows.append({
                    "year": year,
                    "region_code": region_code,
                    "region_name": reg_row.get("region_name", REGION_CODES.get(region_code, "")),
                    "device_category": device_category,
                    "patient_count": reg_row.get("patient_count"),
                    "claim_count": reg_row.get("cost_krw_thousands"),  # renamed in new schema
                    "reimb_ceiling_quarterly_krw": ceiling,
                    "market_price_monthly_krw": monthly,
                    "price_tier": tier,
                    "nhis_pays": burden["nhis_pays"] if burden else None,
                    "patient_burden_krw": burden["patient_pays"] if burden else None,
                    "burden_ratio": burden["burden_ratio"] if burden else None,
                    "coverage_adequacy_ratio": burden["nhis_pays"] / (monthly * 3) if burden else None,
                })
    else:
        # Fallback: synthetic rows from hardcoded constants for 2022–2026
        for year in range(2022, 2027):
            as_of = f"{year}-12-31"
            try:
                ceiling = get_reimb_ceiling(device_category, as_of)
            except ValueError:
                ceiling = None

            for tier in ("low", "mid", "high"):
                monthly = prices[tier]
                burden = None
                if ceiling is not None:
                    burden = compute_quarterly_patient_burden(monthly, ceiling)

                rows.append({
                    "year": year,
                    "region_code": None,
                    "region_name": None,
                    "device_category": device_category,
                    "patient_count": None,
                    "claim_count": None,
                    "reimb_ceiling_quarterly_krw": ceiling,
                    "market_price_monthly_krw": monthly,
                    "price_tier": tier,
                    "nhis_pays": burden["nhis_pays"] if burden else None,
                    "patient_burden_krw": burden["patient_pays"] if burden else None,
                    "burden_ratio": burden["burden_ratio"] if burden else None,
                    "coverage_adequacy_ratio": burden["nhis_pays"] / (monthly * 3) if burden else None,
                })

    master = pd.DataFrame(rows)

    # Add regional disparity scores (only if we have regional data with adoption_rate_pct)
    if "adoption_rate_pct" in master.columns:
        master = score_regional_disparity(master)
    else:
        master["adoption_rate_pct"] = None
        master["adoption_pct_rank"] = None
        master["national_median_ratio"] = None
        master["disparity_flag"] = None

    save_parquet(master, "coverage_master")
    logger.info(f"coverage_master.parquet: {len(master):,} rows × {len(master.columns)} cols")
    logger.info(f"Columns: {list(master.columns)}")


if __name__ == "__main__":
    main()
