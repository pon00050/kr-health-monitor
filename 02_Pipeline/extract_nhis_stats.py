"""
NHIS statistical summary extractor.

Downloads NHIS annual statistical tables from data.go.kr.
NOTE: All NHIS datasets are bulk file downloads, not REST APIs.

Usage:
    python 02_Pipeline/extract_nhis_stats.py [--year-range 2018-2026]
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.nhis_client import download_publication_stats, get_claims_summary, get_checkup_stats
from src.storage import save_parquet

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def parse_year_range(s: str) -> list[int]:
    parts = s.split("-")
    if len(parts) == 2:
        return list(range(int(parts[0]), int(parts[1]) + 1))
    return [int(p) for p in parts]


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract NHIS statistical data")
    parser.add_argument("--year-range", default="2018-2026", help="Year range, e.g. 2018-2026")
    args = parser.parse_args()

    years = parse_year_range(args.year_range)
    logger.info(f"Fetching NHIS stats for years: {years}")

    all_rows: list[pd.DataFrame] = []

    for year in years:
        logger.info(f"  Fetching {year}...")
        df_checkup = get_checkup_stats(year)
        df_claims_t1d = get_claims_summary(year, "E10")
        df_claims_t2d = get_claims_summary(year, "E11")
        df_pubs = download_publication_stats(year)

        for df in [df_checkup, df_claims_t1d, df_claims_t2d, df_pubs]:
            if not df.empty:
                all_rows.append(df)

    if not all_rows:
        logger.warning("No NHIS data retrieved — API key may not be set or data unavailable")
        # Save an empty parquet so downstream steps can detect the gap
        empty = pd.DataFrame(columns=["year", "stat_category", "icd10_code", "region_code",
                                       "value", "unit", "source"])
        save_parquet(empty, "nhis_annual_stats")
        return

    combined = pd.concat(all_rows, ignore_index=True)
    # Normalize to standard schema
    for col in ["icd10_code", "region_code"]:
        if col not in combined.columns:
            combined[col] = None

    save_parquet(combined, "nhis_annual_stats")
    logger.info(f"nhis_annual_stats.parquet: {len(combined):,} rows")


if __name__ == "__main__":
    main()
