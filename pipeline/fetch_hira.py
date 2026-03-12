"""
HIRA device data extractor — two distinct data pulls:

  Part A: HIRA Excel download (지역별 당뇨병 진료현황 2019–2023)
  Part B: HIRA Treatment Material API (CGM product M-codes + coverage status)

Usage:
    python 02_Pipeline/extract_hira_devices.py [--skip-excel] [--skip-api] [--sample N]
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Allow running from project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.hira_client import (
    download_regional_diabetes_stats,
    get_cgm_material_info,
    parse_regional_diabetes_excel,
)
from src.storage import save_parquet

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

RAW_DIR = Path(__file__).resolve().parent.parent / "01_Data" / "raw"


def extract_regional_diabetes(output_dir: Path) -> None:
    """Part A: Download and parse HIRA regional diabetes Excel."""
    logger.info("Part A: HIRA regional diabetes Excel (sno=13702)")
    try:
        xlsx_path = download_regional_diabetes_stats(output_dir)
    except Exception as e:
        logger.error(f"Excel download failed: {e}")
        logger.info("If the download URL has changed, check opendata.hira.or.kr for sno=13702")
        return

    try:
        df = parse_regional_diabetes_excel(xlsx_path)
    except Exception as e:
        logger.error(f"Excel parsing failed: {e}")
        logger.info("The Excel structure may differ from expected — check the file manually")
        return

    if df.empty:
        logger.warning("No data parsed from Excel — skipping parquet save")
        return

    save_parquet(df, "hira_regional_diabetes")
    logger.info(f"hira_regional_diabetes.parquet: {len(df):,} rows")
    logger.info(f"Columns: {list(df.columns)}")
    logger.info(f"Years: {sorted(df['year'].unique())}")
    logger.info(f"Regions: {df['region_code'].nunique()} 시도")


def extract_treatment_materials(sample: int | None = None) -> None:
    """Part B: HIRA Treatment Material API (CGM coverage status + M-codes)."""
    logger.info("Part B: HIRA Treatment Material API (#3074384)")
    try:
        df = get_cgm_material_info()
    except EnvironmentError as e:
        logger.error(str(e))
        return
    except Exception as e:
        logger.error(f"Treatment material API failed: {e}")
        return

    if df.empty:
        logger.warning("No treatment material data returned — API may require activation")
        return

    if sample:
        df = df.head(sample)
        logger.info(f"Sample mode: truncated to {sample} rows")

    save_parquet(df, "hira_treatment_materials")
    logger.info(f"hira_treatment_materials.parquet: {len(df):,} rows")
    if not df.empty:
        logger.info(f"First result:\n{df.iloc[0].to_dict()}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract HIRA device data")
    parser.add_argument("--skip-excel", action="store_true", help="Skip the Excel download (Part A)")
    parser.add_argument("--skip-api", action="store_true", help="Skip the Treatment Material API (Part B)")
    parser.add_argument("--sample", type=int, metavar="N", help="Limit API results to N rows (smoke test)")
    args = parser.parse_args()

    if not args.skip_excel:
        extract_regional_diabetes(RAW_DIR)

    if not args.skip_api:
        extract_treatment_materials(sample=args.sample)

    logger.info("Done. Run `krh status` to verify outputs.")


if __name__ == "__main__":
    main()
