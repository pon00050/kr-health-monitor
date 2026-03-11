"""
MFDS device approval list extractor.

Searches for CGM and related devices; annotates with hardcoded market prices.
NOTE: No price data is available from the MFDS API — prices are from verified research.

Usage:
    python 02_Pipeline/extract_mfds_prices.py [--keyword 연속혈당] [--sample N]
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.mfds_client import get_cgm_devices, search_devices
from src.storage import save_parquet

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract MFDS device approval data")
    parser.add_argument("--keyword", default="연속혈당측정", help="Search keyword (default: 연속혈당측정)")
    parser.add_argument("--sample", type=int, metavar="N", help="Limit results to N rows (smoke test)")
    args = parser.parse_args()

    logger.info(f"Searching MFDS for: {args.keyword}")

    try:
        df = get_cgm_devices()
    except EnvironmentError as e:
        logger.error(str(e))
        sys.exit(1)
    except Exception as e:
        logger.error(f"MFDS API error: {e}")
        sys.exit(1)

    if df.empty:
        logger.warning("No results returned from MFDS API")
        return

    if args.sample:
        df = df.head(args.sample)
        logger.info(f"Sample mode: truncated to {args.sample} rows")

    save_parquet(df, "mfds_device_prices")
    logger.info(f"mfds_device_prices.parquet: {len(df):,} rows")
    logger.info(f"Columns: {list(df.columns)}")
    if not df.empty:
        logger.info(f"Sample:\n{df.head(3).to_string()}")


if __name__ == "__main__":
    main()
