"""
kr-health-monitor pipeline orchestrator.

Usage:
    python 02_Pipeline/pipeline.py [--device cgm_sensor] [--year-range 2018-2026]
                                    [--sample N] [--sleep SECONDS] [--skip-analysis]
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def run_stage(name: str, cmd: list[str], sleep: float = 0.0) -> bool:
    """Run a pipeline stage subprocess. Returns True on success."""
    logger.info(f"[Stage] {name}")
    result = subprocess.run(cmd, cwd=str(PROJECT_ROOT))
    if result.returncode != 0:
        logger.error(f"Stage '{name}' failed with exit code {result.returncode}")
        return False
    if sleep > 0:
        time.sleep(sleep)
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="kr-health-monitor pipeline")
    parser.add_argument("--device", default="cgm_sensor", help="Device category to focus on")
    parser.add_argument("--year-range", default="2018-2026", help="Year range, e.g. 2018-2026")
    parser.add_argument("--sample", type=int, metavar="N", help="Sample N rows per extractor (smoke test)")
    parser.add_argument("--sleep", type=float, default=0.0, metavar="SECONDS",
                        help="Sleep between API calls (rate limiting)")
    parser.add_argument("--skip-analysis", action="store_true", help="Skip analysis runner scripts")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("kr-health-monitor pipeline starting")
    logger.info(f"  Device: {args.device}")
    logger.info(f"  Year range: {args.year_range}")
    if args.sample:
        logger.info(f"  Sample mode: {args.sample} rows")
    logger.info("=" * 60)

    py = sys.executable

    # Stage 1: HIRA regional diabetes Excel + treatment material API
    hira_cmd = [py, "02_Pipeline/extract_hira_devices.py"]
    if args.sample:
        hira_cmd += ["--sample", str(args.sample)]
    run_stage("1. Extract HIRA device data", hira_cmd, args.sleep)

    # Stage 2: MFDS device approvals
    mfds_cmd = [py, "02_Pipeline/extract_mfds_prices.py"]
    if args.sample:
        mfds_cmd += ["--sample", str(args.sample)]
    run_stage("2. Extract MFDS device prices", mfds_cmd, args.sleep)

    # Stage 3: NHIS stats
    nhis_cmd = [py, "02_Pipeline/extract_nhis_stats.py", "--year-range", args.year_range]
    run_stage("3. Extract NHIS stats", nhis_cmd, args.sleep)

    # Stage 4: Transform → coverage_master.parquet
    run_stage("4. Transform → coverage_master.parquet", [py, "02_Pipeline/transform.py"])

    # Stage 5 (optional): Analysis scripts
    if not args.skip_analysis:
        run_stage("5a. Coverage adequacy analysis", [py, "03_Analysis/run_coverage_analysis.py"])
        run_stage("5b. Regional variation analysis", [py, "03_Analysis/run_regional_variation.py"])
        run_stage("5c. Trend analysis", [py, "03_Analysis/run_trend_analysis.py"])

    logger.info("Pipeline complete. Run `krh status` to verify outputs.")


if __name__ == "__main__":
    main()
