"""
krh — kr-health-monitor CLI.

Usage: krh [COMMAND] [OPTIONS]
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent


def _validate_environment() -> None:
    """Pre-flight check. Exits with code 1 and a clear message on failure."""
    import os
    # 1. .env presence
    env_path = PROJECT_ROOT / ".env"
    if not env_path.exists():
        sys.exit("Error: .env not found. Copy .env.example and populate API keys.")
    # 2. Required keys
    sys.path.insert(0, str(PROJECT_ROOT))
    from dotenv import dotenv_values
    env = dotenv_values(env_path)
    for key in ("HIRA_API_KEY", "MFDS_API_KEY"):
        if not env.get(key):
            sys.exit(f"Error: {key} not set in .env")
    # 3. Write access to data/processed/
    from src.store import PROCESSED_DIR
    try:
        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        probe = PROCESSED_DIR / ".write_probe"
        probe.touch()
        probe.unlink()
    except OSError as e:
        sys.exit(f"Error: Cannot write to {PROCESSED_DIR}: {e}")


def cmd_run(args: argparse.Namespace) -> None:
    """Run the full pipeline: extract -> transform -> analyze."""
    _validate_environment()
    cmd = [
        sys.executable, "pipeline/run.py",
        "--device", args.device,
        "--year-range", args.year_range,
        "--sleep", str(args.sleep),
    ]
    if args.sample:
        cmd += ["--sample", str(args.sample)]
    if args.skip_analysis:
        cmd += ["--skip-analysis"]
    subprocess.run(cmd, cwd=str(PROJECT_ROOT), check=True)


def cmd_status(args: argparse.Namespace) -> None:
    """Show parquet inventory (rows, sizes, dates)."""
    sys.path.insert(0, str(PROJECT_ROOT))
    from src.store import inventory
    inv = inventory()
    if inv.empty:
        print("No processed data found. Run `krh run` to populate.")
        return
    print(inv.to_string(index=False))
    if args.verbose:
        from src.config import DATA_SOURCES
        print("\nData sources:")
        for name, meta in DATA_SOURCES.items():
            print(f"  {name}: {meta['description']} ({meta['type']})")


def cmd_audit(args: argparse.Namespace) -> None:
    """Check pipeline freshness (stale outputs)."""
    sys.path.insert(0, str(PROJECT_ROOT))
    from src.freshness import run_audit
    exit_code = run_audit(verbose=args.verbose)
    sys.exit(exit_code)


def cmd_analyze(_args: argparse.Namespace) -> None:
    """Run all three analysis runner scripts."""
    scripts = [
        "analysis/run_coverage_gap.py",
        "analysis/run_regional_equity.py",
        "analysis/run_coverage_trend.py",
    ]
    for script in scripts:
        print(f"Running {script}...")
        subprocess.run([sys.executable, script], cwd=str(PROJECT_ROOT), check=True)
    print("Analysis complete.")


def cmd_report(args: argparse.Namespace) -> None:
    """Generate interactive HTML policy brief."""
    sys.path.insert(0, str(PROJECT_ROOT))
    from src.report import generate_report
    path = generate_report(github_repo=args.github_repo)
    print(f"Report generated: {path}")


def cmd_version(_args: argparse.Namespace) -> None:
    """Show version."""
    try:
        import importlib.metadata
        ver = importlib.metadata.version("kr-health-monitor")
    except Exception:
        pyproject = PROJECT_ROOT / "pyproject.toml"
        text = pyproject.read_text(encoding="utf-8")
        m = re.search(r'^version\s*=\s*"([^"]+)"', text, re.MULTILINE)
        ver = m.group(1) if m else "unknown"
    print(f"kr-health-monitor v{ver}")


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="krh",
        description="Korean NHIS health coverage monitor — CGM/T1D beachhead",
    )
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")
    sub.required = True

    # run
    p_run = sub.add_parser("run", help="Run the full pipeline: extract -> transform -> analyze")
    p_run.add_argument("--device", default="cgm_sensor", help="Device category to focus on")
    p_run.add_argument("--year-range", dest="year_range", default="2018-2026",
                       help="Year range (e.g. 2018-2026)")
    p_run.add_argument("--sample", type=int, default=None,
                       help="Sample N rows per extractor (smoke test)")
    p_run.add_argument("--sleep", type=float, default=0.0,
                       help="Sleep between API calls in seconds")
    p_run.add_argument("--skip-analysis", dest="skip_analysis", action="store_true",
                       help="Skip analysis runner scripts")
    p_run.set_defaults(func=cmd_run)

    # status
    p_status = sub.add_parser("status", help="Show parquet inventory (rows, sizes, dates)")
    p_status.add_argument("-v", "--verbose", action="store_true",
                          help="Show API source metadata")
    p_status.set_defaults(func=cmd_status)

    # audit
    p_audit = sub.add_parser("audit", help="Check pipeline freshness (stale outputs)")
    p_audit.add_argument("--verbose", action="store_true", help="Show all input mtimes")
    p_audit.set_defaults(func=cmd_audit)

    # analyze
    p_analyze = sub.add_parser("analyze", help="Run all three analysis runner scripts")
    p_analyze.set_defaults(func=cmd_analyze)

    # report
    p_report = sub.add_parser("report", help="Generate interactive HTML policy brief")
    p_report.add_argument(
        "--github-repo", dest="github_repo",
        default="pon00050/kr-health-monitor",
        help="GitHub repo slug for the CTA link (e.g. pon00050/kr-health-monitor)",
    )
    p_report.set_defaults(func=cmd_report)

    # version
    p_version = sub.add_parser("version", help="Show version")
    p_version.set_defaults(func=cmd_version)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
