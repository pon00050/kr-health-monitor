"""
Pipeline freshness checker — used by `krh audit`.

Compares mtime of output parquets against their declared inputs.
Stale = any input is newer than its output.
"""

from __future__ import annotations

import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = _PROJECT_ROOT / "data" / "processed"

# DAG: output_name → [list of input names] (parquet stems, no extension)
# Empty list = refreshed from external API, always consider fresh unless missing
AUDIT_DAG: dict[str, list[str]] = {
    "hira_regional_diabetes": [],
    "hira_treatment_materials": [],
    "mfds_device_prices": [],
    "nhis_annual_stats": [],
    "nhis_diabetes_utilization_rate": [],
    "nhis_insulin_claims_monthly": [],
    "coverage_master": [
        "hira_regional_diabetes",
        "mfds_device_prices",
        "nhis_annual_stats",
    ],
}

# Analysis CSV outputs → parquet dependencies
ANALYSIS_DAG: dict[str, list[str]] = {
    "coverage_gap": ["coverage_master"],
    "regional_equity": ["coverage_master"],
    "coverage_trend": ["coverage_master"],
}


def _mtime(name: str, is_csv: bool = False) -> float | None:
    if is_csv:
        analysis_dir = _PROJECT_ROOT / "analysis"
        path = analysis_dir / f"{name}.csv"
    else:
        path = PROCESSED_DIR / f"{name}.parquet"
    if not path.exists():
        return None
    return path.stat().st_mtime


def check_freshness(verbose: bool = False) -> dict[str, bool]:
    """Check freshness of all DAG outputs.

    Returns dict mapping output_name → is_fresh.
    is_fresh = False if output is missing or any input is newer than output.
    """
    results: dict[str, bool] = {}

    all_outputs = list(AUDIT_DAG.items()) + [(k, v) for k, v in ANALYSIS_DAG.items()]

    for output_name, input_names in all_outputs:
        is_csv = output_name in ANALYSIS_DAG
        output_mtime = _mtime(output_name, is_csv=is_csv)

        if output_mtime is None:
            results[output_name] = False
            if verbose:
                print(f"  [MISSING] {output_name}")
            continue

        if not input_names:
            # No local inputs — only check that file exists
            results[output_name] = True
            if verbose:
                import datetime
                ts = datetime.datetime.fromtimestamp(output_mtime).strftime("%Y-%m-%d %H:%M")
                print(f"  [FRESH  ] {output_name}  (last updated: {ts})")
            continue

        stale = False
        for inp in input_names:
            inp_mtime = _mtime(inp)
            if inp_mtime is None:
                stale = True
                if verbose:
                    print(f"  [STALE  ] {output_name}  (input missing: {inp})")
                break
            if inp_mtime > output_mtime:
                stale = True
                if verbose:
                    print(f"  [STALE  ] {output_name}  (input {inp} is newer)")
                break

        if not stale and verbose:
            import datetime
            ts = datetime.datetime.fromtimestamp(output_mtime).strftime("%Y-%m-%d %H:%M")
            print(f"  [FRESH  ] {output_name}  (last updated: {ts})")

        results[output_name] = not stale

    return results


def run_audit(verbose: bool = False) -> int:
    """Print freshness table and return exit code (0 = all fresh, 1 = stale)."""
    print("kr-health-monitor pipeline freshness check")
    print("=" * 50)

    freshness = check_freshness(verbose=verbose)

    if not freshness:
        print("  No pipeline outputs found. Run `krh run` to populate data.")
        return 0  # Empty = no stale outputs technically

    n_stale = sum(1 for v in freshness.values() if not v)
    n_fresh = sum(1 for v in freshness.values() if v)

    if not verbose:
        for name, fresh in freshness.items():
            status = "FRESH  " if fresh else "STALE  "
            print(f"  [{status}] {name}")

    print()
    if n_stale == 0:
        print(f"All {n_fresh} outputs are up to date.")
        return 0
    else:
        print(f"{n_stale} stale output(s) found. Run `krh run` to refresh.")
        return 1
