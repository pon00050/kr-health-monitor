"""
krh — kr-health-monitor CLI.

Usage: krh [COMMAND] [OPTIONS]
"""

from __future__ import annotations

import sys
from pathlib import Path

import typer

app = typer.Typer(
    name="krh",
    help="Korean NHIS health coverage monitor — CGM/T1D beachhead",
    add_completion=False,
)

PROJECT_ROOT = Path(__file__).resolve().parent


@app.command()
def run(
    device: str = typer.Option("cgm_sensor", help="Device category to focus on"),
    year_range: str = typer.Option("2018-2026", "--year-range", help="Year range (e.g. 2018-2026)"),
    sample: int = typer.Option(None, help="Sample N rows per extractor (smoke test)"),
    sleep: float = typer.Option(0.0, help="Sleep between API calls (rate limiting, seconds)"),
    skip_analysis: bool = typer.Option(False, "--skip-analysis", help="Skip analysis runner scripts"),
) -> None:
    """Run the full pipeline: extract -> transform -> analyze."""
    import subprocess
    cmd = [
        sys.executable, "02_Pipeline/pipeline.py",
        "--device", device,
        "--year-range", year_range,
        "--sleep", str(sleep),
    ]
    if sample:
        cmd += ["--sample", str(sample)]
    if skip_analysis:
        cmd += ["--skip-analysis"]
    subprocess.run(cmd, cwd=str(PROJECT_ROOT), check=True)


@app.command()
def status(
    verbose: bool = typer.Option(False, "-v", "--verbose", help="Show API source metadata"),
) -> None:
    """Show parquet inventory (rows, sizes, dates)."""
    sys.path.insert(0, str(PROJECT_ROOT))
    from src.storage import inventory
    inv = inventory()
    if inv.empty:
        typer.echo("No processed data found. Run `krh run` to populate.")
        return
    typer.echo(inv.to_string(index=False))
    if verbose:
        from src.config import DATA_SOURCES
        typer.echo("\nData sources:")
        for name, meta in DATA_SOURCES.items():
            typer.echo(f"  {name}: {meta['description']} ({meta['type']})")


@app.command()
def audit(
    verbose: bool = typer.Option(False, "--verbose", help="Show all input mtimes"),
) -> None:
    """Check pipeline freshness (stale outputs)."""
    sys.path.insert(0, str(PROJECT_ROOT))
    from src.audit import run_audit
    exit_code = run_audit(verbose=verbose)
    raise typer.Exit(exit_code)


@app.command()
def analyze() -> None:
    """Run all three analysis runner scripts."""
    import subprocess
    scripts = [
        "03_Analysis/run_coverage_analysis.py",
        "03_Analysis/run_regional_variation.py",
        "03_Analysis/run_trend_analysis.py",
    ]
    for script in scripts:
        typer.echo(f"Running {script}...")
        subprocess.run([sys.executable, script], cwd=str(PROJECT_ROOT), check=True)
    typer.echo("Analysis complete.")


@app.command()
def report(
    device: str = typer.Option("cgm_sensor", help="Device category (e.g. cgm_sensor)"),
    region: str = typer.Option(None, help="Region code for regional report (e.g. 11 for Seoul)"),
) -> None:
    """Generate HTML coverage gap report."""
    sys.path.insert(0, str(PROJECT_ROOT))
    from src.coverage_index import compute_gap_series
    from src.report import generate_device_report, generate_regional_report

    if region:
        path = generate_regional_report(region)
    else:
        gap_df = compute_gap_series(list(range(2018, 2027)), device)
        path = generate_device_report(device, gap_df=gap_df)

    typer.echo(f"Report generated: {path}")


@app.command()
def version() -> None:
    """Show version."""
    try:
        import importlib.metadata
        ver = importlib.metadata.version("kr-health-monitor")
    except Exception:
        # Fallback: read from pyproject.toml
        import re
        pyproject = PROJECT_ROOT / "pyproject.toml"
        text = pyproject.read_text(encoding="utf-8")
        m = re.search(r'^version\s*=\s*"([^"]+)"', text, re.MULTILINE)
        ver = m.group(1) if m else "unknown"
    typer.echo(f"kr-health-monitor v{ver}")


if __name__ == "__main__":
    app()
