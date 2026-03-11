"""
Parquet I/O utilities and DuckDB helpers.
"""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

# Default processed data directory — resolved relative to this file's project root
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = _PROJECT_ROOT / "01_Data" / "processed"
RAW_DIR = _PROJECT_ROOT / "01_Data" / "raw"


def save_parquet(df: pd.DataFrame, name: str, subdir: str = "processed") -> Path:
    """Save DataFrame as parquet to 01_Data/{subdir}/{name}.parquet."""
    base = PROCESSED_DIR if subdir == "processed" else RAW_DIR
    base.mkdir(parents=True, exist_ok=True)
    path = base / f"{name}.parquet"
    df.to_parquet(path, index=False)
    size_kb = path.stat().st_size / 1024
    logger.info(f"Saved {name}.parquet — {len(df):,} rows × {len(df.columns)} cols ({size_kb:.1f} KB)")
    return path


def load_parquet(name: str, subdir: str = "processed") -> pd.DataFrame:
    """Load parquet from 01_Data/{subdir}/{name}.parquet.

    Raises FileNotFoundError with a helpful message if the file does not exist.
    """
    base = PROCESSED_DIR if subdir == "processed" else RAW_DIR
    path = base / f"{name}.parquet"
    if not path.exists():
        raise FileNotFoundError(
            f"Parquet not found: {path}\n"
            f"Run the relevant extractor first (e.g., python 02_Pipeline/extract_hira_devices.py)"
        )
    return pd.read_parquet(path)


def duckdb_query(sql: str, **named_parquets: Path) -> pd.DataFrame:
    """Execute SQL against named parquet files via in-memory DuckDB.

    Usage:
        duckdb_query(
            "SELECT * FROM master WHERE year = 2023",
            master=Path("01_Data/processed/coverage_master.parquet"),
        )
    """
    con = duckdb.connect()
    for alias, path in named_parquets.items():
        con.execute(f"CREATE VIEW {alias} AS SELECT * FROM read_parquet('{path}')")
    result = con.execute(sql).df()
    con.close()
    return result


def inventory() -> pd.DataFrame:
    """Return a table of all parquets in 01_Data/processed/."""
    if not PROCESSED_DIR.exists():
        return pd.DataFrame(columns=["name", "rows", "cols", "size_mb", "mtime"])

    rows = []
    for p in sorted(PROCESSED_DIR.glob("*.parquet")):
        try:
            df = pd.read_parquet(p)
            rows.append({
                "name": p.stem,
                "rows": len(df),
                "cols": len(df.columns),
                "size_mb": round(p.stat().st_size / 1_048_576, 3),
                "mtime": pd.Timestamp(p.stat().st_mtime, unit="s").isoformat(),
            })
        except Exception as e:
            rows.append({
                "name": p.stem,
                "rows": -1,
                "cols": -1,
                "size_mb": round(p.stat().st_size / 1_048_576, 3),
                "mtime": pd.Timestamp(p.stat().st_mtime, unit="s").isoformat(),
            })
    return pd.DataFrame(rows)
