"""
Parquet I/O utilities and DuckDB helpers.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

# Default processed data directory — resolved relative to this file's project root
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = _PROJECT_ROOT / "data" / "processed"
RAW_DIR = _PROJECT_ROOT / "data" / "raw"


def save_parquet(df: pd.DataFrame, name: str, subdir: str = "processed") -> Path:
    """Save DataFrame as parquet to data/{subdir}/{name}.parquet."""
    base = PROCESSED_DIR if subdir == "processed" else RAW_DIR
    base.mkdir(parents=True, exist_ok=True)
    path = base / f"{name}.parquet"
    df.to_parquet(path, index=False)
    size_kb = path.stat().st_size / 1024
    logger.info(f"Saved {name}.parquet — {len(df):,} rows × {len(df.columns)} cols ({size_kb:.1f} KB)")
    return path


def load_parquet(name: str, subdir: str = "processed") -> pd.DataFrame:
    """Load parquet from data/{subdir}/{name}.parquet.

    Raises FileNotFoundError with a helpful message if the file does not exist.
    """
    base = PROCESSED_DIR if subdir == "processed" else RAW_DIR
    path = base / f"{name}.parquet"
    if not path.exists():
        raise FileNotFoundError(
            f"Parquet not found: {path}\n"
            f"Run the relevant extractor first (e.g., python pipeline/fetch_hira.py)"
        )
    return pd.read_parquet(path)


def duckdb_query(sql: str, **named_parquets: Path) -> pd.DataFrame:
    """Execute SQL against named parquet files via in-memory DuckDB.

    Usage:
        duckdb_query(
            "SELECT * FROM master WHERE year = 2023",
            master=Path("data/processed/coverage_master.parquet"),
        )
    """
    con = duckdb.connect()
    for alias, path in named_parquets.items():
        con.execute(f"CREATE VIEW {alias} AS SELECT * FROM read_parquet('{path}')")
    result = con.execute(sql).df()
    con.close()
    return result


def find_checkup_csv(data_dir: Path) -> Path | None:
    """Find the NHIS 건강검진정보 CSV in data_dir using os.listdir.

    Uses os.listdir instead of Path.exists() to avoid a Windows cp1252 bug where
    Path("Korean string").exists() returns False even when the file is present.

    Args:
        data_dir: Root raw data directory (e.g. project_root / "Data" / "raw").

    Returns the Path to the .CSV file, or None if not found.
    """
    try:
        for folder in os.listdir(str(data_dir)):
            if "20241231" in folder:
                folder_path = data_dir / folder
                try:
                    for fname in os.listdir(str(folder_path)):
                        if fname.upper().endswith(".CSV"):
                            return folder_path / fname
                except OSError:
                    continue
    except OSError:
        pass
    return None


def find_t1d_csv(data_dir: Path) -> Path | None:
    """Find the NHIS 제1형 당뇨병 환자 수 CSV in data_dir using os.listdir.

    Uses os.listdir instead of Path.exists() to avoid a Windows cp1252 bug where
    Path("Korean string").exists() returns False even when the file is present.

    Args:
        data_dir: Root raw data directory (e.g. project_root / "Data" / "raw").

    Returns the Path to the CSV file, or None if not found.
    """
    try:
        for fname in os.listdir(str(data_dir)):
            if "제1형 당뇨병 환자 수" in fname and fname.upper().endswith(".CSV"):
                return data_dir / fname
    except OSError:
        pass
    return None


def find_consumables_csvs(data_dir: Path) -> list[Path]:
    """Return all 소모성재료 CSVs found in data_dir, sorted by filename (chronological).

    Uses os.listdir instead of Path.glob() to avoid a Windows cp1252 bug where
    Path("Korean string").exists() returns False even when the file is present.

    Args:
        data_dir: Root raw data directory (e.g. project_root / "Data" / "raw").

    Returns sorted list of matching CSV paths (may be empty).
    """
    results = []
    try:
        for fname in os.listdir(str(data_dir)):
            if "소모성재료" in fname and fname.upper().endswith(".CSV"):
                results.append(data_dir / fname)
    except OSError:
        pass
    return sorted(results, key=lambda p: p.name)


def find_consumables_csv(data_dir: Path) -> Path | None:
    """Find the first NHIS 소모성재료 CSV in data_dir.

    Convenience wrapper around find_consumables_csvs() for single-file callers.
    Use find_consumables_csvs() when all years are needed.

    Returns the Path to the first matching CSV, or None if not found.
    """
    results = find_consumables_csvs(data_dir)
    return results[0] if results else None


def find_cgm_utilization_csv(data_dir: Path) -> Path | None:
    """Find the NHIS CGM utilization CSV in data_dir.

    Matches filenames containing '연속혈당측정' (CGM sensor).

    Returns the Path to the CSV file, or None if not found.
    """
    try:
        for fname in os.listdir(str(data_dir)):
            if "연속혈당측정" in fname and fname.upper().endswith(".CSV"):
                return data_dir / fname
    except OSError:
        pass
    return None


def find_annual_diabetes_info_xlsx(data_dir: Path) -> Path | None:
    """Find the NHIS 연도별 당뇨병 진료정보 XLSX in data_dir.

    Contains 3 sheets: E10-E14 by age (2010-2023), 요양비 registered patients (2019-2024),
    diabetes device users (2020-2024).

    Uses the unique phrase '연도별 당뇨병 진료정보' to avoid matching other XLSX files
    (e.g., HIRA regional diabetes Excel or sigungu utilization XLSX).

    Returns the Path to the XLSX file, or None if not found.
    """
    try:
        for fname in os.listdir(str(data_dir)):
            if "연도별 당뇨병 진료정보" in fname and fname.upper().endswith(".XLSX"):
                return data_dir / fname
    except OSError:
        pass
    return None


def find_t1d_age_annual_csv(data_dir: Path) -> Path | None:
    """Find the NHIS T1D 1-year age granularity CSV in data_dir.

    Matches filenames containing '1형 당뇨병 연도별 연령별'.

    Returns the Path to the CSV file, or None if not found.
    """
    try:
        for fname in os.listdir(str(data_dir)):
            if "1형 당뇨병" in fname and "연령별" in fname and fname.upper().endswith(".CSV"):
                return data_dir / fname
    except OSError:
        pass
    return None


def find_sigungu_t1d_t2d_xlsx(data_dir: Path) -> Path | None:
    """Find the NHIS 시군구-level T1D+T2D XLSX in data_dir.

    Matches filenames containing '시군구별' and '당뇨병'.

    Returns the Path to the XLSX file, or None if not found.
    """
    try:
        for fname in os.listdir(str(data_dir)):
            if "시군구" in fname and "당뇨병" in fname and fname.upper().endswith(".XLSX"):
                return data_dir / fname
    except OSError:
        pass
    return None


def find_t2d_sigungu_csv(data_dir: Path) -> Path | None:
    """Find the NHIS T2D clinical by institution type per 시군구 CSV in data_dir.

    Matches filenames containing '2형 당뇨병' and '시군구'.

    Returns the Path to the CSV file, or None if not found.
    """
    try:
        for fname in os.listdir(str(data_dir)):
            if "2형 당뇨병" in fname and "시군구" in fname and fname.upper().endswith(".CSV"):
                return data_dir / fname
    except OSError:
        pass
    return None


def find_diabetes_utilization_csvs(data_dir: Path) -> list[Path]:
    """Return all 당뇨병의료이용률 CSVs found in data_dir, sorted by filename.

    Matches filenames containing '당뇨병의료이용률' (NHIS diabetes medical utilization
    rate series, 2002–2024). Six overlapping files cover different date ranges;
    the caller is expected to merge them via parse_diabetes_utilization_rate_csvs().

    Returns sorted list of matching CSV paths (may be empty).
    """
    results = []
    try:
        for fname in os.listdir(str(data_dir)):
            if "당뇨병의료이용률" in fname and fname.upper().endswith(".CSV"):
                results.append(data_dir / fname)
    except OSError:
        pass
    return sorted(results, key=lambda p: p.name)


def find_insulin_claims_csv(data_dir: Path) -> Path | None:
    """Find the NHIS 인슐린 주사 청구건수 CSV in data_dir.

    Matches filenames containing '인슐린 주사'. Monthly insulin injection claims
    by age group, 2016–2023.

    Returns the Path to the CSV file, or None if not found.
    """
    try:
        for fname in os.listdir(str(data_dir)):
            if "인슐린 주사" in fname and fname.upper().endswith(".CSV"):
                return data_dir / fname
    except OSError:
        pass
    return None


def inventory() -> pd.DataFrame:
    """Return a table of all parquets in data/processed/."""
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
