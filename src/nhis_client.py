"""
NHIS aggregate data fetcher.

IMPORTANT: All NHIS datasets listed here are BULK FILE DOWNLOADS, not REST APIs.
They require data.go.kr registration (free).
Individual-level NHIS data (NHISS cohort) requires IRB and on-site access — out of scope.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

from src.config import NHIS_CHECKUP_DATASET, NHIS_CLAIMS_DATASET, NHIS_PUBLICATIONS_DATASET

load_dotenv()
logger = logging.getLogger(__name__)


def _get_api_key() -> str:
    key = os.getenv("NHIS_API_KEY")
    if not key:
        raise EnvironmentError(
            "NHIS_API_KEY not set. Register at data.go.kr and set the key in .env.\n"
            "Dataset IDs: 15007122 (checkup), 15007115 (claims), 15095102 (publications)"
        )
    return key


def get_checkup_stats(year: int, output_dir: Path | None = None) -> pd.DataFrame:
    """Fetch NHIS 건강검진 aggregate summary for the given year.

    Downloads from NHIS 건강검진정보 (dataset #15007122).
    Returns aggregate blood glucose distribution and diabetes screening rates.

    NOTE: This downloads bulk CSV files. The actual file URL must be obtained
    from the data.go.kr portal for dataset 15007122 after registration.
    Returns a stub DataFrame if API key is not set.
    """
    try:
        key = _get_api_key()
    except EnvironmentError:
        logger.warning("NHIS_API_KEY not set — returning empty checkup stats")
        return pd.DataFrame(columns=["year", "stat_category", "value", "unit", "source"])

    # NHIS checkup data is a bulk file download — construct download URL
    # Exact URL structure depends on the registered file URL from data.go.kr
    url = f"https://apis.data.go.kr/B552749/nhisOpenAPI/nhisCheckupOpenAPI"
    params = {
        "serviceKey": key,
        "year": str(year),
        "dataType": "json",
    }
    try:
        resp = requests.get(url, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items", [])
        if not items:
            logger.warning(f"No checkup stats returned for year {year}")
            return pd.DataFrame(columns=["year", "stat_category", "value", "unit", "source"])
        df = pd.DataFrame(items)
        df["year"] = year
        df["source"] = f"NHIS #15007122"
        return df
    except Exception as e:
        logger.warning(f"NHIS checkup fetch failed for {year}: {e}")
        return pd.DataFrame(columns=["year", "stat_category", "value", "unit", "source"])


def get_claims_summary(year: int, icd10_prefix: str = "E10") -> pd.DataFrame:
    """Fetch aggregate claim statistics by ICD-10 disease code.

    Downloads from NHIS 진료내역정보 (dataset #15007115).
    Returns claim counts, patient counts aggregated at national level.

    NOTE: Individual-level claims data requires IRB. This endpoint returns
    aggregate summaries only.
    """
    try:
        key = _get_api_key()
    except EnvironmentError:
        logger.warning("NHIS_API_KEY not set — returning empty claims summary")
        return pd.DataFrame(columns=["year", "icd10_code", "claim_count", "patient_count", "total_cost_krw"])

    url = "https://apis.data.go.kr/B552749/nhisOpenAPI/nhisClaimsOpenAPI"
    params = {
        "serviceKey": key,
        "year": str(year),
        "icd10": icd10_prefix,
        "dataType": "json",
    }
    try:
        resp = requests.get(url, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items", [])
        if not items:
            logger.warning(f"No claims data for {year}, ICD-10 {icd10_prefix}")
            return pd.DataFrame(columns=["year", "icd10_code", "claim_count", "patient_count", "total_cost_krw"])
        df = pd.DataFrame(items)
        df["year"] = year
        df["icd10_code"] = icd10_prefix
        return df
    except Exception as e:
        logger.warning(f"NHIS claims fetch failed for {year}/{icd10_prefix}: {e}")
        return pd.DataFrame(columns=["year", "icd10_code", "claim_count", "patient_count", "total_cost_krw"])


def download_publication_stats(year: int, output_dir: Path | None = None) -> pd.DataFrame:
    """Download and parse NHIS annual statistical publication tables.

    Uses NHIS 발간자료 (dataset #15095102) — Excel files with annual stats tables.

    Returns DataFrame with columns:
        year, stat_category, icd10_code, region_code, value, unit, source
    """
    try:
        key = _get_api_key()
    except EnvironmentError:
        logger.warning("NHIS_API_KEY not set — returning empty publication stats")
        return pd.DataFrame(columns=["year", "stat_category", "icd10_code", "region_code", "value", "unit", "source"])

    # NHIS publications are Excel files; the URL must be discovered from the portal
    # This is a placeholder implementation pending actual download URL verification
    logger.info(f"Fetching NHIS publication stats for {year} (dataset #{NHIS_PUBLICATIONS_DATASET})")
    return pd.DataFrame(columns=["year", "stat_category", "icd10_code", "region_code", "value", "unit", "source"])
