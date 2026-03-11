"""
HIRA API wrapper — three separate data sources with different access patterns:

  1. Treatment Material API (data.go.kr #3074384 — REST API)
  2. Regional Diabetes Excel (opendata.hira.or.kr — file download)
  3. Medical Institution API (data.go.kr #15001699 — REST API)
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

from src.config import HIRA_INSTITUTION_BASE, HIRA_TREATMENT_MATERIAL_BASE, REGION_CODES

load_dotenv()
logger = logging.getLogger(__name__)

_DEFAULT_PAGE_SIZE = 100
_MAX_RETRIES = 3
_RETRY_DELAY = 2.0


def _get_api_key() -> str:
    key = os.getenv("HIRA_API_KEY")
    if not key:
        raise EnvironmentError(
            "HIRA_API_KEY not set. Register at data.go.kr and set the key in .env.\n"
            "Activation typically takes 1–2 hours after registration."
        )
    return key


def _paginate(url: str, params: dict, item_key: str = "item") -> list[dict]:
    """Fetch all pages from a HIRA REST endpoint.

    Args:
        url: API endpoint URL
        params: Base query parameters (without pageNo)
        item_key: Key in response JSON/XML that contains the item list

    Returns list of all item dicts across all pages.
    """
    all_items: list[dict] = []
    page_no = 1
    total_count = None

    while True:
        params["pageNo"] = page_no
        params["numOfRows"] = _DEFAULT_PAGE_SIZE

        for attempt in range(_MAX_RETRIES):
            try:
                resp = requests.get(url, params=params, timeout=30)
                resp.raise_for_status()
                break
            except requests.RequestException as e:
                if attempt == _MAX_RETRIES - 1:
                    raise
                logger.warning(f"Request failed (attempt {attempt + 1}/{_MAX_RETRIES}): {e}")
                time.sleep(_RETRY_DELAY)

        data = resp.json()

        # Unwrap common HIRA API response envelope
        body = data.get("response", data).get("body", data)
        if total_count is None:
            total_count = int(body.get("totalCount", 0))

        items = body.get("items", {})
        if isinstance(items, dict):
            items = items.get(item_key, [])
        if isinstance(items, dict):
            items = [items]  # single result — wrap in list
        if not items:
            break

        all_items.extend(items)

        if len(all_items) >= total_count:
            break
        page_no += 1

    return all_items


def get_cgm_material_info(api_key: str | None = None) -> pd.DataFrame:
    """Search HIRA treatment material database for CGM products.

    Uses HIRA Treatment Material API (data.go.kr #3074384).

    Returns DataFrame with columns:
        product_name, coverage_status (급여/비급여), max_unit_price_krw, m_code
    """
    key = api_key or _get_api_key()
    params = {
        "serviceKey": key,
        "type": "json",
        "searchNm": "연속혈당측정",
    }
    items = _paginate(HIRA_TREATMENT_MATERIAL_BASE, params)

    if not items:
        logger.warning("No CGM treatment material results returned — API may be inactive or params wrong")
        return pd.DataFrame(columns=["product_name", "coverage_status", "max_unit_price_krw", "m_code"])

    rows = []
    for item in items:
        rows.append({
            "product_name": item.get("itemNm", item.get("ITEM_NM", "")),
            "coverage_status": item.get("npayKdCd", item.get("NPAY_KD_CD", "")),
            "max_unit_price_krw": _safe_float(item.get("maxUnitPrice", item.get("MAX_UNIT_PRICE"))),
            "m_code": item.get("itemCd", item.get("ITEM_CD", "")),
        })
    return pd.DataFrame(rows)


def download_regional_diabetes_stats(output_dir: Path) -> Path:
    """Download 지역별 당뇨병 진료현황(2019–2023) Excel from HIRA opendata.

    No API key required for file downloads.

    Returns path to downloaded .xlsx file.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "hira_regional_diabetes_2019_2023.xlsx"

    if out_path.exists():
        logger.info(f"Excel already exists at {out_path} — skipping download")
        return out_path

    # HIRA Open Data portal direct download URL for SNO 13702
    url = "https://opendata.hira.or.kr/op/opc/olapHiraData.do"
    params = {
        "sno": "13702",
        "format": "xls",
    }
    logger.info(f"Downloading HIRA regional diabetes Excel (sno=13702)...")
    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()

    with open(out_path, "wb") as f:
        f.write(resp.content)
    logger.info(f"Saved to {out_path} ({len(resp.content) / 1024:.0f} KB)")
    return out_path


def parse_regional_diabetes_excel(xlsx_path: Path) -> pd.DataFrame:
    """Parse HIRA 지역별 당뇨병 진료현황 Excel.

    Expected format: columns for 시도, year, patient counts, claim counts, costs.
    Maps Korean region names → standard 2-digit 시도 codes.

    Returns DataFrame with columns:
        year, region_code, region_name, patient_count, claim_count, total_cost_krw
    """
    df_raw = pd.read_excel(xlsx_path, header=None, engine="openpyxl")

    # Reverse lookup: region_name → region_code
    name_to_code = {v: k for k, v in REGION_CODES.items()}

    rows = []
    # Excel parsing is structure-dependent; this is a best-effort parser
    # The actual structure must be verified after download
    for _, row in df_raw.iterrows():
        region_name = str(row.iloc[0]).strip()
        if region_name not in name_to_code:
            continue
        region_code = name_to_code[region_name]
        for year_offset, year in enumerate(range(2019, 2024)):
            try:
                patient_count = int(row.iloc[1 + year_offset * 3])
                claim_count = int(row.iloc[2 + year_offset * 3])
                total_cost_krw = float(row.iloc[3 + year_offset * 3])
                rows.append({
                    "year": year,
                    "region_code": region_code,
                    "region_name": region_name,
                    "patient_count": patient_count,
                    "claim_count": claim_count,
                    "total_cost_krw": total_cost_krw,
                })
            except (ValueError, IndexError):
                continue

    if not rows:
        logger.warning("parse_regional_diabetes_excel: no rows parsed — Excel structure may differ from expected")

    return pd.DataFrame(rows)


def get_facility_counts(region_code: str | None = None, api_key: str | None = None) -> pd.DataFrame:
    """Return endocrinology facility counts per region (diabetes clinic access proxy).

    Uses HIRA Medical Institution API (data.go.kr #15001699).

    Returns DataFrame with columns:
        region_code, region_name, facility_type, count
    """
    key = api_key or _get_api_key()
    params = {
        "serviceKey": key,
        "type": "json",
        "clCd": "11",  # 의원 (clinic) class code
    }
    if region_code:
        params["sidoCd"] = region_code

    items = _paginate(HIRA_INSTITUTION_BASE, params)

    rows = []
    for item in items:
        rows.append({
            "region_code": str(item.get("sidoCd", ""))[:2],
            "facility_type": item.get("clCdNm", ""),
            "count": int(item.get("yadmNm", 0) or 0),
        })

    df = pd.DataFrame(rows)
    if not df.empty and "region_code" in df.columns:
        df["region_name"] = df["region_code"].map(REGION_CODES).fillna("알수없음")
    return df


def _safe_float(value) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value).replace(",", ""))
    except (ValueError, TypeError):
        return None
