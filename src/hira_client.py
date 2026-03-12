"""
HIRA API wrapper — three separate data sources with different access patterns:

  1. Treatment Material API (data.go.kr #3074384 — REST API, XML response)
  2. Regional Diabetes Excel (opendata.hira.or.kr — file download)
  3. Medical Institution API (data.go.kr #15001698 — REST API, XML response)

Both REST APIs return XML. The original scaffold incorrectly assumed JSON.
"""

from __future__ import annotations

import logging
import os
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

from src.config import (
    HIRA_INSTITUTION_BASE,
    HIRA_INSTITUTION_ENDPOINT,
    HIRA_TREATMENT_MATERIAL_BASE,
    HIRA_TREATMENT_MATERIAL_ENDPOINT,
    REGION_CODE_REVERSE,
    REGION_CODES,
)

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


def _parse_xml_response(text: str, item_key: str = "item") -> tuple[int, list[dict]]:
    """Parse HIRA XML response envelope.

    Both HIRA REST APIs return XML with this envelope:
      <response>
        <header><resultCode>00</resultCode></header>
        <body>
          <items><item>...</item></items>
          <totalCount>N</totalCount>
        </body>
      </response>

    Returns (total_count, list of item dicts).
    """
    root = ET.fromstring(text)

    total_el = root.find(".//totalCount")
    total_count = int(total_el.text) if total_el is not None and total_el.text else 0

    items = []
    for item_el in root.findall(f".//{item_key}"):
        item = {child.tag: child.text for child in item_el}
        items.append(item)

    return total_count, items


def _paginate(url: str, params: dict, item_key: str = "item") -> list[dict]:
    """Fetch all pages from a HIRA REST endpoint (XML).

    Args:
        url: Full API endpoint URL (base + endpoint path)
        params: Base query parameters (without pageNo/numOfRows)
        item_key: XML tag name for individual item elements

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

        count, items = _parse_xml_response(resp.text, item_key)

        if total_count is None:
            total_count = count
            logger.info(f"Total records: {total_count}")

        if not items:
            break

        all_items.extend(items)

        if len(all_items) >= total_count:
            break
        page_no += 1

    if total_count is not None and len(all_items) < total_count:
        logger.warning(
            f"_paginate: fetched {len(all_items)} items but totalCount={total_count}. "
            f"Possible pagination issue — re-run or increase numOfRows."
        )
    else:
        logger.info(f"_paginate: fetched all {len(all_items)} items (totalCount={total_count})")

    return all_items


def get_cgm_material_info(api_key: str | None = None) -> pd.DataFrame:
    """Search HIRA treatment material database for CGM products.

    Uses HIRA 치료재료정보조회서비스 (data.go.kr #3074384).
    Response format: XML.

    Returns DataFrame with columns:
        product_name, coverage_status (급여/비급여), max_unit_price_krw, m_code
    """
    key = api_key or _get_api_key()
    url = HIRA_TREATMENT_MATERIAL_BASE + HIRA_TREATMENT_MATERIAL_ENDPOINT
    params = {
        "serviceKey": key,
        # Search by 중분류코드 900085 = 연속혈당측정용전극 — gets ALL CGM items reliably.
        # NOTE: itmNm search requires English text (product names are stored in English).
        # mdivCd is more stable than keyword search.
        "mdivCd": "900085",
    }
    items = _paginate(url, params)

    if not items:
        logger.warning("No CGM treatment material results returned — API may be inactive or params wrong")
        return pd.DataFrame(columns=["product_name", "coverage_status", "max_unit_price_krw", "m_code"])

    rows = []
    for item in items:
        rows.append({
            "product_name": item.get("itmNm", ""),
            "coverage_status": item.get("payTpNm", ""),       # 급여구분: 급여/비급여/급여중지/삭제
            "max_unit_price_krw": _safe_float(item.get("mxUnprc")),  # 상한단가 (None for 비급여)
            "m_code": item.get("mcatCd", ""),                 # 치료재료코드 (e.g. BM0600EC)
            "importer": item.get("impEntpNm", ""),            # 수입업체명
            "manufacturer": item.get("mnfEntpNm", ""),        # 제조업체명
            "subcategory": item.get("mdivCdNm", ""),          # 중분류명
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

    Actual file structure (verified 2026-03-11):
      Sheets: 작성기준, 시도별, 시군구별, 시군구별 및 성별, 시군구별 및 연령별, ...
      시도별 sheet: rows = region, cols = year groups (2019–2023),
                   each year = 요양급여비용, 입내원일수, 환자수

    NOTE: The original scaffold parser assumed a flat row structure which does not
    match the actual file. This function uses the 시도별 sheet only.
    Maps Korean region names → standard 2-digit 시도 codes.

    Returns DataFrame with columns:
        year, region_code, region_name, patient_count, visit_days,
        cost_krw_thousands, icd_scope, source
    """

    df_raw = pd.read_excel(xlsx_path, sheet_name="시도별", header=None, engine="openpyxl")

    # Find header row: contains '시도' in first column and year values
    header_row = None
    for i, row in df_raw.iterrows():
        if "시도" in str(row.iloc[0]) or any("2019" in str(v) for v in row.values):
            header_row = i
            break

    if header_row is None:
        logger.warning("parse_regional_diabetes_excel: could not find header row")
        return pd.DataFrame(columns=["year", "region_code", "region_name", "patient_count",
                                     "visit_days", "cost_krw_thousands", "icd_scope", "source"])

    years = [2019, 2020, 2021, 2022, 2023]
    rows = []

    for _, row in df_raw.iloc[header_row + 2:].iterrows():
        region_name = str(row.iloc[0]).strip()
        if region_name not in REGION_CODE_REVERSE:
            continue
        region_code = REGION_CODE_REVERSE[region_name]
        for year_offset, year in enumerate(years):
            try:
                # Each year occupies 3 columns: 요양급여비용(원), 입내원일수, 환자수
                base_col = 1 + year_offset * 3
                cost_won = float(row.iloc[base_col])
                visit_days = int(row.iloc[base_col + 1])
                patient_count = int(row.iloc[base_col + 2])
                rows.append({
                    "year": year,
                    "region_code": region_code,
                    "region_name": region_name,
                    "patient_count": patient_count,
                    "visit_days": visit_days,
                    "cost_krw_thousands": cost_won / 1000,
                })
            except (ValueError, IndexError):
                continue

    if not rows:
        logger.warning("parse_regional_diabetes_excel: no rows parsed — verify sheet structure")

    df = pd.DataFrame(rows)
    df["icd_scope"] = "E10-E14"
    df["source"] = "hira_regional_diabetes_2024"
    return df


def get_facility_counts(region_code: str | None = None, api_key: str | None = None) -> pd.DataFrame:
    """Return facility records per region from HIRA 병원정보서비스.

    Uses HIRA 병원정보서비스 (data.go.kr #15001698).
    Response format: XML. Returns one record per facility (not aggregate counts).

    Returns DataFrame with columns:
        region_code, region_name, facility_name, facility_type, address
    """
    key = api_key or _get_api_key()
    url = HIRA_INSTITUTION_BASE + HIRA_INSTITUTION_ENDPOINT
    params = {
        "serviceKey": key,
        "clCd": "11",  # 의원 (clinic) class code
    }
    if region_code:
        params["sidoCd"] = region_code

    items = _paginate(url, params)

    rows = []
    for item in items:
        rows.append({
            "region_code": str(item.get("sidoCd", ""))[:2],
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["region_code", "region_name", "facility_count"])

    counts = df.groupby("region_code").size().reset_index(name="facility_count")
    counts["facility_count"] = counts["facility_count"].astype(int)
    counts["region_name"] = counts["region_code"].map(REGION_CODES).fillna("알수없음")
    return counts[["region_code", "region_name", "facility_count"]]


def _safe_float(value) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value).replace(",", ""))
    except (ValueError, TypeError):
        return None
