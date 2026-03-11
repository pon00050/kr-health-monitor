"""
MFDS 의료기기정보포털 client (data.go.kr dataset 15057456).

CRITICAL: The MFDS API contains NO price data.
Market prices are hardcoded constants from verified research in src/config.py.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import date

import pandas as pd
import requests
from dotenv import load_dotenv

from src.config import MARKET_PRICES_KRW, MFDS_API_BASE, MFDS_DEVICE_LIST_ENDPOINT

load_dotenv()
logger = logging.getLogger(__name__)

_DEFAULT_PAGE_SIZE = 100
_MAX_RETRIES = 3
_RETRY_DELAY = 2.0


def _get_api_key() -> str:
    key = os.getenv("MFDS_API_KEY")
    if not key:
        raise EnvironmentError(
            "MFDS_API_KEY not set. Register at data.go.kr and set the key in .env."
        )
    return key


def _parse_date(yyyymmdd: str) -> date | None:
    if not yyyymmdd or len(str(yyyymmdd)) != 8:
        return None
    s = str(yyyymmdd)
    try:
        return date(int(s[:4]), int(s[4:6]), int(s[6:8]))
    except ValueError:
        return None


def search_devices(keyword: str, api_key: str | None = None) -> pd.DataFrame:
    """Search MFDS device approval list by keyword.

    Endpoint: getMdlpPrdlstPrmisnList04 (data.go.kr #15057456)
    No price data is available — this is by design.

    Returns DataFrame with columns:
        device_name, manufacturer, approved_date, approval_number, grade, model
    """
    key = api_key or _get_api_key()
    url = MFDS_API_BASE + MFDS_DEVICE_LIST_ENDPOINT

    all_items: list[dict] = []
    page_no = 1
    total_count = None

    while True:
        params = {
            "serviceKey": key,
            "pageNo": page_no,
            "numOfRows": _DEFAULT_PAGE_SIZE,
            "prduct": keyword,
            "type": "json",
        }

        for attempt in range(_MAX_RETRIES):
            try:
                resp = requests.get(url, params=params, timeout=30)
                resp.raise_for_status()
                break
            except requests.RequestException as e:
                if attempt == _MAX_RETRIES - 1:
                    raise
                logger.warning(f"MFDS request failed (attempt {attempt + 1}): {e}")
                time.sleep(_RETRY_DELAY)

        data = resp.json()
        body = data.get("response", data).get("body", data)

        if total_count is None:
            total_count = int(body.get("totalCount", 0))
            logger.info(f"MFDS search '{keyword}': {total_count} total results")

        items = body.get("items", {})
        if isinstance(items, dict):
            items = items.get("item", [])
        if isinstance(items, dict):
            items = [items]
        if not items:
            break

        all_items.extend(items)
        if len(all_items) >= total_count:
            break
        page_no += 1

    if not all_items:
        return pd.DataFrame(columns=["device_name", "manufacturer", "approved_date",
                                      "approval_number", "grade", "model"])

    rows = []
    for item in all_items:
        rows.append({
            "device_name": item.get("PRDUCT", ""),
            "manufacturer": item.get("ENTRPS", ""),
            "approved_date": _parse_date(item.get("PRMISN_DT", "")),
            "approval_number": item.get("PRDUCT_PRMISN_NO", ""),
            "grade": item.get("GRADE", ""),
            "model": item.get("TYPE_NAME", ""),
        })
    return pd.DataFrame(rows)


def get_cgm_devices(api_key: str | None = None) -> pd.DataFrame:
    """Search MFDS for CGM devices and annotate with hardcoded market prices.

    Returns DataFrame with columns:
        device_name, manufacturer, model, approved_date, device_category,
        list_price_krw (always null — not in MFDS),
        market_price_low, market_price_mid, market_price_high
    """
    key = api_key or _get_api_key()
    df = search_devices("연속혈당측정", api_key=key)

    df["device_category"] = "cgm_sensor"
    df["list_price_krw"] = None  # MFDS has no price data — always null

    # Annotate with hardcoded market price ranges from verified research
    prices = MARKET_PRICES_KRW["cgm_sensor"]
    df["market_price_low"] = prices["low"]
    df["market_price_mid"] = prices["mid"]
    df["market_price_high"] = prices["high"]

    return df
