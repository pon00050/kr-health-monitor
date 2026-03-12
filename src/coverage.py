"""
Coverage adequacy index computations.

All monetary amounts:
  - 기준금액 (reimb_ceiling): stored as QUARTERLY (3-month) amounts
  - Market prices: stored as MONTHLY amounts
  - Functions convert between quarterly/monthly as needed and document the convention.
"""

from __future__ import annotations

import pandas as pd

from src.policy import (
    MARKET_PRICES_KRW,
    NHIS_REIMB_HISTORY,
    NHIS_REIMBURSEMENT_RATIO,
)


def get_reimb_ceiling(device_category: str, as_of_date: str) -> float:
    """Return the 기준금액 for device_category effective on as_of_date.

    For CGM: returns quarterly amount (₩210,000).
    For insulin_pump: returns per-device amount (₩1,700,000).

    Raises KeyError if device_category not in NHIS_REIMB_HISTORY.
    Raises ValueError if as_of_date is before any coverage entry.
    """
    entries = NHIS_REIMB_HISTORY[device_category]
    applicable = [(date, amount, gosi) for date, amount, gosi in entries if date <= as_of_date]
    if not applicable:
        raise ValueError(
            f"No 기준금액 entry for '{device_category}' on or before {as_of_date}. "
            f"Coverage start: {entries[0][0]}"
        )
    # Most recent entry on or before as_of_date
    applicable.sort(key=lambda x: x[0], reverse=True)
    return applicable[0][1]


def compute_quarterly_patient_burden(
    market_price_monthly_krw: float,
    reimb_ceiling_quarterly_krw: float,
) -> dict:
    """Compute patient burden for one quarter of CGM usage.

    Args:
        market_price_monthly_krw: Actual market price per month in KRW.
        reimb_ceiling_quarterly_krw: NHIS 기준금액 per quarter in KRW.

    Returns dict with:
        market_quarterly: market cost for 3 months
        nhis_pays: NHIS reimbursement for the quarter
        patient_pays: patient out-of-pocket for the quarter
        burden_ratio: patient_pays / market_quarterly
        patient_monthly_equiv: patient_pays / 3 (monthly equivalent)

    Mathematical guarantees:
        nhis_pays = min(market_quarterly, reimb_ceiling_quarterly) × 0.70
        For CGM at current prices (market always > ceiling):
            nhis_pays = 210,000 × 0.70 = 147,000 (fixed)
            patient_pays = market_quarterly - 147,000
            burden_ratio ∈ [0.68, 0.83] for ₩155K–₩280K/month
    """
    if market_price_monthly_krw <= 0:
        raise ValueError(f"market_price_monthly_krw must be positive, got {market_price_monthly_krw}")
    if reimb_ceiling_quarterly_krw <= 0:
        raise ValueError(f"reimb_ceiling_quarterly_krw must be positive, got {reimb_ceiling_quarterly_krw}")

    market_quarterly = market_price_monthly_krw * 3
    nhis_pays = min(market_quarterly, reimb_ceiling_quarterly_krw) * NHIS_REIMBURSEMENT_RATIO
    patient_pays = market_quarterly - nhis_pays
    burden_ratio = patient_pays / market_quarterly

    return {
        "market_quarterly": market_quarterly,
        "nhis_pays": nhis_pays,
        "patient_pays": patient_pays,
        "burden_ratio": burden_ratio,
        "patient_monthly_equiv": patient_pays / 3,
    }


def compute_coverage_adequacy_ratio(
    reimb_ceiling_quarterly: float,
    market_price_monthly: float,
) -> float:
    """Fraction of market cost that NHIS covers.

    = nhis_pays / market_quarterly
    Range for CGM: ~0.175–0.316 (coverage is very poor relative to market)
    """
    if market_price_monthly <= 0:
        raise ValueError("market_price_monthly must be positive")
    market_quarterly = market_price_monthly * 3
    nhis_pays = min(market_quarterly, reimb_ceiling_quarterly) * NHIS_REIMBURSEMENT_RATIO
    return nhis_pays / market_quarterly


def compute_gap_series(years: list[int], device_category: str = "cgm_sensor") -> pd.DataFrame:
    """Construct a coverage gap time series for the given years.

    Uses hardcoded 기준금액 history and market price ranges from config.

    Returns DataFrame with columns:
        year, reimb_ceiling_quarterly, market_price_monthly_low/mid/high,
        nhis_pays_quarterly, patient_pays_quarterly_low/mid/high,
        burden_ratio_low/mid/high, coverage_ratio_low/mid/high,
        patient_burden_ratio_mid (alias for burden_ratio_mid),
        patient_pays_quarterly_mid (alias for patient_pays_quarterly_mid)
    """
    price_tiers = MARKET_PRICES_KRW.get(device_category, {})
    if not price_tiers:
        raise ValueError(f"No market price data for device category: {device_category}")

    rows = []
    for year in years:
        as_of = f"{year}-12-31"
        try:
            ceiling = get_reimb_ceiling(device_category, as_of)
        except ValueError:
            # Before coverage started — no 기준금액
            ceiling = None

        row = {"year": year, "reimb_ceiling_quarterly": ceiling}

        for tier in ("low", "mid", "high"):
            monthly = price_tiers.get(tier)
            row[f"market_price_monthly_{tier}"] = monthly
            if ceiling is not None and monthly is not None:
                result = compute_quarterly_patient_burden(monthly, ceiling)
                row[f"nhis_pays_quarterly"] = result["nhis_pays"]  # same for all tiers when market > ceiling
                row[f"patient_pays_quarterly_{tier}"] = result["patient_pays"]
                row[f"burden_ratio_{tier}"] = result["burden_ratio"]
                row[f"coverage_ratio_{tier}"] = compute_coverage_adequacy_ratio(ceiling, monthly)
            else:
                row[f"patient_pays_quarterly_{tier}"] = None
                row[f"burden_ratio_{tier}"] = None
                row[f"coverage_ratio_{tier}"] = None

        rows.append(row)

    df = pd.DataFrame(rows)
    # Add convenience aliases expected by tests
    if "burden_ratio_mid" in df.columns:
        df["patient_burden_ratio_mid"] = df["burden_ratio_mid"]
    if "patient_pays_quarterly_mid" in df.columns:
        df["patient_pays_quarterly_mid"] = df["patient_pays_quarterly_mid"]
    return df


def compute_coverage_adequacy_index(df: pd.DataFrame) -> pd.DataFrame:
    """Batch compute coverage adequacy index for a DataFrame.

    Input DataFrame must have columns:
        reimb_ceiling_quarterly_krw, market_price_monthly_krw

    Adds columns: nhis_pays, patient_pays, burden_ratio, coverage_adequacy_ratio
    """
    required = {"reimb_ceiling_quarterly_krw", "market_price_monthly_krw"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    results = df.apply(
        lambda row: pd.Series(
            compute_quarterly_patient_burden(
                row["market_price_monthly_krw"],
                row["reimb_ceiling_quarterly_krw"],
            )
        ),
        axis=1,
    )
    out = df.copy()
    out["nhis_pays"] = results["nhis_pays"]
    out["patient_pays"] = results["patient_pays"]
    out["burden_ratio"] = results["burden_ratio"]
    out["coverage_adequacy_ratio"] = out.apply(
        lambda row: compute_coverage_adequacy_ratio(
            row["reimb_ceiling_quarterly_krw"], row["market_price_monthly_krw"]
        ),
        axis=1,
    )
    return out
