"""
End-to-end output quality gates.
All tests run without live API calls.
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.coverage_index import (
    compute_coverage_adequacy_ratio,
    compute_gap_series,
    compute_quarterly_patient_burden,
)


# ── Patient burden acceptance criteria ────────────────────────────────────────

def test_cgm_patient_burden_exceeds_65_pct_even_cheapest():
    """Even at cheapest market price (₩155K/month), burden > 65%."""
    result = compute_quarterly_patient_burden(155_000, 210_000)
    assert result["burden_ratio"] > 0.65


def test_cgm_patient_burden_below_90_pct():
    """Sanity check: burden < 90% (NHIS does pay something)."""
    result = compute_quarterly_patient_burden(280_000, 210_000)
    assert result["burden_ratio"] < 0.90


def test_nhis_pays_same_regardless_of_market_price_above_ceiling():
    """Once market exceeds 기준금액, NHIS pays a fixed ₩147K regardless of market price."""
    result_155 = compute_quarterly_patient_burden(155_000, 210_000)
    result_280 = compute_quarterly_patient_burden(280_000, 210_000)
    # Both markets exceed the quarterly ceiling of ₩210K: 155×3=465K > 210K; 280×3=840K > 210K
    assert result_155["nhis_pays"] == result_280["nhis_pays"] == 147_000


def test_gap_series_burden_constant_2022_to_2024():
    """기준금액 unchanged 2022–2024; ceiling must be 210,000 for all post-coverage years."""
    series = compute_gap_series([2022, 2023, 2024], "cgm_sensor")
    # All three years should have the same ceiling (2022-08-01 entry applies)
    assert (series["reimb_ceiling_quarterly"] == 210_000).all()


def test_coverage_adequacy_ratio_always_below_one_for_cgm():
    """CGM market price always exceeds 기준금액; NHIS always partially underpays."""
    for monthly in [155_000, 200_000, 280_000, 400_000]:
        ratio = compute_coverage_adequacy_ratio(210_000, monthly)
        assert ratio < 1.0, f"ratio ≥ 1.0 at ₩{monthly:,}/month (ratio={ratio:.3f})"


def test_coverage_adequacy_ratio_above_zero():
    """NHIS must pay something (ratio > 0)."""
    ratio = compute_coverage_adequacy_ratio(210_000, 280_000)
    assert ratio > 0.0


# ── Regional fixture quality gates ────────────────────────────────────────────

def test_regional_diabetes_fixture_has_17_regions(regional_fixture):
    assert regional_fixture["region_code"].nunique() == 17


def test_regional_diabetes_fixture_patient_counts_positive(regional_fixture):
    assert (regional_fixture["patient_count"] > 0).all()


def test_regional_diabetes_fixture_seoul_exceeds_gangwon(regional_fixture):
    """Known disparity: Seoul patient count >> Gangwon-do."""
    seoul = regional_fixture[regional_fixture["region_code"] == "11"]["patient_count"].iloc[0]
    gangwon = regional_fixture[regional_fixture["region_code"] == "32"]["patient_count"].iloc[0]
    assert seoul > gangwon * 5, f"Seoul ({seoul:,}) should be >5× Gangwon ({gangwon:,})"


def test_regional_diabetes_fixture_has_required_columns(regional_fixture):
    required = {"region_code", "region_name", "year", "patient_count"}
    missing = required - set(regional_fixture.columns)
    assert not missing, f"Missing columns: {missing}"


def test_regional_fixture_region_codes_are_two_digit_strings(regional_fixture):
    for code in regional_fixture["region_code"]:
        assert isinstance(code, str), f"region_code not a string: {code!r}"
        assert len(code) == 2, f"region_code not 2 chars: {code!r}"
        assert code.isdigit(), f"region_code not numeric: {code!r}"


# ── MFDS fixture quality gates ────────────────────────────────────────────────

def test_mfds_cgm_products_include_known_brands(mfds_fixture):
    """Verified approved brands from MFDS research must be present."""
    brands = mfds_fixture["device_name"].str.lower()
    assert any(brands.str.contains("libre|리브레", case=False, na=False)), (
        "FreeStyle Libre not found in MFDS fixture"
    )


def test_mfds_cgm_products_include_dexcom(mfds_fixture):
    brands = mfds_fixture["device_name"].str.lower()
    assert any(brands.str.contains("dexcom", case=False, na=False))


def test_mfds_cgm_fixture_has_5_products(mfds_fixture):
    assert len(mfds_fixture) == 5


def test_mfds_market_prices_in_reasonable_range(mfds_fixture):
    """Market prices should be ₩155K–₩400K/month."""
    assert (mfds_fixture["market_price_low"] >= 100_000).all()
    assert (mfds_fixture["market_price_high"] <= 500_000).all()


# ── Gap series end-to-end ──────────────────────────────────────────────────────

def test_gap_series_full_range_2018_to_2026():
    """Gap series should cover all 9 years."""
    series = compute_gap_series(list(range(2018, 2027)), "cgm_sensor")
    assert len(series) == 9
    assert set(series["year"]) == set(range(2018, 2027))


def test_gap_series_pre_2022_has_no_ceiling():
    """Before NHIS coverage (Aug 2022), 기준금액 is null."""
    series = compute_gap_series([2019, 2020, 2021], "cgm_sensor")
    assert series["reimb_ceiling_quarterly"].isna().all()


def test_gap_series_nhis_pays_is_147000_post_2022():
    """NHIS always pays ₩147,000/quarter for CGM at current market prices (market > ceiling)."""
    series = compute_gap_series([2023, 2024], "cgm_sensor")
    valid = series["nhis_pays_quarterly"].dropna()
    assert (valid == 147_000).all()
