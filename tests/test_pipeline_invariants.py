"""
Schema and formula correctness invariants.
All tests run without live API calls.
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

# Ensure project root is on path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import NHIS_REIMB_HISTORY, REGION_CODES
from src.coverage_index import (
    compute_coverage_adequacy_ratio,
    compute_gap_series,
    compute_quarterly_patient_burden,
    get_reimb_ceiling,
)
from src.storage import inventory, load_parquet, save_parquet


# ── 기준금액 config invariants ─────────────────────────────────────────────────

def test_cgm_ceiling_is_quarterly():
    """기준금액 is ₩210,000 per quarter (3 months), NOT monthly."""
    entry = NHIS_REIMB_HISTORY["cgm_sensor"][0]
    assert entry[1] == 210_000


def test_reimb_history_2022_entry_exists():
    entries = NHIS_REIMB_HISTORY["cgm_sensor"]
    dates = [e[0] for e in entries]
    assert "2022-08-01" in dates


def test_reimb_history_insulin_pump_has_1700000():
    entries = NHIS_REIMB_HISTORY["insulin_pump"]
    amounts = [e[1] for e in entries]
    assert 1_700_000 in amounts


def test_get_reimb_ceiling_returns_quarterly_amount():
    ceiling = get_reimb_ceiling("cgm_sensor", "2023-01-01")
    assert ceiling == 210_000


def test_get_reimb_ceiling_raises_before_coverage_start():
    with pytest.raises(ValueError, match="No 기준금액"):
        get_reimb_ceiling("cgm_sensor", "2021-12-31")


def test_get_reimb_ceiling_unknown_device_raises():
    with pytest.raises(KeyError):
        get_reimb_ceiling("nonexistent_device", "2023-01-01")


# ── Burden calculation math ────────────────────────────────────────────────────

def test_cgm_burden_calculation_at_low_market_price():
    """market ₩155K/month → ₩465K/quarter; NHIS pays 70% of ₩210K = ₩147K
    patient pays: ₩465K - ₩147K = ₩318K/quarter; burden = 318/465 ≈ 68.4%
    """
    result = compute_quarterly_patient_burden(
        market_price_monthly_krw=155_000,
        reimb_ceiling_quarterly_krw=210_000,
    )
    assert abs(result["nhis_pays"] - 147_000) < 1
    assert abs(result["patient_pays"] - 318_000) < 1
    assert 0.68 < result["burden_ratio"] < 0.69


def test_cgm_burden_calculation_at_mid_market_price():
    """market ₩200K/month → ₩600K/quarter; patient pays ₩453K; burden = 75.5%"""
    result = compute_quarterly_patient_burden(200_000, 210_000)
    assert abs(result["nhis_pays"] - 147_000) < 1
    assert abs(result["patient_pays"] - 453_000) < 1
    assert 0.75 < result["burden_ratio"] < 0.76


def test_cgm_burden_in_known_range():
    """All CGM scenarios: burden must be 65–85%."""
    for monthly_price in [155_000, 200_000, 280_000]:
        result = compute_quarterly_patient_burden(monthly_price, 210_000)
        assert 0.65 < result["burden_ratio"] < 0.85, (
            f"Failed at ₩{monthly_price:,}/month: burden={result['burden_ratio']:.3f}"
        )


def test_burden_raises_on_zero_market_price():
    with pytest.raises(ValueError):
        compute_quarterly_patient_burden(0, 210_000)


def test_burden_raises_on_negative_ceiling():
    with pytest.raises(ValueError):
        compute_quarterly_patient_burden(200_000, -1)


def test_patient_monthly_equiv_is_quarterly_divided_by_3():
    result = compute_quarterly_patient_burden(200_000, 210_000)
    assert abs(result["patient_monthly_equiv"] - result["patient_pays"] / 3) < 0.01


# ── Coverage ratio invariants ──────────────────────────────────────────────────

def test_coverage_ratio_range_for_cgm():
    """nhis_pays / market_quarterly; ₩147K/₩465K = 0.316; ₩147K/₩840K = 0.175"""
    ratio_low = compute_coverage_adequacy_ratio(210_000, 155_000)
    ratio_high = compute_coverage_adequacy_ratio(210_000, 280_000)
    assert 0.15 < ratio_high < 0.20    # worst case (most expensive sensor)
    assert 0.30 < ratio_low < 0.35     # best case (cheapest sensor)
    assert ratio_low > ratio_high      # cheaper market → better coverage ratio


def test_coverage_ratio_raises_on_zero():
    with pytest.raises(ValueError):
        compute_coverage_adequacy_ratio(210_000, 0)


# ── Gap series ─────────────────────────────────────────────────────────────────

def test_gap_series_has_correct_columns():
    series = compute_gap_series(list(range(2022, 2027)), "cgm_sensor")
    assert len(series) == 5
    required = ["year", "reimb_ceiling_quarterly", "patient_burden_ratio_mid", "patient_pays_quarterly_mid"]
    for col in required:
        assert col in series.columns, f"Missing column: {col}"


def test_gap_series_pre_coverage_has_null_ceiling():
    """Before 2022-08-01, there is no 기준금액."""
    series = compute_gap_series([2020, 2021], "cgm_sensor")
    assert series["reimb_ceiling_quarterly"].isna().all()


def test_gap_series_post_coverage_has_210000():
    series = compute_gap_series([2022, 2023, 2024], "cgm_sensor")
    post = series[series["reimb_ceiling_quarterly"].notna()]
    assert (post["reimb_ceiling_quarterly"] == 210_000).all()


# ── Region codes ───────────────────────────────────────────────────────────────

def test_region_codes_count():
    assert len(REGION_CODES) == 17


def test_region_codes_format():
    """All codes must be 2-character numeric strings."""
    for code in REGION_CODES:
        assert len(code) == 2, f"Code '{code}' is not 2 characters"
        assert code.isdigit(), f"Code '{code}' is not all digits"


def test_seoul_code_is_11():
    assert REGION_CODES.get("11") == "서울"


def test_jeju_code_is_39():
    assert REGION_CODES.get("39") == "제주"


# ── Storage roundtrip ──────────────────────────────────────────────────────────

def test_storage_roundtrip(tmp_path, monkeypatch):
    import src.storage as storage_mod
    monkeypatch.setattr(storage_mod, "PROCESSED_DIR", tmp_path)

    df = pd.DataFrame({"year": [2022, 2023], "value": [1.0, 2.0]})
    save_parquet(df, "test_table")
    loaded = load_parquet("test_table")
    pd.testing.assert_frame_equal(df, loaded)


def test_load_missing_parquet_raises():
    with pytest.raises(FileNotFoundError):
        load_parquet("nonexistent_table_xyz_999")


def test_inventory_returns_dataframe(tmp_path, monkeypatch):
    import src.storage as storage_mod
    monkeypatch.setattr(storage_mod, "PROCESSED_DIR", tmp_path)
    inv = inventory()
    assert isinstance(inv, pd.DataFrame)
    assert len(inv) == 0  # Empty dir returns empty DataFrame, not error


def test_inventory_shows_saved_parquet(tmp_path, monkeypatch):
    import src.storage as storage_mod
    monkeypatch.setattr(storage_mod, "PROCESSED_DIR", tmp_path)
    df = pd.DataFrame({"a": [1, 2, 3]})
    save_parquet(df, "my_table")
    inv = inventory()
    assert "my_table" in inv["name"].values
    assert inv.loc[inv["name"] == "my_table", "rows"].iloc[0] == 3
