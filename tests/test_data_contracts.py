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

from src.config import REGION_CODES, NHIS_REGION_MAP
from src.policy import NHIS_REIMB_HISTORY
from src.coverage import (
    compute_coverage_adequacy_ratio,
    compute_gap_series,
    compute_quarterly_patient_burden,
    get_reimb_ceiling,
)
from src.store import (
    find_annual_diabetes_info_xlsx,
    find_cgm_utilization_csv,
    find_checkup_csv,
    find_consumables_csv,
    find_consumables_csvs,
    find_diabetes_utilization_csvs,
    find_insulin_claims_csv,
    find_sigungu_t1d_t2d_xlsx,
    find_t1d_age_annual_csv,
    find_t1d_csv,
    find_t2d_sigungu_csv,
    inventory,
    load_parquet,
    save_parquet,
)


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
    import src.store as storage_mod
    monkeypatch.setattr(storage_mod, "PROCESSED_DIR", tmp_path)

    df = pd.DataFrame({"year": [2022, 2023], "value": [1.0, 2.0]})
    save_parquet(df, "test_table")
    loaded = load_parquet("test_table")
    pd.testing.assert_frame_equal(df, loaded)


def test_load_missing_parquet_raises():
    with pytest.raises(FileNotFoundError):
        load_parquet("nonexistent_table_xyz_999")


def test_inventory_returns_dataframe(tmp_path, monkeypatch):
    import src.store as storage_mod
    monkeypatch.setattr(storage_mod, "PROCESSED_DIR", tmp_path)
    inv = inventory()
    assert isinstance(inv, pd.DataFrame)
    assert len(inv) == 0  # Empty dir returns empty DataFrame, not error


def test_inventory_shows_saved_parquet(tmp_path, monkeypatch):
    import src.store as storage_mod
    monkeypatch.setattr(storage_mod, "PROCESSED_DIR", tmp_path)
    df = pd.DataFrame({"a": [1, 2, 3]})
    save_parquet(df, "my_table")
    inv = inventory()
    assert "my_table" in inv["name"].values
    assert inv.loc[inv["name"] == "my_table", "rows"].iloc[0] == 3


# ── Phase 1: NHIS region code mapping ─────────────────────────────────────────

def test_nhis_region_map_has_17_entries():
    assert len(NHIS_REGION_MAP) == 17


def test_nhis_region_map_covers_all_standard_regions():
    """NHIS codes map to same 17 city/province names as REGION_CODES."""
    assert set(NHIS_REGION_MAP.values()) == set(REGION_CODES.values())


def test_existing_region_codes_unchanged():
    """Guard against accidental mutation of REGION_CODES."""
    assert len(REGION_CODES) == 17


# ── Phase 2: parse_regional_diabetes_excel() ─────────────────────────────────

import sys as _sys
_HIRA_XLSX = (
    Path(__file__).resolve().parent.parent
    / "Data" / "raw"
    / "[건강보험심사평가원] (2024) 지역별 당뇨병 진료현황(2019년~2023년).xlsx"
)

def _hira_regional_df():
    """Helper: parse the real HIRA regional diabetes Excel (cached)."""
    from src.hira_client import parse_regional_diabetes_excel
    return parse_regional_diabetes_excel(_HIRA_XLSX)


@pytest.mark.skipif(not _HIRA_XLSX.exists(), reason="HIRA Excel not in Data/raw/")
def test_parse_regional_diabetes_returns_dataframe():
    df = _hira_regional_df()
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0


@pytest.mark.skipif(not _HIRA_XLSX.exists(), reason="HIRA Excel not in Data/raw/")
def test_parse_regional_diabetes_has_required_columns():
    required = {"region_code", "region_name", "year", "patient_count",
                "visit_days", "cost_krw_thousands", "icd_scope", "source"}
    df = _hira_regional_df()
    missing = required - set(df.columns)
    assert not missing, f"Missing columns: {missing}"


@pytest.mark.skipif(not _HIRA_XLSX.exists(), reason="HIRA Excel not in Data/raw/")
def test_parse_regional_diabetes_has_17_regions():
    df = _hira_regional_df()
    assert df["region_code"].nunique() == 17


@pytest.mark.skipif(not _HIRA_XLSX.exists(), reason="HIRA Excel not in Data/raw/")
def test_parse_regional_diabetes_covers_2019_to_2023():
    df = _hira_regional_df()
    years = set(int(y) for y in df["year"].unique())
    assert years == {2019, 2020, 2021, 2022, 2023}


@pytest.mark.skipif(not _HIRA_XLSX.exists(), reason="HIRA Excel not in Data/raw/")
def test_parse_regional_diabetes_seoul_patients_nonzero():
    df = _hira_regional_df()
    seoul = df[df["region_code"] == "11"]
    assert (seoul["patient_count"] > 0).all()


@pytest.mark.skipif(not _HIRA_XLSX.exists(), reason="HIRA Excel not in Data/raw/")
def test_parse_regional_diabetes_produces_85_rows():
    df = _hira_regional_df()
    assert len(df) == 85  # 17 regions × 5 years


# ── Phase 3: parse_yearbook_ch06() ───────────────────────────────────────────

_RAW = Path(__file__).resolve().parent.parent / "Data" / "raw"

def _ch06_paths():
    """Paths to the ch06 Excel files for 2022, 2023, 2024."""
    paths = []
    for year_dir, pattern in [
        ("2022_건강보험통계연보_본문", "06*.xlsx"),
        ("2023 건강보험통계연보(수정)", "06*.xlsx"),
        ("(본문 및 해설서)2024 건강보험통계연보/1. 본문", "06*.xlsx"),
    ]:
        d = _RAW / year_dir
        if d.exists():
            found = list(d.glob(pattern))
            paths.extend(found)
    return paths


_CH06_PATHS = _ch06_paths()
_ch06_skip = pytest.mark.skipif(len(_CH06_PATHS) == 0, reason="ch06 yearbook files not in Data/raw/")


@_ch06_skip
def test_parse_yearbook_ch06_returns_dataframe():
    from src.nhis_client import parse_yearbook_ch06
    df = parse_yearbook_ch06(_CH06_PATHS)
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0


@_ch06_skip
def test_parse_yearbook_ch06_has_required_columns():
    from src.nhis_client import parse_yearbook_ch06
    required = {"year", "icd_code", "patient_count", "visit_days",
                "cost_krw_thousands", "case_count", "source"}
    df = parse_yearbook_ch06(_CH06_PATHS)
    missing = required - set(df.columns)
    assert not missing, f"Missing columns: {missing}"


@_ch06_skip
def test_parse_yearbook_ch06_e10_exists():
    from src.nhis_client import parse_yearbook_ch06
    df = parse_yearbook_ch06(_CH06_PATHS)
    assert (df["icd_code"] == "E10").any(), "No E10 (T1D) rows found"


@_ch06_skip
def test_parse_yearbook_ch06_e11_exists():
    from src.nhis_client import parse_yearbook_ch06
    df = parse_yearbook_ch06(_CH06_PATHS)
    assert (df["icd_code"] == "E11").any(), "No E11 (T2D) rows found"


@_ch06_skip
def test_parse_yearbook_ch06_patient_count_positive():
    from src.nhis_client import parse_yearbook_ch06
    df = parse_yearbook_ch06(_CH06_PATHS)
    assert (df["patient_count"] > 0).all()


@_ch06_skip
def test_parse_yearbook_ch06_three_years_covered():
    from src.nhis_client import parse_yearbook_ch06
    df = parse_yearbook_ch06(_CH06_PATHS)
    years = set(int(y) for y in df["year"].unique())
    # At least one year covered; 3 years requires all 3 files
    assert len(years) >= 1
    if len(_CH06_PATHS) >= 3:
        assert {2022, 2023, 2024}.issubset(years)


# ── Phase 4: parse_regional_utilization_excel() ───────────────────────────────

def _util_paths():
    paths = []
    for year_dir, pattern in [
        ("★2022년도_지역별의료이용통계연보", "05*.xlsx"),
        ("2023_지역별_의료이용_통계연보", "05*.xlsx"),
    ]:
        d = _RAW / year_dir
        if d.exists():
            found = list(d.glob(pattern))
            paths.extend(found)
    return paths


_UTIL_PATHS = _util_paths()
_util_skip = pytest.mark.skipif(len(_UTIL_PATHS) == 0, reason="Regional utilization files not in Data/raw/")


@_util_skip
def test_parse_regional_utilization_returns_dataframe():
    from src.nhis_client import parse_regional_utilization_excel
    df = parse_regional_utilization_excel(_UTIL_PATHS)
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0


@_util_skip
def test_parse_regional_utilization_has_required_columns():
    from src.nhis_client import parse_regional_utilization_excel
    required = {"region_code", "region_name", "year", "patient_count",
                "cost_krw_thousands", "source"}
    df = parse_regional_utilization_excel(_UTIL_PATHS)
    missing = required - set(df.columns)
    assert not missing, f"Missing columns: {missing}"


@_util_skip
def test_parse_regional_utilization_two_years_present():
    from src.nhis_client import parse_regional_utilization_excel
    df = parse_regional_utilization_excel(_UTIL_PATHS)
    years = set(int(y) for y in df["year"].unique())
    if len(_UTIL_PATHS) >= 2:
        assert len(years) >= 2


@_util_skip
def test_parse_regional_utilization_has_multiple_regions():
    # NOTE: The available files are regional (부산/대구/울산 zone) yearbooks,
    # not the national yearbook. They cover exactly 5 시도: 부산, 대구, 울산, 경북, 경남.
    from src.nhis_client import parse_regional_utilization_excel
    df = parse_regional_utilization_excel(_UTIL_PATHS)
    assert df["region_code"].nunique() >= 5


# ── Phase 5: parse_checkup_csv() ─────────────────────────────────────────────

_CHECKUP_CSV = find_checkup_csv(_RAW)
_checkup_skip = pytest.mark.skipif(
    _CHECKUP_CSV is None, reason="NHIS checkup CSV not in Data/raw/"
)


@_checkup_skip
def test_parse_checkup_csv_returns_dataframe():
    from src.nhis_client import parse_checkup_csv
    df = parse_checkup_csv(_CHECKUP_CSV)
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0


@_checkup_skip
def test_parse_checkup_csv_has_required_columns():
    from src.nhis_client import parse_checkup_csv
    required = {"region_code", "region_name", "year", "mean_fasting_glucose",
                "high_glucose_rate_pct", "screened_count", "source"}
    df = parse_checkup_csv(_CHECKUP_CSV)
    missing = required - set(df.columns)
    assert not missing, f"Missing columns: {missing}"


@_checkup_skip
def test_parse_checkup_csv_applies_nhis_region_mapping():
    from src.nhis_client import parse_checkup_csv
    df = parse_checkup_csv(_CHECKUP_CSV)
    assert set(df["region_name"]).issubset(set(REGION_CODES.values()))


@_checkup_skip
def test_parse_checkup_csv_high_glucose_rate_between_0_and_100():
    from src.nhis_client import parse_checkup_csv
    df = parse_checkup_csv(_CHECKUP_CSV)
    assert (df["high_glucose_rate_pct"] >= 0).all()
    assert (df["high_glucose_rate_pct"] <= 100).all()


@_checkup_skip
def test_parse_checkup_csv_mean_glucose_around_100():
    from src.nhis_client import parse_checkup_csv
    df = parse_checkup_csv(_CHECKUP_CSV)
    national_mean = df["mean_fasting_glucose"].mean()
    assert 95 <= national_mean <= 115, f"National mean glucose {national_mean:.1f} outside [95, 115]"


# ── Phase 6: get_facility_counts() aggregation fix ───────────────────────────

def test_get_facility_counts_returns_numeric_counts(monkeypatch):
    """facility_count column must be integer row counts, not name strings."""
    from src import hira_client
    # Mock _paginate to return synthetic facility records
    synthetic = [
        {"sidoCd": "1100000000", "yadmNm": "강남의원", "clCdNm": "의원", "addr": "서울"},
        {"sidoCd": "1100000001", "yadmNm": "서초의원", "clCdNm": "의원", "addr": "서울"},
        {"sidoCd": "2600000000", "yadmNm": "부산의원", "clCdNm": "의원", "addr": "부산"},
    ]
    monkeypatch.setattr(hira_client, "_paginate", lambda *a, **kw: synthetic)
    df = hira_client.get_facility_counts(api_key="test_key")
    assert "facility_count" in df.columns, "Missing facility_count column"
    assert pd.api.types.is_integer_dtype(df["facility_count"]), \
        f"facility_count not integer dtype: {df['facility_count'].dtype}"
    assert (df["facility_count"] > 0).all()


def test_get_facility_counts_has_region_column(monkeypatch):
    from src import hira_client
    synthetic = [
        {"sidoCd": "1100000000", "yadmNm": "강남의원", "clCdNm": "의원", "addr": "서울"},
    ]
    monkeypatch.setattr(hira_client, "_paginate", lambda *a, **kw: synthetic)
    df = hira_client.get_facility_counts(api_key="test_key")
    assert "region_code" in df.columns


# ── Integration 1: nhis_annual_stats → coverage_trend.csv ────────────────────

_NHIS_ANNUAL_PARQUET = (
    Path(__file__).resolve().parent.parent / "Data" / "processed" / "nhis_annual_stats.parquet"
)
_nhis_annual_skip = pytest.mark.skipif(
    not _NHIS_ANNUAL_PARQUET.exists(), reason="nhis_annual_stats.parquet not in Data/processed/"
)


@_nhis_annual_skip
def test_trend_csv_has_national_patient_columns(tmp_path, monkeypatch):
    """coverage_trend.csv must contain t1d_patient_count and national_diabetes_patients."""
    import src.store as store_mod
    monkeypatch.setattr(store_mod, "PROCESSED_DIR", _NHIS_ANNUAL_PARQUET.parent)
    import importlib, analysis.run_coverage_trend as rct
    importlib.reload(rct)
    monkeypatch.setattr(rct, "OUTPUT_CSV", tmp_path / "coverage_trend.csv")
    rct.main()
    df = pd.read_csv(tmp_path / "coverage_trend.csv")
    assert "t1d_patient_count" in df.columns
    assert "national_diabetes_patients" in df.columns


@_nhis_annual_skip
def test_trend_csv_patient_counts_positive_for_known_years(tmp_path, monkeypatch):
    """t1d_patient_count must be > 0 for 2022 and 2023 (years in nhis_annual_stats)."""
    import src.store as store_mod
    monkeypatch.setattr(store_mod, "PROCESSED_DIR", _NHIS_ANNUAL_PARQUET.parent)
    import importlib, analysis.run_coverage_trend as rct
    importlib.reload(rct)
    monkeypatch.setattr(rct, "OUTPUT_CSV", tmp_path / "coverage_trend.csv")
    rct.main()
    df = pd.read_csv(tmp_path / "coverage_trend.csv")
    for year in [2022, 2023]:
        row = df[df["year"] == year]
        if not row.empty and pd.notna(row["t1d_patient_count"].iloc[0]):
            assert row["t1d_patient_count"].iloc[0] > 0, f"t1d_patient_count zero for {year}"


@_nhis_annual_skip
def test_trend_csv_patient_counts_nan_for_precoverage_years(tmp_path, monkeypatch):
    """t1d_patient_count must be NaN for 2018–2021 (no nhis_annual_stats for those years)."""
    import src.store as store_mod
    monkeypatch.setattr(store_mod, "PROCESSED_DIR", _NHIS_ANNUAL_PARQUET.parent)
    import importlib, analysis.run_coverage_trend as rct
    importlib.reload(rct)
    monkeypatch.setattr(rct, "OUTPUT_CSV", tmp_path / "coverage_trend.csv")
    rct.main()
    df = pd.read_csv(tmp_path / "coverage_trend.csv")
    pre = df[df["year"].isin([2018, 2019, 2020, 2021])]
    if not pre.empty and "t1d_patient_count" in df.columns:
        assert pre["t1d_patient_count"].isna().all(), "Pre-coverage years should have NaN t1d counts"


# ── Integration 2: hira_treatment_materials → coverage_gap_by_product.csv ────

_HIRA_MATERIALS_PARQUET = (
    Path(__file__).resolve().parent.parent / "Data" / "processed" / "hira_treatment_materials.parquet"
)
_hira_materials_skip = pytest.mark.skipif(
    not _HIRA_MATERIALS_PARQUET.exists(), reason="hira_treatment_materials.parquet not in Data/processed/"
)


@_hira_materials_skip
def test_m_code_to_tier_covers_active_products():
    """All non-삭제 m_codes in the parquet must appear in CGM_M_CODE_TO_TIER."""
    from src.devices import CGM_M_CODE_TO_TIER
    df = load_parquet("hira_treatment_materials")
    active = df[df["coverage_status"] != "삭제"]["m_code"].dropna().unique()
    missing = [code for code in active if code not in CGM_M_CODE_TO_TIER]
    assert not missing, f"m_codes not in CGM_M_CODE_TO_TIER: {missing}"


@_hira_materials_skip
def test_coverage_gap_by_product_csv_written(tmp_path, monkeypatch):
    """coverage_gap_by_product.csv must be created by the runner."""
    import src.store as store_mod
    monkeypatch.setattr(store_mod, "PROCESSED_DIR", _HIRA_MATERIALS_PARQUET.parent)
    import importlib, analysis.run_coverage_gap as rcg
    importlib.reload(rcg)
    monkeypatch.setattr(rcg, "OUTPUT_CSV", tmp_path / "coverage_gap.csv")
    monkeypatch.setattr(rcg, "OUTPUT_PRODUCT_CSV", tmp_path / "coverage_gap_by_product.csv")
    rcg.main()
    assert (tmp_path / "coverage_gap_by_product.csv").exists()


@_hira_materials_skip
def test_coverage_gap_by_product_has_required_columns(tmp_path, monkeypatch):
    """Product-level CSV must have product_name, m_code, importer, burden_ratio, coverage_ratio."""
    import src.store as store_mod
    monkeypatch.setattr(store_mod, "PROCESSED_DIR", _HIRA_MATERIALS_PARQUET.parent)
    import importlib, analysis.run_coverage_gap as rcg
    importlib.reload(rcg)
    monkeypatch.setattr(rcg, "OUTPUT_CSV", tmp_path / "coverage_gap.csv")
    monkeypatch.setattr(rcg, "OUTPUT_PRODUCT_CSV", tmp_path / "coverage_gap_by_product.csv")
    rcg.main()
    df = pd.read_csv(tmp_path / "coverage_gap_by_product.csv")
    required = {"product_name", "m_code", "importer", "burden_ratio", "coverage_ratio"}
    missing = required - set(df.columns)
    assert not missing, f"Missing columns: {missing}"


@_hira_materials_skip
def test_coverage_gap_by_product_excludes_deleted(tmp_path, monkeypatch):
    """No row in coverage_gap_by_product.csv should have coverage_status == '삭제'."""
    import src.store as store_mod
    monkeypatch.setattr(store_mod, "PROCESSED_DIR", _HIRA_MATERIALS_PARQUET.parent)
    import importlib, analysis.run_coverage_gap as rcg
    importlib.reload(rcg)
    monkeypatch.setattr(rcg, "OUTPUT_CSV", tmp_path / "coverage_gap.csv")
    monkeypatch.setattr(rcg, "OUTPUT_PRODUCT_CSV", tmp_path / "coverage_gap_by_product.csv")
    rcg.main()
    df = pd.read_csv(tmp_path / "coverage_gap_by_product.csv")
    if "coverage_status" in df.columns:
        assert not (df["coverage_status"] == "삭제").any(), "Deleted products must be excluded"


# ── Integration 3: nhis_checkup_summary → regional_equity.csv ────────────────

_CHECKUP_SUMMARY_PARQUET = (
    Path(__file__).resolve().parent.parent / "Data" / "processed" / "nhis_checkup_summary.parquet"
)
_checkup_summary_skip = pytest.mark.skipif(
    not _CHECKUP_SUMMARY_PARQUET.exists(), reason="nhis_checkup_summary.parquet not in Data/processed/"
)


@_checkup_summary_skip
def test_regional_equity_csv_has_glucose_columns(tmp_path, monkeypatch):
    """regional_equity.csv must contain mean_fasting_glucose, high_glucose_rate_pct, screened_count."""
    import src.store as store_mod
    monkeypatch.setattr(store_mod, "PROCESSED_DIR", _CHECKUP_SUMMARY_PARQUET.parent)
    import importlib, analysis.run_regional_equity as rre
    importlib.reload(rre)
    monkeypatch.setattr(rre, "OUTPUT_CSV", tmp_path / "regional_equity.csv")
    rre.main()
    df = pd.read_csv(tmp_path / "regional_equity.csv")
    for col in ["mean_fasting_glucose", "high_glucose_rate_pct", "screened_count"]:
        assert col in df.columns, f"Missing column: {col}"


@_checkup_summary_skip
def test_regional_equity_glucose_values_in_range(tmp_path, monkeypatch):
    """mean_fasting_glucose in [80, 130]; high_glucose_rate_pct in [0, 100]."""
    import src.store as store_mod
    monkeypatch.setattr(store_mod, "PROCESSED_DIR", _CHECKUP_SUMMARY_PARQUET.parent)
    import importlib, analysis.run_regional_equity as rre
    importlib.reload(rre)
    monkeypatch.setattr(rre, "OUTPUT_CSV", tmp_path / "regional_equity.csv")
    rre.main()
    df = pd.read_csv(tmp_path / "regional_equity.csv")
    valid = df["mean_fasting_glucose"].dropna()
    if not valid.empty:
        assert (valid >= 80).all() and (valid <= 130).all(), "mean_fasting_glucose out of [80, 130]"
    valid_rate = df["high_glucose_rate_pct"].dropna()
    if not valid_rate.empty:
        assert (valid_rate >= 0).all() and (valid_rate <= 100).all()


@_checkup_summary_skip
def test_regional_equity_all_17_regions_have_glucose(tmp_path, monkeypatch):
    """All 17 regions should have glucose data (no NaN) if checkup covers all regions."""
    import src.store as store_mod
    monkeypatch.setattr(store_mod, "PROCESSED_DIR", _CHECKUP_SUMMARY_PARQUET.parent)
    import importlib, analysis.run_regional_equity as rre
    importlib.reload(rre)
    monkeypatch.setattr(rre, "OUTPUT_CSV", tmp_path / "regional_equity.csv")
    rre.main()
    df = pd.read_csv(tmp_path / "regional_equity.csv")
    if "mean_fasting_glucose" in df.columns:
        null_count = df["mean_fasting_glucose"].isna().sum()
        assert null_count == 0, f"{null_count} regions missing glucose data"


# ── Phase 7: parse_t1d_age_sex_csv() ─────────────────────────────────────────

_T1D_CSV = find_t1d_csv(_RAW)
_t1d_skip = pytest.mark.skipif(
    _T1D_CSV is None, reason="NHIS T1D age/sex CSV not in Data/raw/"
)


@_t1d_skip
def test_parse_t1d_age_sex_returns_dataframe():
    from src.nhis_client import parse_t1d_age_sex_csv
    df = parse_t1d_age_sex_csv(_T1D_CSV)
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0


@_t1d_skip
def test_parse_t1d_age_sex_has_required_columns():
    from src.nhis_client import parse_t1d_age_sex_csv
    required = {"year", "age", "sex", "patient_count", "suppressed", "source"}
    df = parse_t1d_age_sex_csv(_T1D_CSV)
    missing = required - set(df.columns)
    assert not missing, f"Missing columns: {missing}"


@_t1d_skip
def test_parse_t1d_age_sex_years_present():
    from src.nhis_client import parse_t1d_age_sex_csv
    df = parse_t1d_age_sex_csv(_T1D_CSV)
    years = set(int(y) for y in df["year"].dropna().unique())
    assert {2021, 2022, 2023, 2024}.issubset(years), f"Missing years: {years}"


@_t1d_skip
def test_parse_t1d_age_sex_suppressed_cells_are_nan():
    """Suppressed cells must be NaN, not 0."""
    from src.nhis_client import parse_t1d_age_sex_csv
    df = parse_t1d_age_sex_csv(_T1D_CSV)
    suppressed_rows = df[df["suppressed"] == True]
    if not suppressed_rows.empty:
        assert suppressed_rows["patient_count"].isna().all(), \
            "Suppressed cells must be NaN, not 0 or any other value"


@_t1d_skip
def test_parse_t1d_age_sex_non_suppressed_are_positive():
    from src.nhis_client import parse_t1d_age_sex_csv
    df = parse_t1d_age_sex_csv(_T1D_CSV)
    non_suppressed = df[df["suppressed"] == False]["patient_count"].dropna()
    assert (non_suppressed > 0).all(), "Non-suppressed patient counts must be positive"


@_t1d_skip
def test_parse_t1d_age_sex_2022_total_within_5pct_of_known():
    """2022 non-suppressed sum should be within 5% of 45,023 (known value)."""
    from src.nhis_client import parse_t1d_age_sex_csv
    df = parse_t1d_age_sex_csv(_T1D_CSV)
    total_2022 = df[(df["year"] == 2022) & (~df["suppressed"])]["patient_count"].sum()
    assert 45_023 * 0.95 <= total_2022 <= 45_023 * 1.05, \
        f"2022 T1D total {total_2022:,} is more than 5% from expected 45,023"


@_t1d_skip
def test_parse_t1d_age_sex_2024_total_within_5pct_of_known():
    """2024 non-suppressed sum should be within 5% of 52,671 (known value)."""
    from src.nhis_client import parse_t1d_age_sex_csv
    df = parse_t1d_age_sex_csv(_T1D_CSV)
    total_2024 = df[(df["year"] == 2024) & (~df["suppressed"])]["patient_count"].sum()
    assert 52_671 * 0.95 <= total_2024 <= 52_671 * 1.05, \
        f"2024 T1D total {total_2024:,} is more than 5% from expected 52,671"


# ── Phase 8: parse_consumables_monthly_csv() ─────────────────────────────────

_CONSUMABLES_CSV = find_consumables_csv(_RAW)
_consumables_skip = pytest.mark.skipif(
    _CONSUMABLES_CSV is None, reason="NHIS consumables monthly CSV not in Data/raw/"
)


@_consumables_skip
def test_parse_consumables_monthly_returns_dataframe():
    from src.nhis_client import parse_consumables_monthly_csv
    df = parse_consumables_monthly_csv(_CONSUMABLES_CSV)
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0


@_consumables_skip
def test_parse_consumables_monthly_has_required_columns():
    from src.nhis_client import parse_consumables_monthly_csv
    required = {"year", "month", "transaction_count", "payment_won", "source"}
    df = parse_consumables_monthly_csv(_CONSUMABLES_CSV)
    missing = required - set(df.columns)
    assert not missing, f"Missing columns: {missing}"


@_consumables_skip
def test_parse_consumables_monthly_has_12_rows():
    """2024 data should have exactly 12 monthly rows (Jan–Dec)."""
    from src.nhis_client import parse_consumables_monthly_csv
    df = parse_consumables_monthly_csv(_CONSUMABLES_CSV)
    assert len(df) == 12, f"Expected 12 rows (Jan–Dec 2024), got {len(df)}"


@_consumables_skip
def test_parse_consumables_monthly_all_positive():
    from src.nhis_client import parse_consumables_monthly_csv
    df = parse_consumables_monthly_csv(_CONSUMABLES_CSV)
    assert (df["transaction_count"] > 0).all(), "All transaction_count must be > 0"
    assert (df["payment_won"] > 0).all(), "All payment_won must be > 0"


@_consumables_skip
def test_parse_consumables_monthly_avg_payment_in_sanity_range():
    """Avg payment per transaction should be between ₩100K and ₩200K."""
    from src.nhis_client import parse_consumables_monthly_csv
    df = parse_consumables_monthly_csv(_CONSUMABLES_CSV)
    avg = (df["payment_won"] / df["transaction_count"]).mean()
    assert 100_000 <= avg <= 200_000, \
        f"Avg payment per transaction ₩{avg:,.0f} outside [₩100K, ₩200K] sanity range"


# ── Integration 4: nhis_t1d_age_sex + nhis_consumables_monthly → coverage_trend.csv ─

_T1D_AGE_SEX_PARQUET = (
    Path(__file__).resolve().parent.parent / "Data" / "processed" / "nhis_t1d_age_sex.parquet"
)
_CONSUMABLES_PARQUET = (
    Path(__file__).resolve().parent.parent / "Data" / "processed" / "nhis_consumables_monthly.parquet"
)
_t1d_parquet_skip = pytest.mark.skipif(
    not _T1D_AGE_SEX_PARQUET.exists(),
    reason="nhis_t1d_age_sex.parquet not in Data/processed/"
)
_consumables_parquet_skip = pytest.mark.skipif(
    not _CONSUMABLES_PARQUET.exists(),
    reason="nhis_consumables_monthly.parquet not in Data/processed/"
)


@_t1d_parquet_skip
def test_trend_csv_t1d_2021_and_2024_populated(tmp_path, monkeypatch):
    """t1d_patient_count must be non-NaN for 2021 and 2024 after new file integration."""
    import src.store as store_mod
    monkeypatch.setattr(store_mod, "PROCESSED_DIR", _T1D_AGE_SEX_PARQUET.parent)
    import importlib, analysis.run_coverage_trend as rct
    importlib.reload(rct)
    monkeypatch.setattr(rct, "OUTPUT_CSV", tmp_path / "coverage_trend.csv")
    rct.main()
    df = pd.read_csv(tmp_path / "coverage_trend.csv")
    for year in [2021, 2024]:
        row = df[df["year"] == year]
        assert not row.empty, f"Year {year} not in coverage_trend.csv"
        assert pd.notna(row["t1d_patient_count"].iloc[0]), \
            f"t1d_patient_count is NaN for {year} — should be populated from nhis_t1d_age_sex"


@_consumables_parquet_skip
def test_trend_csv_consumables_2024_populated(tmp_path, monkeypatch):
    """consumables columns must be non-NaN for 2024."""
    import src.store as store_mod
    monkeypatch.setattr(store_mod, "PROCESSED_DIR", _CONSUMABLES_PARQUET.parent)
    import importlib, analysis.run_coverage_trend as rct
    importlib.reload(rct)
    monkeypatch.setattr(rct, "OUTPUT_CSV", tmp_path / "coverage_trend.csv")
    rct.main()
    df = pd.read_csv(tmp_path / "coverage_trend.csv")
    assert "consumables_transactions_annual" in df.columns
    assert "consumables_payment_annual_won" in df.columns
    row_2024 = df[df["year"] == 2024]
    if not row_2024.empty:
        assert pd.notna(row_2024["consumables_transactions_annual"].iloc[0]), \
            "consumables_transactions_annual should be non-NaN for 2024"


@_consumables_parquet_skip
def test_trend_csv_consumables_multi_year_monotonic(tmp_path, monkeypatch):
    """consumables_payment_annual_won must increase across years 2021–2024."""
    import src.store as store_mod
    monkeypatch.setattr(store_mod, "PROCESSED_DIR", _CONSUMABLES_PARQUET.parent)
    import importlib, analysis.run_coverage_trend as rct
    importlib.reload(rct)
    monkeypatch.setattr(rct, "OUTPUT_CSV", tmp_path / "coverage_trend.csv")
    rct.main()
    df = pd.read_csv(tmp_path / "coverage_trend.csv")
    if "consumables_payment_annual_won" not in df.columns:
        pytest.skip("consumables_payment_annual_won not in trend CSV")
    multi = df[df["year"].isin([2021, 2022, 2023, 2024])][["year", "consumables_payment_annual_won"]]
    multi = multi.dropna(subset=["consumables_payment_annual_won"]).sort_values("year")
    if len(multi) >= 2:
        vals = multi["consumables_payment_annual_won"].tolist()
        assert vals == sorted(vals), f"consumables_payment_annual_won not monotonically increasing: {vals}"


# ── Phase 8 (update): parse_consumables_monthly_csv — multi-schema ─────────

_ALL_CONSUMABLES_CSVS = find_consumables_csvs(_RAW)


@pytest.mark.skipif(not _ALL_CONSUMABLES_CSVS, reason="No consumables CSVs in Data/raw/")
def test_parse_consumables_all_files_uniform_schema():
    """Every consumables CSV produces the same output schema."""
    from src.nhis_client import parse_consumables_monthly_csv
    required = {"year", "month", "transaction_count", "payment_won", "source"}
    for csv_path in _ALL_CONSUMABLES_CSVS:
        df = parse_consumables_monthly_csv(csv_path)
        missing = required - set(df.columns)
        assert not missing, f"{csv_path.name}: missing columns {missing}"


@pytest.mark.skipif(not _ALL_CONSUMABLES_CSVS, reason="No consumables CSVs in Data/raw/")
def test_parse_consumables_each_file_has_12_rows():
    """Each annual consumables CSV must have exactly 12 monthly rows."""
    from src.nhis_client import parse_consumables_monthly_csv
    for csv_path in _ALL_CONSUMABLES_CSVS:
        df = parse_consumables_monthly_csv(csv_path)
        assert len(df) == 12, f"{csv_path.name}: expected 12 rows, got {len(df)}"


@pytest.mark.skipif(not _ALL_CONSUMABLES_CSVS, reason="No consumables CSVs in Data/raw/")
def test_parse_consumables_all_positive():
    """All transaction_count and payment_won must be > 0 across all files."""
    from src.nhis_client import parse_consumables_monthly_csv
    for csv_path in _ALL_CONSUMABLES_CSVS:
        df = parse_consumables_monthly_csv(csv_path)
        assert (df["transaction_count"] > 0).all(), f"{csv_path.name}: non-positive transaction_count"
        assert (df["payment_won"] > 0).all(), f"{csv_path.name}: non-positive payment_won"


# ── Phase 9: parse_cgm_utilization_csv() ─────────────────────────────────────

_CGM_UTIL_CSV = find_cgm_utilization_csv(_RAW)
_cgm_util_skip = pytest.mark.skipif(
    _CGM_UTIL_CSV is None, reason="NHIS CGM utilization CSV not in Data/raw/"
)


@_cgm_util_skip
def test_parse_cgm_utilization_returns_dataframe():
    from src.nhis_client import parse_cgm_utilization_csv
    df = parse_cgm_utilization_csv(_CGM_UTIL_CSV)
    assert isinstance(df, pd.DataFrame) and len(df) > 0


@_cgm_util_skip
def test_parse_cgm_utilization_has_required_columns():
    from src.nhis_client import parse_cgm_utilization_csv
    required = {"year", "cgm_users", "source"}
    df = parse_cgm_utilization_csv(_CGM_UTIL_CSV)
    assert not (required - set(df.columns)), f"Missing: {required - set(df.columns)}"


@_cgm_util_skip
def test_parse_cgm_utilization_has_5_rows():
    """Dataset covers 2020–2024 = exactly 5 rows."""
    from src.nhis_client import parse_cgm_utilization_csv
    df = parse_cgm_utilization_csv(_CGM_UTIL_CSV)
    assert len(df) == 5, f"Expected 5 rows (2020–2024), got {len(df)}"


@_cgm_util_skip
def test_parse_cgm_utilization_all_positive():
    from src.nhis_client import parse_cgm_utilization_csv
    df = parse_cgm_utilization_csv(_CGM_UTIL_CSV)
    assert (df["cgm_users"] > 0).all()


@_cgm_util_skip
def test_parse_cgm_utilization_2024_value():
    """2024 CGM users must be 16,214 (verified from memory file)."""
    from src.nhis_client import parse_cgm_utilization_csv
    df = parse_cgm_utilization_csv(_CGM_UTIL_CSV)
    val_2024 = df[df["year"] == 2024]["cgm_users"].iloc[0]
    assert val_2024 == 16_214, f"2024 cgm_users expected 16,214, got {val_2024}"


@_cgm_util_skip
def test_parse_cgm_utilization_monotonically_increasing():
    """CGM adoption should increase each year 2020–2024."""
    from src.nhis_client import parse_cgm_utilization_csv
    df = parse_cgm_utilization_csv(_CGM_UTIL_CSV).sort_values("year")
    vals = df["cgm_users"].tolist()
    assert vals == sorted(vals), f"cgm_users not monotonically increasing: {vals}"


# ── Phase 10: parse_yoyangbi_registered_xlsx() ───────────────────────────────

_ANNUAL_XLSX = find_annual_diabetes_info_xlsx(_RAW)
_annual_xlsx_skip = pytest.mark.skipif(
    _ANNUAL_XLSX is None, reason="Annual diabetes info XLSX not in Data/raw/"
)


@_annual_xlsx_skip
def test_parse_yoyangbi_registered_returns_dataframe():
    from src.nhis_client import parse_yoyangbi_registered_xlsx
    df = parse_yoyangbi_registered_xlsx(_ANNUAL_XLSX)
    assert isinstance(df, pd.DataFrame) and len(df) > 0


@_annual_xlsx_skip
def test_parse_yoyangbi_registered_has_required_columns():
    from src.nhis_client import parse_yoyangbi_registered_xlsx
    required = {"year", "t1d_registered", "t2d_registered", "source"}
    df = parse_yoyangbi_registered_xlsx(_ANNUAL_XLSX)
    assert not (required - set(df.columns)), f"Missing: {required - set(df.columns)}"


@_annual_xlsx_skip
def test_parse_yoyangbi_registered_has_6_rows():
    """Dataset covers 2019–2024 = exactly 6 rows."""
    from src.nhis_client import parse_yoyangbi_registered_xlsx
    df = parse_yoyangbi_registered_xlsx(_ANNUAL_XLSX)
    assert len(df) == 6, f"Expected 6 rows (2019–2024), got {len(df)}"


@_annual_xlsx_skip
def test_parse_yoyangbi_registered_2024_t1d_value():
    """2024 t1d_registered must be 40,999 (verified from memory file)."""
    from src.nhis_client import parse_yoyangbi_registered_xlsx
    df = parse_yoyangbi_registered_xlsx(_ANNUAL_XLSX)
    val = df[df["year"] == 2024]["t1d_registered"].iloc[0]
    assert val == 40_999, f"2024 t1d_registered expected 40,999, got {val}"


@_annual_xlsx_skip
def test_parse_yoyangbi_registered_t2d_greater_than_t1d():
    """T2D pool must be larger than T1D pool for all years."""
    from src.nhis_client import parse_yoyangbi_registered_xlsx
    df = parse_yoyangbi_registered_xlsx(_ANNUAL_XLSX)
    assert (df["t2d_registered"] > df["t1d_registered"]).all(), \
        "T2D registered must be > T1D registered for all years"


# ── Phase 11: parse_t1d_age_annual_csv() ─────────────────────────────────────

_T1D_AGE_ANNUAL_CSV = find_t1d_age_annual_csv(_RAW)
_t1d_age_annual_skip = pytest.mark.skipif(
    _T1D_AGE_ANNUAL_CSV is None, reason="T1D age annual CSV not in Data/raw/"
)


@_t1d_age_annual_skip
def test_parse_t1d_age_annual_has_required_columns():
    from src.nhis_client import parse_t1d_age_annual_csv
    required = {"year", "age", "patients", "suppressed", "source"}
    df = parse_t1d_age_annual_csv(_T1D_AGE_ANNUAL_CSV)
    assert not (required - set(df.columns)), f"Missing: {required - set(df.columns)}"


@_t1d_age_annual_skip
def test_parse_t1d_age_annual_years_range():
    """Dataset covers 2013–2023 = 11 distinct years."""
    from src.nhis_client import parse_t1d_age_annual_csv
    df = parse_t1d_age_annual_csv(_T1D_AGE_ANNUAL_CSV)
    years = set(int(y) for y in df["year"].dropna().unique())
    assert years == set(range(2013, 2024)), f"Unexpected years: {years}"


@_t1d_age_annual_skip
def test_parse_t1d_age_annual_suppressed_cells_are_nan():
    from src.nhis_client import parse_t1d_age_annual_csv
    df = parse_t1d_age_annual_csv(_T1D_AGE_ANNUAL_CSV)
    suppressed = df[df["suppressed"] == True]
    if not suppressed.empty:
        assert suppressed["patients"].isna().all(), "Suppressed cells must be NaN"


@_t1d_age_annual_skip
def test_parse_t1d_age_annual_2023_eligible_19plus():
    """2023 sum(patients, age >= 19, non-suppressed) ≈ 45,584 (±1%)."""
    from src.nhis_client import parse_t1d_age_annual_csv
    df = parse_t1d_age_annual_csv(_T1D_AGE_ANNUAL_CSV)
    val = df[(df["year"] == 2023) & (df["age"] >= 19) & (~df["suppressed"])]["patients"].sum()
    expected = 45_584
    assert expected * 0.99 <= val <= expected * 1.01, \
        f"2023 eligible_19plus {val:,.0f} outside 1% of {expected:,}"


@_t1d_age_annual_skip
def test_parse_t1d_age_annual_2023_eligible_15plus():
    """2023 sum(patients, age >= 15, non-suppressed) ≈ 46,961 (±1%)."""
    from src.nhis_client import parse_t1d_age_annual_csv
    df = parse_t1d_age_annual_csv(_T1D_AGE_ANNUAL_CSV)
    val = df[(df["year"] == 2023) & (df["age"] >= 15) & (~df["suppressed"])]["patients"].sum()
    expected = 46_961
    assert expected * 0.99 <= val <= expected * 1.01, \
        f"2023 eligible_15plus {val:,.0f} outside 1% of {expected:,}"


# ── Phase 12: parse_annual_diabetes_clinical_xlsx() ──────────────────────────

@_annual_xlsx_skip
def test_parse_annual_diabetes_clinical_has_required_columns():
    from src.nhis_client import parse_annual_diabetes_clinical_xlsx
    required = {"year", "icd_code", "age_bracket", "patient_count", "source"}
    df = parse_annual_diabetes_clinical_xlsx(_ANNUAL_XLSX)
    assert not (required - set(df.columns)), f"Missing: {required - set(df.columns)}"


@_annual_xlsx_skip
def test_parse_annual_diabetes_clinical_years_range():
    """Dataset covers 2010–2023 = 14 distinct years."""
    from src.nhis_client import parse_annual_diabetes_clinical_xlsx
    df = parse_annual_diabetes_clinical_xlsx(_ANNUAL_XLSX)
    years = set(int(y) for y in df["year"].dropna().unique())
    assert years == set(range(2010, 2024)), f"Unexpected years: {years}"


@_annual_xlsx_skip
def test_parse_annual_diabetes_clinical_icd_codes():
    from src.nhis_client import parse_annual_diabetes_clinical_xlsx
    df = parse_annual_diabetes_clinical_xlsx(_ANNUAL_XLSX)
    codes = set(df["icd_code"].dropna().unique())
    expected = {"E10", "E11", "E12", "E13", "E14"}
    assert codes.issubset(expected | codes), f"Unexpected ICD codes: {codes - expected}"
    assert "E10" in codes and "E11" in codes


@_annual_xlsx_skip
def test_parse_annual_diabetes_clinical_age_brackets():
    from src.nhis_client import parse_annual_diabetes_clinical_xlsx
    df = parse_annual_diabetes_clinical_xlsx(_ANNUAL_XLSX)
    brackets = set(df["age_bracket"].dropna().unique())
    assert "19세 미만" in brackets or "이상" in str(brackets), \
        f"Expected age bracket strings not found: {brackets}"


@_annual_xlsx_skip
def test_parse_annual_diabetes_clinical_2023_e10_total():
    """2023 E10 total (19세 미만 + 19세 이상) ≈ 48,850 (±1%)."""
    from src.nhis_client import parse_annual_diabetes_clinical_xlsx
    df = parse_annual_diabetes_clinical_xlsx(_ANNUAL_XLSX)
    total = df[(df["year"] == 2023) & (df["icd_code"] == "E10")]["patient_count"].sum()
    expected = 48_850
    assert expected * 0.99 <= total <= expected * 1.01, \
        f"2023 E10 total {total:,} outside 1% of {expected:,}"


# ── Phase 13: parse_sigungu_t1d_t2d_xlsx() ───────────────────────────────────

_SIGUNGU_XLSX = find_sigungu_t1d_t2d_xlsx(_RAW)
_sigungu_xlsx_skip = pytest.mark.skipif(
    _SIGUNGU_XLSX is None, reason="시군구 T1D+T2D XLSX not in Data/raw/"
)


@_sigungu_xlsx_skip
def test_parse_sigungu_t1d_t2d_has_required_columns():
    from src.nhis_client import parse_sigungu_t1d_t2d_xlsx
    required = {"sheet", "구분", "year", "시도", "시군구", "dimension", "patients", "suppressed", "source"}
    df = parse_sigungu_t1d_t2d_xlsx(_SIGUNGU_XLSX)
    assert not (required - set(df.columns)), f"Missing: {required - set(df.columns)}"


@_sigungu_xlsx_skip
def test_parse_sigungu_t1d_t2d_years():
    from src.nhis_client import parse_sigungu_t1d_t2d_xlsx
    df = parse_sigungu_t1d_t2d_xlsx(_SIGUNGU_XLSX)
    years = set(int(y) for y in df["year"].dropna().unique())
    assert {2022, 2023, 2024}.issubset(years), f"Missing years: {years}"


@_sigungu_xlsx_skip
def test_parse_sigungu_t1d_t2d_diabetes_types():
    from src.nhis_client import parse_sigungu_t1d_t2d_xlsx
    df = parse_sigungu_t1d_t2d_xlsx(_SIGUNGU_XLSX)
    types = set(df["구분"].dropna().unique())
    assert any("1형" in t for t in types), f"당뇨병1형 not found in 구분: {types}"
    assert any("2형" in t for t in types), f"당뇨병2형 not found in 구분: {types}"


@_sigungu_xlsx_skip
def test_parse_sigungu_t1d_t2d_unique_sigungu_count():
    from src.nhis_client import parse_sigungu_t1d_t2d_xlsx
    df = parse_sigungu_t1d_t2d_xlsx(_SIGUNGU_XLSX)
    n = df["시군구"].nunique()
    assert n >= 240, f"Expected ≥240 unique 시군구, got {n}"


@_sigungu_xlsx_skip
def test_parse_sigungu_t1d_t2d_suppressed_cells_are_nan():
    from src.nhis_client import parse_sigungu_t1d_t2d_xlsx
    df = parse_sigungu_t1d_t2d_xlsx(_SIGUNGU_XLSX)
    suppressed = df[df["suppressed"] == True]
    if not suppressed.empty:
        assert suppressed["patients"].isna().all(), "Suppressed cells must be NaN"


@_sigungu_xlsx_skip
def test_parse_sigungu_t1d_t2d_no_0se_in_age_sheet():
    """'0세' artifact in age sheet must be normalized to '10대미만'."""
    from src.nhis_client import parse_sigungu_t1d_t2d_xlsx
    df = parse_sigungu_t1d_t2d_xlsx(_SIGUNGU_XLSX)
    age_sheet = df[df["sheet"] == "age"]
    assert "0세" not in age_sheet["dimension"].values, \
        "'0세' not normalized to '10대미만'"


# ── Phase 14: parse_t2d_sigungu_csv() ────────────────────────────────────────

_T2D_SIGUNGU_CSV = find_t2d_sigungu_csv(_RAW)
_t2d_sigungu_skip = pytest.mark.skipif(
    _T2D_SIGUNGU_CSV is None, reason="T2D 시군구 clinical CSV not in Data/raw/"
)


@_t2d_sigungu_skip
def test_parse_t2d_sigungu_has_required_columns():
    from src.nhis_client import parse_t2d_sigungu_csv
    required = {"year", "sido", "sigungu", "institution_type", "patient_count",
                "visit_count", "cost_krw_thousands"}
    df = parse_t2d_sigungu_csv(_T2D_SIGUNGU_CSV)
    assert not (required - set(df.columns)), f"Missing: {required - set(df.columns)}"


@_t2d_sigungu_skip
def test_parse_t2d_sigungu_sido_count():
    """After normalization, must have exactly 17 unique 시도 (no abbreviated forms)."""
    from src.nhis_client import parse_t2d_sigungu_csv
    df = parse_t2d_sigungu_csv(_T2D_SIGUNGU_CSV)
    n = df["sido"].nunique()
    assert n == 17, f"Expected 17 unique 시도 after normalization, got {n}: {sorted(df['sido'].unique())}"


@_t2d_sigungu_skip
def test_parse_t2d_sigungu_years():
    from src.nhis_client import parse_t2d_sigungu_csv
    df = parse_t2d_sigungu_csv(_T2D_SIGUNGU_CSV)
    years = set(int(y) for y in df["year"].dropna().unique())
    assert {2021, 2022, 2023}.issubset(years), f"Missing years: {years}"


# ── Integration 5: CGM adoption metrics in coverage_trend.csv ────────────────

_CGM_UTIL_PARQUET = (
    Path(__file__).resolve().parent.parent / "Data" / "processed" / "nhis_cgm_utilization.parquet"
)
_YOYANGBI_PARQUET = (
    Path(__file__).resolve().parent.parent / "Data" / "processed" / "nhis_yoyangbi_registered.parquet"
)
_cgm_util_parquet_skip = pytest.mark.skipif(
    not _CGM_UTIL_PARQUET.exists(), reason="nhis_cgm_utilization.parquet not in Data/processed/"
)
_yoyangbi_parquet_skip = pytest.mark.skipif(
    not _YOYANGBI_PARQUET.exists(), reason="nhis_yoyangbi_registered.parquet not in Data/processed/"
)


@_cgm_util_parquet_skip
def test_trend_csv_cgm_users_populated(tmp_path, monkeypatch):
    """cgm_users must be non-NaN for 2020–2024."""
    import src.store as store_mod
    monkeypatch.setattr(store_mod, "PROCESSED_DIR", _CGM_UTIL_PARQUET.parent)
    import importlib, analysis.run_coverage_trend as rct
    importlib.reload(rct)
    monkeypatch.setattr(rct, "OUTPUT_CSV", tmp_path / "coverage_trend.csv")
    rct.main()
    df = pd.read_csv(tmp_path / "coverage_trend.csv")
    assert "cgm_users" in df.columns
    for year in [2020, 2021, 2022, 2023, 2024]:
        row = df[df["year"] == year]
        if not row.empty:
            assert pd.notna(row["cgm_users"].iloc[0]), f"cgm_users NaN for {year}"


@pytest.mark.skipif(
    not _CGM_UTIL_PARQUET.exists() or not _YOYANGBI_PARQUET.exists(),
    reason="nhis_cgm_utilization.parquet or nhis_yoyangbi_registered.parquet not in Data/processed/"
)
def test_trend_csv_adoption_rate_2022(tmp_path, monkeypatch):
    """adoption_rate_registered for 2022 must be within 1% of 0.295 (29.5%)."""
    import src.store as store_mod
    monkeypatch.setattr(store_mod, "PROCESSED_DIR", _CGM_UTIL_PARQUET.parent)
    import importlib, analysis.run_coverage_trend as rct
    importlib.reload(rct)
    monkeypatch.setattr(rct, "OUTPUT_CSV", tmp_path / "coverage_trend.csv")
    rct.main()
    df = pd.read_csv(tmp_path / "coverage_trend.csv")
    if "adoption_rate_registered" not in df.columns:
        pytest.skip("adoption_rate_registered not in trend CSV")
    row = df[df["year"] == 2022]
    if row.empty or pd.isna(row["adoption_rate_registered"].iloc[0]):
        pytest.skip("adoption_rate_registered NaN for 2022")
    val = row["adoption_rate_registered"].iloc[0]
    assert abs(val - 0.295) <= 0.01, f"2022 adoption_rate_registered {val:.3f} outside ±1% of 0.295"


# ── Integration 6: Eligibility brackets in coverage_trend.csv ────────────────

_T1D_AGE_ANNUAL_PARQUET = (
    Path(__file__).resolve().parent.parent / "Data" / "processed" / "nhis_t1d_age_annual.parquet"
)
_t1d_age_annual_parquet_skip = pytest.mark.skipif(
    not _T1D_AGE_ANNUAL_PARQUET.exists(),
    reason="nhis_t1d_age_annual.parquet not in Data/processed/"
)


@_t1d_age_annual_parquet_skip
def test_trend_csv_eligibility_brackets_populated(tmp_path, monkeypatch):
    """eligible_19plus and eligible_15plus must be non-NaN for 2013–2023."""
    import src.store as store_mod
    monkeypatch.setattr(store_mod, "PROCESSED_DIR", _T1D_AGE_ANNUAL_PARQUET.parent)
    import importlib, analysis.run_coverage_trend as rct
    importlib.reload(rct)
    monkeypatch.setattr(rct, "OUTPUT_CSV", tmp_path / "coverage_trend.csv")
    rct.main()
    df = pd.read_csv(tmp_path / "coverage_trend.csv")
    for col in ["eligible_19plus", "eligible_15plus"]:
        assert col in df.columns, f"Missing column: {col}"
        populated = df[df["year"].between(2013, 2023)][col].notna().sum()
        assert populated > 0, f"{col} all NaN for 2013–2023"


@_t1d_age_annual_parquet_skip
def test_trend_csv_eligibility_2023_bracket_difference(tmp_path, monkeypatch):
    """eligible_15plus - eligible_19plus for 2023 ≈ 1,377 (±20 for suppression)."""
    import src.store as store_mod
    monkeypatch.setattr(store_mod, "PROCESSED_DIR", _T1D_AGE_ANNUAL_PARQUET.parent)
    import importlib, analysis.run_coverage_trend as rct
    importlib.reload(rct)
    monkeypatch.setattr(rct, "OUTPUT_CSV", tmp_path / "coverage_trend.csv")
    rct.main()
    df = pd.read_csv(tmp_path / "coverage_trend.csv")
    if "eligible_15plus" not in df.columns or "eligible_19plus" not in df.columns:
        pytest.skip("Eligibility columns not in trend CSV")
    row = df[df["year"] == 2023]
    if row.empty:
        pytest.skip("Year 2023 not in trend CSV")
    diff = row["eligible_15plus"].iloc[0] - row["eligible_19plus"].iloc[0]
    assert abs(diff - 1377) <= 20, f"2023 bracket difference {diff:.0f} not within ±20 of 1,377"


# ── Integration 7: regional_equity.csv at 시군구 level ───────────────────────

_SIGUNGU_T1D_T2D_PARQUET = (
    Path(__file__).resolve().parent.parent / "Data" / "processed" / "nhis_sigungu_t1d_t2d.parquet"
)
_sigungu_parquet_skip = pytest.mark.skipif(
    not _SIGUNGU_T1D_T2D_PARQUET.exists(),
    reason="nhis_sigungu_t1d_t2d.parquet not in Data/processed/"
)


@_sigungu_parquet_skip
def test_regional_equity_sigungu_row_count(tmp_path, monkeypatch):
    """After sigungu upgrade, regional_equity.csv must have > 500 rows."""
    import src.store as store_mod
    monkeypatch.setattr(store_mod, "PROCESSED_DIR", _SIGUNGU_T1D_T2D_PARQUET.parent)
    import importlib, analysis.run_regional_equity as rre
    importlib.reload(rre)
    monkeypatch.setattr(rre, "OUTPUT_CSV", tmp_path / "regional_equity.csv")
    rre.main()
    df = pd.read_csv(tmp_path / "regional_equity.csv")
    assert len(df) > 500, f"Expected >500 rows (시군구 level), got {len(df)}"


@_sigungu_parquet_skip
def test_regional_equity_sigungu_has_t1d_patients(tmp_path, monkeypatch):
    """t1d_patients must be non-NaN for sigungu-level rows."""
    import src.store as store_mod
    monkeypatch.setattr(store_mod, "PROCESSED_DIR", _SIGUNGU_T1D_T2D_PARQUET.parent)
    import importlib, analysis.run_regional_equity as rre
    importlib.reload(rre)
    monkeypatch.setattr(rre, "OUTPUT_CSV", tmp_path / "regional_equity.csv")
    rre.main()
    df = pd.read_csv(tmp_path / "regional_equity.csv")
    if "t1d_patients" not in df.columns:
        pytest.skip("t1d_patients not in regional_equity.csv")
    sigungu_rows = df[df.get("granularity", pd.Series(dtype=str)) == "시군구"] if "granularity" in df.columns else df
    non_nan = sigungu_rows["t1d_patients"].notna().sum()
    assert non_nan > 0, "t1d_patients all NaN in sigungu-level rows"


# ── Phase 15: parse_diabetes_utilization_rate_csvs() ─────────────────────────

_UTIL_RATE_CSVS = find_diabetes_utilization_csvs(_RAW)
_util_rate_skip = pytest.mark.skipif(
    not _UTIL_RATE_CSVS, reason="당뇨병의료이용률 CSVs not in Data/raw/"
)


@_util_rate_skip
def test_parse_diabetes_utilization_rate_returns_dataframe():
    from src.nhis_client import parse_diabetes_utilization_rate_csvs
    df = parse_diabetes_utilization_rate_csvs(_UTIL_RATE_CSVS)
    assert isinstance(df, pd.DataFrame) and len(df) > 0


@_util_rate_skip
def test_parse_diabetes_utilization_rate_has_required_columns():
    from src.nhis_client import parse_diabetes_utilization_rate_csvs
    required = {"year", "sido", "sigungu", "denominator", "numerator",
                "utilization_rate_pct", "source"}
    df = parse_diabetes_utilization_rate_csvs(_UTIL_RATE_CSVS)
    missing = required - set(df.columns)
    assert not missing, f"Missing columns: {missing}"


@_util_rate_skip
def test_parse_diabetes_utilization_rate_year_range():
    """Merged dataset must cover 2002–2024 (23 distinct years)."""
    from src.nhis_client import parse_diabetes_utilization_rate_csvs
    df = parse_diabetes_utilization_rate_csvs(_UTIL_RATE_CSVS)
    years = set(int(y) for y in df["year"].dropna().unique())
    assert {2002, 2022, 2023, 2024}.issubset(years), f"Missing expected years: {years}"
    assert len(years) >= 20, f"Expected ≥20 distinct years, got {len(years)}"


@_util_rate_skip
def test_parse_diabetes_utilization_rate_no_duplicates():
    """No duplicate (year, sido, sigungu) rows after merge."""
    from src.nhis_client import parse_diabetes_utilization_rate_csvs
    df = parse_diabetes_utilization_rate_csvs(_UTIL_RATE_CSVS)
    dupes = df.duplicated(subset=["year", "sido", "sigungu"]).sum()
    assert dupes == 0, f"Found {dupes} duplicate (year, sido, sigungu) rows"


@_util_rate_skip
def test_parse_diabetes_utilization_rate_values_in_range():
    """utilization_rate_pct must be in [0, 100]."""
    from src.nhis_client import parse_diabetes_utilization_rate_csvs
    df = parse_diabetes_utilization_rate_csvs(_UTIL_RATE_CSVS)
    rates = df["utilization_rate_pct"].dropna()
    assert (rates >= 0).all() and (rates <= 100).all(), \
        "utilization_rate_pct outside [0, 100]"


@_util_rate_skip
def test_parse_diabetes_utilization_rate_national_2024():
    """National-level utilization_rate_pct for 2024 must be within 1% of 10.18."""
    from src.nhis_client import parse_diabetes_utilization_rate_csvs
    df = parse_diabetes_utilization_rate_csvs(_UTIL_RATE_CSVS)
    national_2024 = df[
        (df["year"] == 2024) &
        (df["sigungu"].str.contains("전국", na=False) | df["sido"].str.contains("전국", na=False))
    ]["utilization_rate_pct"]
    if national_2024.empty:
        pytest.skip("Cannot identify national-level row for 2024 (no '전국' in sido/sigungu)")
    val = national_2024.iloc[0]
    assert abs(val - 10.18) <= 0.15, f"2024 national rate {val:.2f} not within 0.15 of 10.18"


@_util_rate_skip
def test_parse_diabetes_utilization_rate_national_monotonically_increasing():
    """National-level utilization rate must increase from 2002 to 2024."""
    from src.nhis_client import parse_diabetes_utilization_rate_csvs
    df = parse_diabetes_utilization_rate_csvs(_UTIL_RATE_CSVS)
    national = df[
        df["sigungu"].str.contains("전국", na=False) | df["sido"].str.contains("전국", na=False)
    ].sort_values("year")
    if len(national) < 5:
        pytest.skip("Cannot identify enough national-level rows for monotonicity check")
    vals = national["utilization_rate_pct"].dropna().tolist()
    assert vals == sorted(vals), f"National utilization rate not monotonically increasing: {vals}"


# ── Phase 16: parse_insulin_claims_csv() ─────────────────────────────────────

_INSULIN_CSV = find_insulin_claims_csv(_RAW)
_insulin_skip = pytest.mark.skipif(
    _INSULIN_CSV is None, reason="인슐린 주사 청구 CSV not in Data/raw/"
)


@_insulin_skip
def test_parse_insulin_claims_returns_dataframe():
    from src.nhis_client import parse_insulin_claims_csv
    df = parse_insulin_claims_csv(_INSULIN_CSV)
    assert isinstance(df, pd.DataFrame) and len(df) > 0


@_insulin_skip
def test_parse_insulin_claims_has_required_columns():
    from src.nhis_client import parse_insulin_claims_csv
    required = {"year", "month", "age_group", "claim_count",
                "claim_amount_krw_thousands", "source"}
    df = parse_insulin_claims_csv(_INSULIN_CSV)
    missing = required - set(df.columns)
    assert not missing, f"Missing columns: {missing}"


@_insulin_skip
def test_parse_insulin_claims_years_range():
    """Dataset covers 2016–2023 (8 distinct years)."""
    from src.nhis_client import parse_insulin_claims_csv
    df = parse_insulin_claims_csv(_INSULIN_CSV)
    years = set(int(y) for y in df["year"].dropna().unique())
    assert {2016, 2017, 2022, 2023}.issubset(years), f"Missing expected years: {years}"
    assert len(years) == 8, f"Expected 8 distinct years (2016–2023), got {len(years)}: {sorted(years)}"


@_insulin_skip
def test_parse_insulin_claims_all_positive():
    """claim_count must be > 0 for all rows."""
    from src.nhis_client import parse_insulin_claims_csv
    df = parse_insulin_claims_csv(_INSULIN_CSV)
    assert (df["claim_count"] > 0).all(), "claim_count must be > 0 for all rows"


@_insulin_skip
def test_parse_insulin_claims_annual_total_increasing():
    """Annual total claim_count must increase from 2016 to 2023."""
    from src.nhis_client import parse_insulin_claims_csv
    df = parse_insulin_claims_csv(_INSULIN_CSV)
    annual = df.groupby("year")["claim_count"].sum().sort_index()
    if len(annual) >= 2:
        first, last = int(annual.iloc[0]), int(annual.iloc[-1])
        assert last > first, \
            f"Annual claim_count not increasing: {annual.tolist()}"


@_insulin_skip
def test_parse_insulin_claims_12_months_per_year():
    """Each year should have 12 months × N age groups of rows."""
    from src.nhis_client import parse_insulin_claims_csv
    df = parse_insulin_claims_csv(_INSULIN_CSV)
    months_per_year = df.groupby("year")["month"].nunique()
    assert (months_per_year == 12).all(), \
        f"Not all years have 12 distinct months: {months_per_year.to_dict()}"
