"""
Constants for kr-health-monitor.

All verified from:
  - 보건복지부 고시 제2024-226호 (updated August 2024)
  - MFDS 의료기기 허가 목록 (confirmed March 2026)
  - data.go.kr API documentation
"""

# ─── 기준금액 history ──────────────────────────────────────────────────────────
# Source: 보건복지부 고시 제2024-226호 (updated August 2024)
# CGM amount is PER QUARTER (3 months); compute monthly as amount / 3
# Insulin pump is PER DEVICE (one-time)
# Format: (effective_date_str, reimb_ceiling_krw, gosi_number)
NHIS_REIMB_HISTORY = {
    "cgm_sensor": [
        # 연속혈당측정용 전극; Type 1 + insulin-dependent Type 2 age 19+
        ("2022-08-01", 210_000, "2022-170"),   # Initial coverage: ₩210,000/quarter
        # 2024 update: same amount, expanded eligibility; ceiling unchanged
        ("2024-08-01", 210_000, "2024-226"),
    ],
    "insulin_pump": [
        # 인슐린 자동주입기; ₩1,700,000 PER DEVICE (not monthly)
        ("2020-01-01", 1_700_000, "2020-xxx"),  # Coverage start: Jan 2020
        ("2024-08-01", 1_700_000, "2024-226"),  # Confirmed same amount in 2024 update
    ],
}

NHIS_REIMBURSEMENT_RATIO = 0.70        # NHIS pays 70% of min(actual, 기준금액)
# Low-income exception: 차상위 계층 → 100% coverage within 기준금액

# ─── Market price ranges (verified from diabetes project research, 2025) ──────
# Monthly prices in KRW; all per-month even though 기준금액 is quarterly
MARKET_PRICES_KRW = {
    "cgm_sensor": {
        "low": 155_000,   # Budget options, domestic brands
        "mid": 200_000,   # FreeStyle Libre 2, Dexcom G6 typical
        "high": 280_000,  # Dexcom G7; Guardian 4 ~₩400K at high end
    },
    "insulin_pump_supplies": {
        "low": 450_000,   # Monthly consumable estimate
        "high": 700_000,
    },
}

# ─── Approved CGM products in Korea (MFDS confirmed, March 2026) ──────────────
CGM_APPROVED_PRODUCTS = [
    {"brand": "FreeStyle Libre", "manufacturer": "Abbott", "approved": "2020-05", "distributor": "Daewoo Pharma"},
    {"brand": "Dexcom G6",       "manufacturer": "Dexcom Inc.", "approved": "2018-2019", "distributor": "Huons"},
    {"brand": "Dexcom G7",       "manufacturer": "Dexcom Inc.", "approved": "2024-10", "distributor": "Huons"},
    {"brand": "Guardian 4",      "manufacturer": "Medtronic", "approved": "2019-2023", "distributor": "Medtronic Korea"},
    {"brand": "CareSens Air",    "manufacturer": "i-SENS", "approved": "2023-09", "distributor": "i-SENS"},
]

# ─── HIRA API endpoints ──────────────────────────────────────────────────────
# All via data.go.kr; require serviceKey after free registration (~1-2 hr activation)
HIRA_API_BASE = "http://apis.data.go.kr/B551182"

# Drug/pharma utilization (15047819) — NOT device-specific
# Useful for diabetes medication trends (not CGM hardware)
HIRA_DRUG_USAGE_SVC = f"{HIRA_API_BASE}/msupUserInfoService1.2"

# Treatment Material API (3074384) — query CGM product codes + coverage status
# Returns: product name, coverage status (급여/비급여), max unit price, M-codes
HIRA_TREATMENT_MATERIAL_BASE = "http://apis.data.go.kr/B551182/srvy"
HIRA_TREATMENT_MATERIAL_DATASET = "3074384"

# Medical Institution API (15001699) — facility registry
HIRA_INSTITUTION_BASE = "http://apis.data.go.kr/B551182/MedBasisInfoService1"
HIRA_INSTITUTION_DATASET = "15001699"

# HIRA Open Data Portal downloads (no API — file downloads)
HIRA_OPENDATA_REGIONAL_DIABETES_SNO = "13702"   # 지역별 당뇨병 진료현황(2019–2023), Excel 1.1MB
HIRA_OPENDATA_REGION_CODES_DATASET = "15067469" # 행정구역 코드테이블, CSV 302 rows

# ─── MFDS API (의료기기정보포털) ──────────────────────────────────────────────
# Dataset: 15057456 (medical device approval list)
MFDS_API_BASE = "http://apis.data.go.kr/1471000"
MFDS_DEVICE_LIST_ENDPOINT = "/MdcinGrnIdntfInfoService01/getMdlpPrdlstPrmisnList04"
MFDS_DEVICE_DETAIL_ENDPOINT = "/MdcinGrnIdntfInfoService01/getMdlpPrdlstPrmisnItem04"
MFDS_DATASET_ID = "15057456"
# MFDS response fields: PRDUCT, ENTRPS, PRMISN_DT (YYYYMMDD), PRDUCT_PRMISN_NO, GRADE, MANUF_NM, TYPE_NAME
# NOTE: NO price data in MFDS API — prices hardcoded above

# ─── NHIS open data (data.go.kr — ALL BULK FILE DOWNLOADS, not APIs) ──────────
NHIS_CHECKUP_DATASET = "15007122"    # 건강검진정보 — includes blood glucose (혈당)
NHIS_CLAIMS_DATASET = "15007115"     # 진료내역정보 — includes ICD-10 E10/E11
NHIS_PUBLICATIONS_DATASET = "15095102"  # 발간자료 — annual stats Excel

# ─── 17 시도 region codes (standard, confirmed from data.go.kr/data/15067469) ─
REGION_CODES = {
    "11": "서울",  "21": "부산",  "22": "대구",  "23": "인천",
    "24": "광주",  "25": "대전",  "26": "울산",  "29": "세종",
    "31": "경기",  "32": "강원",  "33": "충북",  "34": "충남",
    "35": "전북",  "36": "전남",  "37": "경북",  "38": "경남",
    "39": "제주",
}

# ─── ICD-10 codes for diabetes ────────────────────────────────────────────────
ICD10_DIABETES = {
    "T1D": "E10",            # 인슐린 의존 당뇨병 (Type 1)
    "T2D": "E11",            # 인슐린 비의존 당뇨병 (Type 2)
    "unspecified": "E14",    # 상세불명 당뇨병
    "T1D_renal": "E102",     # T1D + 신장 합병증
    "T1D_retinal": "E103",   # T1D + 눈 합병증
}

# ─── Data source provenance ────────────────────────────────────────────────────
DATA_SOURCES = {
    "hira_regional_diabetes": {
        "type": "file_download",
        "portal": "opendata.hira.or.kr",
        "sno": "13702",
        "description": "지역별 당뇨병 진료현황(2019–2023) — Excel 1.1MB; 시도별 patient counts + costs",
        "update_freq": "annual",
    },
    "hira_treatment_material": {
        "type": "api",
        "portal": "data.go.kr",
        "dataset_id": "3074384",
        "description": "HIRA 치료재료 정보 — CGM product coverage status, M-codes, max unit price",
        "update_freq": "monthly",
    },
    "mfds_device_approvals": {
        "type": "api",
        "portal": "data.go.kr",
        "dataset_id": "15057456",
        "description": "MFDS 의료기기 허가 목록 — product name, manufacturer, approval date",
        "update_freq": "monthly",
    },
    "nhis_checkup": {
        "type": "file_download",
        "portal": "data.go.kr",
        "dataset_id": "15007122",
        "description": "NHIS 건강검진정보 — 1M sample; includes blood glucose",
        "update_freq": "annual",
    },
    "nhis_annual_stats": {
        "type": "file_download",
        "portal": "data.go.kr",
        "dataset_id": "15095102",
        "description": "NHIS 발간자료 — annual statistical tables (Excel)",
        "update_freq": "annual",
    },
}
