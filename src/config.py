"""
Constants for kr-health-monitor.

All verified from:
  - 보건복지부 고시 제2024-226호 (updated August 2024)
  - MFDS 의료기기 허가 목록 (confirmed March 2026)
  - data.go.kr API documentation
"""

from pathlib import Path

# Directory where the user places raw downloaded Excel/CSV files.
# Gitignored; must be created manually. See CLAUDE.md > Privacy Rule.
# Note: capital D ("Data/") is intentional — matches the Windows folder convention.
# Pipeline-generated parquets go to data/processed/ (lowercase d).
DATA_SOURCE_DIR = Path(__file__).resolve().parent.parent / "Data" / "raw" / "used"

# ─── Policy and device constants (moved to dedicated modules) ─────────────────
# Import from src.policy: NHIS_REIMB_HISTORY, NHIS_REIMBURSEMENT_RATIO, MARKET_PRICES_KRW
# Import from src.devices: CGM_APPROVED_PRODUCTS

# ─── HIRA API endpoints ──────────────────────────────────────────────────────
# All via data.go.kr; require serviceKey after free registration (~1-2 hr activation)
HIRA_API_BASE = "http://apis.data.go.kr/B551182"

# Drug/pharma utilization (15047819) — NOT device-specific
# Useful for diabetes medication trends (not CGM hardware)
HIRA_DRUG_USAGE_SVC = f"{HIRA_API_BASE}/msupUserInfoService1.2"

# 치료재료정보조회서비스 (3074384) — 급여/비급여 목록; endpoint returns XML
# Confirmed endpoint: /getPaymentNonPaymentList1.2
# Returns: 치료재료코드, 품목명, 급여구분, 분류정보
# NOTE: same HIRA_API_KEY used for both HIRA APIs below
HIRA_TREATMENT_MATERIAL_BASE = "https://apis.data.go.kr/B551182/mcatInfoService1.2"
HIRA_TREATMENT_MATERIAL_ENDPOINT = "/getPaymentNonPaymentList1.2"
HIRA_TREATMENT_MATERIAL_DATASET = "3074384"

# 병원정보서비스 (15001698) — hospital basic list; query by region code + facility class
# Returns: 요양기관명, 주소, 전화번호, URL per facility record
HIRA_INSTITUTION_BASE = "https://apis.data.go.kr/B551182/hospInfoServicev2"
HIRA_INSTITUTION_ENDPOINT = "/getHospBasisList"
HIRA_INSTITUTION_DATASET = "15001698"

# HIRA Open Data Portal downloads (no API — file downloads)
HIRA_OPENDATA_REGIONAL_DIABETES_SNO = "13702"   # 지역별 당뇨병 진료현황(2019–2023), Excel 1.1MB
HIRA_OPENDATA_REGION_CODES_DATASET = "15067469" # 행정구역 코드테이블, CSV 302 rows

# ─── MFDS API (의료기기정보포털) ──────────────────────────────────────────────
# Dataset: 15057456 (medical device approval list)
MFDS_API_BASE = "https://apis.data.go.kr/1471000"
MFDS_DEVICE_LIST_ENDPOINT = "/MdlpPrdlstPrmisnInfoService05/getMdlpPrdlstPrmisnList04"
MFDS_DEVICE_DETAIL_ENDPOINT = "/MdlpPrdlstPrmisnInfoService05/getMdlpPrdlstPrmisnItem04"
MFDS_DATASET_ID = "15057456"
# MFDS response fields: PRDUCT, ENTRPS, PRMISN_DT (YYYYMMDD), PRDUCT_PRMISN_NO, GRADE, MANUF_NM, TYPE_NAME
# NOTE: NO price data in MFDS API — prices hardcoded above

# ─── NHIS open data (data.go.kr — ALL BULK FILE DOWNLOADS, not APIs) ──────────
NHIS_CHECKUP_DATASET = "15007122"    # 건강검진정보 — includes blood glucose (혈당)
NHIS_CLAIMS_DATASET = "15007115"     # 진료내역정보 — includes ICD-10 E10/E11
NHIS_PUBLICATIONS_DATASET = "15095102"  # 발간자료 — annual stats Excel
NHIS_DIABETES_CONSUMABLES_DATASET = "15114317"  # 당뇨병환자 등록현황 및 당뇨병소모성재료 지급현황
# URL: https://www.data.go.kr/data/15114317/fileData.do
# XLSX file download; covers through 2023-12-31
# Contains: diabetes patient registration counts + CGM/소모성재료 reimbursement records
# Key value: may include product-level and regional CGM utilization — the data our pipeline
# currently cannot obtain from any other source

# ─── 17 시도 region codes (standard, confirmed from data.go.kr/data/15067469) ─
REGION_CODES = {
    "11": "서울",  "21": "부산",  "22": "대구",  "23": "인천",
    "24": "광주",  "25": "대전",  "26": "울산",  "29": "세종",
    "31": "경기",  "32": "강원",  "33": "충북",  "34": "충남",
    "35": "전북",  "36": "전남",  "37": "경북",  "38": "경남",
    "39": "제주",
}

# Reverse mapping: region name → 2-digit code (e.g. "경기" → "31")
REGION_CODE_REVERSE: dict[str, str] = {v: k for k, v in REGION_CODES.items()}

# ─── NHIS-internal 시도 codes (국민건강보험공단_건강검진정보 CSV) ─────────────────────
# These differ from standard 행안부/HIRA 2-digit codes above.
# Mapping verified 2026-03-11 against 국민건강보험공단_건강검진정보_2024.CSV
NHIS_REGION_MAP = {
    "11": "서울",
    "26": "부산",
    "27": "대구",
    "28": "인천",
    "29": "광주",
    "30": "대전",
    "31": "울산",
    "36": "세종",
    "41": "경기",
    "42": "강원",
    "43": "충북",
    "44": "충남",
    "45": "전북",
    "46": "전남",
    "47": "경북",
    "48": "경남",
    "49": "제주",
}

# ─── 시도 name normalization map (abbreviated → full name) ───────────────────
# Source: T2D 시군구 CSV (dataset 15145378) uses abbreviated 시도 names.
# Verified against 17 standard 시도 codes (data.go.kr/data/15067469).
SIDO_NAME_MAP = {
    "경남": "경상남도",
    "경북": "경상북도",
    "전남": "전라남도",
    "전북": "전라북도",
    "충남": "충청남도",
    "충북": "충청북도",
    "부산시": "부산광역시",
    "대구시": "대구광역시",
    "인천시": "인천광역시",
    "광주시": "광주광역시",
    "대전시": "대전광역시",
    "울산시": "울산광역시",
    "강원도": "강원특별자치도",  # 2023 행정구역 개편
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
    "nhis_diabetes_consumables": {
        "type": "file_download",
        "portal": "data.go.kr",
        "dataset_id": "15114317",
        "url": "https://www.data.go.kr/data/15114317/fileData.do",
        "description": "NHIS 당뇨병환자 등록현황 및 당뇨병소모성재료 등 지급현황 — XLSX through 2023-12-31; CGM reimbursement records",
        "update_freq": "annual",
        "status": "not_downloaded",  # Download and inspect before adding to pipeline
    },
}
