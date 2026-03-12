# Methodology: NHIS CGM Coverage Adequacy Analysis

**Project:** kr-health-monitor
**Version:** 2026-03-12
**Scope:** Continuous Glucose Monitor (CGM) sensors, Korea national health insurance system

---

## 1. Research Objective

Quantify the gap between the NHIS 기준금액 (reimbursement ceiling) for CGM sensors and
real-world market prices, estimate the resulting patient out-of-pocket burden, identify
regional disparities in diabetes patient distribution, and track whether the ceiling has
kept pace with market costs over time.

---

## 2. Data Sources

Twelve distinct dataset sources feed the pipeline. Each has a different access pattern.
Sources are organized by extractor script.

### 2.1 HIRA 지역별 당뇨병 진료현황 (Regional Diabetes Statistics)

| Attribute | Detail |
|-----------|--------|
| Provider | 건강보험심사평가원 (HIRA) |
| Portal | opendata.hira.or.kr |
| File | `[건강보험심사평가원] (2024) 지역별 당뇨병 진료현황(2019년~2023년).xlsx` |
| Access | Direct file download (no API key); place manually in `Data/raw/` |
| Coverage | 17 시도 × 5 years (2019–2023) = 85 rows |
| Output parquet | `hira_regional_diabetes.parquet` |
| Key columns | `year`, `region_code`, `region_name`, `patient_count`, `visit_days`, `cost_krw_thousands` |
| ICD scope | E10–E14 (all diabetes) |
| Granularity | Provincial (시도) level only; municipality (시군구) level exists in a separate sheet but is not used from this file |

This is the primary denominator for the regional equity analysis. It counts total diabetes
patients per province, not CGM users specifically — CGM-specific prescription counts by
region are not available in any public data source.

The opendata.hira.or.kr portal does not support headless (non-browser) file downloads.
The pipeline uses a locally cached copy; `fetch_hira.py` Part A falls back to a
download attempt only if the local file is absent.

### 2.2 HIRA 치료재료정보조회서비스 (Treatment Material Catalog)

| Attribute | Detail |
|-----------|--------|
| Provider | 건강보험심사평가원 (HIRA) |
| Portal | data.go.kr, dataset 3074384 |
| Endpoint | `https://apis.data.go.kr/B551182/mcatInfoService1.2/getPaymentNonPaymentList1.2` |
| Access | REST API (XML response), requires `serviceKey` |
| Query parameter | `mdivCd=900085` (연속혈당측정용전극 classification code) |
| Records returned | 11 CGM products (as of March 2026) |
| Output parquet | `hira_treatment_materials.parquet` |
| Key columns | `product_name`, `coverage_status`, `max_unit_price_krw`, `m_code`, `importer`, `manufacturer`, `subcategory` |

All 11 products are classified **비급여** (non-reimbursable) in the standard 치료재료 catalog.
`max_unit_price_krw` is null for all items — the HIRA catalog does not store the 기준금액
for CGM because it operates through a separate 고시 policy mechanism (see Section 3.1).

**Critical note on search parameters:** The `itmNm` field in this API only matches English
text. Korean keyword search returns zero results. Querying by `mdivCd=900085` (the 중분류코드
for 연속혈당측정용전극) is the correct method. Product names in the database are English
(e.g., "FREESTYLE LIBRE CGM SENSOR", "DEXCOM G7 CGM SENSOR").

**Important limitation:** The HIRA catalog confirms product existence and coverage
classification but cannot be used to derive the 기준금액 value. That figure comes
exclusively from the policy documents (고시) coded directly into the system.

### 2.3 MFDS 의료기기 품목허가 목록 (Device Approval List)

| Attribute | Detail |
|-----------|--------|
| Provider | 식품의약품안전처 (MFDS) |
| Portal | data.go.kr, dataset 15057456 |
| Endpoint | `https://apis.data.go.kr/1471000/MdlpPrdlstPrmisnInfoService05/getMdlpPrdlstPrmisnList04` |
| Access | REST API (JSON response), requires `serviceKey` |
| Query | Korean keyword: 연속혈당측정 |
| Output parquet | `mfds_device_prices.parquet` |
| Key columns | `device_name`, `manufacturer`, `approved_date`, `approval_number`, `grade`, `model` |

The MFDS API contains **no price data**. Its role is confirming which products are
legally approved for sale in Korea and when they received approval. Market prices are
hardcoded constants from external research (see Section 3.2).

`build_master.py` gracefully handles absence of `mfds_device_prices.parquet` with a
warning and falls back to market prices from `src/policy.py` directly, so MFDS API
unavailability does not affect any computed outputs.

### 2.4 NHIS 건강보험통계연보 ch06 (Annual Statistics Yearbook, Chapter 6)

| Attribute | Detail |
|-----------|--------|
| Provider | 국민건강보험공단 (NHIS) |
| Portal | data.go.kr, dataset 15095102 |
| Access | Bulk Excel file download (manual); **not automatable via REST API** |
| Files used | ch06 Excel files for 2022, 2023, and 2024 |
| Sheet pattern | `6-3` family (질병분류별 진료현황, total figures) |
| Output parquet | `nhis_annual_stats.parquet` |
| Key columns | `year`, `icd_code`, `patient_count`, `visit_days`, `cost_krw_thousands`, `case_count` |
| ICD codes extracted | E10 (Type 1), E11 (Type 2), E14 (unspecified) |

Each row in the 6-3 sheets contains two disease entries side by side (offset 0 and offset 8).
The parser `parse_yearbook_ch06()` in `src/nhis_client.py` scans for ICD code matches in
column `offset + 1` and reads patient count from `offset + 3`. Monetary values are in
천원 (1,000 KRW) units.

### 2.5 NHIS 건강검진정보 (Health Checkup Survey)

| Attribute | Detail |
|-----------|--------|
| Provider | 국민건강보험공단 (NHIS) |
| Portal | data.go.kr, dataset 15007122 |
| Access | Bulk CSV download (annual); **not automatable via REST API** |
| File | `국민건강보험공단_건강검진정보_2024.CSV` (87.7 MB, ~1M rows, cp949 encoding) |
| Output parquet | `nhis_checkup_summary.parquet` (17 rows) |
| Key columns | `시도코드` (NHIS-internal region codes), `식전혈당(공복혈당)` (fasting blood glucose) |
| Output | 17-row regional summary: mean fasting glucose, high-glucose rate, screened count |

**Region code translation required:** NHIS checkup data uses a different internal coding
system from the standard 행안부/HIRA 2-digit codes. The mapping `NHIS_REGION_MAP` in
`src/config.py` translates NHIS codes (11, 26–31, 36, 41–49) to standard province names,
which are then reverse-mapped to HIRA codes.

### 2.6 NHIS 당뇨병환자 소모성재료 지급현황 (Diabetes Consumables Monthly Payments)

| Attribute | Detail |
|-----------|--------|
| Provider | 국민건강보험공단 (NHIS) |
| Portal | data.go.kr, dataset 15114317 |
| Access | CSV file download (per year); manual download, place in `Data/raw/` |
| Coverage | 2021–2024 (one CSV per year) |
| Output parquet | `nhis_consumables_monthly.parquet` (48 rows after full run) |
| Key columns | `year`, `month`, `transaction_count`, `payment_won` |
| Parsed by | `parse_consumables_monthly_csv()` in `src/nhis_client.py` |
| Pipeline stage | fetch_nhis.py Part D |

**Scope note:** These records cover T1D + T2D 소모성재료 (consumables) reimbursements,
not CGM specifically. The `transaction_count` field counts claim transactions, not unique
beneficiaries. 2024 total payment was ₩111.2B.

### 2.7 NHIS CGM 이용 현황 (CGM Utilization, Unique Users)

| Attribute | Detail |
|-----------|--------|
| Provider | 국민건강보험공단 (NHIS) |
| Access | CSV file download; place in `Data/raw/` |
| Coverage | 2020–2024 (5 data points) |
| Output parquet | `nhis_cgm_utilization.parquet` (5 rows) |
| Key columns | `year`, `cgm_users` |
| Parsed by | `parse_cgm_utilization_csv()` in `src/nhis_client.py` |
| Pipeline stage | fetch_nhis.py Part E |

This is the deduplicated CGM user count per year — the numerator for the
`adoption_rate_registered` metric in `coverage_trend.csv`.

### 2.8 NHIS 요양비 등록 수급자 현황 (Registered 요양비 Beneficiaries)

| Attribute | Detail |
|-----------|--------|
| Provider | 국민건강보험공단 (NHIS) |
| Access | XLSX file (Sheet 2 of annual diabetes info file); place in `Data/raw/` |
| Coverage | 2019–2024 (6 rows) |
| Output parquet | `nhis_yoyangbi_registered.parquet` |
| Key columns | `year`, `t1d_registered`, `t2d_registered` |
| Parsed by | `parse_yoyangbi_registered_xlsx()` in `src/nhis_client.py` |
| Pipeline stage | fetch_nhis.py Part F |

This is the denominator for `adoption_rate_registered`: the number of T1D patients
formally registered to receive 요양비 reimbursement for CGM. It is smaller than the
total T1D patient count because it excludes patients who are eligible but not registered.

### 2.9 NHIS T1D 환자 수 (연도별 연령별 성별)

| Attribute | Detail |
|-----------|--------|
| Provider | 국민건강보험공단 (NHIS) |
| Access | CSV file download; place in `Data/raw/` |
| Coverage | 2021–2024 (one row per year × age × sex combination) |
| Output parquet | `nhis_t1d_age_sex.parquet` (~804 rows) |
| Key columns | `year`, `age_band`, `sex`, `patient_count`, `suppressed` |
| Parsed by | `parse_t1d_age_sex_csv()` in `src/nhis_client.py` |
| Pipeline stage | fetch_nhis.py Part C |

Small cells are suppressed (patient count < 10) and flagged `suppressed=True`.
National totals per year are derived by summing non-suppressed rows, which slightly
understates the true count.

### 2.10 NHIS T1D 연도별 1세 단위 연령별 현황

| Attribute | Detail |
|-----------|--------|
| Provider | 국민건강보험공단 (NHIS) |
| Access | CSV file download; place in `Data/raw/` |
| Coverage | 2013–2023 (1-year age granularity) |
| Output parquet | `nhis_t1d_age_annual.parquet` (~1,109 rows) |
| Key columns | `year`, `age`, `patients`, `suppressed` |
| Parsed by | `parse_t1d_age_annual_csv()` in `src/nhis_client.py` |
| Pipeline stage | fetch_nhis.py Part G |

Used to compute `eligible_19plus` (T1D patients age ≥ 19) and `eligible_15plus`
(age ≥ 15) for the eligibility pool columns in `coverage_trend.csv`. Pre-2018 values
reflect ICD coding drift and should not be compared to post-2020 values.

### 2.11 NHIS 연도별 당뇨병 진료정보 (Annual Diabetes Clinical + Device Info)

| Attribute | Detail |
|-----------|--------|
| Provider | 국민건강보험공단 (NHIS) |
| Access | XLSX file (3-sheet workbook); place in `Data/raw/` |
| Coverage | 2010–2023 (Sheet 1: E10-E14 age-split) |
| Output parquets | `nhis_e10_age_split.parquet` (~158 rows) |
| Parsed by | `parse_annual_diabetes_clinical_xlsx()` in `src/nhis_client.py` |
| Pipeline stage | fetch_nhis.py Part H |

Sheet 1 of this file covers E10–E14 by age bracket. Sheet 2 (요양비 registered
beneficiaries) is parsed separately as Part F.

### 2.12 NHIS 시군구별 T1D+T2D 현황 (Municipality-Level Diabetes)

| Attribute | Detail |
|-----------|--------|
| Provider | 국민건강보험공단 (NHIS) |
| Access | XLSX file (sex sheet + age sheet); place in `Data/raw/` |
| Coverage | 2022–2024 (~247 시군구 × 3 years) |
| Output parquet | `nhis_sigungu_t1d_t2d.parquet` (~18,000 rows) |
| Key columns | `year`, `시도`, `시군구`, `구분` (T1D/T2D), `dimension` (sex/age), `patients` |
| Parsed by | `parse_sigungu_t1d_t2d_xlsx()` in `src/nhis_client.py` |
| Pipeline stage | fetch_nhis.py Part I |

Provides sub-provincial granularity for the regional equity analysis. The runner
`run_regional_equity.py` uses this to build 시군구-level equity rows in addition
to the standard 시도-level rows from Section 2.1.

### 2.13 NHIS T2D 시군구별 의료기관 종별 진료현황

| Attribute | Detail |
|-----------|--------|
| Provider | 국민건강보험공단 (NHIS) |
| Access | CSV file download; place in `Data/raw/` |
| Coverage | 2021–2023 (~4,000 rows) |
| Output parquet | `nhis_t2d_sigungu_clinical.parquet` |
| Key columns | `year`, `sido`, `sigungu`, `institution_type`, `visit_count` |
| Parsed by | `parse_t2d_sigungu_csv()` in `src/nhis_client.py` |
| Pipeline stage | fetch_nhis.py Part J |

Used to compute `primary_care_pct` (의원 visits / total visits per 시군구) in the
시군구-level regional equity output.

### 2.14 NHIS 당뇨병의료이용률 (Diabetes Utilization Rate, Multi-year)

| Attribute | Detail |
|-----------|--------|
| Provider | 국민건강보험공단 (NHIS) |
| Access | Multiple CSV files by year; place in `Data/raw/` |
| Coverage | 2002–2024 (시도/시군구 level) |
| Output parquet | `nhis_diabetes_utilization_rate.parquet` |
| Parsed by | `parse_diabetes_utilization_rate_csvs()` in `src/nhis_client.py` |
| Pipeline stage | fetch_nhis.py Part K |

### 2.15 NHIS 인슐린 주사 청구 현황 (Insulin Claims Monthly)

| Attribute | Detail |
|-----------|--------|
| Provider | 국민건강보험공단 (NHIS) |
| Access | CSV file download; place in `Data/raw/` |
| Coverage | 2016–2023 (monthly, by age) |
| Output parquet | `nhis_insulin_claims_monthly.parquet` |
| Parsed by | `parse_insulin_claims_csv()` in `src/nhis_client.py` |
| Pipeline stage | fetch_nhis.py Part L |

---

## 3. Policy Constants

### 3.1 NHIS 기준금액 (Reimbursement Ceiling)

The 기준금액 is not stored in any API or database accessible to this system. It is
encoded directly from the official 보건복지부 고시 documents into `src/policy.py` as
`NHIS_REIMB_HISTORY`:

```python
NHIS_REIMB_HISTORY = {
    "cgm_sensor": [
        ("2022-08-01", 210_000, "2022-170"),   # ₩210,000/quarter; T1D age 19+ eligible
        ("2024-08-01", 210_000, "2024-226"),   # Same ceiling; expanded to insulin-dep T2D
    ],
    "insulin_pump": [
        ("2020-01-01", 1_700_000, "2020-xxx"), # ₩1,700,000 per device (one-time)
        ("2024-08-01", 1_700_000, "2024-226"), # Confirmed same amount in 2024 update
    ],
}

NHIS_REIMBURSEMENT_RATIO = 0.70  # NHIS pays 70% of min(actual, 기준금액)
```

**Unit convention:** The 기준금액 for CGM is quarterly. All computations in the system
use quarterly as the base unit. Monthly equivalents are derived as `quarterly / 3`
only for display purposes.

**NHIS reimbursement ratio:** NHIS pays 70% of `min(actual_price, 기준금액)`.
Low-income exception (차상위 계층): 100% within the 기준금액 — not modeled in this system.

**Eligibility:** Coverage under 고시 2022-170 applies to Type 1 diabetes patients aged
19+. The 2024-226 update expanded eligibility to insulin-dependent Type 2 patients, but
did not change the ceiling amount. This system does not model the eligible vs. non-eligible
population split.

**기준금액 lookup:** `get_reimb_ceiling(device_category, as_of_date)` in `src/coverage.py`
returns the most recent entry in `NHIS_REIMB_HISTORY` on or before `as_of_date`. This
correctly assigns:
- 2018–2021: raises `ValueError` (no coverage; treated as null in gap series)
- 2022–2026: ₩210,000 (ceiling frozen since 고시 2022-170)

### 3.2 Market Price Ranges

Market prices are hardcoded point-in-time estimates verified from diabetes advocacy
community research (2025). They are monthly figures, stored in `src/policy.py` as
`MARKET_PRICES_KRW`:

```python
MARKET_PRICES_KRW = {
    "cgm_sensor": {
        "low":  155_000,  # Budget domestic options
        "mid":  200_000,  # FreeStyle Libre 2, Dexcom G6 typical
        "high": 280_000,  # Dexcom G7; Guardian 4 at upper end
    },
    "insulin_pump_supplies": {
        "low":  450_000,  # Monthly consumable estimate
        "high": 700_000,
    },
}
```

These are static constants. The system has no mechanism for historical price tracking
or future price projection. Year-over-year trend output uses these same values for
all coverage years.

---

## 4. Core Computations

### 4.1 Patient Burden (Coverage Gap)

Implemented in `src/coverage.py → compute_quarterly_patient_burden()`.

For a given (market price, 기준금액) pair, computed per quarter:

```
market_quarterly        = market_price_monthly × 3

nhis_pays               = min(market_quarterly, reimb_ceiling_quarterly) × 0.70

patient_pays            = market_quarterly − nhis_pays

burden_ratio            = patient_pays / market_quarterly

patient_monthly_equiv   = patient_pays / 3
```

At current values (market always exceeds the ceiling):

```
nhis_pays               = 210,000 × 0.70 = ₩147,000/quarter  (fixed, regardless of sensor)

Sensor (low):   patient_pays = (155,000 × 3) − 147,000 = ₩318,000/quarter  → burden 68.4%
Sensor (mid):   patient_pays = (200,000 × 3) − 147,000 = ₩453,000/quarter  → burden 75.5%
Sensor (high):  patient_pays = (280,000 × 3) − 147,000 = ₩693,000/quarter  → burden 82.5%
```

Since market price always exceeds the ceiling, `nhis_pays` is constant (₩147,000/quarter)
regardless of which sensor a patient uses. Burden ratio is entirely determined by the
market price of the chosen sensor.

### 4.2 Coverage Adequacy Ratio

Implemented in `src/coverage.py → compute_coverage_adequacy_ratio()`.

```
coverage_adequacy_ratio = nhis_pays / market_quarterly
                        = min(market_quarterly, reimb_ceiling_quarterly) × 0.70
                          ─────────────────────────────────────────────────────
                                         market_quarterly
```

At mid-market prices: `147,000 / 600,000 = 0.245` (NHIS covers 24.5% of cost).
Range for CGM: approximately 0.175–0.316 across the three price tiers.

### 4.3 Gap Time Series

Implemented in `src/coverage.py → compute_gap_series(years, device_category)`.

Iterates over a list of years and applies the burden computation for each
(year, price tier) combination. For years before coverage began (before 2022-08-01),
`reimb_ceiling_quarterly` and all derived fields are null.

Output columns include convenience aliases:
- `patient_burden_ratio_mid` (alias for `burden_ratio_mid`)
- `patient_pays_quarterly_mid` (alias for `patient_pays_quarterly_mid`)

Since market prices are not time-varying in this system, burden ratios are identical
for every year from 2022 to the present. The trend CSV shows the ceiling is frozen,
but cannot show whether the gap is widening or narrowing due to price changes.

### 4.4 Coverage Trend Enrichment

`run_coverage_trend.py` performs six sequential merge operations after calling
`compute_gap_series(2018–2030)`:

1. **Merge 1 — `nhis_annual_stats`:** Adds `t1d_patient_count` (E10 sum per year) and
   `national_diabetes_patients` (all ICD codes per year).

2. **Merge 2 — `nhis_t1d_age_sex`:** Supplements `t1d_patient_count` for years missing
   from `nhis_annual_stats` (particularly 2021 and 2024), by summing non-suppressed
   patient_count rows per year.

3. **Merge 3 — `nhis_consumables_monthly`:** Adds `consumables_transactions_annual`
   and `consumables_payment_annual_won` (T1D + T2D 소모성재료, not CGM-only).

4. **Merge 4 — `nhis_cgm_utilization`:** Adds `cgm_users` (deduplicated CGM users,
   2020–2024).

5. **Merge 5 — `nhis_yoyangbi_registered`:** Adds `t1d_registered`, `t2d_registered`,
   and computes:
   - `adoption_rate_registered = cgm_users / t1d_registered` (preferred metric;
     CGM uptake among formally registered 요양비 beneficiaries)
   - `adoption_rate_total = cgm_users / t1d_patient_count` (broader denominator)

6. **Merge 6 — `nhis_t1d_age_annual`:** Adds `eligible_19plus` (T1D patients age ≥ 19,
   eligibility criterion under 고시 2022-170) and `eligible_15plus` (age ≥ 15,
   under 고시 2024-226).

**Column disambiguation — two metrics not interchangeable:**
- `patient_share_pct` in `regional_equity.csv` = regional_patients / national_total × 100.
  This is a geographic distribution metric (share of national diabetes burden by province),
  not a CGM adoption rate.
- `adoption_rate_registered` in `coverage_trend.csv` = cgm_users / t1d_registered × 100.
  This is the actual CGM uptake rate among registered beneficiaries — a policy
  effectiveness metric.

### 4.5 Regional Equity Scoring

Implemented in `src/equity.py → score_regional_disparity()`.

Input: a DataFrame with column `patient_share_pct` (each region's share of national
diabetes patients for a given year). This is computed in `run_regional_equity.py` as:

```
patient_share_pct = region_patient_count / national_patient_count × 100
```

The function adds three columns:

```
share_pct_rank       = rank(patient_share_pct, ascending=False)
                       1 = highest share, 17 = lowest

national_median_ratio = patient_share_pct / median(patient_share_pct)
                        1.0 = at the national median

disparity_flag        = True  if  patient_share_pct < median × 0.50
```

**Disparity index** (`compute_disparity_index()`):

```
disparity_index = max(patient_share_pct) / min(patient_share_pct)
```

A value of ~50.6 means 경기 (the most populous province) has 50.6 times the diabetes
patient share of 세종 (the smallest). This reflects natural population distribution,
not differential access quality.

### 4.6 시군구-Level Equity Extension

`run_regional_equity.py` builds an additional set of sub-provincial rows from
`nhis_sigungu_t1d_t2d.parquet` using `_build_sigungu_equity()`. This function:

1. Filters to the sex sheet (`sheet == "sex"`) and all-sex total rows
   (`dimension` in `["계", "전체", "합계"]`).
2. Pivots by `구분` (1형/2형 당뇨병) to produce `t1d_patients` and `t2d_patients`
   per (year, 시도, 시군구).
3. Computes `t1d_t2d_ratio = t1d_patients / t2d_patients`.
4. Optionally joins `nhis_t2d_sigungu_clinical` to compute
   `primary_care_pct = 의원 visits / total visits per 시군구`.

Output rows carry `granularity = "시군구"` to distinguish them from the 시도-level
rows (`granularity = "시도"`).

---

## 5. Pipeline Architecture

The pipeline is organized into three stages: extract, transform, and analyze.

```
Stage 1a  pipeline/fetch_hira.py
          ├── Part A: Local HIRA Excel (or download fallback)
          │          → hira_regional_diabetes.parquet  (85 rows: 17 시도 × 5 years)
          └── Part B: HIRA Treatment Material REST API (mdivCd=900085)
                     → hira_treatment_materials.parquet  (11 CGM products)

Stage 1b  pipeline/fetch_mfds.py
          └── MFDS Device Approval REST API (keyword: 연속혈당측정)
                     → mfds_device_prices.parquet

Stage 1c  pipeline/fetch_nhis.py  (12 parts; all are file-based parsers)
          ├── Part A: 건강보험통계연보 ch06 Excel (2022 + 2023 + 2024)
          │          → nhis_annual_stats.parquet
          ├── Part B: 건강검진정보 CSV
          │          → nhis_checkup_summary.parquet  (17 rows)
          ├── Part C: T1D age/sex CSV (2021–2024)
          │          → nhis_t1d_age_sex.parquet  (~804 rows)
          ├── Part D: Diabetes consumables monthly CSV (all years: 2021–2024)
          │          → nhis_consumables_monthly.parquet  (48 rows)
          ├── Part E: CGM utilization CSV (unique CGM users 2020–2024)
          │          → nhis_cgm_utilization.parquet  (5 rows)
          ├── Part F: 요양비 registered beneficiaries XLSX (Sheet 2)
          │          → nhis_yoyangbi_registered.parquet  (6 rows)
          ├── Part G: T1D by 1-year age band CSV (2013–2023)
          │          → nhis_t1d_age_annual.parquet  (~1,109 rows)
          ├── Part H: Annual diabetes clinical XLSX (Sheet 1: E10-E14 age-split)
          │          → nhis_e10_age_split.parquet  (~158 rows)
          ├── Part I: 시군구-level T1D+T2D XLSX (sex + age sheets)
          │          → nhis_sigungu_t1d_t2d.parquet  (~18,000 rows)
          ├── Part J: T2D clinical by institution type per 시군구 CSV (2021–2023)
          │          → nhis_t2d_sigungu_clinical.parquet  (~4,000 rows)
          ├── Part K: 당뇨병의료이용률 merged multi-year CSVs (2002–2024)
          │          → nhis_diabetes_utilization_rate.parquet
          └── Part L: Insulin claims monthly CSV (2016–2023)
                     → nhis_insulin_claims_monthly.parquet

Stage 2   pipeline/build_master.py
          ├── Loads: hira_regional_diabetes, mfds_device_prices, nhis_annual_stats
          ├── Applies: get_reimb_ceiling() per year × device_category
          ├── Expands: 3 price tiers (low/mid/high) per (year, region) row
          ├── Computes: nhis_pays, patient_burden_krw, burden_ratio, coverage_adequacy_ratio
          ├── Adds: regional disparity scores (if patient_share_pct column present)
          └── Output: coverage_master.parquet  (255 rows: 17 regions × 5 years × 3 tiers)
                     (or 15 synthetic rows × 3 tiers if regional parquet absent)

Stage 3a  analysis/run_coverage_gap.py
          ├── compute_gap_series(2018–2026, "cgm_sensor")
          │          → analysis/coverage_gap.csv  (9 rows)
          └── [optional] join hira_treatment_materials + CGM_M_CODE_TO_TIER
                     → analysis/coverage_gap_by_product.csv  (~25 rows per active product)

Stage 3b  analysis/run_regional_equity.py
          ├── score_regional_disparity() per year → 시도-level rows
          ├── [optional] _build_sigungu_equity() from nhis_sigungu_t1d_t2d
          ├── [optional] join nhis_checkup_summary (glucose severity columns)
          └── Output: analysis/regional_equity.csv
                     (85 rows 시도-level + ~700+ rows 시군구-level if parquets present)

Stage 3c  analysis/run_coverage_trend.py
          ├── compute_gap_series(2018–2030) — includes 2027–2030 projection years
          ├── Merge 1: nhis_annual_stats → t1d_patient_count, national_diabetes_patients
          ├── Merge 2: nhis_t1d_age_sex → fills t1d_patient_count gaps (2021, 2024)
          ├── Merge 3: nhis_consumables_monthly → consumables annual aggregates
          ├── Merge 4: nhis_cgm_utilization → cgm_users
          ├── Merge 5: nhis_yoyangbi_registered → t1d_registered, adoption_rate_registered
          ├── Merge 6: nhis_t1d_age_annual → eligible_19plus, eligible_15plus
          └── Output: analysis/coverage_trend.csv  (13 rows: 2018–2030)
```

All intermediate data is stored as Parquet (Apache Arrow format) in `data/processed/`.
All final outputs are CSV in `analysis/`. Both directories are gitignored.

**File detection:** All NHIS parsers use `os.listdir()`-based file finders in `src/store.py`
(e.g., `find_t1d_csv()`, `find_consumables_csvs()`) to locate input files by substring
match against the filename. This approach correctly handles Korean Unicode filenames on
all platforms without relying on glob pattern matching, which can behave inconsistently
with non-ASCII characters on Windows.

---

## 6. `coverage_master.parquet` Schema

The master table is the join of regional data and policy constants. Each row represents
one (year, region, price tier) combination. Expected dimensions: 255 rows × 17 columns
(85 region-years × 3 price tiers) when `hira_regional_diabetes.parquet` is present.

| Column | Type | Source | Notes |
|--------|------|--------|-------|
| `year` | int | HIRA regional diabetes Excel | 2019–2023 |
| `region_code` | str | HIRA | 2-digit 시도 code (e.g. "11" = 서울) |
| `region_name` | str | HIRA / config | Korean province name |
| `device_category` | str | hardcoded | "cgm_sensor" for all rows |
| `patient_count` | int | HIRA | Diabetes patients (E10–E14) in that region/year |
| `claim_count` | float | HIRA | Maps to `cost_krw_thousands` from regional data |
| `reimb_ceiling_quarterly_krw` | float | policy.py | ₩210,000 from 2022; null pre-2022 |
| `market_price_monthly_krw` | int | policy.py | ₩155K / ₩200K / ₩280K by tier |
| `price_tier` | str | hardcoded | "low", "mid", or "high" |
| `nhis_pays` | float | computed | ₩147,000/quarter when coverage applies |
| `patient_burden_krw` | float | computed | `market_quarterly − nhis_pays` |
| `burden_ratio` | float | computed | Patient's share of total cost |
| `coverage_adequacy_ratio` | float | computed | NHIS share of total cost |
| `adoption_rate_pct` | float | computed | Null unless regional adoption data available |
| `adoption_pct_rank` | int | computed | Null unless `adoption_rate_pct` is populated |
| `national_median_ratio` | float | computed | Null unless disparity scoring applied |
| `disparity_flag` | bool | computed | Null unless disparity scoring applied |

---

## 7. Known Methodological Limitations

### 7.1 Market prices are static point-in-time estimates

The three price tiers (₩155K / ₩200K / ₩280K monthly) are hardcoded constants from
2025 research. The system has no mechanism for:
- Historical price changes (CGM prices have been declining as competition increases)
- Future projections beyond what is hard-coded
- Per-product price differentiation within a tier

Consequence: burden ratios are identical across all years from 2022 to the present in
all three output CSVs. The trend CSV shows the ceiling is frozen, but cannot show
whether the gap is widening or narrowing due to price movement.

### 7.2 Diabetes patient count ≠ CGM adoption rate

The denominator for regional equity is total diabetes patients (E10–E14), not CGM users.
This is the only regionally-disaggregated diabetes metric available in public data.
The actual number of CGM users per region is unknown and not available in any public API.

The 50.6× disparity index reflects population concentration between provinces
(경기 vs. 세종), not differential access to CGM technology. A meaningful adoption-rate
analysis by region would require prescription records by device category and region,
which require IRB approval and on-site NHIS data access.

### 7.3 Eligibility population not modeled

The analysis uses all diabetes patients (E10–E14) as the denominator in some contexts.
The actual eligible population for CGM coverage under 고시 2022-170/2024-226 is a subset:
- Type 1 diabetes patients aged 19+ (고시 2022-170)
- Insulin-dependent Type 2 patients aged 19+ (added by 고시 2024-226)

Using total E10–E14 as the denominator understates the coverage adequacy problem for
eligible patients specifically. The `eligible_19plus` and `eligible_15plus` columns in
`coverage_trend.csv` provide the age-gated T1D counts from 1-year granularity data.

### 7.4 Low-income exception not modeled

차상위 계층 (near-poverty households) receive 100% coverage within the 기준금액, reducing
their effective burden to ₩0 for the portion below the ceiling. This system applies
the standard 70% ratio uniformly to all patients.

### 7.5 HIRA 치료재료 catalog confirms classification, not 기준금액

All 11 CGM sensors in the HIRA 치료재료 database are classified as **비급여** (non-reimbursable).
`max_unit_price_krw` is null for all items. This does not contradict the project premise:
the ₩210,000/quarter 기준금액 from 고시 2022-170 operates through the 요양급여 기준 mechanism
as a separate policy layer, not through the standard 치료재료 급여 catalog. The catalog
shows the product's default classification (비급여 for general patients); eligible T1D
patients claim coverage separately under the 고시 criteria.

### 7.6 HIRA regional download not automatable

The HIRA opendata.hira.or.kr portal does not support headless (non-browser) file
downloads. The Excel file used in Stage 1a Part A was downloaded manually and cached
locally. Pipeline automation uses the cached file; it cannot re-download it
programmatically without browser automation.

### 7.7 NHIS data requires manual download

All NHIS datasets are bulk file downloads, not REST APIs. The ch06 Excel files,
건강검진정보 CSV, and all other NHIS source files must be downloaded manually from
nhiss.nhis.or.kr and data.go.kr respectively, and placed in `Data/raw/` before
the pipeline can run. The pipeline detects their presence using `os.listdir()`-based
finders to ensure correct Unicode handling of Korean filenames on all platforms.

### 7.8 Consumables data covers T1D + T2D, not CGM-specific

`nhis_consumables_monthly.parquet` covers all diabetes consumables (소모성재료)
reimbursements for both T1D and T2D patients. It does not isolate CGM sensor claims.
`transaction_count` is a claim transaction count, not a unique beneficiary count.

### 7.9 Adoption rate denominator options

Two adoption rate metrics exist with different interpretations:
- `adoption_rate_registered` (preferred): `cgm_users / t1d_registered` — denominator
  is only patients formally registered in the 요양비 system. Likely to overstate uptake
  if some registered patients are inactive.
- `adoption_rate_total`: `cgm_users / t1d_patient_count` — broader denominator; includes
  T1D patients not yet registered, which may understate uptake among eligible patients.

### 7.10 No 2024 regional utilization Excel

The 지역별의료이용통계연보 for 2024 is available as PDF only; no Excel version was
published. Regional data from this source is therefore limited to 2022 and 2023
(and 2019–2023 for the HIRA regional diabetes file).

---

## 8. Reproducibility

All computations are deterministic given the same input files. There are no
random elements, sampling steps, or model training involved.

```bash
# 1. Install dependencies
uv sync --extra dev

# 2. Run all NHIS parsers (requires source files in Data/raw/)
uv run python pipeline/fetch_nhis.py
# Skip specific parts with --skip-util-rate, --skip-insulin, etc.
# Smoke-test with --sample N to limit each parquet to N rows

# 3. Run HIRA and MFDS extractors (requires API key in .env for Part B)
uv run python pipeline/fetch_hira.py
uv run python pipeline/fetch_mfds.py

# 4. Build master table
uv run python pipeline/build_master.py

# 5. Run analysis
uv run python analysis/run_coverage_gap.py
uv run python analysis/run_regional_equity.py
uv run python analysis/run_coverage_trend.py

# Or use the CLI orchestrator:
uv run python pipeline/run.py --device cgm_sensor --year-range 2019-2024
```

**Source files required in `Data/raw/`** (capital D, Windows convention; defined
in `src/config.py` as `DATA_SOURCE_DIR`):

| File | Required by |
|------|-------------|
| `[건강보험심사평가원] (2024) 지역별 당뇨병 진료현황(2019년~2023년).xlsx` | fetch_hira.py Part A |
| `건강보험통계연보 ch06 Excel (2022, 2023, 2024)` | fetch_nhis.py Part A |
| `국민건강보험공단_건강검진정보_2024.CSV` | fetch_nhis.py Part B |
| File containing `제1형 당뇨병 환자 수` in name | fetch_nhis.py Part C |
| Files containing `소모성재료` in name (one per year) | fetch_nhis.py Part D |
| File containing `연속혈당측정` in name | fetch_nhis.py Part E |
| File containing `연도별 당뇨병 진료정보` in name | fetch_nhis.py Parts F + H |
| File containing `1형 당뇨병 연도별 연령별` in name | fetch_nhis.py Part G |
| File containing `시군구` and `당뇨병` in name | fetch_nhis.py Part I |
| File containing `2형 당뇨병` and `시군구` in name | fetch_nhis.py Part J |
| Files containing `당뇨병의료이용률` in name | fetch_nhis.py Part K |
| File containing `인슐린 주사` in name | fetch_nhis.py Part L |

**API keys in `.env`:**
- `HIRA_API_KEY` — data.go.kr unified key (covers HIRA treatment material + MFDS endpoints)
- All three approved REST APIs (HIRA 치료재료, HIRA 병원정보, MFDS) share the same single
  key issued per data.go.kr account.

---

## 9. Output Files

All CSV outputs include a `generated_at` column with the ISO date of generation.
All HTML outputs are located in `analysis/reports/` and are committed to the repository
(un-gitignored via `.gitignore` exception). All Parquet intermediates and CSV outputs
are gitignored (regenerated from source files).

| File | Rows | Description |
|------|------|-------------|
| `analysis/coverage_gap.csv` | 9 | Burden ratio and gap amount per year (2018–2026), 3 price tiers |
| `analysis/coverage_gap_by_product.csv` | ~25 | Per-product burden by coverage year; requires `hira_treatment_materials.parquet` |
| `analysis/regional_equity.csv` | 85+ | Per-region disparity scores per year (2019–2023) 시도-level; 700+ rows if 시군구 parquets present |
| `analysis/coverage_trend.csv` | 13 | Coverage ratio over time (2018–2030 incl. projections); T1D counts, CGM users, adoption rates, eligibility pools |
| `analysis/reports/cgm_coverage_report.html` | — | Interactive HTML policy brief; Plotly charts; generated by `krh report` |
| `data/processed/coverage_master.parquet` | 255 | Master table (17 regions × 5 years × 3 tiers) |
| `data/processed/nhis_annual_stats.parquet` | varies | E10/E11/E14 patient counts by year |
| `data/processed/nhis_checkup_summary.parquet` | 17 | Regional fasting glucose summary |
| `data/processed/nhis_t1d_age_sex.parquet` | ~804 | T1D by year × age band × sex |
| `data/processed/nhis_consumables_monthly.parquet` | 48 | Monthly diabetes consumables payments 2021–2024 |
| `data/processed/nhis_cgm_utilization.parquet` | 5 | Deduplicated CGM users 2020–2024 |
| `data/processed/nhis_yoyangbi_registered.parquet` | 6 | Registered 요양비 T1D/T2D beneficiaries 2019–2024 |
| `data/processed/nhis_t1d_age_annual.parquet` | ~1,109 | T1D by year × 1-year age band 2013–2023 |
| `data/processed/nhis_e10_age_split.parquet` | ~158 | E10-E14 by year × age bracket 2010–2023 |
| `data/processed/nhis_sigungu_t1d_t2d.parquet` | ~18,000 | T1D+T2D by 시군구 × sex+age 2022–2024 |
| `data/processed/nhis_t2d_sigungu_clinical.parquet` | ~4,000 | T2D by institution type per 시군구 2021–2023 |
| `data/processed/nhis_diabetes_utilization_rate.parquet` | varies | 당뇨병의료이용률 by 시도/시군구 2002–2024 |
| `data/processed/nhis_insulin_claims_monthly.parquet` | varies | Monthly insulin injection claims by age 2016–2023 |
| `data/processed/hira_regional_diabetes.parquet` | 85 | HIRA 지역별 당뇨병 진료현황 2019–2023 |
| `data/processed/hira_treatment_materials.parquet` | 11 | CGM products from HIRA 치료재료 catalog |
| `data/processed/mfds_device_prices.parquet` | varies | CGM device approvals from MFDS |

---

*Document version: 2026-03-12*
*Source code: github.com/pon00050/kr-health-monitor*
