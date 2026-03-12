# Pipeline Architecture — kr-health-monitor

---

## 1. Overview

The kr-health-monitor pipeline collects public health insurance data from three Korean
government data sources (HIRA, MFDS, NHIS), computes the gap between the NHIS
reimbursement ceiling for CGM sensors and actual market prices, and produces a set of
policy analysis CSVs and an interactive HTML report. The pipeline is designed for
reproducibility: all computations are deterministic, no model training or random
sampling is involved, and every output can be regenerated from the same source files.

The pipeline is organized into three stages: extract (fetch source data → Parquet),
transform (join parquets → coverage_master.parquet), and analyze (apply coverage and
equity computations → CSV outputs). The three stages are independently runnable. The
analysis stage can operate on committed Parquet files without re-fetching source data,
which makes the analysis layer fast to iterate on and independent of API availability.

---

## 2. Data Sources

| Source | Access Method | What It Contributes |
|--------|--------------|---------------------|
| HIRA 지역별 당뇨병 진료현황 | Manual Excel download (opendata.hira.or.kr) | Diabetes patient counts by province (시도) × year, 2019–2023; primary regional denominator |
| HIRA 치료재료정보조회서비스 | REST API (XML), data.go.kr #3074384 | CGM product catalog: 11 products, M-codes, 비급여 classification, importer |
| MFDS 의료기기 품목허가 | REST API (JSON), data.go.kr #15057456 | CGM device approval dates and manufacturers |
| NHIS 건강보험통계연보 ch06 | Bulk Excel download, data.go.kr #15095102 | National E10/E11/E14 patient counts by year (yearbook chapters 2022–2024) |
| NHIS 건강검진정보 | Bulk CSV download, data.go.kr #15007122 | Regional fasting blood glucose mean + high-glucose rate |
| NHIS T1D 환자 수 (age/sex) | Bulk CSV download | T1D patient counts 2021–2024 by age band and sex |
| NHIS 소모성재료 지급현황 | Bulk CSV download (one per year) | Monthly diabetes consumables claim counts and payment totals, 2021–2024 |
| NHIS CGM 이용 현황 | Bulk CSV download | Deduplicated CGM users per year, 2020–2024; numerator for adoption rate |
| NHIS 요양비 등록 수급자 | Bulk XLSX download | Registered 요양비 T1D/T2D beneficiaries per year; denominator for adoption rate |
| NHIS T1D 연도별 1세 단위 | Bulk CSV download | T1D by 1-year age band, 2013–2023; used for eligibility pool estimates |
| NHIS 연도별 당뇨병 진료정보 | Bulk XLSX download (3-sheet workbook) | E10-E14 by age bracket (Sheet 1); registered beneficiary counts (Sheet 2) |
| NHIS 시군구별 T1D+T2D | Bulk XLSX download | ~247 시군구 × 3 years; enables sub-provincial equity analysis |
| NHIS T2D 시군구별 임상 | Bulk CSV download | T2D by institution type per 시군구 2021–2023; used for primary_care_pct |
| NHIS 당뇨병의료이용률 | Bulk CSV download (multiple files) | 당뇨병의료이용률 by 시도/시군구, 2002–2024 |
| NHIS 인슐린 주사 청구 | Bulk CSV download | Monthly insulin injection claims by age, 2016–2023 |

Market prices (₩155K / ₩200K / ₩280K per month across three tiers) and NHIS policy
constants (기준금액 history, reimbursement ratio) are hardcoded in `src/policy.py`
from verified public sources. They are not fetched from any API.

---

## 3. Design Decisions

### 3.1 Parquet for intermediate storage

All extracted data is stored as Apache Arrow Parquet files in `data/processed/`. Parquet
preserves column types across pipeline runs (integers remain integers, booleans remain
booleans), making downstream computations predictable without type-inference heuristics.
Parquet also compresses efficiently — the largest files in this project are tens of
kilobytes — and loads into pandas or DuckDB in milliseconds. The alternative, writing
CSVs at each stage, would require re-parsing type information on every read and risks
silent type coercions (e.g., region codes like "09" being parsed as integers).

### 3.2 DuckDB for cross-dataset queries

`src/store.py` exposes a `duckdb_query()` helper that creates in-memory DuckDB views
over named Parquet files and executes SQL against them. DuckDB is chosen over pandas
merge chains for multi-table joins because it expresses complex joins cleanly in SQL,
handles larger-than-memory datasets gracefully, and produces reproducible query plans.
For the current data volumes (all Parquets combined fit comfortably in memory), the
practical difference is ergonomic rather than performance-driven.

### 3.3 Bulk file download for NHIS data

The NHIS data portal (nhiss.nhis.or.kr) and the relevant data.go.kr datasets do not
expose individual-query REST APIs for statistical tables. The data is published as
annual Excel and CSV bulk files. The pipeline therefore uses file-based parsers
(`src/nhis_client.py`) that read these local files directly. This design is consistent
with how the data is actually distributed and avoids building a REST layer around a
non-existent endpoint.

File discovery uses `os.listdir()`-based substring matching (via finder functions in
`src/store.py`, e.g., `find_t1d_csv()`, `find_consumables_csvs()`). This approach
correctly handles Korean Unicode filenames on Windows and Linux without relying on
glob patterns, which can produce inconsistent results with non-ASCII characters
depending on the platform's locale settings.

### 3.4 Hardcoded market prices

The MFDS API, the HIRA treatment material catalog, and all NHIS bulk files contain no
price data. CGM sensor prices are set by importers and distributors and are not reported
to any publicly accessible government registry. The three price tiers (low/mid/high)
in `src/policy.py` are point-in-time estimates from verified external research (2025),
representing the observed retail price range across currently available CGM sensors in
Korea. Hardcoding these values makes the price assumptions explicit and auditable rather
than silently derived from an opaque source.

### 3.5 Separated fetch → transform → analyze stages

The pipeline separates data extraction (fetch_hira.py, fetch_mfds.py, fetch_nhis.py),
joining (build_master.py), and analysis (run_coverage_gap.py, run_regional_equity.py,
run_coverage_trend.py) into distinct scripts. This separation has three practical benefits.
First, the analysis stage can be re-run against existing Parquet files without re-fetching
from APIs or re-parsing large Excel files. Second, each stage can be skipped, retried, or
replaced independently without affecting the others. Third, the analysis scripts in
`analysis/` are plain Python files that read committed Parquet intermediates, making them
runnable by any contributor without API keys.

---

## 4. Data Flow

```
Source Files (manual download → Data/raw/)
────────────────────────────────────────────────────────────────
  HIRA Excel (지역별 당뇨병 2019-2023)
  NHIS ch06 Yearbook (2022, 2023, 2024)
  NHIS 건강검진 CSV
  NHIS T1D / CGM / Consumables / Sigungu CSVs + XLSXs
  (12+ individual files, Korean Unicode filenames)

Stage 1 — Extract                        Stage 1 outputs (data/processed/)
─────────────────────────────────        ────────────────────────────────────
pipeline/fetch_hira.py
  Part A: parse HIRA Excel          →    hira_regional_diabetes.parquet
  Part B: HIRA REST API (XML)       →    hira_treatment_materials.parquet

pipeline/fetch_mfds.py
  MFDS REST API (JSON)              →    mfds_device_prices.parquet

pipeline/fetch_nhis.py
  Parts A–L: 12 file parsers        →    nhis_annual_stats.parquet
                                         nhis_checkup_summary.parquet
                                         nhis_t1d_age_sex.parquet
                                         nhis_consumables_monthly.parquet
                                         nhis_cgm_utilization.parquet
                                         nhis_yoyangbi_registered.parquet
                                         nhis_t1d_age_annual.parquet
                                         nhis_e10_age_split.parquet
                                         nhis_sigungu_t1d_t2d.parquet
                                         nhis_t2d_sigungu_clinical.parquet
                                         nhis_diabetes_utilization_rate.parquet
                                         nhis_insulin_claims_monthly.parquet

          src/policy.py (hardcoded)
          NHIS_REIMB_HISTORY          (기준금액 ₩210,000/quarter since 2022-08-01)
          MARKET_PRICES_KRW           (₩155K / ₩200K / ₩280K per month)
                  ↓
Stage 2 — Transform
─────────────────────────────────────────────────────────────────
pipeline/build_master.py
  ├── Load hira_regional_diabetes + nhis_annual_stats + mfds_device_prices
  ├── Expand to (year × region × price_tier) rows
  ├── Apply get_reimb_ceiling() per year
  ├── Compute nhis_pays, patient_burden_krw, burden_ratio, coverage_adequacy_ratio
  └── Output: data/processed/coverage_master.parquet (255 rows × 17 cols)

Stage 3 — Analyze
─────────────────────────────────────────────────────────────────
analysis/run_coverage_gap.py
  ├── compute_gap_series(2018–2026)
  ├── Output: analysis/coverage_gap.csv              (9 rows)
  └── [+ hira_treatment_materials] coverage_gap_by_product.csv

analysis/run_regional_equity.py
  ├── score_regional_disparity() per year (시도-level)
  ├── [+ nhis_sigungu_t1d_t2d] _build_sigungu_equity() (시군구-level)
  ├── [+ nhis_checkup_summary] glucose severity columns
  └── Output: analysis/regional_equity.csv           (85+ rows)

analysis/run_coverage_trend.py
  ├── compute_gap_series(2018–2030)
  ├── Merge nhis_annual_stats, nhis_t1d_age_sex, nhis_consumables_monthly,
  │         nhis_cgm_utilization, nhis_yoyangbi_registered, nhis_t1d_age_annual
  └── Output: analysis/coverage_trend.csv            (13 rows)

Report
─────────────────────────────────────────────────────────────────
krh report (src/report.py)
  ├── Reads analysis/*.csv (committed snapshots)
  └── Output: analysis/reports/cgm_coverage_report.html
```

---

## 5. Reproducibility

All outputs can be regenerated from scratch using publicly available data files and a
free data.go.kr API account.

**Prerequisites:**

1. Python environment with `uv`:
   ```bash
   uv sync --extra dev
   ```

2. Source data files in `Data/raw/` (capital D; defined in `src/config.py` as
   `DATA_SOURCE_DIR`). Download each file manually from the portals listed in
   Section 2. The pipeline's finder functions locate files by filename substring,
   so exact filenames as published by the portals are used without renaming.

3. API key in `.env`:
   ```
   HIRA_API_KEY=<your data.go.kr key>
   ```
   One key covers HIRA (치료재료 + 병원정보) and MFDS endpoints. Register at
   data.go.kr (free; approved in 1–2 business days).

**Regenerate all outputs:**

```bash
# Extract all NHIS file-based data (Parts A–L)
uv run python pipeline/fetch_nhis.py

# Extract HIRA and MFDS API data
uv run python pipeline/fetch_hira.py
uv run python pipeline/fetch_mfds.py

# Build master table
uv run python pipeline/build_master.py

# Run analysis
uv run python analysis/run_coverage_gap.py
uv run python analysis/run_regional_equity.py
uv run python analysis/run_coverage_trend.py

# Generate HTML report (requires --extra report --extra viz)
uv sync --extra report --extra viz
krh report
```

Alternatively, the CLI orchestrates Steps 3–5:

```bash
krh run --device cgm_sensor --year-range 2019-2024
krh analyze
krh report
```

Use `--sample N` on any pipeline script to limit output to N rows per parquet for
smoke-testing without processing full files.

**Verification:**

```bash
krh status          # Lists all parquets with row counts and modification times
krh audit           # Flags parquets older than 30 days
```

---

## 6. What the Pipeline Cannot Do

**No real-time price data.** Market prices are hardcoded point-in-time estimates.
The pipeline cannot track price changes over time or project future burden ratios
under price change scenarios. All burden ratios in the output CSVs are identical for
every coverage year because the ceiling is frozen and prices are static.

**No CGM adoption data by region.** CGM-specific utilization counts by province are
not available in any public dataset. The `regional_equity.csv` regional metric is
total diabetes patient share per province, not CGM adoption rate. A per-region CGM
adoption analysis would require HIRA prescription records by device category and
region, which are not published and require institutional data access.

**No eligible-vs-non-eligible patient split.** The analysis uses total diabetes
patient counts (E10–E14) as denominators in several contexts. The eligible subset
(T1D age 19+ under 고시 2022-170; expanded under 고시 2024-226) is a fraction of
total diabetes patients, and no public dataset provides the eligible count by region.
The `eligible_19plus` and `eligible_15plus` columns in `coverage_trend.csv` cover only
national T1D counts, not by region.

**No low-income patient modeling.** 차상위 계층 patients receive 100% coverage within
the 기준금액. This system applies the standard 70% NHIS ratio uniformly. Burden ratios
for low-income patients are therefore overstated.

**No API automation for NHIS data.** The bulk file downloads that supply the majority of
this pipeline's data are not automatable via REST API. Pipeline runs require periodic
manual file downloads from nhiss.nhis.or.kr when new annual publications are released.

**No MFDS price data.** The MFDS device approval API (and every other public API
inspected) contains no price information. The HIRA 치료재료 catalog lists CGM sensors
as 비급여 with null `max_unit_price_krw`. There is no publicly accessible API or bulk
file that provides current or historical CGM sensor retail prices in Korea.
