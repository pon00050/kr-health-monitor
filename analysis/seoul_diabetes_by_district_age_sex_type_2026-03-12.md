# Seoul Diabetics — Statistics by District, Age, Sex, and Type (2024)

**Generated:** 2026-03-12
**Scope:** Seoul Special City (서울특별시), all 25 자치구
**Data as of:** 2024 (utilization rate series extends 2002–2024)

---

## Data Sources

| Table | Source Dataset | File | Coverage |
|---|---|---|---|
| 1 — District × Sex | NHIS 지역별의료이용통계연보, 시군구별 당뇨병1형/2형 성별 | `Data/processed/nhis_sigungu_t1d_t2d.parquet` | 2022–2024 |
| 2 — District × Age | NHIS 지역별의료이용통계연보, 시군구별 당뇨병1형/2형 연령대별 | `Data/processed/nhis_sigungu_t1d_t2d.parquet` | 2022–2024 |
| 3 — Utilization Rate | NHIS 당뇨병의료이용통계연보, 시군구별 이용률 | `Data/processed/nhis_diabetes_utilization_rate.parquet` | 2002–2024 |
| 4 — Year-on-Year | Derived from Tables 1–2, sex sheet aggregation | `Data/processed/nhis_sigungu_t1d_t2d.parquet` | 2022–2024 |

**ICD-10 codes:** E10 (당뇨병1형, Type 1), E11 (당뇨병2형, Type 2)
**Population base (Table 3):** NHIS insured population (건강보험 가입자)
**Count definition (Tables 1–2):** Patients who sought care under the ICD-10 code during the reference year — not prevalence estimates.

---

## Table 1. T1D and T2D Patient Counts by District × Sex (2024)

*Source: NHIS 지역별의료이용통계연보 — `nhis_sigungu_t1d_t2d.parquet`, sheet=`sex`, year=2024*

| 구 | T1D 남 | T1D 여 | T1D 계 | T2D 남 | T2D 여 | T2D 계 | 합계 |
|---|---:|---:|---:|---:|---:|---:|---:|
| 송파구 | 418 | 351 | **769** | 22,494 | 15,673 | **38,167** | **38,936** |
| 강서구 | 320 | 248 | **568** | 20,334 | 15,925 | **36,259** | **36,827** |
| 은평구 | 231 | 217 | **448** | 18,646 | 15,926 | **34,572** | **35,020** |
| 노원구 | 295 | 306 | **601** | 18,113 | 15,362 | **33,475** | **34,076** |
| 강동구 | 309 | 257 | **566** | 17,588 | 12,653 | **30,241** | **30,807** |
| 구로구 | 261 | 192 | **453** | 16,890 | 13,407 | **30,297** | **30,750** |
| 중랑구 | 246 | 195 | **441** | 16,343 | 13,461 | **29,804** | **30,245** |
| 성북구 | 260 | 214 | **474** | 15,065 | 12,411 | **27,476** | **27,950** |
| 관악구 | 266 | 205 | **471** | 15,075 | 11,929 | **27,004** | **27,475** |
| 양천구 | 227 | 189 | **416** | 15,634 | 11,425 | **27,059** | **27,475** |
| 강남구 | 310 | 262 | **572** | 16,121 | 10,161 | **26,282** | **26,854** |
| 도봉구 | 197 | 172 | **369** | 13,045 | 10,912 | **23,957** | **24,326** |
| 영등포구 | 248 | 192 | **440** | 13,718 | 10,086 | **23,804** | **24,244** |
| 동대문구 | 183 | 169 | **352** | 12,957 | 10,351 | **23,308** | **23,660** |
| 동작구 | 224 | 192 | **416** | 12,933 | 10,231 | **23,164** | **23,580** |
| 강북구 | 171 | 170 | **341** | 11,934 | 11,060 | **22,994** | **23,335** |
| 광진구 | 223 | 188 | **411** | 11,785 | 9,412 | **21,197** | **21,608** |
| 마포구 | 211 | 183 | **394** | 11,745 | 9,267 | **21,012** | **21,406** |
| 서대문구 | 169 | 141 | **310** | 10,770 | 9,142 | **19,912** | **20,222** |
| 서초구 | 224 | 189 | **413** | 12,141 | 7,653 | **19,794** | **20,207** |
| 금천구 | 149 | 117 | **266** | 10,292 | 8,069 | **18,361** | **18,627** |
| 성동구 | 142 | 125 | **267** | 9,898 | 7,383 | **17,281** | **17,548** |
| 용산구 | 125 | 90 | **215** | 7,074 | 5,318 | **12,392** | **12,607** |
| 종로구 | 83 | 74 | **157** | 4,974 | 3,937 | **8,911** | **9,068** |
| 중구 | 75 | 68 | **143** | 4,779 | 3,981 | **8,760** | **8,903** |
| **서울 합계** | **5,567** | **4,706** | **10,273** | — | — | **605,483** | **615,756** |

**T1D sex ratio:** 54.2% male / 45.8% female.
**T2D sex ratio:** approximately 57% male / 43% female across districts.

---

## Table 2. T1D Patient Counts by District × Age Band (2024)

*Source: NHIS 지역별의료이용통계연보 — `nhis_sigungu_t1d_t2d.parquet`, sheet=`age`, year=2024, 구분=당뇨병1형*
*"—" = suppressed (cell value <5, redacted by NHIS for privacy)*

| 구 | <10 | 10대 | 20대 | 30대 | 40대 | 50대 | 60대 | 70대 | 80대 | 90대+ | 계 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 송파구 | 8 | 43 | 59 | 98 | 102 | 106 | 148 | 144 | 55 | 6 | **769** |
| 노원구 | 0 | 27 | 45 | 59 | 65 | 95 | 108 | 123 | 69 | 5 | **596** |
| 강남구 | 6 | 45 | 59 | 70 | 90 | 93 | 86 | 77 | 38 | 8 | **572** |
| 강서구 | 0 | 21 | 48 | 91 | 86 | 93 | 109 | 86 | 28 | 0 | **562** |
| 강동구 | 6 | 29 | 48 | 84 | 68 | 85 | 104 | 111 | 28 | 0 | **563** |
| 성북구 | 5 | 34 | 52 | 53 | 59 | 75 | 77 | 77 | 40 | 0 | **472** |
| 관악구 | 0 | 20 | 68 | 84 | 51 | 69 | 66 | 72 | 39 | 0 | **469** |
| 구로구 | 8 | 20 | 37 | 62 | 61 | 73 | 87 | 73 | 28 | 0 | **449** |
| 은평구 | 0 | 26 | 32 | 74 | 55 | 78 | 79 | 72 | 27 | 0 | **443** |
| 중랑구 | 0 | 14 | 51 | 59 | 64 | 53 | 88 | 77 | 30 | 0 | **436** |
| 영등포구 | 0 | 26 | 50 | 59 | 47 | 80 | 82 | 72 | 20 | 0 | **436** |
| 동작구 | 5 | 16 | 66 | 58 | 49 | 63 | 66 | 66 | 25 | 0 | **414** |
| 서초구 | 0 | 15 | 37 | 46 | 61 | 68 | 73 | 71 | 32 | 8 | **411** |
| 광진구 | 0 | 19 | 60 | 46 | 45 | 66 | 84 | 53 | 27 | 8 | **408** |
| 양천구 | 0 | 23 | 44 | 56 | 60 | 72 | 70 | 58 | 26 | 0 | **409** |
| 마포구 | 0 | 21 | 46 | 57 | 53 | 76 | 62 | 46 | 24 | 5 | **390** |
| 도봉구 | 0 | 15 | 20 | 35 | 39 | 60 | 77 | 64 | 52 | 0 | **362** |
| 동대문구 | — | 14 | 45 | 44 | 42 | 57 | 65 | 57 | 23 | 5 | **352** |
| 강북구 | 0 | 19 | 27 | 36 | 42 | 61 | 58 | 62 | 31 | 0 | **336** |
| 서대문구 | 6 | 8 | 32 | 44 | 40 | 56 | 63 | 36 | 23 | 0 | **308** |
| 금천구 | — | 11 | 34 | 50 | 31 | 41 | 47 | 39 | 13 | — | **266** |
| 성동구 | 5 | 8 | 30 | 34 | 20 | 37 | 65 | 45 | 22 | 0 | **266** |
| 용산구 | 5 | 10 | 25 | 33 | 32 | 32 | 39 | 22 | 12 | 5 | **215** |
| 종로구 | — | 10 | 17 | 21 | 21 | 23 | 30 | 22 | 12 | 0 | **156** |
| 중구 | 0 | 0 | 11 | 27 | 25 | 14 | 23 | 23 | 14 | 0 | **137** |

**T2D age breakdown** is available at the same granularity in `nhis_sigungu_t1d_t2d.parquet` (sheet=`age`, 구분=당뇨병2형) and can be queried separately.

---

## Table 3. Diabetes Utilization Rate by District (2024)

*Source: NHIS 당뇨병의료이용통계 — `nhis_diabetes_utilization_rate.parquet`, sido=서울특별시, year=2024*
*Definition: (당뇨병 진료 수진자 수) ÷ (건강보험 가입자 수) × 100. Covers all diabetes types (ICD-10 E10–E14).*
*Sorted descending by utilization rate.*

| 구 | 보험가입자 | 당뇨 진료자 | 이용률 |
|---|---:|---:|---:|
| 강북구 | 285,629 | 35,410 | **12.40%** |
| 도봉구 | 301,657 | 34,903 | **11.57%** |
| 중랑구 | 378,341 | 43,038 | **11.38%** |
| 금천구 | 242,974 | 26,383 | **10.86%** |
| 은평구 | 458,810 | 48,029 | **10.47%** |
| 동대문구 | 346,063 | 34,628 | **10.01%** |
| 구로구 | 418,340 | 41,714 | **9.97%** |
| 중구 | 125,084 | 12,384 | **9.90%** |
| 노원구 | 488,843 | 48,183 | **9.86%** |
| 성북구 | 423,527 | 41,520 | **9.80%** |
| 강서구 | 556,193 | 52,724 | **9.48%** |
| 종로구 | 142,123 | 13,264 | **9.33%** |
| 서대문구 | 307,894 | 28,626 | **9.30%** |
| **서울 전체** | **9,393,648** | **858,613** | **9.14%** |
| 성동구 | 277,106 | 24,905 | **8.99%** |
| 양천구 | 428,470 | 38,078 | **8.89%** |
| 강동구 | 451,990 | 39,812 | **8.81%** |
| 용산구 | 211,939 | 18,460 | **8.71%** |
| 관악구 | 489,269 | 42,442 | **8.67%** |
| 광진구 | 342,741 | 29,664 | **8.65%** |
| 영등포구 | 397,215 | 34,174 | **8.60%** |
| 동작구 | 378,725 | 31,813 | **8.40%** |
| 마포구 | 362,703 | 28,636 | **7.90%** |
| 송파구 | 640,387 | 48,125 | **7.51%** |
| 서초구 | 401,799 | 26,772 | **6.66%** |
| 강남구 | 535,826 | 34,926 | **6.52%** |

Utilization rate time series (2002–2024) is available in the same parquet for all 25 구.

---

## Table 4. Year-on-Year Seoul Totals (2022–2024)

*Source: `nhis_sigungu_t1d_t2d.parquet`, sheet=`sex`, aggregated across all 25 서울 구*

| Year | T1D 환자 | T2D 환자 | T1D YoY 증감 | T2D YoY 증감 |
|------|---:|---:|---:|---:|
| 2022 | 8,611 | 570,230 | — | — |
| 2023 | 9,794 | 587,849 | +1,183 (+13.7%) | +17,619 (+3.1%) |
| 2024 | 10,273 | 605,483 | +479 (+4.9%) | +17,634 (+3.0%) |

---

## Notes on Interpretation

1. **Count vs. prevalence.** Tables 1–2 count patients who sought care under ICD-10 E10/E11 in the reference year. They undercount patients who did not access healthcare and do not distinguish new vs. existing diagnoses.

2. **Utilization rate denominator mismatch (Table 3 vs. Tables 1–2).** The utilization rate dataset uses total insured population as denominator and covers all diabetes types (E10–E14). This yields ~858K for Seoul 2024 vs. ~615K from Tables 1–2, which cover only E10+E11 and use a per-disease patient count methodology.

3. **Suppressed cells.** NHIS suppresses cells with fewer than 5 patients (shown as "—") to protect privacy. Districts with small T1D pediatric populations (종로구, 중구, 금천구) have several suppressed age cells.

4. **Geographic equity pattern.** Northern/northeastern districts (강북, 도봉, 중랑, 금천, 은평) consistently show the highest diabetes utilization rates — likely reflecting older age structures and lower-income demographics. Affluent southern districts (강남, 서초, 송파) sit 2–3 percentage points below the Seoul average despite high absolute patient counts driven by large populations.

5. **T1D trajectory.** Seoul T1D counts grew 19.3% over two years (8,611 → 10,273), outpacing T2D growth (6.2%). The 2022–2023 jump (+13.7%) likely reflects expanded CGM coverage eligibility under 고시 2022-170 improving diagnosis capture, not solely true incidence growth.
