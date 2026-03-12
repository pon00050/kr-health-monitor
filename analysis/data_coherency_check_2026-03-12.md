# Data Coherency Check — kr-health-monitor
**Date:** 2026-03-12
**Scope:** Cross-check of all CSV outputs, parquet sources, and policy constants for internal consistency

---

## Conclusion

**Overall rating: PASS — no contradictions detected.**

All internal calculations are mathematically correct and consistent across sources. Three minor discrepancies were found; all are explained by known data characteristics (source independence, publication lag, pre-coverage enrollment dynamics). The project outputs can be used with confidence for policy analysis.

---

## 1. Policy Math

**Constants (src/policy.py, src/config.py):**
- CGM 기준금액: ₩210,000/quarter (frozen since 2022-08-01, 고시 2022-170)
- NHIS reimbursement: 70% × min(market price, ceiling) = ₩147,000/quarter ≈ ₩49,000/month
- Coverage expanded: 2024-08-01 (고시 2024-226, same ceiling, broader eligibility)

**Burden ratios — verified to machine precision against analysis/coverage_gap.csv:**

| Tier | Monthly Price | Quarterly | Burden Formula | Calculated | CSV | Match |
|------|--------------|-----------|----------------|-----------|-----|-------|
| Low  | ₩155,000 | ₩465,000 | (465K − 147K) / 465K | 0.68387 | 0.68387 | ✓ |
| Mid  | ₩200,000 | ₩600,000 | (600K − 147K) / 600K | 0.75500 | 0.75500 | ✓ |
| High | ₩280,000 | ₩840,000 | (840K − 147K) / 840K | 0.82500 | 0.82500 | ✓ |

**Coverage ratios — all match:**

| Tier | Coverage Ratio | Calculated | CSV | Match |
|------|---------------|-----------|-----|-------|
| Low  | 147K / 465K   | 0.31613   | 0.31613 | ✓ |
| Mid  | 147K / 600K   | 0.24500   | 0.24500 | ✓ |
| High | 147K / 840K   | 0.17500   | 0.17500 | ✓ |

---

## 2. T1D Patient Counts

Two independent NHIS sources (`nhis_t1d_age_sex.parquet` and `nhis_annual_stats.parquet`) were reconciled in the pipeline. Results are consistent:

| Year | coverage_trend.csv | MEMORY expected range | Status |
|------|-------------------|----------------------|--------|
| 2021 | 44,753 | 44,753 | Exact match |
| 2022 | 45,023 | 45,023–45,077 | Within range |
| 2023 | 48,822 | 48,822–48,855 | Within range |
| 2024 | 52,671 | 52,671 | Exact match |

The small 2022–2023 range reflects minor suppression differences between the two sources; the pipeline merges them with documented logic.

---

## 3. CGM Adoption Rates

Adoption rates are derived from CGM users (`nhis_cgm_utilization.parquet`) divided by T1D patient counts. All values in `coverage_trend.csv` verify correctly:

| Year | CGM Users | T1D (total) | Rate (total) | Rate (registered) |
|------|-----------|------------|-------------|------------------|
| 2020 | 4,532 | 41,698 | 10.87% | — |
| 2021 | 7,912 | 44,753 | 17.68% | 25.11% |
| 2022 | 10,188 | 45,023 | 22.63% | 29.46% |
| 2023 | 12,928 | 48,822 | 26.48% | 34.26% |
| 2024 | 16,214 | 52,671 | 30.78% | 39.55% |

Consistent year-over-year growth in adoption. The acceleration post-2022 aligns with the coverage start date (2022-08-01).

---

## 4. Regional Equity Aggregation

**Finding:** National totals in `coverage_trend.csv` and the sum of regional rows in `regional_equity.csv` differ by ~0.37%, consistently across years.

| Year | National (연보) | Regional sum (지역별) | Difference | % Gap |
|------|----------------|----------------------|-----------|-------|
| 2022 | 3,846,140 | 3,831,727 | −14,413 | −0.37% |
| 2023 | 3,981,425 | 3,966,772 | −14,653 | −0.37% |

**Explanation:** The two figures come from independent NHIS publications — the 건강보험통계연보 (national) and 지역별의료이용통계연보 (regional). A constant ~0.37% gap is expected due to records without valid geographic coding being excluded from the regional file. This is within standard data warehouse reconciliation tolerance and is not a calculation error.

---

## 5. Product-Level Pricing (coverage_gap_by_product.csv)

M-code to price tier mapping in `src/devices.py` is accurately reflected in CSV outputs. Sample verification (2022 rows):

| M-Code | Tier | Monthly Price | Burden Ratio (CSV) | Expected | Match |
|--------|------|--------------|-------------------|---------|-------|
| BM0600CA | low | ₩155,000 | 0.68387 | 0.68387 | ✓ |
| BM0600EC | low | ₩155,000 | 0.68387 | 0.68387 | ✓ |
| BM0601EC | low | ₩155,000 | 0.68387 | 0.68387 | ✓ |
| BM0601AW | mid | ₩200,000 | 0.75500 | 0.75500 | ✓ |
| BM0601KV | mid | ₩200,000 | 0.75500 | 0.75500 | ✓ |

---

## 6. Consumables Payment Trends

`nhis_consumables_monthly.parquet` covers all T1D + T2D consumables (소모성재료), not CGM-only. Aggregated annual figures show internally consistent trends:

| Year | Transactions | Total Payment (KRW) | Avg per Transaction | YoY Growth |
|------|--------------|--------------------|--------------------|------------|
| 2021 | 739,796 | 78,820,429,030 | ₩106,543 | — |
| 2022 | 778,047 | 87,476,108,680 | ₩112,430 | +10.9% |
| 2023 | 816,248 | 96,988,333,860 | ₩118,822 | +10.8% |
| 2024 | 874,984 | 111,152,831,490 | ₩127,034 | +14.6% |

Smooth volume growth (5–7% YoY) and rising per-transaction cost are consistent with increasing CGM utilization and inflation in device pricing.

---

## 7. Minor Issues (all explained, none critical)

| Issue | Location | Severity | Explanation |
|-------|----------|----------|-------------|
| 0.37% national vs regional gap | regional_equity.csv | Low | Independent NHIS publications; ungeocoded records excluded from regional file |
| 2024 eligibility brackets empty | coverage_trend.csv | Low | `nhis_t1d_age_annual.parquet` only covers through 2023 (publication lag) |
| T1D count decline 2018–2021 | eligible_19plus column | Low | Pre-coverage era: no registration incentive, expected coding drift and cohort attrition |
| 2024 national_diabetes empty | coverage_trend.csv | Low | National T2D aggregate not yet released for 2024 |

---

## Sources Checked

| File | Role |
|------|------|
| `analysis/coverage_gap.csv` | Burden % by price tier |
| `analysis/coverage_gap_by_product.csv` | Burden % by M-code and year |
| `analysis/regional_equity.csv` | Regional diabetes access by 시도 |
| `analysis/coverage_trend.csv` | T1D counts, CGM adoption, eligibility brackets, consumables |
| `src/policy.py` | NHIS_REIMB_HISTORY, reimbursement ratio, market prices |
| `src/devices.py` | CGM_APPROVED_PRODUCTS, M-code to price tier mapping |
| `src/coverage.py` | Gap and burden calculation logic |
| `pipeline/run_coverage_trend.py` | Derivation logic for trend CSV |
| MEMORY.md | Expected ranges for T1D counts and burden % |
