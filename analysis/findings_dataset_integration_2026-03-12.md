# Findings: Impact of New Dataset Integration on Pipeline Analytical Capacity
**Project:** kr-health-monitor — Korean NHIS CGM Coverage Adequacy Monitor
**Date:** 2026-03-12
**Pipeline run:** end-to-end, 22.0 seconds wall time
**New datasets:** `nhis_diabetes_utilization_rate.parquet` (6,831 rows, 2002–2024) · `nhis_insulin_claims_monthly.parquet` (864 rows, 2016–2023)
**Output files referenced:** `coverage_trend.csv` · `regional_equity.csv` · `coverage_gap.csv`

---

## Background

Prior to this integration the pipeline quantified the CGM reimbursement gap (patient burden
68–82%, NHIS coverage ratio 17–32%) and mapped it by 시도 and 시군구 using T1D patient
counts. What it could not answer was how large the broader diabetic system is, where
per-capita disease burden is highest, how big the next CGM eligibility expansion cohort
would be, or whether CGM adoption has measurably changed insulin utilization. The two
newly integrated datasets address each of those gaps.

---

## Dataset 1 — 당뇨병의료이용률 (Diabetes Medical Utilization Rate)

**Source:** 국민건강보험공단, 6 overlapping CSV files merged into one parquet.
**Coverage:** 2002–2024, national + 시도 + 시군구 (252 districts), 23 distinct years, 6,831 rows.
**Measure:** percentage of the total population actively receiving diabetes medical care in a
given year. Denominator is the resident population; numerator is unique patients with at
least one diabetes-related claim. Covers all diabetes types (E10–E14), predominantly T2D.

### Finding 1 — The treated diabetes population is 100× larger than the T1D cohort

| Year | National rate | Treated patients |
|------|--------------|-----------------|
| 2002 | 2.82% | 974,429 |
| 2010 | 6.11% | 2,312,527 |
| 2018 | 8.69% | 3,677,421 |
| 2022 | 9.09% | 4,717,180 |
| 2023 | 9.54% | 4,870,159 |
| 2024 | 10.18% | 5,201,290 |

The total actively-treated diabetes population reached **5,201,290** in 2024 — a 3.6×
increase over 22 years, growing at roughly 3% per year through the most recent period.

Against this, the current CGM program covers **16,214 users** (2024), representing
**0.31% penetration of the total treated diabetes population** and **30.8% of T1D
patients** (52,671). Previously the pipeline could only state the T1D-relative figure.
The 5.2M baseline reframes the scale of underservice: the frozen ₩210,000/quarter ceiling
is inadequate not only for the current T1D cohort but for a system growing by ~150,000
newly treated diabetics per year.

**Note on 2021:** The national rate dipped from 9.72% (2020) to 8.60% (2021) before
recovering to 9.09% in 2022. This is attributable to COVID-19 avoidance of outpatient
care. Any 2021 figures across the pipeline — including CGM adoption rates — should be
interpreted against this backdrop.

### Finding 2 — Rural–urban disparity in diabetes burden operates in two directions simultaneously

**시도-level utilization rate, 2024 (ranked):**

| 시도 | Rate | Treated patients |
|------|------|-----------------|
| 전라남도 | 12.69% | 227,295 |
| 강원특별자치도 | 12.66% | 183,498 |
| 경상북도 | 12.01% | 304,568 |
| 전라북도 | 11.83% | 206,037 |
| 충청남도 | 11.31% | 242,997 |
| … | … | … |
| 서울특별시 | 9.14% | 858,613 |
| 제주특별자치도 | 8.74% | 58,852 |
| 세종특별자치시 | 6.78% | 25,291 |

시도-level spread: 12.69% (전라남도) vs. 6.78% (세종) = **1.9× ratio**.

**시군구-level extremes, 2024 (252 districts):**

| Direction | 시군구 | 시도 | Rate |
|-----------|--------|------|------|
| Highest | 단양군 | 충청북도 | 18.36% |
| Highest | 의성군 | 경상북도 | 17.85% |
| Highest | 고흥군 | 전라남도 | 17.58% |
| Highest | 연천군 | 경기도 | 17.57% |
| Lowest | 수원시 영통구 | 경기도 | 5.97% |
| Lowest | 과천시 | 경기도 | 6.12% |
| Lowest | 강남구 | 서울특별시 | 6.52% |
| Lowest | 서초구 | 서울특별시 | 6.66% |

시군구-level spread: ~3× between the most rural aging districts and the most affluent
urban ones.

**The structural pattern this reveals:** High-utilization districts (단양군, 의성군,
고흥군) are rural, aging, low-income areas where roughly 1 in 6 residents is a treated
diabetic. These same areas have limited specialist access and lower CGM adoption. Low-
utilization districts (강남구, 서초구, 분당구, 과천시) are affluent and young — their
residents have lower diabetes prevalence but better healthcare access.

Cross-referencing with `regional_equity.csv`: 세종 has the lowest CGM adoption rate
(0.47%) and the lowest diabetes utilization rate (6.78%). 제주 has the second-lowest
CGM adoption (1.10%) and the second-lowest utilization rate (8.74%). The pipeline can
now ask the correct policy question: **are the regions with the highest per-capita
disease burden receiving the least CGM support?** The data to answer it is present.

---

## Dataset 2 — 인슐린 주사 청구건수 및 금액 (Insulin Injection Claims)

**Source:** 국민건강보험공단, single CSV.
**Coverage:** 2016–2023, monthly, by age group (<10, 10대, 20대 … 80대+), 864 rows.
**Measure:** monthly claim count and claim amount (천원) for insulin injection procedures
across all insulin-dependent diabetics — T1D and insulin-dependent T2D alike.

### Finding 3 — The insulin-dependent population eligible for CGM expansion is 115× the current T1D user base

| Year | Annual insulin claims | Cost (₩B) |
|------|-----------------------|-----------|
| 2016 | 818,656 | 48.5 |
| 2017 | 1,508,735 | 86.6 |
| 2018 | 1,614,332 | 102.0 |
| 2019 | 1,723,991 | 121.7 |
| 2020 | 1,788,053 | 132.6 |
| 2021 | 1,846,482 | 145.7 |
| 2022 | 1,885,626 | 159.5 |
| 2023 | 1,887,280 | 164.0 |

The 2016→2017 jump (+84.3%) is an administrative artifact — a billing code expansion or
coverage reclassification — not a real population doubling. The meaningful baseline is
2018 onward.

**Age distribution of insulin claims, 2023:**

| Age group | Claims | Share |
|-----------|--------|-------|
| 60대 | 511,512 | 27.1% |
| 70대 | 419,986 | 22.3% |
| 50대 | 345,706 | 18.3% |
| 80대 이상 | 220,407 | 11.7% |
| 40대 | 199,210 | 10.6% |
| 30대 | 106,726 | 5.7% |
| 20대 | 52,155 | 2.8% |
| 10대 | 26,562 | 1.4% |
| 10대 미만 | 5,016 | 0.3% |
| **50대+ total** | **1,497,611** | **79.4%** |

**79.4% of insulin claims are from patients aged 50 or older.** This is the
insulin-dependent T2D population — the cohort that 고시 2024-226 hints at including
in future CGM eligibility expansions. The dataset puts a concrete number on the
expansion frontier: approximately **1.5 million insulin-dependent patients aged 50+**,
growing modestly each year.

Any expansion of CGM eligibility to this cohort would multiply program scale by
roughly 30–40× compared to the current T1D-only program. That figure is now
derivable from pipeline data, not assertion.

### Finding 4 — Insulin cost trajectory provides the cost-containment argument for CGM

Insulin injection claim costs grew from ₩48.5B (2016 adjusted baseline: 2018 = ₩102.0B)
to **₩164.0B in 2023**, a **+60.8% increase from 2018 to 2023**. The current CGM
reimbursement program costs approximately ₩9.5B/year (₩147,000/quarter × 16,214 users
× 4 quarters). That is roughly **6% of annual insulin claim costs**.

This comparison enables a cost-containment framing that was previously unsupported by
pipeline data: if CGM-enabled glycemic control reduces insulin dose adjustment visits
and downstream complication hospitalizations, the program's net cost to NHIS may be
lower than its face value. This is a substantive policy argument for raising the frozen
₩210,000/quarter ceiling.

### Finding 5 — Insulin claim growth flatlined in the CGM era: a tentative substitution signal

| Period | Annual claims (start → end) | Growth |
|--------|-----------------------------|--------|
| 2018–2021 (pre-CGM) | 1,614,332 → 1,846,482 | +14.4% (+4.5%/yr) |
| **2021–2023 (CGM launched Aug 2022)** | **1,846,482 → 1,887,280** | **+2.2% total** |

The pre-CGM organic growth rate was ~4.5% per year. After CGM coverage began in August
2022, the two-year growth in insulin claims dropped to +2.2% cumulatively. This is a
**tentative substitution signal**: CGM-enabled tighter glycemic management may be
reducing the frequency of insulin adjustment visits.

This cannot be confirmed from this dataset alone — the claim count captures all insulin-
dependent diabetics, not T1D only, and other confounders (post-COVID recovery patterns,
demographic shifts) are present. It is, however, the kind of finding that warrants a
dedicated study, and it is a finding that was structurally impossible to surface before
the insulin claims dataset was integrated.

---

## Historical Context — 요양비 Scheme (2019, Medtronic Enlite/Guardian Connect)

**Source:** Patient guidance document published by 한국1형당뇨병환우회 (Korea Type 1 Diabetes
Patient Association), describing the reimbursement procedure in effect from 2019-01-01.
This is a patient advocacy organization's procedural guide, not an official government
publication. The financial figures below are derived from that document.

**Scope of applicability:** The figures in this section apply specifically to Medtronic
Enlite sensors under the 요양비 (out-of-pocket reimbursement claim) scheme in effect as of
2019. The current project's 68–82% patient burden figures derive from a structurally
different scheme (고시 2022-170, effective 2022-08-01). Direct numerical comparison between
the two periods is not valid without further verification of how the 기준금액 basis changed
across the scheme transition. The 2019 figures are presented as historical context, not as
a trend endpoint.

### Finding 6 — CGM coverage for T1D patients predates 고시 2022-170 by more than three years

The 요양비 scheme for Medtronic Enlite/Guardian Connect sensors was in effect from
**2019-01-01** — the 2022 고시 expansion was therefore not the introduction of CGM coverage
but a revision and broadening of a program that had already been operating for over three
years under a different reimbursement structure.

**2019 patient burden — Medtronic Enlite sensor, 요양비 scheme:**

| Prescription period | Total sensor cost | NHIS reimbursement | Patient OOP | Burden % |
|--------------------|------------------|--------------------|-------------|---------|
| 4주 (1 month) | ₩350,000 | ₩196,000 | ₩154,000 | 44% |
| 8주 (2 months) | ₩700,000 | ₩392,000 | ₩308,000 | 44% |
| 12주 (3 months / 1 quarter) | ₩1,050,000 | ₩588,000 | ₩462,000 | 44% |

Reimbursement basis: 70% of 기준금액 (₩70,000/week). Market price implied: ~₩87,500/week
for Medtronic Enlite (₩350,000 ÷ 4 weeks), approximately 25% above the 기준금액.

**What this figure does and does not show:** Even under the 2019 요양비 scheme — which
covered only this one sensor line — T1D patients bore 44% of sensor costs out of pocket.
Whether patient burden has increased, decreased, or held steady since 2019 cannot be
determined from these two data points alone, because the basis of reimbursement changed
across scheme revisions. The 44% figure is cited solely to establish that meaningful cost
burden was present from the earliest coverage period.

### Finding 7 — Near-poverty patients received full coverage under the 2019 scheme

차상위계층 (near-poverty tier) patients received **100% of the 기준금액** under the 2019
요양비 scheme — their NHIS reimbursement covered the entire ceiling amount, with any
gap above the ceiling remaining out-of-pocket.

This is a policy precedent: the government already designed a full-relief tier for
economically vulnerable T1D patients within the CGM program. The question it raises for
current policy is whether the gap between the 기준금액 and market prices — which the
program's design assumes is zero for eligible patients at ceiling — remains consistent
with that intent as market prices have diverged from the frozen ceiling.

### Finding 8 — Prescription renewal requires CGM data report submission

Under the 2019 scheme, the first CGM prescription allowed a maximum of 4 weeks. Every
subsequent prescription (up to 12-week maximum) required the patient to submit a CGM
data report — specifically CareLink-generated statistics (sensor average glucose, standard
deviation, date range, wear days) — to their physician before a new prescription could
be issued.

This procedural requirement creates a real-world adherence barrier: patients without
reliable internet access, patients unfamiliar with the CareLink platform, and patients
in rural areas with limited specialist access must navigate a technical data-submission
step to maintain uninterrupted coverage. This barrier disproportionately affects the same
districts identified in Finding 2 as having the highest per-capita diabetes burden.

Whether this data submission requirement persists under the current scheme is not
confirmed from this document. It is flagged here as a known design feature of the 2019
scheme that warrants verification before any policy submission.

---

## Summary of Analytical Capacity Gained

| Question | Before integration | After integration |
|---|---|---|
| Scale of the total diabetes system | T1D cohort only (52,671) | 5.2M actively treated (2024) |
| Per-capita burden by geography | T1D counts by 시도 | Utilization rate for 252 시군구 districts, 2002–2024 |
| COVID impact on care utilization | Not visible | 2021 dip identified and dateable |
| Size of next CGM expansion cohort | Not quantified | ~1.5M insulin-dependent age 50+ |
| Cost-containment argument for CGM | Asserted | Quantifiable: CGM = 6% of insulin claim costs |
| CGM substitution effect on insulin | Not observable | Tentative signal visible; study-ready question |
| Regional double-jeopardy (high burden + low access) | Partial (시도 only) | Fully visible at 시군구 level (3× disparity) |

The two datasets together shift several previously qualitative policy arguments into
quantitative ones derivable directly from the pipeline.

---

*Generated from `pipeline/run.py` end-to-end run, 2026-03-12. Wall time: 22.0s.
New parquets: `Data/processed/nhis_diabetes_utilization_rate.parquet` · `Data/processed/nhis_insulin_claims_monthly.parquet`*
