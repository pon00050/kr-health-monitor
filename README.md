# kr-health-monitor

**Public infrastructure for monitoring NHIS coverage adequacy gaps in Korean medical device reimbursement.**

---

## The Gap: 89% Non-Adoption Despite Legal Coverage

Since August 2022, continuous glucose monitors (CGM) for Type 1 diabetes patients have been covered by Korea's National Health Insurance (NHIS). Yet adoption remains below 11% of eligible patients.

**The core finding: coverage ≠ access.**

The NHIS 기준금액 (reimbursement ceiling) is frozen at ₩70,000/month (₩210,000/quarter). Market prices range from ₩155,000–₩280,000/month. NHIS covers only ₩49,000/month of that — leaving patients to pay **68–82% out-of-pocket**.

This repository quantifies that gap using exclusively free, public data.

---

## Architecture

```
Data Sources                  Pipeline               Analytics
────────────────────          ───────────────────    ──────────────────────
HIRA opendata.hira.or.kr  →   extract_hira       →  coverage_adequacy.csv
MFDS data.go.kr/15057456  →   extract_mfds       →  regional_variation.csv
NHIS data.go.kr/15095102  →   extract_nhis       →  trend_analysis.csv
                               ↓                      ↓
                           transform.py          krh report --device cgm_sensor
                               ↓
                          coverage_master.parquet
```

**Three-layer architecture (same as kr-forensic-finance):**
- Layer 1: Python automation (extractors → parquet → DuckDB analytics)
- Layer 2: AI-assisted synthesis (optional; Claude API for narrative analysis)
- Layer 3: Human judgment (policy recommendations, regulatory submissions)

---

## Quick Start

```bash
# 1. Clone and install
git clone https://github.com/pon00050/kr-health-monitor
cd kr-health-monitor
uv sync

# 2. Set API keys (optional for coverage math; required for live data)
cp .env.example .env
# Edit .env with your data.go.kr API key

# 3. Run coverage math (works without API keys — hardcoded constants)
python 03_Analysis/run_coverage_analysis.py
python 03_Analysis/run_regional_variation.py

# 4. Check outputs
krh status

# 5. Full pipeline with live API data (requires API keys)
krh run --sample 5 --sleep 0.5
```

---

## Data Sources

| Source | Type | Data | Key Required |
|--------|------|------|-------------|
| HIRA opendata.hira.or.kr (sno=13702) | File download | 지역별 당뇨병 진료현황 2019–2023 | No |
| HIRA Treatment Material API (data.go.kr #3074384) | REST API | CGM product M-codes, coverage status | Yes |
| MFDS 의료기기허가 (data.go.kr #15057456) | REST API | Device approvals, manufacturers | Yes |
| NHIS 발간자료 (data.go.kr #15095102) | File download | Annual statistical tables | Yes |
| NHIS 건강검진 (data.go.kr #15007122) | File download | Blood glucose, screening rates | Yes |

All data is publicly available at no cost. Market prices are hardcoded from verified research (MFDS API does not provide price data).

---

## Information User Types

| User | What they get |
|------|---------------|
| T1D patients & families | How much they'll actually pay; regional access comparison |
| Endocrinologists | Evidence base for 기준금액 adjustment advocacy |
| Health journalists | Quantified gap story with regional disparity data |
| NGOs / 사협 | Policy brief materials; gap widening projection |
| NHIS/HIRA policy staff | Benchmarking tool for 기준금액 review |
| Academic researchers | Clean parquet datasets for secondary analysis |
| Medical device companies | Market access analysis; reimbursement gap sizing |
| Health economists | Coverage adequacy index methodology |
| International health orgs | Korea CGM coverage case study |

---

## Folder Structure

```
kr-health-monitor/
├── 00_Reference/         ← GITIGNORED — local reference docs only
├── 01_Data/
│   ├── raw/              ← Downloaded files (never modify)
│   └── processed/        ← Parquet outputs (gitignored)
├── 02_Pipeline/          ← Extractors + transform + orchestrator
├── 03_Analysis/          ← Marimo apps + runner scripts
├── src/                  ← Core modules (config, clients, analytics)
├── tests/                ← Pytest suite (no live API calls required)
├── cli.py                ← `krh` CLI (Typer)
└── pyproject.toml        ← uv project config
```

---

## CLI Reference

```bash
krh run [--device cgm_sensor] [--year-range 2018-2026] [--sample N]
krh status [-v]        # Parquet inventory
krh audit [--verbose]  # Freshness check
krh analyze            # Run all three analysis scripts
krh report --device cgm_sensor
krh version
```

---

## Contributing

This is a public infrastructure project. Contributions welcome:

1. **Data verification** — cross-check 기준금액 against 보건복지부 고시
2. **Regional analysis** — obtain CGM-specific utilization data from HIRA (contact 033-739-1057)
3. **Additional devices** — insulin pumps, oxygen therapy, other 급여 devices
4. **International comparison** — comparable gap analysis for other OECD countries

Open an issue before starting large changes.

---

## Disclaimer

This repository contains policy research using public data. It is not medical advice, investment advice, or a fraud determination. All outputs are hypotheses for human review.

Data sources: NHIS, HIRA, MFDS — all public Korean government data.

---

*Built with the same infrastructure as [kr-forensic-finance](https://github.com/pon00050/kr-forensic-finance).*
