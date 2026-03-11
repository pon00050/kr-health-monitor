# CLAUDE.md — kr-health-monitor

Project instructions for Claude Code.

---

## Project Identity

Korean NHIS coverage adequacy monitor. Builds public infrastructure for quantifying
the gap between NHIS reimbursement ceilings (기준금액) and real-world market prices for
medical devices (starting with CGM/T1D). Outputs are policy analysis, not investment advice.

---

## Development Environment

`uv` for package management. Analysis scripts in `03_Analysis/` as plain `.py` files.

> **Marimo is optional.** Use runners (`run_*.py`) instead of `marimo edit` directly.
> `krh` is the primary CLI interface.

```bash
# Install dependencies
uv sync --extra dev

# Full pipeline
python 02_Pipeline/pipeline.py --device cgm_sensor --year-range 2018-2026 --sample 5

# Analysis runners
python 03_Analysis/run_coverage_analysis.py   # → 03_Analysis/coverage_adequacy.csv
python 03_Analysis/run_regional_variation.py  # → 03_Analysis/regional_variation.csv
python 03_Analysis/run_trend_analysis.py      # → 03_Analysis/trend_analysis.csv

# Check inventory
krh status
krh audit
```

---

## Privacy Rule

This is a **public GitHub repository**. The following directories/files are **gitignored**:
- `00_Reference/` — all local reference docs
- `01_Data/` — all raw and processed data
- `.env` — API keys
- `KNOWN_ISSUES.md`, `CHANGELOG.md` — local tracking
- `03_Analysis/*.csv` — regenerated outputs

**Never commit API keys. Never commit patient data.**

---

## Development Workflow

**uv.lock rule:** After any change to `pyproject.toml` dependencies, run `uv lock`
and commit both `pyproject.toml` and `uv.lock` together.

**Test-driven development:** Write tests first (RED), then implement (GREEN).
Run `python -m pytest tests/ -v` before any commit touching source code.

---

## Key Data Facts (verified March 2026)

- CGM 기준금액: ₩210,000 per QUARTER (3 months) — NOT monthly
- Coverage start: 2022-08-01 (고시 2022-170)
- 2024 update: 고시 2024-226; same ceiling, expanded eligibility
- NHIS pays: 70% of min(actual, 기준금액) = ₩147,000/quarter (market always > ceiling)
- Patient burden: 68–82% depending on sensor (₩155K–₩280K/month range)
- MFDS API has NO price data — prices are hardcoded constants
- NHIS datasets are bulk file downloads, NOT REST APIs
- CGM regional adoption data is NOT in public APIs — HIRA Excel is the denominator only

---

## Encoding

Windows 11 environment. Always use `encoding="utf-8"` in Python file operations.
Korean text in cp1252 shells will look garbled — this is expected.
