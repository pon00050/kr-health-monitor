# CLAUDE.md — kr-health-monitor

Project instructions for Claude Code.

---

## Project Identity

Korean NHIS coverage adequacy monitor. Builds public infrastructure for quantifying
the gap between NHIS reimbursement ceilings (기준금액) and real-world market prices for
medical devices (starting with CGM/T1D). Outputs are policy analysis, not investment advice.

---

## Development Environment

`uv` for package management. Analysis scripts in `analysis/` as plain `.py` files.

> **Marimo is optional.** Use runners (`run_*.py`) instead of `marimo edit` directly.
> `krh` is the primary CLI interface.

```bash
# Install dependencies (core + dev)
uv sync --extra dev

# Optional extras
uv sync --extra viz      # Marimo + Plotly (for interactive notebooks)
uv sync --extra report   # Jinja2 (for HTML report generation via `krh report`)

# All scripts must be run via uv or inside an activated venv:
#   uv run python pipeline/fetch_nhis.py
# Running bare `python pipeline/fetch_nhis.py` may fail with ImportError.

# Full pipeline
python pipeline/run.py --device cgm_sensor --year-range 2018-2026 --sample 5

# Analysis runners
python analysis/run_coverage_gap.py      # → analysis/coverage_gap.csv
python analysis/run_regional_equity.py   # → analysis/regional_equity.csv
python analysis/run_coverage_trend.py    # → analysis/coverage_trend.csv

# Check inventory
krh status
krh audit
```

**Path convention (capital D vs lowercase d):**
- `Data/raw/used/` — user-placed source files (gitignored; capital D, Windows convention).
  Defined in `src/config.py` as `DATA_SOURCE_DIR`. Place downloaded Excel/CSV files here.
- `data/processed/` — pipeline-generated parquets (gitignored; lowercase d).
  Defined in `src/store.py` as `PROCESSED_DIR`.

---

## Privacy Rule

This is a **public GitHub repository**. The following directories/files are **gitignored**:
- `Reference/` — all local reference docs
- `Data/` — all raw and processed data
- `.env` — API keys
- `KNOWN_ISSUES.md`, `CHANGELOG.md` — local tracking
- `analysis/*.csv` — regenerated outputs

**Never commit API keys. Never commit patient data.**

---

## Development Workflow

**uv.lock rule:** After any change to `pyproject.toml` dependencies, run `uv lock`
and commit both `pyproject.toml` and `uv.lock` together.

**Test-driven development:** Write tests first (RED), then implement (GREEN).
Run `python -m pytest tests/ -v` before any commit touching source code.

**Bug logging rule:** Every time a bug is encountered and resolved, append a new entry
to `memory/bugs.md` before moving on. No exceptions. Each entry must include:
- A sequential ID (BUG-NNN)
- File and function where the bug lives
- **Symptom** — what observable behavior revealed the bug
- **Investigation** — every hypothesis tried and how it was eliminated (including dead ends)
- **Root cause** — the actual underlying reason, not just the surface fix
- **Fix** — exactly what changed in the code
- **Lesson** — the generalizable rule to avoid this class of bug in future

The investigation section is mandatory even when the fix is obvious. The dead ends and
wrong hypotheses are as important to record as the solution — they prevent re-investigating
the same dead ends in future sessions.

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

**Column naming — two distinct metrics, not interchangeable:**
- `patient_share_pct` in `regional_equity.csv` = regional_patients / national_total × 100
  (share of national diabetes patients by region — geographic distribution metric)
- `adoption_rate_registered` in `coverage_trend.csv` = cgm_users / t1d_registered × 100
  (actual CGM uptake rate — policy effectiveness metric)

---

## Encoding

Windows 11 environment. Always use `encoding="utf-8"` in Python file operations.
Korean text in cp1252 shells will look garbled — this is expected.
