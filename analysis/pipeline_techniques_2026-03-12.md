# Pipeline Techniques — Data Cleaning and Integration
**Project:** kr-health-monitor — Korean NHIS CGM Coverage Adequacy Monitor
**Date:** 2026-03-12
**Context:** Technical walkthrough of the data engineering skills required to build and extend
this pipeline, with Excel equivalents for each operation.

---

## Overview

The pipeline ingests Korean government administrative claims data (bulk CSV and XLSX downloads),
cleans and normalises it, stores it as Parquet, and feeds three analysis scripts. The data
engineering required is solidly intermediate Python/Pandas — not research-grade statistics, but
well above Excel-level manipulation. The critical difficulty is not algorithmic; it is defensive
data handling against real-world government file quirks.

---

## Technique 1 — Encoding-aware file reads

**What the pipeline does:**
```python
pd.read_csv(path, encoding="cp949", dtype=str)
```

Korean government files are published in CP949 (a Microsoft Korean encoding), not UTF-8.
Reading without specifying the encoding produces garbled text or immediate crashes.
`dtype=str` loads every column as raw text, deferring numeric conversion to a controlled step.

**Excel equivalent:**
When you open a `.csv` in Excel and the Korean text looks like `Ã¹Ã³...`, you use
*Data → From Text/CSV → File Origin: Korean (949)*. Same problem, same fix —
Excel just exposes it through a GUI wizard instead of a parameter.

**Skill level:** Beginner-to-intermediate. Knowing that the problem exists and which encoding
to specify is the whole trick. The parameter itself is one word.

---

## Technique 2 — Multi-file merge with deduplication

**What the pipeline does:**
Six overlapping CSVs cover the same date range from different release vintages
(e.g., one file covers 2002–2020; a later release covers 2002–2018 again plus correction).
The merge strategy:
```python
frames = [pd.read_csv(f, ...) for f in sorted_paths]
combined = pd.concat(frames, ignore_index=True)
combined = combined.drop_duplicates(subset=["year", "sido", "sigungu"], keep="last")
```
Files are sorted by filename (which encodes the release date), so `keep="last"` means
the newest release wins at any overlapping row.

**Excel equivalent:**
This is the most painful operation to replicate in Excel. The closest approach:
1. Copy all 6 sheets into one master sheet (manual or via Power Query *Append Queries*)
2. Use *Data → Remove Duplicates* on the year/region columns, keeping the last occurrence

Power Query's *Append* + *Remove Duplicates* is the real equivalent.
In plain Excel without Power Query, this would require VBA or hours of manual work.

**Skill level:** Intermediate. The logic is simple (`concat` + `drop_duplicates`); the
judgment call — which file wins at overlapping years, and why — requires understanding
the data provenance.

---

## Technique 3 — Column detection by substring matching

**What the pipeline does:**
Korean government files use inconsistent column headers across releases
(e.g., `분모(명)` vs `분모` vs `denominator_명`). Rather than hardcoding exact column names,
the pipeline maps by substring:
```python
col_map = {
    "지표연도": "year",
    "시도": "sido",
    "시군구": "sigungu",
    "분모": "denominator",
    "분자": "numerator",
    "지표값": "utilization_rate_pct",
}
df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
```
A column named `분모(명)` matches the key `분모` because `k in df.columns` checks for
exact match — so the full column name must be known. For partial-match scenarios,
the pipeline uses a loop over `df.columns` checking `key in col_name`.

**Excel equivalent:**
This has no direct Excel equivalent. In Excel you would manually rename column headers
in each file before combining. With Power Query you can use *Transform → Rename Columns*
with a lookup table. In VBA it would be a loop over `Range("1:1")`.

**Skill level:** Intermediate. The dictionary comprehension and `if k in df.columns` guard
are patterns that take some familiarity with Pandas to write confidently.

---

## Technique 4 — Privacy suppression handling (asterisk cells)

**What the pipeline does:**
NHIS suppresses small counts (typically < 5 patients) by replacing the numeric value
with `*` to prevent re-identification. The pipeline converts these to `NaN`, not `0`:
```python
combined[col] = pd.to_numeric(combined[col], errors="coerce").astype("Int64")
```
`errors="coerce"` silently converts any non-numeric value (including `*`, `-`, blank)
to `NaN`. Using `Int64` (capital I, nullable integer) rather than `int64` preserves
the distinction between "zero" and "suppressed/unknown."

**Excel equivalent:**
Excel stores `*` as text in a numeric column, turning the cell green with a warning
triangle. If you use *Format Cells → Number*, the `*` becomes `0` — which is wrong.
The correct Excel approach: *Find & Replace* `*` with blank, then the cell is empty
(not zero). `NaN` in Pandas is the programmatic equivalent of an empty numeric cell.

**Skill level:** Intermediate. Knowing to use `errors="coerce"` rather than casting
directly, and understanding the semantic difference between `NaN` and `0` in patient
count data, is a meaningful design decision.

---

## Technique 5 — Comma-formatted number cleaning

**What the pipeline does:**
Korean government CSVs often format large numbers with commas (`1,234,567`).
Pandas does not parse these as integers by default:
```python
combined[col] = combined[col].str.replace(",", "", regex=False).str.strip()
combined[col] = pd.to_numeric(combined[col], errors="coerce")
```
`.str.strip()` removes invisible whitespace that frequently appears around values
in government exports.

**Excel equivalent:**
*Find & Replace* `,` with nothing (blank), then format the column as Number.
Or: multiply the column by `1` — Excel sometimes auto-converts text-numbers.
Power Query handles this more cleanly with *Transform → Data Type → Whole Number*.

**Skill level:** Beginner. The operations are simple; the knowledge that this is
necessary is the only non-obvious part.

---

## Technique 6 — Windows locale bug workaround (Korean filenames)

**What the pipeline does:**
On Windows 11 with a cp1252 locale, Python's `Path.exists()` and `Path.glob()` silently
fail to match filenames that contain Korean characters — the OS filesystem call uses
the wrong encoding to compare names. The workaround:
```python
for fname in os.listdir(str(data_dir)):
    if "당뇨병의료이용률" in fname and fname.upper().endswith(".CSV"):
        results.append(data_dir / fname)
```
`os.listdir()` uses the Windows API directly and returns correctly decoded filenames.
The substring match then works because both strings are proper Unicode.

**Excel equivalent:**
Excel does not have this problem — it uses the Windows shell file picker which
handles Korean filenames natively. This bug is specific to Python's `pathlib` on
non-Unicode Windows locales and has no Excel analogue.

**Skill level:** Intermediate-to-advanced (situational). The bug is subtle: code that
works on macOS/Linux or a UTF-8 Windows system fails silently on a cp1252 system.
Diagnosing it requires knowing that `pathlib` and `os` use different Windows APIs.

---

## Technique 7 — Multi-sheet XLSX parsing with sheet detection

**What the pipeline does:**
The 건강보험통계연보 yearbook files contain up to 150 sheets. The relevant sheet
(E10–E14 diabetes data) is not always on the same sheet index across years.
The pipeline detects it by content:
```python
xl = pd.ExcelFile(path)
for sheet in xl.sheet_names:
    df = xl.parse(sheet, header=None)
    if df.astype(str).apply(lambda c: c.str.contains("E10")).any().any():
        target_sheet = sheet
        break
```
Then reads only that sheet with a specified header row offset.

**Excel equivalent:**
Manually clicking through sheet tabs to find "E10" in the data, then using that
sheet. With Power Query: *Get Data → From Workbook → Navigator* lets you preview
sheets before loading. For automation across multiple years' files, VBA `For Each ws
In Workbook.Sheets` with a search is the equivalent.

**Skill level:** Intermediate. The nested `.any().any()` (row-wise then column-wise
boolean reduction) is idiomatic Pandas that takes practice to write fluently.

---

## Technique 8 — Parquet as the interchange format

**What the pipeline does:**
Each parsed DataFrame is saved as Parquet rather than CSV:
```python
df.to_parquet(out_path, index=False)
```
Parquet preserves column types (including nullable `Int64`, `float64`, `string`)
across reads. CSV does not — every read requires re-inferring or re-specifying types.
Parquet is also ~5–10× faster to read and ~3–5× smaller on disk for this data.

**Excel equivalent:**
Saving as `.xlsx` instead of `.csv` is the closest analogue — Excel preserves
column formats (number, date, text) in xlsx but not in csv. Parquet is to CSV
what xlsx is to csv, except Parquet is designed for machine-to-machine use,
not human editing.

**Skill level:** Beginner (using it). The single-line save/load API is trivial.
Understanding *why* Parquet over CSV is intermediate — it requires knowing about
type preservation and columnar storage.

---

## Technique 9 — DuckDB for cross-parquet SQL queries

**What the pipeline does:**
Once all Parquet files are generated, DuckDB allows SQL queries across them
without loading them all into memory:
```python
import duckdb
con = duckdb.connect()
result = con.execute("""
    SELECT u.sigungu, u.utilization_rate_pct, c.cgm_users
    FROM 'data/processed/nhis_diabetes_utilization_rate.parquet' u
    JOIN 'data/processed/nhis_cgm_utilization.parquet' c
      ON u.year = c.year
    WHERE u.year = 2024
""").df()
```

**Excel equivalent:**
Power Query relationships + Data Model (Power Pivot). This is Excel's equivalent
of a multi-table join — you define relationships between tables loaded from
different sources, then write DAX measures to query across them.
DuckDB is significantly faster and requires no GUI setup.

**Skill level:** Intermediate. Writing the SQL is straightforward if you know SQL.
The non-obvious part is knowing DuckDB can query Parquet files directly from
their path without a load step.

---

## Skill Level Summary

| Technique | Difficulty | Excel Equivalent |
|-----------|-----------|-----------------|
| Encoding-aware read (`cp949`) | Beginner | File origin in CSV import wizard |
| Multi-file concat + dedup | Intermediate | Power Query Append + Remove Duplicates |
| Column detection/rename | Intermediate | Manual rename or Power Query lookup |
| Privacy suppression → NaN | Intermediate | Find & Replace `*` → blank |
| Comma-number cleaning | Beginner | Find & Replace `,` → blank |
| Windows Korean filename bug | Intermediate/Advanced | Not applicable (no Excel analogue) |
| Multi-sheet XLSX detection | Intermediate | VBA sheet loop or Power Query Navigator |
| Parquet I/O | Beginner (API) | Save as `.xlsx` vs `.csv` |
| DuckDB cross-parquet SQL | Intermediate | Power Pivot + DAX |

**Overall assessment:** The pipeline requires solid intermediate Python/Pandas skill —
not data science or machine learning, but production-grade data engineering.
The hardest parts are not the algorithms; they are the defensive patterns:
knowing what can go wrong with real government files (encoding, suppression,
inconsistent headers, overlapping releases) and handling each case explicitly.

An experienced Excel/Power Query user could replicate most of this logic,
but it would take significantly longer, would not be reproducible from the command
line, and would not produce Parquet outputs suitable for downstream SQL analysis.
The pipeline's value is that it is fully automated, version-controlled, and
re-runnable by anyone who clones the repo.

---

*Written 2026-03-12 in response to a question about techniques used during
dataset integration (Phases 15–16: 당뇨병의료이용률 + 인슐린 주사 청구 CSVs).*
