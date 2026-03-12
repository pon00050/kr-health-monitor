# kr-health-monitor

**공개 데이터로 측정하는 NHIS 연속혈당측정기 급여 적정성 격차**
**Quantifying the NHIS reimbursement gap for CGM devices in Korea — using only public data**

For further inquiries, please contact Jisoo at pon00050@gmail.com
---

**인터랙티브 보고서 (Python 불필요):**
[cgm_coverage_report.html — 분석 결과 바로 보기](https://raw.githack.com/pon00050/kr-health-monitor/master/analysis/reports/cgm_coverage_report.html)
— Plotly 인터랙티브 차트, Python 없이 브라우저에서 바로 실행됩니다.

**Visual summary (no Python required):**
[cgm_coverage_report.html — View analysis](https://raw.githack.com/pon00050/kr-health-monitor/master/analysis/reports/cgm_coverage_report.html)
— Interactive Plotly charts, runs directly in your browser.

---

## 핵심 수치 / Key Numbers

| 지표 | 수치 | 비고 |
|------|------|------|
| 환자 월 실비 부담률 | **68–82%** | 건강보험 적용 후에도 시장가의 대부분을 환자가 직접 부담 |
| NHIS 월 실질 지원액 | **₩49,000** | 기준금액(₩210,000/분기)의 70% ÷ 3 |
| 기준금액 동결 기간 | **2022년 8월 이후** | 고시 2022-170 이후 한 차례도 조정 없음 |
| CGM 실제 이용률 | **~11%** | 급여 수혜 가능 1형 당뇨 환자 대비 실제 사용자 비율 |
| 1형 당뇨 환자 수 | **45,023 → 52,671명** | 2022 → 2024; 등록 환자는 늘고 있으나 이용률은 낮은 상태 |

> Even with insurance, T1D patients in Korea pay 68–82% of CGM costs out of pocket.
> The reimbursement ceiling has been frozen since August 2022 while patient numbers grow.
> Only ~11% of eligible T1D patients actually use CGM.

---

## 왜 이 문제가 중요한가 / Why This Matters

건강보험이 CGM을 급여 항목으로 인정하고 있음에도 불구하고,
실제 사용 환자 비율은 전체 수혜 가능 환자의 약 11%에 불과합니다.

**나머지 ~89%의 환자들은 공공 데이터에서 보이지 않습니다.**
건강보험 청구 기록은 이미 기기를 구매한 환자의 상환 내역만 기록합니다.
선불로 구매할 여력이 없어 포기한 환자는 어떤 통계에도 나타나지 않습니다.

이 저장소는 그 격차를 정량화합니다.
자매 프로젝트(아래)는 실제로 배제된 환자가 누구인지를 추적합니다.

> Korea's NHIS covers CGM for T1D patients — yet ~89% of eligible patients don't use it.
> Public claims data is blind to patients who couldn't afford to pay upfront.
> This repository quantifies the gap. The sister project (below) tracks who falls through it.

---

## 분석 결과 요약 / Findings

| 분석 항목 | 결과 | 정책적 의미 |
|-----------|------|------------|
| 기준금액 동결 (2022-08-01~) | ₩210,000/분기 변동 없음 | 기준금액이 물가 상승을 반영하지 못함 |
| 환자 본인부담률 | 시장가 대비 68–82% | 급여 적용 환자도 비용의 대부분을 직접 부담 |
| 1형 당뇨 환자 수 | 45K(2022) → 52K(2024) | 적용 대상은 증가, 기준금액은 동결 |
| CGM 이용률 | 등록 수급자 대비 ~11% | 급여가 존재하나 실제 접근으로 이어지지 않음 |
| 지역별 분포 | 경기·서울 집중 (전국의 ~44%) | 지역별 의료 접근성 불균형 |
| 소모성재료 지급액 | 2024년 ₩111.2B | 프로그램은 가동 중이나 수혜자는 일부에 불과 |

---

## 자매 프로젝트 / Sister Project: CGM접근성사회적협동조합

이 저장소는 한국 CGM 급여 정책 개혁을 위한 증거 인프라의 한 축입니다.

자매 프로젝트인 **CGM접근성사회적협동조합**은 1형 당뇨 환자의 선불 부담을 제거합니다.
환자는 본인부담금(30%)만 납부하고, 협동조합이 나머지를 선지급한 뒤
건강보험공단으로부터 70%를 직접 청구합니다(국민건강보험법 제49조 위임청구).

### 두 프로젝트가 서로를 필요로 하는 이유

```
kr-health-monitor → 협동조합
  지역별 형평성 분석  →  아웃리치 우선순위 지역 식별
  T1D 환자 수(연령×성별)  →  지역별 수혜 가능 규모 추정
  기준금액 격차(₩/분기)  →  환자당 운전자본 모델 검증
  소모성재료 지급 추이  →  건보 환급 현금흐름 예측

협동조합 → kr-health-monitor
  경제적 배제 환자 수  →  이용 격차의 재정적 원인 입증
  소득계층별 이용률  →  지역 형평성 분석에 사회경제 차원 추가
  장벽 제거 후 순응도  →  요양급여화 논의의 before/after 근거
```

### 공동의 정책 목표

두 프로젝트 모두 같은 방향의 개혁을 지향합니다: CGM을 요양비에서 요양급여로 전환.

현행 요양비 시스템의 세 가지 구조적 문제:
1. **접근 장벽** — 선불 구매 후 상환 방식이 자금 여력이 없는 환자를 배제
2. **데이터 단절** — 요양비는 국가 의료 IT와 연계되지 않아 제품 추적 및 관리 사각지대 발생
3. **가격 통제 실패** — 요양급여와 달리 요양비는 가격 하방 압력을 행사할 수 없어, 한국 CGM 가격이 일부 제품에서 일본을 상회

kr-health-monitor는 격차와 그 확대를 수치로 보여줍니다.
협동조합은 누가 그 격차에 빠지는지를 기록합니다.
두 프로젝트가 함께, 기준금액 조정 또는 요양급여화 개혁 제안에 필요한
근거 — 정책이 실패하고 있다는 것뿐 아니라, 누구에게, 어디서, 장벽 제거 후 어떤 변화가 일어나는지 — 를 생성합니다.

---

## 데이터 한계 / Known Limitations

1. **CGM 제품별 청구 건수는 공개 파일에서 직접 확인 불가.** 제품 수준 급여 실적은 HIRA 카탈로그와 전국 T1D 환자 수 기반 추정치.
2. **2024년 데이터는 환자 수에 한해 확보.** 2024년 진료일수·비용은 연보 파싱 미완으로 제외.
3. **시장 가격은 공개 연구 기반 고정 상수 (2026년 3월 검증).** 실시간 데이터 피드 아님.
4. **소모성재료 지급 데이터는 CGM 전용이 아님.** 1형+2형 당뇨 소모성재료 전체 포함; 지급건수 ≠ 고유 수혜자 수.
5. **이용률 분모는 요양비 등록 수급자.** 등록하지 않은 급여 가능 환자는 포함되지 않아 실제 이용률이 과대 추정될 수 있음.

---

## 개발자용 / For Developers

### Quick Start

```bash
# 1. Clone and install
git clone https://github.com/pon00050/kr-health-monitor
cd kr-health-monitor
uv sync

# 2. Set API keys (required for live data fetch)
cp .env.example .env
# Edit .env with your data.go.kr API key

# 3. Run analysis (uses committed CSVs — no API keys needed)
python analysis/run_coverage_gap.py       # → analysis/coverage_gap.csv
python analysis/run_regional_equity.py    # → analysis/regional_equity.csv
python analysis/run_coverage_trend.py     # → analysis/coverage_trend.csv

# 4. Generate interactive HTML report
uv sync --extra report --extra viz
krh report

# 5. Full pipeline with live API data
python pipeline/run.py --device cgm_sensor --year-range 2019-2024
```

### Architecture

```
Data Sources                    Pipeline                 Outputs
──────────────────────          ──────────────────────   ───────────────────────────────
HIRA opendata.hira.or.kr    →   pipeline/fetch_hira  →  analysis/coverage_gap.csv
MFDS data.go.kr             →   pipeline/fetch_mfds  →  analysis/coverage_gap_by_product.csv
NHIS data.go.kr             →   pipeline/fetch_nhis  →  analysis/regional_equity.csv
                                        ↓                analysis/coverage_trend.csv
                             pipeline/build_master              ↓
                                        ↓                krh report
                             data/processed/*.parquet           ↓
                                                   cgm_coverage_report.html
```

→ [Full methodology](METHODOLOGY.md) | [Pipeline architecture](PIPELINE.md)

### CLI Reference

```bash
krh run [--device cgm_sensor] [--year-range 2018-2026] [--sample N]
krh status [-v]         # Parquet inventory
krh audit [--verbose]   # Freshness check
krh analyze             # Run all three analysis scripts
krh report              # Generate interactive HTML report
krh version
```

### Folder Structure

```
kr-health-monitor/
├── pipeline/             ← Extractors (fetch_hira, fetch_mfds, fetch_nhis) + build_master + run
├── analysis/             ← Runner scripts + committed policy CSVs + reports/
├── src/                  ← Core modules (config, policy, devices, coverage, equity, store, clients)
├── tests/                ← Pytest suite (133 tests, no live API calls required)
├── cli.py                ← `krh` CLI
└── pyproject.toml        ← uv project config
```

### Data Sources

| Source | Type | Data |
|--------|------|------|
| HIRA Treatment Material API (data.go.kr #3074384) | REST API | CGM product M-codes, coverage status |
| MFDS 의료기기허가 (data.go.kr #15057456) | REST API | Device approvals, manufacturers |
| NHIS 발간자료 (data.go.kr #15095102) | File download | Annual statistical tables |
| NHIS 건강검진 (data.go.kr #15007122) | File download | Blood glucose, screening rates |

All data is publicly available at no cost. API keys are required for live fetches (data.go.kr account, free).

### Contributing

1. **Data verification** — cross-check 기준금액 against 보건복지부 고시
2. **Regional analysis** — obtain CGM-specific utilization data from HIRA
3. **Additional devices** — insulin pumps, oxygen therapy, other 급여 devices
4. **International comparison** — comparable gap analysis for other OECD countries

Open an issue before starting large changes.

---

## 면책 조항 / Disclaimer

본 저장소는 공공 데이터(NHIS, MFDS, HIRA)를 활용한 정책 연구 자료입니다.
의료 조언, 투자 권고, 또는 법적 판단이 아닙니다.
NHIS, HIRA, MFDS와 무관한 독립 연구입니다.

This repository contains policy research using public data.
It is not medical advice, investment advice, or a legal determination.
Not affiliated with or endorsed by NHIS, HIRA, or MFDS.
