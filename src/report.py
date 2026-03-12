"""
HTML policy brief generator — kr-health-monitor.

Reads from analysis/*.csv (committed snapshots) and produces a single
self-contained interactive HTML file with embedded Plotly charts.

Requires: jinja2, plotly  →  uv sync --extra report --extra viz
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_ANALYSIS_DIR = _PROJECT_ROOT / "analysis"
_REPORTS_DIR = _ANALYSIS_DIR / "reports"


# ── Data loaders ──────────────────────────────────────────────────────────────

def _load_csv(name: str) -> pd.DataFrame:
    path = _ANALYSIS_DIR / f"{name}.csv"
    if not path.exists():
        raise FileNotFoundError(
            f"{path} not found. Run `krh analyze` first to generate analysis CSVs."
        )
    return pd.read_csv(path, encoding="utf-8")


# ── Chart builders ────────────────────────────────────────────────────────────

def _build_ceiling_chart(gap_df: pd.DataFrame) -> str:
    """Frozen ceiling vs market price range — monthly KRW (2018–2026)."""
    try:
        import plotly.graph_objects as go
    except ImportError:
        raise ImportError("plotly is required. Install with: uv sync --extra viz")

    gap = gap_df.copy()
    gap["nhis_monthly"] = gap["nhis_pays_quarterly"] / 3
    years = gap["year"].tolist()

    fig = go.Figure()

    # Market range band — upper boundary (invisible line, fill target)
    fig.add_trace(go.Scatter(
        x=years,
        y=gap["market_price_monthly_high"].tolist(),
        mode="lines",
        line=dict(color="rgba(239,68,68,0)", width=0),
        showlegend=False,
        hoverinfo="skip",
    ))
    # Market range band — fill from low to high
    fig.add_trace(go.Scatter(
        x=years,
        y=gap["market_price_monthly_low"].tolist(),
        mode="lines",
        line=dict(color="rgba(239,68,68,0)", width=0),
        fill="tonexty",
        fillcolor="rgba(239,68,68,0.12)",
        name="시장가 범위 (최저–최고)",
        hovertemplate="시장가 범위: ₩%{y:,}/월<extra></extra>",
    ))
    # Market mid line
    fig.add_trace(go.Scatter(
        x=years,
        y=gap["market_price_monthly_mid"].tolist(),
        mode="lines",
        line=dict(color="rgb(239,68,68)", width=2, dash="dot"),
        name="시장가 중간값",
        hovertemplate="시장가 중간: ₩%{y:,}/월<extra></extra>",
    ))
    # NHIS monthly contribution (only post-2022)
    covered = gap[gap["nhis_monthly"].notna()].copy()
    if not covered.empty:
        fig.add_trace(go.Scatter(
            x=covered["year"].tolist(),
            y=covered["nhis_monthly"].tolist(),
            mode="lines+markers",
            line=dict(color="rgb(37,99,235)", width=3),
            marker=dict(size=7),
            name="건강보험 월 지원액 (₩49,000)",
            hovertemplate="건강보험 지원: ₩%{y:,}/월<extra></extra>",
        ))

    # Annotation: coverage start
    fig.add_vline(
        x=2022,
        line_dash="dash",
        line_color="rgba(37,99,235,0.45)",
        annotation_text="급여 시작<br>(고시 2022-170)",
        annotation_position="top right",
        annotation_font_size=11,
        annotation_font_color="rgb(37,99,235)",
    )

    fig.update_layout(
        title=dict(
            text="월별 실비 부담 구조: 건강보험 지원액 vs 시장 가격",
            font=dict(size=15, color="#1e293b"),
        ),
        xaxis=dict(title="연도", dtick=1, showgrid=True, gridcolor="rgba(0,0,0,0.05)"),
        yaxis=dict(
            title="월 금액 (원)",
            tickformat=",.0f",
            tickprefix="₩",
            showgrid=True,
            gridcolor="rgba(0,0,0,0.05)",
        ),
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        plot_bgcolor="white",
        paper_bgcolor="white",
        margin=dict(t=80, b=60, l=90, r=20),
        height=420,
    )
    return fig.to_json()


def _build_adoption_chart(trend_df: pd.DataFrame) -> str:
    """CGM actual users vs registered eligible pool, with adoption rate overlay."""
    try:
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots
    except ImportError:
        raise ImportError("plotly is required. Install with: uv sync --extra viz")

    df = trend_df[trend_df["cgm_users"].notna()].copy()
    df = df.sort_values("year")
    years = df["year"].tolist()

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Bars: registered T1D beneficiaries (denominator)
    if "t1d_registered" in df.columns and df["t1d_registered"].notna().any():
        fig.add_trace(go.Bar(
            x=years,
            y=df["t1d_registered"].tolist(),
            name="등록 T1D 요양비 수급자",
            marker_color="rgba(147,197,253,0.75)",
            hovertemplate="등록 수급자: %{y:,}명<extra></extra>",
        ), secondary_y=False)

    # Bars: actual CGM users
    fig.add_trace(go.Bar(
        x=years,
        y=df["cgm_users"].tolist(),
        name="실제 CGM 이용자",
        marker_color="rgba(37,99,235,0.85)",
        hovertemplate="CGM 이용자: %{y:,}명<extra></extra>",
    ), secondary_y=False)

    # Line: adoption rate
    if "adoption_rate_registered" in df.columns and df["adoption_rate_registered"].notna().any():
        rate = (df["adoption_rate_registered"] * 100).round(1)
        fig.add_trace(go.Scatter(
            x=years,
            y=rate.tolist(),
            mode="lines+markers+text",
            name="실제 사용률 (%)",
            line=dict(color="rgb(234,88,12)", width=2.5),
            marker=dict(size=8, color="rgb(234,88,12)"),
            text=[f"{v:.1f}%" if pd.notna(v) else "" for v in rate],
            textposition="top center",
            textfont=dict(size=11, color="rgb(234,88,12)"),
            hovertemplate="사용률: %{y:.1f}%<extra></extra>",
        ), secondary_y=True)

    fig.update_layout(
        title=dict(
            text="CGM 실제 이용자 수 vs 등록 수급자 (2020–2024)",
            font=dict(size=15, color="#1e293b"),
        ),
        barmode="overlay",
        xaxis=dict(title="연도", dtick=1, showgrid=True, gridcolor="rgba(0,0,0,0.05)"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        plot_bgcolor="white",
        paper_bgcolor="white",
        margin=dict(t=80, b=60, l=80, r=80),
        height=420,
    )
    fig.update_yaxes(
        title_text="인원 수 (명)",
        tickformat=",",
        secondary_y=False,
        showgrid=True,
        gridcolor="rgba(0,0,0,0.05)",
    )
    fig.update_yaxes(
        title_text="사용률 (%)",
        ticksuffix="%",
        secondary_y=True,
        showgrid=False,
        range=[0, 100],
    )
    return fig.to_json()


def _build_regional_chart(equity_df: pd.DataFrame) -> str:
    """Regional patient share by 시도 — horizontal bar, sorted."""
    try:
        import plotly.graph_objects as go
    except ImportError:
        raise ImportError("plotly is required. Install with: uv sync --extra viz")

    sido = equity_df[equity_df["granularity"] == "시도"].copy()
    latest_year = int(sido["year"].max())
    sido = sido[sido["year"] == latest_year].copy()
    sido = sido.sort_values("patient_share_pct", ascending=True)

    colors = [
        "rgba(239,68,68,0.70)" if row.get("disparity_flag") else "rgba(37,99,235,0.70)"
        for _, row in sido.iterrows()
    ]

    fig = go.Figure(go.Bar(
        x=sido["patient_share_pct"].tolist(),
        y=sido["region_name"].tolist(),
        orientation="h",
        marker_color=colors,
        text=[f"{v:.1f}%" for v in sido["patient_share_pct"]],
        textposition="outside",
        hovertemplate="%{y}: 전국의 %{x:.2f}%<extra></extra>",
    ))

    fig.update_layout(
        title=dict(
            text=f"시도별 당뇨병 환자 분포 ({latest_year}년) — 전국 대비 비율",
            font=dict(size=15, color="#1e293b"),
        ),
        xaxis=dict(
            title="전국 환자 중 비율 (%)",
            ticksuffix="%",
            showgrid=True,
            gridcolor="rgba(0,0,0,0.05)",
        ),
        yaxis=dict(title=""),
        plot_bgcolor="white",
        paper_bgcolor="white",
        margin=dict(t=60, b=60, l=80, r=100),
        height=520,
        annotations=[dict(
            text="빨간색: 전국 평균보다 현저히 낮은 지역",
            xref="paper", yref="paper",
            x=1, y=-0.11,
            xanchor="right",
            showarrow=False,
            font=dict(size=11, color="rgb(239,68,68)"),
        )],
    )
    return fig.to_json()


# ── Template ──────────────────────────────────────────────────────────────────

_TEMPLATE = """<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{{ title }}</title>
  <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: 'Apple SD Gothic Neo', 'Malgun Gothic', 'Noto Sans KR', sans-serif;
      background: #f8fafc; color: #1e293b; line-height: 1.6;
    }
    .page { max-width: 960px; margin: 0 auto; padding: 32px 24px 80px; }

    /* Header */
    .report-header { border-bottom: 3px solid #1d4ed8; padding-bottom: 24px; margin-bottom: 40px; }
    .report-header h1 { font-size: 1.75rem; color: #1d4ed8; font-weight: 700; }
    .report-header .subtitle { font-size: 0.95rem; color: #475569; margin-top: 6px; }
    .report-header .meta { font-size: 0.78rem; color: #94a3b8; margin-top: 10px; }

    /* KPI cards */
    .kpis {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 16px;
      margin-bottom: 48px;
    }
    .kpi {
      background: white; border-radius: 12px; padding: 24px 20px;
      box-shadow: 0 1px 3px rgba(0,0,0,0.08); border-top: 4px solid #1d4ed8;
    }
    .kpi.warning { border-top-color: #dc2626; }
    .kpi-label { font-size: 0.82rem; font-weight: 600; color: #64748b; margin-bottom: 8px; text-transform: uppercase; letter-spacing: 0.03em; }
    .kpi-value { font-size: 2.4rem; font-weight: 800; color: #1e293b; line-height: 1.1; }
    .kpi.warning .kpi-value { color: #dc2626; }
    .kpi-note { font-size: 0.75rem; color: #94a3b8; margin-top: 8px; }

    /* Chart sections */
    .section {
      background: white; border-radius: 12px; padding: 28px;
      margin-bottom: 28px; box-shadow: 0 1px 3px rgba(0,0,0,0.06);
    }
    .section h2 { font-size: 1.05rem; color: #1e293b; font-weight: 700; margin-bottom: 8px; }
    .section .context {
      font-size: 0.84rem; color: #64748b; margin-bottom: 20px;
      border-left: 3px solid #e2e8f0; padding-left: 12px; line-height: 1.7;
    }

    /* More data panel */
    .more-data {
      background: #eff6ff; border: 1px solid #bfdbfe;
      border-radius: 12px; padding: 28px; margin-bottom: 28px;
    }
    .more-data h2 { font-size: 1.05rem; color: #1d4ed8; font-weight: 700; margin-bottom: 10px; }
    .more-data .lead { font-size: 0.875rem; color: #374151; margin-bottom: 16px; }
    .dataset-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
      gap: 12px; margin-bottom: 20px;
    }
    .dataset-item {
      background: white; border-radius: 8px; padding: 14px 16px; font-size: 0.84rem;
    }
    .dataset-item strong { display: block; color: #1e293b; margin-bottom: 4px; font-size: 0.87rem; }
    .dataset-item span { color: #64748b; }
    .cta { font-size: 0.875rem; color: #1e293b; }
    .cta a { color: #1d4ed8; font-weight: 600; text-decoration: none; }
    .cta a:hover { text-decoration: underline; }

    /* Disclaimer */
    .disclaimer {
      font-size: 0.75rem; color: #94a3b8;
      border-top: 1px solid #e2e8f0; padding-top: 20px; margin-top: 8px; line-height: 1.7;
    }
  </style>
</head>
<body>
<div class="page">

  <header class="report-header">
    <h1>{{ title }}</h1>
    <p class="subtitle">연속혈당측정기(CGM) 건강보험 급여 적정성 분석 — 1형 당뇨 환자 실비 부담 현황</p>
    <p class="meta">
      데이터 기준: {{ data_date }} &nbsp;|&nbsp;
      생성일: {{ generated_at }} &nbsp;|&nbsp;
      출처: 국민건강보험공단(NHIS), 건강보험심사평가원(HIRA), 식품의약품안전처(MFDS)
    </p>
  </header>

  <div class="kpis">
    {% for kpi in kpis %}
    <div class="kpi {{ kpi.cls }}">
      <div class="kpi-label">{{ kpi.label }}</div>
      <div class="kpi-value">{{ kpi.value }}</div>
      <div class="kpi-note">{{ kpi.note }}</div>
    </div>
    {% endfor %}
  </div>

  {% if ceiling_chart_json %}
  <div class="section">
    <h2>① 급여 기준금액 동결 — 시장가 대비 격차</h2>
    <p class="context">
      건강보험은 CGM 기준금액(₩210,000/분기)의 70%인 <strong>₩147,000/분기(월 약 ₩49,000)</strong>를 지원합니다.
      2022년 8월 고시 2022-170 시행 이후 기준금액은 한 차례도 조정되지 않았습니다.
      시장가(₩155,000–₩280,000/월)와의 격차는 구조적으로 고착화되어 있습니다.
    </p>
    <div id="ceiling-chart"></div>
  </div>
  {% endif %}

  {% if adoption_chart_json %}
  <div class="section">
    <h2>② CGM 실제 이용률 — 급여 적용에도 낮은 접근성</h2>
    <p class="context">
      급여 혜택을 받을 수 있는 1형 당뇨 요양비 등록 수급자 중
      <strong>실제로 CGM을 사용하는 환자 비율</strong>입니다.
      이용률이 낮다면 경제적 부담(높은 자기부담금)이 접근성을 여전히 제한하고 있음을 시사합니다.
    </p>
    <div id="adoption-chart"></div>
  </div>
  {% endif %}

  {% if regional_chart_json %}
  <div class="section">
    <h2>③ 지역별 당뇨병 환자 분포 — 수도권 집중과 지방 소외</h2>
    <p class="context">
      시도별 전국 당뇨병 환자 중 차지하는 비율입니다.
      경기·서울 두 지역이 전체의 약 44%를 차지합니다.
      <span style="color:#dc2626;font-weight:600;">빨간색</span>으로 표시된 지역은 인구·의료 인프라 대비
      환자 비율이 현저히 낮아 접근성 불평등이 우려되는 지역입니다.
    </p>
    <div id="regional-chart"></div>
  </div>
  {% endif %}

  <div class="more-data">
    <h2>이 분석에는 더 많은 데이터가 있습니다</h2>
    <p class="lead">
      이 페이지는 공개된 핵심 지표만 요약합니다.
      아래 데이터셋은 추가 분석 및 협업 요청 시 제공 가능합니다.
    </p>
    <div class="dataset-grid">
      <div class="dataset-item">
        <strong>제품별 부담률</strong>
        <span>25개 CGM 제품 각각의 환자 자기부담 비율 및 NHIS 보상 수준 비교</span>
      </div>
      <div class="dataset-item">
        <strong>시군구 수준 분포</strong>
        <span>247개 시군구별 1형·2형 당뇨 환자 수 (2022–2024)</span>
      </div>
      <div class="dataset-item">
        <strong>연령별 T1D 코호트</strong>
        <span>만 나이 1세 단위 1형 당뇨 환자 수 추이 (2013–2023, 약 1,100개 행)</span>
      </div>
      <div class="dataset-item">
        <strong>인슐린 청구 월별 추이</strong>
        <span>연령대별 인슐린 주사 청구 건수 월별 시계열 (2016–2023)</span>
      </div>
    </div>
    <p class="cta">
      데이터 요청, 협업 또는 문의는
      <a href="https://github.com/{{ github_repo }}" target="_blank" rel="noopener">GitHub 저장소</a>에서
      Issue 또는 Discussion을 열어주세요.
    </p>
  </div>

  <p class="disclaimer">
    이 보고서는 공공 데이터(NHIS, MFDS, HIRA)를 기반으로 자동 생성된 정책 분석 자료입니다.
    의료 조언이나 투자 권고가 아니며, 정책 연구 및 공익 목적으로만 사용하십시오.
    시장 가격은 {{ data_date }} 기준 공개 정보이며 실제 구매 조건에 따라 다를 수 있습니다.
  </p>

</div><!-- .page -->

{% if ceiling_chart_json %}
<script>
  (function() {
    var fig = {{ ceiling_chart_json | safe }};
    Plotly.newPlot('ceiling-chart', fig.data, fig.layout, {responsive: true});
  })();
</script>
{% endif %}
{% if adoption_chart_json %}
<script>
  (function() {
    var fig = {{ adoption_chart_json | safe }};
    Plotly.newPlot('adoption-chart', fig.data, fig.layout, {responsive: true});
  })();
</script>
{% endif %}
{% if regional_chart_json %}
<script>
  (function() {
    var fig = {{ regional_chart_json | safe }};
    Plotly.newPlot('regional-chart', fig.data, fig.layout, {responsive: true});
  })();
</script>
{% endif %}

</body>
</html>"""


# ── Public entry point ────────────────────────────────────────────────────────

def generate_report(
    output_path: Path | None = None,
    github_repo: str = "pon00050/kr-health-monitor",
) -> Path:
    """Generate the public-facing interactive HTML policy brief.

    Reads from analysis/*.csv (committed snapshots).
    Requires: jinja2, plotly  →  uv sync --extra report --extra viz

    Args:
        output_path: Destination .html path.
                     Defaults to analysis/reports/cgm_coverage_report.html.
        github_repo: GitHub repo slug shown in the CTA link.

    Returns path to generated HTML file.
    """
    try:
        from jinja2 import Template
    except ImportError:
        raise ImportError(
            "jinja2 is required for report generation. "
            "Install with: uv sync --extra report"
        )

    _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    if output_path is None:
        output_path = _REPORTS_DIR / "cgm_coverage_report.html"

    # Load committed CSVs
    gap_df = _load_csv("coverage_gap")
    trend_df = _load_csv("coverage_trend")
    equity_df = _load_csv("regional_equity")

    data_date = (
        gap_df["generated_at"].iloc[0]
        if "generated_at" in gap_df.columns else "N/A"
    )
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")

    # KPI 1: OOP burden range (latest year with coverage)
    covered = gap_df[gap_df["burden_ratio_low"].notna()]
    if not covered.empty:
        latest_gap = covered.iloc[-1]
        b_low = int(latest_gap["burden_ratio_low"] * 100)
        b_high = int(latest_gap["burden_ratio_high"] * 100)
        burden_str = f"{b_low}–{b_high}%"
    else:
        burden_str = "N/A"

    # KPI 2: CGM adoption rate (latest year with data)
    adoption_str = "N/A"
    adoption_year = ""
    if "adoption_rate_registered" in trend_df.columns:
        valid = trend_df[trend_df["adoption_rate_registered"].notna()].sort_values("year")
        if not valid.empty:
            rate = valid.iloc[-1]["adoption_rate_registered"]
            adoption_year = str(int(valid.iloc[-1]["year"]))
            adoption_str = f"{rate * 100:.1f}%"

    kpis = [
        {
            "cls": "warning",
            "label": "환자 월 실비 부담률",
            "value": burden_str,
            "note": "건강보험 적용 후에도 시장가의 68–82%를 환자가 직접 부담",
        },
        {
            "cls": "",
            "label": f"CGM 실제 이용률 ({adoption_year})",
            "value": adoption_str,
            "note": "등록 T1D 요양비 수급자 중 실제 CGM을 사용하는 환자 비율",
        },
        {
            "cls": "",
            "label": "기준금액 동결 기간",
            "value": "2년 7개월+",
            "note": "2022년 8월 이후 ₩210,000/분기 — 한 차례도 조정 없음",
        },
    ]

    # Build charts (each degrades gracefully if plotly is missing)
    ceiling_json = adoption_json = regional_json = None
    try:
        ceiling_json = _build_ceiling_chart(gap_df)
    except Exception as exc:
        logger.warning(f"Ceiling chart skipped: {exc}")
    try:
        adoption_json = _build_adoption_chart(trend_df)
    except Exception as exc:
        logger.warning(f"Adoption chart skipped: {exc}")
    try:
        regional_json = _build_regional_chart(equity_df)
    except Exception as exc:
        logger.warning(f"Regional chart skipped: {exc}")

    template = Template(_TEMPLATE)
    html = template.render(
        title="NHIS CGM 급여 적정성 분석 보고서",
        data_date=data_date,
        generated_at=generated_at,
        kpis=kpis,
        ceiling_chart_json=ceiling_json,
        adoption_chart_json=adoption_json,
        regional_chart_json=regional_json,
        github_repo=github_repo,
    )

    output_path.write_text(html, encoding="utf-8")
    logger.info(f"Report generated: {output_path}")
    return output_path


# ── Legacy stubs (kept so existing tests don't break) ─────────────────────────

def generate_device_report(
    device_category: str,
    output_path: Path | None = None,
    gap_df: pd.DataFrame | None = None,
) -> Path:
    """Deprecated: use generate_report() instead."""
    logger.warning("generate_device_report() is deprecated. Use generate_report().")
    return generate_report(output_path=output_path)


def generate_regional_report(
    region_code: str,
    output_path: Path | None = None,
    regional_df: pd.DataFrame | None = None,
) -> Path:
    """Deprecated: regional analysis is now embedded in generate_report()."""
    logger.warning("generate_regional_report() is deprecated. Use generate_report().")
    return generate_report(output_path=output_path)
