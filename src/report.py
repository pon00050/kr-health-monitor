"""
HTML report generator for device/region coverage analysis.
Uses Jinja2 templates with embedded Plotly charts.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

_REPORTS_DIR = Path(__file__).resolve().parent.parent / "03_Analysis" / "reports"

# Minimal Jinja2 template — inline to avoid external template files
_DEVICE_REPORT_TEMPLATE = """<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <title>{{ title }}</title>
  <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
  <style>
    body { font-family: 'Noto Sans KR', sans-serif; max-width: 1200px; margin: 0 auto; padding: 20px; }
    h1 { color: #1a365d; }
    .metric { display: inline-block; margin: 10px; padding: 15px; background: #f0f4f8; border-radius: 8px; }
    .metric-value { font-size: 24px; font-weight: bold; color: #2b6cb0; }
    table { border-collapse: collapse; width: 100%; margin: 20px 0; }
    th, td { border: 1px solid #ddd; padding: 8px 12px; text-align: right; }
    th { background: #2b6cb0; color: white; }
    .flag { color: #e53e3e; font-weight: bold; }
  </style>
</head>
<body>
  <h1>{{ title }}</h1>
  <p>생성일: {{ generated_at }} | 데이터 출처: NHIS 고시, MFDS, HIRA</p>

  <h2>핵심 지표</h2>
  <div class="metrics">
    {% for metric in metrics %}
    <div class="metric">
      <div>{{ metric.label }}</div>
      <div class="metric-value">{{ metric.value }}</div>
    </div>
    {% endfor %}
  </div>

  {% if gap_chart_json %}
  <h2>기준금액 vs 시장가 격차</h2>
  <div id="gap-chart"></div>
  <script>
    Plotly.newPlot('gap-chart', {{ gap_chart_json | safe }}.data, {{ gap_chart_json | safe }}.layout);
  </script>
  {% endif %}

  {% if burden_table_html %}
  <h2>환자 부담금 분석</h2>
  {{ burden_table_html | safe }}
  {% endif %}

  <p style="color: #718096; font-size: 12px; margin-top: 40px;">
    이 보고서는 공공 데이터(NHIS, MFDS, HIRA)를 기반으로 자동 생성된 분석 자료입니다.
    의료 조언이나 투자 권고가 아니며, 정책 연구 목적으로만 사용하십시오.
  </p>
</body>
</html>"""


def generate_device_report(
    device_category: str,
    output_path: Path | None = None,
    gap_df: pd.DataFrame | None = None,
) -> Path:
    """Generate an HTML coverage gap report for a device category.

    Args:
        device_category: e.g., "cgm_sensor"
        output_path: Output .html path; defaults to 03_Analysis/reports/{device_category}_report.html
        gap_df: Optional pre-computed gap series DataFrame (from coverage_index.compute_gap_series)

    Returns path to generated HTML file.
    """
    try:
        from jinja2 import Template
    except ImportError:
        raise ImportError("jinja2 is required for report generation: uv add jinja2")

    import datetime

    _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    if output_path is None:
        output_path = _REPORTS_DIR / f"{device_category}_report.html"

    title = f"NHIS 급여 적정성 분석 — {device_category}"
    generated_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

    metrics = []
    burden_table_html = None
    gap_chart_json = None

    if gap_df is not None and not gap_df.empty:
        # Summary metrics
        latest = gap_df.dropna(subset=["burden_ratio_mid"]).iloc[-1] if "burden_ratio_mid" in gap_df.columns else None
        if latest is not None:
            metrics = [
                {"label": "기준금액 (분기)", "value": f"₩{int(latest.get('reimb_ceiling_quarterly', 0)):,}"},
                {"label": "환자 부담 비율 (중간 시장가)", "value": f"{latest.get('burden_ratio_mid', 0)*100:.1f}%"},
                {"label": "NHIS 지급액 (분기)", "value": f"₩{int(latest.get('nhis_pays_quarterly', 0)):,}"},
            ]

        # Burden table
        display_cols = [c for c in ["year", "reimb_ceiling_quarterly", "burden_ratio_low",
                                     "burden_ratio_mid", "burden_ratio_high"] if c in gap_df.columns]
        if display_cols:
            burden_table_html = gap_df[display_cols].to_html(index=False, classes="burden-table")

    template = Template(_DEVICE_REPORT_TEMPLATE)
    html = template.render(
        title=title,
        generated_at=generated_at,
        metrics=metrics,
        gap_chart_json=gap_chart_json,
        burden_table_html=burden_table_html,
    )

    output_path.write_text(html, encoding="utf-8")
    logger.info(f"Report generated: {output_path}")
    return output_path


def generate_regional_report(
    region_code: str,
    output_path: Path | None = None,
    regional_df: pd.DataFrame | None = None,
) -> Path:
    """Generate an HTML coverage report for a specific region.

    Args:
        region_code: 2-digit 시도 code (e.g., "11" for Seoul)
        output_path: Output .html path
        regional_df: Optional pre-computed regional DataFrame

    Returns path to generated HTML file.
    """
    from src.config import REGION_CODES
    try:
        from jinja2 import Template
    except ImportError:
        raise ImportError("jinja2 is required for report generation")

    import datetime

    _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    region_name = REGION_CODES.get(region_code, region_code)
    if output_path is None:
        output_path = _REPORTS_DIR / f"region_{region_code}_{region_name}_report.html"

    title = f"지역별 CGM 급여 분석 — {region_name} ({region_code})"
    generated_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    metrics = []
    burden_table_html = None

    if regional_df is not None and not regional_df.empty:
        region_row = regional_df[regional_df["region_code"] == region_code]
        if not region_row.empty:
            row = region_row.iloc[0]
            metrics = [
                {"label": "CGM 도입률", "value": f"{row.get('adoption_rate_pct', 0):.1f}%"},
                {"label": "전국 중앙값 대비", "value": f"{row.get('national_median_ratio', 1):.2f}×"},
                {"label": "전국 순위", "value": f"{int(row.get('adoption_pct_rank', 0))}위 / 17"},
            ]
        burden_table_html = regional_df.to_html(index=False)

    template = Template(_DEVICE_REPORT_TEMPLATE)
    html = template.render(
        title=title,
        generated_at=generated_at,
        metrics=metrics,
        gap_chart_json=None,
        burden_table_html=burden_table_html,
    )

    output_path.write_text(html, encoding="utf-8")
    logger.info(f"Regional report generated: {output_path}")
    return output_path
