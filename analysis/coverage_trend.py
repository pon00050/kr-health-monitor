"""
Trend analysis — Marimo app.

Analyzes YoY adoption trends and gap widening since 2022 NHIS coverage.
Use run_coverage_trend.py runner to execute non-interactively.
"""

import marimo

__generated_with = "0.1.0"
app = marimo.App(width="wide", app_title="CGM 급여 격차 추세 분석")


@app.cell
def __():
    import sys
    from pathlib import Path

    _root = Path(__file__).resolve().parent.parent
    if str(_root) not in sys.path:
        sys.path.insert(0, str(_root))

    import pandas as pd
    import plotly.graph_objects as go
    from src.coverage import compute_gap_series
    return Path, compute_gap_series, go, pd, sys


@app.cell
def __(compute_gap_series):
    years = list(range(2018, 2027))
    gap_series = compute_gap_series(years, "cgm_sensor")
    gap_series
    return gap_series, years


@app.cell
def __(gap_series, go):
    """Chart: Gap widening trend — 기준금액 vs market price over time"""
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=gap_series["year"],
        y=gap_series["market_price_monthly_mid"].fillna(method="ffill"),
        mode="lines+markers",
        name="시장가 (중간, 월)",
        line=dict(color="#fc8181", width=2),
    ))
    fig.add_trace(go.Scatter(
        x=gap_series["year"],
        y=gap_series["market_price_monthly_low"].fillna(method="ffill"),
        mode="lines",
        name="시장가 (저가, 월)",
        line=dict(color="#68d391", dash="dot"),
    ))

    # 기준금액 monthly equivalent (only post-2022)
    post = gap_series[gap_series["reimb_ceiling_quarterly"].notna()].copy()
    post["ceiling_monthly"] = post["reimb_ceiling_quarterly"] / 3
    fig.add_trace(go.Bar(
        x=post["year"],
        y=post["ceiling_monthly"],
        name="기준금액 (월 환산)",
        marker_color="#4299e1",
        opacity=0.7,
    ))

    fig.update_layout(
        title="CGM 기준금액 동결 vs 시장가 추세 (2018–2026)",
        xaxis_title="연도",
        yaxis_title="금액 (원/월)",
        yaxis_tickformat=",",
        barmode="overlay",
        annotations=[
            dict(x=2022.5, y=200000,
                 text="← 급여 시행<br>기준금액 ₩70K/월 고정",
                 showarrow=True, arrowhead=2, ax=40, ay=-40),
        ],
    )
    fig
    return fig, post


@app.cell
def __(gap_series, pd):
    """Projection: if gap continues at current rate, coverage ratio by 2030"""
    has_mid = gap_series["coverage_ratio_mid"].notna()
    df_proj = gap_series[has_mid][["year", "coverage_ratio_mid"]].copy()

    if len(df_proj) >= 2:
        import numpy as np
        x = df_proj["year"].values
        y = df_proj["coverage_ratio_mid"].values
        try:
            coeffs = np.polyfit(x, y, 1)
            proj_years = list(range(df_proj["year"].max() + 1, 2031))
            proj_ratios = [coeffs[0] * yr + coeffs[1] for yr in proj_years]
            proj_df = pd.DataFrame({"year": proj_years, "coverage_ratio_mid": proj_ratios, "projected": True})
            df_proj["projected"] = False
            combined = pd.concat([df_proj, proj_df], ignore_index=True)
            print(f"Projected coverage ratio in 2030: {proj_ratios[-1]*100:.1f}%")
        except Exception:
            combined = df_proj
            combined["projected"] = False
    else:
        combined = df_proj
        combined["projected"] = False if not df_proj.empty else pd.Series([], dtype=bool)

    combined
    return combined, df_proj


@app.cell
def __(gap_series, pd):
    """Output table for CSV export"""
    output_cols = [
        "year", "reimb_ceiling_quarterly",
        "market_price_monthly_low", "market_price_monthly_mid", "market_price_monthly_high",
        "burden_ratio_low", "burden_ratio_mid", "burden_ratio_high",
        "coverage_ratio_low", "coverage_ratio_mid", "coverage_ratio_high",
    ]
    available = [c for c in output_cols if c in gap_series.columns]
    output_df = gap_series[available].copy()
    output_df
    return available, output_df, output_cols


if __name__ == "__main__":
    app.run()
