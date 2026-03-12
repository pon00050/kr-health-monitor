"""
Coverage adequacy analysis — Marimo app.

Analyzes the 기준금액 vs market price gap for CGM sensors.
Use run_coverage_gap.py runner to execute non-interactively.
"""

import marimo

__generated_with = "0.1.0"
app = marimo.App(width="wide", app_title="NHIS CGM 급여 적정성 분석")


@app.cell
def __():
    import sys
    from pathlib import Path

    # Allow running from project root or this directory
    _root = Path(__file__).resolve().parent.parent
    if str(_root) not in sys.path:
        sys.path.insert(0, str(_root))

    import pandas as pd
    import plotly.graph_objects as go
    from src.policy import MARKET_PRICES_KRW, NHIS_REIMB_HISTORY
    from src.coverage import compute_gap_series
    return Path, MARKET_PRICES_KRW, NHIS_REIMB_HISTORY, compute_gap_series, go, pd, sys


@app.cell
def __(compute_gap_series):
    years = list(range(2018, 2027))
    gap_series = compute_gap_series(years, "cgm_sensor")
    gap_series
    return gap_series, years


@app.cell
def __(gap_series, go):
    """Chart 1: 기준금액 vs market price over 2018–2026"""
    fig1 = go.Figure()

    post_coverage = gap_series[gap_series["reimb_ceiling_quarterly"].notna()].copy()

    if not post_coverage.empty:
        # Monthly equivalent of quarterly 기준금액
        post_coverage["ceiling_monthly_equiv"] = post_coverage["reimb_ceiling_quarterly"] / 3

        fig1.add_trace(go.Bar(
            x=post_coverage["year"],
            y=post_coverage["ceiling_monthly_equiv"],
            name="기준금액 (월 환산)",
            marker_color="#2b6cb0",
        ))
        fig1.add_trace(go.Scatter(
            x=gap_series["year"],
            y=gap_series["market_price_monthly_low"],
            name="시장가 (저가)",
            line=dict(dash="dot", color="#68d391"),
        ))
        fig1.add_trace(go.Scatter(
            x=gap_series["year"],
            y=gap_series["market_price_monthly_mid"],
            name="시장가 (중간)",
            line=dict(color="#f6ad55"),
        ))
        fig1.add_trace(go.Scatter(
            x=gap_series["year"],
            y=gap_series["market_price_monthly_high"],
            name="시장가 (고가)",
            line=dict(dash="dash", color="#fc8181"),
        ))

    fig1.update_layout(
        title="연속혈당측정기(CGM) 기준금액 vs 시장가 (월 기준)",
        xaxis_title="연도",
        yaxis_title="금액 (원/월)",
        yaxis_tickformat=",",
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    fig1
    return fig1, post_coverage


@app.cell
def __(gap_series, go):
    """Chart 2: Patient monthly burden ratio over time"""
    fig2 = go.Figure()

    has_burden = gap_series["burden_ratio_mid"].notna()
    df2 = gap_series[has_burden]

    if not df2.empty:
        fig2.add_trace(go.Scatter(
            x=df2["year"],
            y=df2["burden_ratio_low"] * 100,
            fill=None,
            mode="lines",
            name="환자 부담률 (저가)",
            line=dict(color="#68d391"),
        ))
        fig2.add_trace(go.Scatter(
            x=df2["year"],
            y=df2["burden_ratio_high"] * 100,
            fill="tonexty",
            mode="lines",
            name="환자 부담률 (고가)",
            line=dict(color="#fc8181"),
        ))
        fig2.add_trace(go.Scatter(
            x=df2["year"],
            y=df2["burden_ratio_mid"] * 100,
            mode="lines+markers",
            name="환자 부담률 (중간가)",
            line=dict(color="#ed8936", width=3),
        ))

    fig2.update_layout(
        title="CGM 급여 시행(2022.8) 이후 환자 부담률 (%)",
        xaxis_title="연도",
        yaxis_title="환자 부담률 (%)",
        yaxis=dict(ticksuffix="%", range=[0, 100]),
        annotations=[dict(
            x=2022, y=75,
            text="↑ 급여 시행 (2022.8)",
            showarrow=True, arrowhead=2,
        )] if not df2.empty else [],
    )
    fig2
    return df2, fig2


@app.cell
def __(gap_series, go):
    """Chart 3: Coverage adequacy index (target: ≥0.70, actual: ~0.25–0.32)"""
    fig3 = go.Figure()

    has_ratio = gap_series["coverage_ratio_mid"].notna()
    df3 = gap_series[has_ratio]

    if not df3.empty:
        fig3.add_trace(go.Bar(
            x=df3["year"],
            y=df3["coverage_ratio_mid"] * 100,
            name="급여 적정성 지수 (중간가 기준)",
            marker_color="#4299e1",
        ))

    fig3.add_hline(
        y=70,
        line_dash="dash",
        line_color="red",
        annotation_text="목표: 70% (NHIS 취지)",
    )

    fig3.update_layout(
        title="CGM 급여 적정성 지수 (NHIS 지급액 / 시장가)",
        xaxis_title="연도",
        yaxis_title="급여 적정성 (%)",
        yaxis=dict(ticksuffix="%", range=[0, 100]),
    )
    fig3
    return df3, fig3


@app.cell
def __(gap_series, pd):
    """Output table for CSV export"""
    output_cols = [
        "year", "reimb_ceiling_quarterly",
        "market_price_monthly_low", "market_price_monthly_mid", "market_price_monthly_high",
        "nhis_pays_quarterly",
        "patient_pays_quarterly_low", "patient_pays_quarterly_mid", "patient_pays_quarterly_high",
        "burden_ratio_low", "burden_ratio_mid", "burden_ratio_high",
        "coverage_ratio_low", "coverage_ratio_mid", "coverage_ratio_high",
    ]
    available_cols = [c for c in output_cols if c in gap_series.columns]
    output_df = gap_series[available_cols].copy()
    output_df
    return available_cols, output_cols, output_df


if __name__ == "__main__":
    app.run()
