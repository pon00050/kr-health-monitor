"""
NHIS reimbursement policy constants.

Source: 보건복지부 고시 제2024-226호 (updated August 2024)
"""

# ─── 기준금액 history ──────────────────────────────────────────────────────────
# CGM amount is PER QUARTER (3 months); compute monthly as amount / 3
# Insulin pump is PER DEVICE (one-time)
# Format: (effective_date_str, reimb_ceiling_krw, gosi_number)
NHIS_REIMB_HISTORY = {
    "cgm_sensor": [
        # 연속혈당측정용 전극; Type 1 + insulin-dependent Type 2 age 19+
        ("2022-08-01", 210_000, "2022-170"),   # Initial coverage: ₩210,000/quarter
        # 2024 update: same amount, expanded eligibility; ceiling unchanged
        ("2024-08-01", 210_000, "2024-226"),
    ],
    "insulin_pump": [
        # 인슐린 자동주입기; ₩1,700,000 PER DEVICE (not monthly)
        ("2020-01-01", 1_700_000, "2020-xxx"),  # Coverage start: Jan 2020
        ("2024-08-01", 1_700_000, "2024-226"),  # Confirmed same amount in 2024 update
    ],
}

NHIS_REIMBURSEMENT_RATIO = 0.70        # NHIS pays 70% of min(actual, 기준금액)
# Low-income exception: 차상위 계층 → 100% coverage within 기준금액

# ─── Market price ranges (verified from diabetes project research, 2025) ──────
# Monthly prices in KRW; all per-month even though 기준금액 is quarterly
MARKET_PRICES_KRW = {
    "cgm_sensor": {
        "low": 155_000,   # Budget options, domestic brands
        "mid": 200_000,   # FreeStyle Libre 2, Dexcom G6 typical
        "high": 280_000,  # Dexcom G7; Guardian 4 ~₩400K at high end
    },
    "insulin_pump_supplies": {
        "low": 450_000,   # Monthly consumable estimate
        "high": 700_000,
    },
}
