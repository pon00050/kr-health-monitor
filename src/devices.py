"""
Approved CGM device catalog (MFDS confirmed, March 2026).
"""

# ─── HIRA 치료재료 m_code → price tier mapping (verified via live API, March 2026) ─
# Excludes BM0603AW (Enlite, 삭제)
CGM_M_CODE_TO_TIER = {
    "BM0600EC": "low",   # FreeStyle Libre
    "BM0601EC": "low",   # FreeStyle Libre 2
    "BM0601KV": "mid",   # Dexcom (original)
    "BM0602KV": "mid",   # Dexcom G6
    "BM6003KV": "high",  # Dexcom G7
    "BM0604AW": "high",  # Guardian Sensor 3
    "BM0605AW": "high",  # Guardian 4 Standalone
    "BM0606AW": "high",  # Guardian 4 Pump Integrated
    "BM0601AW": "mid",   # Glucose Sensing Device (Medtronic)
    "BM0600CA": "low",   # CareSens Air / 바로잰 FIT
}

# ─── Approved CGM products in Korea (MFDS confirmed, March 2026) ──────────────
CGM_APPROVED_PRODUCTS = [
    {"brand": "FreeStyle Libre", "manufacturer": "Abbott", "approved": "2020-05", "distributor": "Daewoo Pharma"},
    {"brand": "Dexcom G6",       "manufacturer": "Dexcom Inc.", "approved": "2018-2019", "distributor": "Huons"},
    {"brand": "Dexcom G7",       "manufacturer": "Dexcom Inc.", "approved": "2024-10", "distributor": "Huons"},
    {"brand": "Guardian 4",      "manufacturer": "Medtronic", "approved": "2019-2023", "distributor": "Medtronic Korea"},
    {"brand": "CareSens Air",    "manufacturer": "i-SENS", "approved": "2023-09", "distributor": "i-SENS"},
]
