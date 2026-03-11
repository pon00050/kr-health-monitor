"""Shared pytest fixtures — no live API calls."""

import pandas as pd
import pytest
from pathlib import Path

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def regional_fixture():
    """Synthetic 17-region × year DataFrame mirroring hira_regional_diabetes.parquet schema."""
    return pd.read_csv(
        FIXTURES_DIR / "regional_diabetes_sample.csv",
        dtype={"region_code": str},
    )


@pytest.fixture
def mfds_fixture():
    """Synthetic MFDS CGM product list mirroring mfds_device_approvals.parquet schema."""
    return pd.read_csv(FIXTURES_DIR / "mfds_cgm_sample.csv")
