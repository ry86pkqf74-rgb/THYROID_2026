"""M044 Table 2 — person-year incidence uses FU>0 for numerator and denominator."""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "scripts"))
import m044_ete_fit_models as m044  # noqa: E402


def test_py_rate_excludes_zero_fu_events_from_numerator() -> None:
    """Three rows: two with FU>0 (one path-proven), one with FU=0 but path-proven.

    Cohort path-proven n = 2 (both count toward proportion).
    PY rate uses only FU>0: 1 event / 5 PY = 20 per 100 PY (not 2/5).
    """
    df = pd.DataFrame(
        {
            "research_id": [1, 2, 3],
            "ete_group": ["Microscopic ETE"] * 3,
            "followup_years": [3.0, 2.0, 0.0],
            "recurrence_path_proven": [True, False, True],
            "is_implausible_date_quarantine": [False, False, False],
            "recurrence_status_final": ["path_proven", "none", "path_proven"],
            "recurrence_imaging_then_path_confirmed": [False, False, False],
        }
    )
    out = m044.build_table2_recurrence_summary(df)
    row = out.loc[out["ete_group"] == "Microscopic ETE"].iloc[0]
    assert row["path_proven_n"] == 2
    assert row["path_proven_n_positive_fu"] == 1
    assert row["person_years_positive_fu"] == pytest.approx(5.0)
    assert row["pp_per_100py"] == pytest.approx(20.0)


@pytest.mark.integration
def test_parquet_table2_py_columns_internally_consistent() -> None:
    """If analytic parquet exists, PY columns match path_proven_n_positive_fu / pos_PY."""

    pq = REPO / "data" / "m044" / "analytic_file_v1.parquet"
    if not pq.exists():
        pytest.skip("data/m044/analytic_file_v1.parquet not built")

    df = pd.read_parquet(pq)
    tbl = m044.build_table2_recurrence_summary(df)

    for _, row in tbl.iterrows():
        py = float(row["person_years_positive_fu"])
        if py <= 0:
            assert row["pp_per_100py"] == 0
            continue
        expect = 100.0 * float(row["path_proven_n_positive_fu"]) / py
        assert abs(float(row["pp_per_100py"]) - round(expect, 2)) <= 0.015


@pytest.mark.integration
def test_motherduck_publication_table2_golden_three_groups() -> None:
    """Optional CI/manual gate vs canonical MotherDuck counts — export parquet must match manuscript."""

    pq = REPO / "data" / "m044" / "analytic_file_v1.parquet"
    if not pq.exists():
        pytest.skip("data/m044/analytic_file_v1.parquet not built")
    import os

    if os.environ.get("M044_TABLE2_GOLDEN_CHECK") != "1":
        pytest.skip("set M044_TABLE2_GOLDEN_CHECK=1 to enforce manuscript freeze counts")

    df = pd.read_parquet(pq)
    assert len(df) == 4128
    tbl = m044.build_table2_recurrence_summary(df)

    mic = tbl.loc[tbl["ete_group"] == "Microscopic ETE"].iloc[0]
    assert mic["path_proven_n"] == 80
    assert mic["path_proven_n_positive_fu"] == 78
    np.testing.assert_allclose(mic["person_years_positive_fu"], 8137.3, rtol=0, atol=0.15)
    np.testing.assert_allclose(mic["pp_per_100py"], 0.96, atol=0.02)

    gross = tbl.loc[tbl["ete_group"] == "Gross ETE"].iloc[0]
    assert gross["path_proven_n"] == 105
    assert gross["path_proven_n_positive_fu"] == 103
    np.testing.assert_allclose(gross["person_years_positive_fu"], 4138.3, rtol=0, atol=0.15)
    np.testing.assert_allclose(gross["pp_per_100py"], 2.49, atol=0.02)

    noneg = tbl.loc[tbl["ete_group"] == "No/negative ETE"].iloc[0]
    assert noneg["path_proven_n"] == 18
    assert noneg["path_proven_n_positive_fu"] == 17
    np.testing.assert_allclose(noneg["person_years_positive_fu"], 700.8, rtol=0, atol=0.15)
    np.testing.assert_allclose(noneg["pp_per_100py"], 2.43, atol=0.02)
