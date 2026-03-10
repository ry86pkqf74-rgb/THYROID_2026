"""Shared fixtures for the THYROID_2026 test suite."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


@pytest.fixture
def sample_op_note() -> str:
    return (
        "OPERATIVE REPORT: The patient underwent a total thyroidectomy "
        "with central neck dissection (level VI). The right recurrent "
        "laryngeal nerve was identified and preserved. Estimated blood "
        "loss was 25 mL. Parathyroid autotransplant was performed into "
        "the left sternocleidomastoid muscle. No complications noted. "
        "Pathology: BRAF V600E mutation detected. pT1a N0 M0, Stage I."
    )


@pytest.fixture
def sample_hp_note() -> str:
    return (
        "HISTORY AND PHYSICAL: 55-year-old female with history of "
        "hypertension, type 2 diabetes mellitus, obesity, and GERD. "
        "She is on levothyroxine 125 mcg daily, calcitriol 0.25 mcg, "
        "and calcium carbonate 500 mg TID. She denies hypocalcemia "
        "symptoms. No evidence of BRAF mutation on molecular testing. "
        "Patient reports no vocal cord paralysis post-operatively."
    )


@pytest.fixture
def sample_negated_note() -> str:
    return (
        "Post-operative course was unremarkable. No hypocalcemia. "
        "No evidence of hematoma or seroma. Patient denies "
        "vocal cord weakness. No RLN injury identified. "
        "Ruled out chyle leak. Without hypoparathyroidism."
    )
