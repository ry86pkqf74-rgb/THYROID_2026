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


@pytest.fixture
def sample_molecular_report() -> str:
    return (
        "ThyroSeq v3 Genomic Classifier results: BRAF V600E mutation detected. "
        "NRAS Q61R not detected. EIF1AX mutation not detected. TERT promoter "
        "mutation detected. No copy number alterations identified. RET/PTC fusion "
        "not detected. Gene expression classifier: suspicious for malignancy. "
        "Bethesda IV. Specimen: left thyroid FNA. Risk of malignancy: high "
        "probability (>95%). Loss of heterozygosity not detected. PAX8-PPARG "
        "fusion negative. TP53 mutation not detected."
    )


@pytest.fixture
def sample_rai_report() -> str:
    return (
        "Nuclear Medicine Report: Post-thyroidectomy I-131 therapy. "
        "Patient received 150 mCi radioactive iodine for remnant ablation. "
        "Pre-treatment whole body scan showed uptake in thyroid bed. "
        "Post-therapy scan at 7 days demonstrates iodine-avid uptake in "
        "thyroid bed, no distant metastases. Stimulated thyroglobulin 2.5 ng/mL, "
        "TSH 45 mIU/L. 24-hour uptake was 3.2%."
    )


@pytest.fixture
def sample_us_report() -> str:
    return (
        "THYROID ULTRASOUND: Right lobe: 2.3 x 1.5 x 1.2 cm solid hypoechoic "
        "nodule with microcalcifications, irregular margins, taller than wide. "
        "ACR TI-RADS 5 (TR5). Left lobe: 0.8 cm isoechoic nodule, well-defined "
        "margins, no calcifications. TI-RADS 2. No suspicious cervical "
        "lymphadenopathy. Stable compared to prior. Multinodular goiter."
    )


@pytest.fixture
def sample_detailed_op_note() -> str:
    return (
        "OPERATIVE NOTE: Total thyroidectomy with bilateral central neck "
        "dissection. Intraoperative nerve monitoring (NIM) was used throughout. "
        "The right recurrent laryngeal nerve was identified and preserved with "
        "intact signal. Left RLN identified and preserved. Two parathyroid glands "
        "were autotransplanted into the right sternocleidomastoid. One parathyroid "
        "was inadvertently devascularized. Tumor was adherent to the strap muscles "
        "which were resected en bloc. No tracheal invasion. No esophageal involvement. "
        "Berry's ligament was carefully dissected. EBL 50 mL. JP drain placed. "
        "Specimen oriented and sent to pathology. Frozen section: papillary carcinoma."
    )


@pytest.fixture
def sample_path_report() -> str:
    return (
        "PATHOLOGY: Papillary thyroid carcinoma, tall cell variant. Tumor size "
        "2.1 cm. Multifocal, 3 separate foci of carcinoma. Minimal extrathyroidal "
        "extension. Capsular invasion present. No perineural invasion. Extensive "
        "vascular invasion. Lymphatic invasion identified. Margins negative. "
        "Extranodal extension present. 4 of 12 lymph nodes positive with metastatic "
        "carcinoma. NIFTP not applicable. Poorly differentiated component identified. "
        "Consultation diagnosis by expert pathologist reviewed."
    )


@pytest.fixture
def sample_conflicting_path() -> str:
    return (
        "Original diagnosis: follicular thyroid carcinoma, minimally invasive. "
        "Consultation diagnosis: papillary thyroid carcinoma, follicular variant, "
        "encapsulated. Expert review confirms revised diagnosis."
    )
