from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

_SCRIPT_PATH = ROOT / "scripts" / "m036_ata_2025_rss.py"


def _load_script():
    spec = importlib.util.spec_from_file_location("_m036_ata_2025_rss", _SCRIPT_PATH)
    mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    sys.modules[spec.name] = mod  # type: ignore[index]
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


_M = _load_script()


def _base_row(**overrides):
    row = {
        "research_id": "1001",
        "histology_final": "PTC",
        "tumor_size_cm_dominant": 1.2,
        "multifocal_flag_path": False,
        "ete_grade_final": "none",
        "gross_ete_flag": False,
        "vascular_invasion_final": "none",
        "vascular_invasion_grade": "none",
        "vascular_vessel_count": None,
        "margin_status_final": "R0",
        "margin_r_class_v10": "R0",
        "margin_involved_any": False,
        "ln_positive_final": 0,
        "ln_rollup_total_examined": 1,
        "ln_rollup_total_positive": 0,
        "ln_rollup_largest_deposit_cm": None,
        "braf_positive_final": False,
        "tert_positive_final": False,
        "tp53_positive_v7": False,
        "molecular_risk_tier": "wild_type",
        "high_risk_molecular_v7": False,
        "mol_has_fusion": False,
        "ajcc8_stage_group": "I",
        "ajcc8_t_stage": "T1b",
        "ajcc8_n_stage": "N0",
        "ajcc8_m_stage": "M0",
        "distant_mets_proxy": False,
    }
    row.update(overrides)
    return row


def test_niftp_is_low_risk_even_when_size_missing():
    result = _M.classify_ata_2025(_base_row(histology_final="NIFTP", tumor_size_cm_dominant=None))

    assert result.category == "low"
    assert result.rule_triggered == "low:niftp"
    assert result.missing_inputs == ""


def test_gross_ete_takes_high_risk_precedence_over_low_features():
    result = _M.classify_ata_2025(
        _base_row(
            tumor_size_cm_dominant=0.7,
            ajcc8_t_stage="T4a",
            gross_ete_flag=True,
            braf_positive_final=True,
        )
    )

    assert result.category == "high"
    assert result.rule_triggered == "high:gross_ete_or_t4"


def test_stage_group_ivb_without_m1_does_not_trigger_distant_metastasis():
    result = _M.classify_ata_2025(_base_row(ajcc8_stage_group="IVB", ajcc8_m_stage="M0"))

    assert result.category == "low"
    assert result.rule_triggered == "low:intrathyroidal_ptc_le4cm_n0"


def test_extensive_vascular_invasion_uses_vessel_threshold_for_high_risk():
    result = _M.classify_ata_2025(
        _base_row(
            histology_final="follicular carcinoma",
            vascular_invasion_final="present",
            vascular_vessel_count=5,
        )
    )

    assert result.category == "high"
    assert result.rule_triggered == "high:extensive_vascular_invasion"


def test_four_vessels_is_minor_vascular_invasion_not_high_risk():
    result = _M.classify_ata_2025(
        _base_row(
            histology_final="follicular carcinoma",
            vascular_invasion_final="present",
            vascular_vessel_count=4,
        )
    )

    assert result.category == "intermediate"
    assert result.rule_triggered == "intermediate:minor_vascular_invasion"


def test_r1_margin_alone_does_not_trigger_high_risk_incomplete_resection():
    result = _M.classify_ata_2025(_base_row(margin_status_final="R1", margin_r_class_v10="R1"))

    assert result.category == "low"
    assert result.rule_triggered == "low:intrathyroidal_ptc_le4cm_n0"


def test_rx_margin_alone_is_unknown_not_incomplete_resection():
    result = _M.classify_ata_2025(_base_row(margin_status_final="Rx", margin_r_class_v10="Rx"))

    assert result.category == "low"
    assert result.rule_triggered == "low:intrathyroidal_ptc_le4cm_n0"


def test_r2_margin_is_high_risk_per_corrected_incomplete_resection_rule():
    result = _M.classify_ata_2025(_base_row(margin_status_final="R2", margin_r_class_v10="R2"))

    assert result.category == "high"
    assert result.rule_triggered == "high:incomplete_resection_r2"


def test_braf_alone_is_intermediate_with_corrected_rule_trigger():
    result = _M.classify_ata_2025(_base_row(braf_positive_final=True))

    assert result.category == "intermediate"
    assert result.rule_triggered == "intermediate:braf_v600e_alone"


def test_intrathyroidal_ptc_under_4cm_n0_is_low_risk():
    result = _M.classify_ata_2025(_base_row(tumor_size_cm_dominant=3.8))

    assert result.category == "low"
    assert result.rule_triggered == "low:intrathyroidal_ptc_le4cm_n0"


def test_missing_size_and_nodal_data_logs_uncalculable_inputs():
    result = _M.classify_ata_2025(
        _base_row(
            tumor_size_cm_dominant=None,
            tumor_size_cm=None,
            ln_positive_final=None,
            ln_rollup_total_positive=None,
            ln_rollup_total_examined=None,
            ajcc8_n_stage=None,
        )
    )

    assert result.category == "uncalculable"
    assert result.rule_triggered == "uncalculable:insufficient_anatomic_risk_data"
    assert "tumor_size_cm_dominant" in result.missing_inputs
    assert "ln_positive_final" in result.missing_inputs


def test_reclassification_direction_orders_uncalculable_below_low():
    assert _M.reclassification_direction(None, "high") == "up"
    assert _M.reclassification_direction("high", "low") == "down"
    assert _M.reclassification_direction("intermediate", "intermediate") == "same"
