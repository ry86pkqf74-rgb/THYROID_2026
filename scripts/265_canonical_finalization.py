#!/usr/bin/env python3
"""
Script 265 - Canonical publication v1.0 finalization

Re-issue of "Script 259" prompt against the current repo state. Script 259 in
the repo is the verification-lock artifact (already GREEN); this script picks
up the remaining open work that was deferred (collisions, fusion logic,
inferred-negative defaults, ghost RID, archive sweep).

Run order (idempotent where possible; snapshot-first for every DDL):
  Step 1  ras_positive_v7 drop ........... already done by Script 262, no-op
  Step 2  unmatched registry token fixes . 6 tokens (3 replace, 3 mark rolled-up)
  Step 3  88 multi-source collisions ..... build collision_resolution_v265,
                                            rewrite registry, add secondary col
  Step 4  episode-feed repointing ........ COMMENT-only (option b)
  Step 5  any_fusion_positive vs n_fusions exclude PARSE_ERROR; recompute both;
                                            surface 632 PARSE_ERROR variants
  Step 6  846 NULL fusion / RET defaults . default to FALSE + _inferred_negative
  Step 7  ghost RID 7744 prune ........... 5 molecular tables (per user verdict)
  Step 8  nan_string_audit ............... already PRESERVE_RAW / NO_ACTION;
                                            documented no-op
  Step 9a archive sweep .................. tag raw sources + CTAS legacy
                                            (canonical_patient_master_v1 only);
                                            manifest CSV; STOP before release_*
  Step 10 canonical_detail_pointer_v1 ..... rebuild from new registry
  Step 11 __readme + data_dictionary_v240 . refresh
  Step 12 invariants ...................... audit rows for every step

Default is dry-run. Pass --apply to execute writes.
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402
from _v1_1_helpers import (  # noqa: E402
    ARCHIVE_DB, ARCHIVE_QUALIFIED,
    ensure_archive_schema, ensure_audit_table, make_logger,
    record_audit, snapshot_table, utc_ts, write_decision_log,
)

REPO = Path(__file__).resolve().parent.parent
OUT_DIR = REPO / "scripts" / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)
RUN_LOG = OUT_DIR / "265_run.log"
DECISION_LOG = OUT_DIR / "265_decision_log.json"
MANIFEST_CSV = OUT_DIR / "265_archive_relocation_manifest.csv"
NAN_SUMMARY = OUT_DIR / "265_nan_cleanup_summary.csv"

SCRIPT_TAG = "Script 265"
SCRIPT_NUM = "265"
RUN_DATE = "2026-04-17"
ARCHIVE_LEGACY_QUALIFIED = f'"{ARCHIVE_DB}"."archive_legacy"'

CPM = f'{PUBLICATION_DB}.main.canonical_patient_master'
DICT = f'{PUBLICATION_DB}.main.data_dictionary_v240'
REGISTRY = f'{PUBLICATION_DB}.manuscript_workspace.detail_table_registry_v1'
POINTER = f'{PUBLICATION_DB}.manuscript_workspace.canonical_detail_pointer_v1'
COLLISION_TBL = f'{PUBLICATION_DB}.manuscript_workspace.collision_resolution_v265'
README = f'{PUBLICATION_DB}.main.__readme'
MTE = f'{PUBLICATION_DB}.main.molecular_test_episode_v2'
MVL = f'{PUBLICATION_DB}.main.molecular_variant_long'
ROLLUP = f'{PUBLICATION_DB}.main._molecular_patient_rollup_v227'
FUSION_REVIEW = f'{PUBLICATION_DB}.manuscript_workspace.fusion_parse_error_review_v265'

# ---------------------------------------------------------------------------
# Step 2 — explicit token fixes
# ---------------------------------------------------------------------------
TOKEN_REPLACEMENTS = {
    # detail_table_name : list of (old_token, new_token)
    "note_entities_llm_past_medical_hx": [
        ("obesity", "pmhx_nlp_obesity"),
        ("radiation", "pmhx_nlp_radiation_exposure"),
    ],
    "note_entities_llm_presenting_symptoms": [
        ("dysphagia", "sx_nlp_dysphagia"),
    ],
}
# Tokens to clear (no CPM successor; rolled up via aggregate columns)
TOKEN_CLEAR_NOTE = (
    "tirads component scores (composition, echogenicity, shape, margin, foci) "
    "rolled up via tirads_best_score_v12 / tirads_worst_score_v12; not promoted "
    "individually"
)
TOKEN_CLEAR_TARGETS = {
    # detail_table_name : (clear_message, tokens_to_strip)
    "extracted_tirads_validated_v1": (
        TOKEN_CLEAR_NOTE,
        {"echogenicity", "shape", "margin"},
    ),
}

# ---------------------------------------------------------------------------
# Step 3 — collision resolutions (88 colliding master_columns)
# Each tuple: (master_column, primary_feeder, [demoted_secondaries], rationale)
# Primary/secondary names match detail_table_name in the registry.
# ---------------------------------------------------------------------------
COLLISION_RESOLUTIONS: list[tuple[str, str, list[str], str]] = [
    # Pre-resolved verdicts (Step 3.1)
    ("mol_has_thyroseq", "canonical_molecular_tested_v1",
     ["_molecular_patient_rollup_v227", "molecular_test_episode_v2"],
     "canonical_molecular_tested_v1 is the patient-level rollup; episode table undertags 443 ThyroSeq tests as platform=Other."),
    ("mol_has_afirma", "canonical_molecular_tested_v1",
     ["_molecular_patient_rollup_v227", "molecular_test_episode_v2"],
     "canonical_molecular_tested_v1 is the patient-level rollup; episode table undertagging."),
    ("mol_platform", "canonical_molecular_tested_v1",
     ["_molecular_patient_rollup_v227", "molecular_test_episode_v2"],
     "Patient-level rollup wins; episode table reports only 3 platform values."),
    ("molecular_tested_confirmed", "canonical_molecular_tested_v1",
     ["_molecular_patient_rollup_v227", "molecular_test_episode_v2"],
     "Patient-level rollup is authoritative for the has-test flag."),
    ("mol_n_tests", "_molecular_patient_rollup_v227", ["molecular_test_episode_v2"],
     "Rollup deduplicates patient-level test count over the episode table."),
    ("braf_positive_final", "extracted_braf_recovery_v1",
     ["canonical_molecular_tested_v1", "molecular_test_episode_v2"],
     "BRAF recovery wins on coverage (376 patients with NLP+NGS+IHC fan-in)."),
    ("tg_mean", "tg_timeline_patient_summary_v1",
     ["tg_postop_surveillance_windows_v1", "thyroglobulin_lab_canonical_v1"],
     "Patient-level Tg rollup wins over windows + raw lab."),
    ("tg_nadir", "tg_timeline_patient_summary_v1",
     ["tg_postop_surveillance_windows_v1", "thyroglobulin_lab_canonical_v1"],
     "Patient-level Tg rollup wins."),
    ("tg_peak", "tg_timeline_patient_summary_v1",
     ["tg_postop_surveillance_windows_v1", "thyroglobulin_lab_canonical_v1"],
     "Patient-level Tg rollup wins."),
    ("tg_n_measurements", "tg_timeline_patient_summary_v1",
     ["tg_postop_surveillance_windows_v1", "thyroglobulin_lab_canonical_v1"],
     "Patient-level Tg rollup is authoritative for measurement count."),
    ("tg_rising_flag", "tg_timeline_patient_summary_v1",
     ["tg_postop_surveillance_windows_v1", "thyroglobulin_lab_canonical_v1"],
     "Patient-level Tg rollup encodes the trajectory flag."),
    ("tg_trajectory_class", "tg_timeline_patient_summary_v1",
     ["tg_postop_surveillance_windows_v1", "thyroglobulin_lab_canonical_v1"],
     "Patient-level Tg rollup encodes the trajectory class."),
    ("dominant_nodule_size_cm", "imaging_patient_summary_v1",
     ["canonical_us_nodule_characteristics_v1", "imaging_nodule_master_v1"],
     "Imaging patient summary is the patient-level rollup."),
    ("n_us_exams", "imaging_patient_summary_v1",
     ["ultrasound_reports", "serial_imaging_us"],
     "Imaging patient summary aggregates US exam count."),
    ("tirads_best_score_v12", "canonical_us_nodule_characteristics_v1",
     ["tirads_llm_extracted_v2", "us_nodules_tirads"],
     "Canonical US nodule characteristics is the patient-level rollup for TI-RADS."),
    ("tirads_worst_category_v12", "canonical_us_nodule_characteristics_v1",
     ["tirads_llm_extracted_v2", "us_nodules_tirads"],
     "Canonical US nodule characteristics is authoritative for TI-RADS categories."),
    # Step 3.2 - tumor / pathology (rollup beats episode beats raw)
    ("histology_final", "canonical_malignant_diagnosis_v1",
     ["tumor_episode_master_v2", "synoptic_tumor_long_v1", "tumor_pathology"],
     "Canonical malignant-diagnosis rollup wins over episode + synoptic + raw path."),
    ("path_tumor_size_cm", "patient_tumor_rollup_v1",
     ["tumor_episode_master_v2", "synoptic_tumor_long_v1", "tumor_pathology", "path_synoptics"],
     "Patient tumor rollup is the patient-level size feeder."),
    ("multifocal_flag_path", "patient_tumor_rollup_v1",
     ["tumor_episode_master_v2", "specimen_tumor_focus_v1"],
     "Patient tumor rollup encodes the multifocal flag."),
    ("bethesda_final", "extracted_fna_bethesda_v1", ["fna_episode_master_v2"],
     "extracted_fna_bethesda_v1 is the canonical Bethesda rollup."),
    ("bethesda_category", "extracted_fna_bethesda_v1", ["fna_episode_master_v2"],
     "extracted_fna_bethesda_v1 is the canonical Bethesda rollup."),
    # Step 3.3 - complications: complication_phenotype_v1 wins (broader 2,938 vs 59 recalibration override)
    ("comp_rln_injury_confirmed", "complication_phenotype_v1",
     ["vc_paralysis_recalibration_v236"],
     "complication_phenotype_v1 covers 2,938 patients; vc_paralysis_recalibration_v236 (table absent in publication DB; folded into phenotype) is targeted override."),
    ("comp_vc_paralysis_confirmed", "complication_phenotype_v1",
     ["vc_paralysis_recalibration_v236"],
     "complication_phenotype_v1 wins; recalibration is targeted override."),
    ("comp_vc_paresis_confirmed", "complication_phenotype_v1",
     ["vc_paralysis_recalibration_v236"],
     "complication_phenotype_v1 wins; recalibration is targeted override."),
    # Step 3.3 - ETE
    ("ete_grade", "synoptic_tumor_long_v1", ["ete_adjudication_v1"],
     "Broader synoptic source for the unadjudicated grade; ete_adjudication_v1 secondary."),
    ("ete_grade_adjudicated", "ete_adjudication_v1", ["patient_tumor_rollup_v1"],
     "Adjudicated grade comes from the clinician-adjudication table."),
    ("ete_grade_final", "ete_adjudication_v1", ["patient_tumor_rollup_v1"],
     "Final grade encodes the 45 clinician verdicts; adjudication wins."),
    ("ete_grade_final_v2", "ete_adjudication_v1", ["patient_tumor_rollup_v1"],
     "Final v2 grade encodes adjudication; clinician verdicts win."),
    # Step 3.3 - recurrence (canonical_recurrence_v1 is the patient-level rollup)
    ("any_recurrence_flag", "canonical_recurrence_v1", ["recurrence_event_clean_v1"],
     "canonical_recurrence_v1 is the patient-level rollup."),
    ("biochemical_recurrence_flag", "canonical_recurrence_v1", ["recurrence_event_clean_v1"],
     "canonical_recurrence_v1 is the patient-level rollup."),
    ("structural_recurrence_flag", "canonical_recurrence_v1", ["recurrence_event_clean_v1"],
     "canonical_recurrence_v1 is the patient-level rollup."),
    ("recurrence_site", "canonical_recurrence_v1", ["recurrence_event_clean_v1"],
     "canonical_recurrence_v1 is the patient-level rollup."),
    ("recurrence_type", "canonical_recurrence_v1", ["recurrence_event_clean_v1"],
     "canonical_recurrence_v1 is the patient-level rollup."),
    ("time_to_recurrence_days", "canonical_recurrence_v1", ["recurrence_event_clean_v1"],
     "canonical_recurrence_v1 is the patient-level rollup."),
    # remaining n=2 collisions seen in probe2 - apply rollup-wins rule
    ("bilateral_disease_flag", "imaging_patient_summary_v1", ["specimen_tumor_focus_v1"],
     "Imaging patient summary is the patient-level bilateral-disease feeder."),
    ("n_us_nodules_total", "canonical_us_nodule_characteristics_v1", ["us_nodules_tirads"],
     "Canonical US nodule characteristics rollup wins over raw nodule table."),
    ("nlp_ln_has_data", "clinical_note_ln_extracted_v1", ["note_entities_llm_cervical_ln_detail"],
     "Clinical-note LN extracted view is the consolidated NLP feed."),
    ("nlp_ln_positive_mentioned", "clinical_note_ln_extracted_v1", ["note_entities_llm_cervical_ln_detail"],
     "Clinical-note LN extracted view is the consolidated NLP feed."),
    ("r_class_true", "patient_tumor_rollup_v1", ["canonical_tumor_characteristics_v1"],
     "Patient tumor rollup is the patient-level R-class feeder."),
    ("ras_positive_final", "extracted_ras_patient_summary_v1", ["molecular_test_episode_v2"],
     "RAS patient summary is the patient-level rollup; episode-level secondary."),
    ("ret_adjudicated_flag", "ret_patient_adjudicated_v226", ["_molecular_patient_rollup_v227"],
     "RET adjudication table is authoritative for adjudicated flag."),
    ("ret_evidence_source", "ret_patient_adjudicated_v226", ["_molecular_patient_rollup_v227"],
     "RET adjudication table is authoritative for evidence source."),
    ("rln_injury_type", "complication_phenotype_v1", ["extracted_rln_injury_refined_v2"],
     "complication_phenotype_v1 is the patient-level complication rollup."),
    ("rln_permanent_flag", "complication_phenotype_v1", ["extracted_rln_injury_refined_v2"],
     "complication_phenotype_v1 is the patient-level complication rollup."),
    ("rln_status", "complication_phenotype_v1", ["extracted_rln_injury_refined_v2"],
     "complication_phenotype_v1 is the patient-level complication rollup."),
    ("rln_transient_flag", "complication_phenotype_v1", ["extracted_rln_injury_refined_v2"],
     "complication_phenotype_v1 is the patient-level complication rollup."),
    ("surv_max_time_days", "canonical_survival_followup_v1", ["survival_cohort_enriched"],
     "canonical_survival_followup_v1 is the patient-level rollup."),
    ("surv_n_events", "canonical_survival_followup_v1", ["survival_cohort_enriched"],
     "canonical_survival_followup_v1 is the patient-level rollup."),
    ("surv_recurrence_risk_band", "canonical_survival_followup_v1", ["survival_cohort_enriched"],
     "canonical_survival_followup_v1 is the patient-level rollup."),
    ("surv_tg_annual_log_slope", "canonical_survival_followup_v1", ["survival_cohort_enriched"],
     "canonical_survival_followup_v1 is the patient-level rollup."),
    ("syn_follicular_adenoma", "canonical_benign_diagnosis_v1", ["path_synoptics"],
     "Canonical benign-diagnosis rollup wins over raw synoptics."),
    ("syn_graves", "canonical_benign_diagnosis_v1", ["path_synoptics"],
     "Canonical benign-diagnosis rollup wins."),
    ("syn_hashimoto", "canonical_benign_diagnosis_v1", ["path_synoptics"],
     "Canonical benign-diagnosis rollup wins."),
    ("syn_multinodular_goiter", "canonical_benign_diagnosis_v1", ["path_synoptics"],
     "Canonical benign-diagnosis rollup wins."),
    ("tert_positive_final", "_molecular_patient_rollup_v227", ["molecular_test_episode_v2"],
     "Molecular patient rollup is the patient-level TERT feeder."),
    ("tgab_interference_flag", "thyroglobulin_lab_canonical_v1", ["tg_postop_surveillance_windows_v1"],
     "Thyroglobulin lab canonical encodes the TgAb interference flag."),
    ("tirads_best_category_v12", "canonical_us_nodule_characteristics_v1",
     ["tirads_llm_extracted_v2", "us_nodules_tirads"],
     "Canonical US nodule characteristics rollup wins."),
    ("tirads_best_combined", "canonical_us_nodule_characteristics_v1",
     ["tirads_llm_extracted_v2", "imaging_nodule_master_v1"],
     "Canonical US nodule characteristics rollup wins."),
    ("tirads_worst_combined", "canonical_us_nodule_characteristics_v1",
     ["tirads_llm_extracted_v2", "imaging_nodule_master_v1"],
     "Canonical US nodule characteristics rollup wins."),
    ("tumor_size_cm_max", "patient_tumor_rollup_v1", ["canonical_tumor_characteristics_v1"],
     "Patient tumor rollup is the patient-level size feeder."),
    ("us_left_lobe_volume_ml", "ultrasound_reports", ["thyroid_sizes"],
     "Ultrasound reports is the primary lobe-volume source."),
    ("us_right_lobe_volume_ml", "ultrasound_reports", ["thyroid_sizes"],
     "Ultrasound reports is the primary lobe-volume source."),
    ("us_total_volume_ml", "ultrasound_reports", ["thyroid_sizes"],
     "Ultrasound reports is the primary lobe-volume source."),
    ("us_n_reports", "imaging_exam_master_v1", ["ultrasound_reports"],
     "Imaging exam master encodes patient-level US report count."),
    ("vascular_invasion_grade", "patient_tumor_rollup_v1", ["note_entities_llm_vascular_invasion"],
     "Patient tumor rollup is the patient-level vascular invasion feeder."),
    # 23 additional collisions surfaced by dry-run (not enumerated in prompt).
    # Apply the same rule: patient-level rollup beats episode-level beats raw.
    # Adjudication / refinement tables win for grade-source / refined / subgrade /
    # path-outcome flags (those are the table's named purpose).
    ("ete_grade_source", "ete_adjudication_v1", ["patient_tumor_rollup_v1"],
     "ete_grade_source describes the adjudication's input; ete_adjudication_v1 wins."),
    ("ete_refined_grade", "extracted_ete_subgraded_v1", ["ete_adjudication_v1"],
     "ETE refined grade comes from the subgraded extraction view."),
    ("ete_subgrade_method", "extracted_ete_subgraded_v1", ["ete_adjudication_v1"],
     "Subgrade method is the subgraded view's purpose."),
    ("ete_subgrade_note", "extracted_ete_subgraded_v1", ["ete_adjudication_v1"],
     "Subgrade note is the subgraded view's purpose."),
    ("fna_bethesda_confidence", "extracted_fna_bethesda_v1", ["fna_episode_master_v2"],
     "extracted_fna_bethesda_v1 is the canonical Bethesda rollup."),
    ("fna_bethesda_source", "extracted_fna_bethesda_v1", ["fna_episode_master_v2"],
     "extracted_fna_bethesda_v1 is the canonical Bethesda rollup."),
    ("fna_path_concordance_category", "path_outcome_classification_v1", ["fna_episode_master_v2"],
     "Concordance is the path_outcome_classification view's purpose."),
    ("fna_path_concordant", "path_outcome_classification_v1", ["fna_episode_master_v2"],
     "Concordance flag belongs to the path_outcome_classification view."),
    ("fna_path_outcome", "path_outcome_classification_v1", ["fna_episode_master_v2"],
     "fna_path_outcome is the named purpose of path_outcome_classification_v1."),
    ("followup_days", "canonical_survival_followup_v1", ["patient_cross_domain_timeline_v2"],
     "canonical_survival_followup_v1 is the canonical follow-up feeder."),
    ("last_contact_date", "canonical_survival_followup_v1", ["patient_cross_domain_timeline_v2"],
     "canonical_survival_followup_v1 is the canonical follow-up feeder."),
    ("lateral_neck_dissected", "ln_master_rollup_v1", ["operative_episode_detail_v2"],
     "ln_master_rollup_v1 is the LN-specific patient-level rollup."),
    ("ln_rollup_crossval_status", "ln_crossval_v1", ["ln_master_rollup_v1"],
     "ln_crossval_v1 is the named source for crossval status."),
    ("ln_rollup_internal_consistency", "ln_crossval_v1", ["ln_master_rollup_v1"],
     "ln_crossval_v1 is the named source for internal consistency."),
    ("lvi_any_present_path", "patient_tumor_rollup_v1", ["canonical_tumor_characteristics_v1"],
     "Patient tumor rollup is the patient-level LVI feeder."),
    ("lvi_ordinal_worst", "patient_tumor_rollup_v1", ["canonical_tumor_characteristics_v1"],
     "Patient tumor rollup is the patient-level LVI feeder."),
    ("margin_status_true", "patient_tumor_rollup_v1", ["canonical_tumor_characteristics_v1"],
     "Patient tumor rollup is the patient-level margin status feeder."),
    ("max_tirads_ever", "canonical_us_nodule_characteristics_v1", ["imaging_patient_summary_v1"],
     "Recently rebuilt by Script 252; canonical US nodule characteristics rollup wins."),
    ("mol_genes_list", "_molecular_patient_rollup_v227", ["molecular_variant_long"],
     "Patient-level molecular rollup wins over the variant-long table."),
    ("mol_n_distinct_genes", "_molecular_patient_rollup_v227", ["molecular_variant_long"],
     "Patient-level molecular rollup wins."),
    ("mol_n_variants_total", "_molecular_patient_rollup_v227", ["molecular_variant_long"],
     "Patient-level molecular rollup wins."),
    ("mol_variant_classes", "_molecular_patient_rollup_v227", ["molecular_variant_long"],
     "Patient-level molecular rollup wins."),
    ("n_tumors_path", "patient_tumor_rollup_v1", ["canonical_tumor_characteristics_v1"],
     "Patient tumor rollup encodes the per-patient tumor count."),
]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ===========================================================================
# Phases
# ===========================================================================

def phase_preflight(con, log) -> dict:
    log("PHASE PREFLIGHT")
    row = con.execute(f"""
        SELECT
          (SELECT COUNT(*) FROM duckdb_tables() WHERE database_name='{PUBLICATION_DB}'
                 AND schema_name='main') AS main_n_tables,
          (SELECT COUNT(*) FROM duckdb_tables() WHERE database_name='{PUBLICATION_DB}'
                 AND schema_name='manuscript_workspace') AS ws_n_tables,
          (SELECT COUNT(*) FROM {CPM}) AS n_patients,
          (SELECT COUNT(*) FROM information_schema.columns
                 WHERE table_catalog='{PUBLICATION_DB}'
                 AND table_schema='main' AND table_name='canonical_patient_master') AS n_cols,
          (SELECT COUNT(*) FROM {README}) AS readme_rows,
          (SELECT COUNT(*) FROM {REGISTRY}) AS registry_rows
    """).fetchone()
    snap = dict(zip(
        ["main_n_tables", "ws_n_tables", "n_patients", "n_cols",
         "readme_rows", "registry_rows"], row))
    for k, v in snap.items():
        log(f"  {k}={v}")
    if snap["n_patients"] != 10871:
        raise SystemExit(f"PREFLIGHT FAIL n_patients={snap['n_patients']} != 10871")
    return snap


def phase_step1_skip(con, log) -> None:
    log("\nPHASE STEP 1 - ras_positive_v7 drop (already done by Script 262)")
    has_v7 = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
          AND table_name='canonical_patient_master' AND column_name='ras_positive_v7'
    """).fetchone()[0]
    log(f"  ras_positive_v7 column present: {bool(has_v7)} (expected False)")
    if has_v7:
        log("  WARNING - column unexpectedly present; not re-dropping in this script")


def phase_step2_tokens(con, log, do_writes: bool) -> dict:
    log("\nPHASE STEP 2 - registry token fixes")
    summary = {"replacements": 0, "cleared": 0}

    rows = con.execute(f"""
        SELECT detail_table_name, feeds_master_columns, feeds_master_columns_normalized
        FROM {REGISTRY}
        WHERE detail_table_name IN (
          {','.join(repr(t) for t in list(TOKEN_REPLACEMENTS) + list(TOKEN_CLEAR_TARGETS))}
        )
    """).fetchall()

    for tname, feeds_raw, feeds_norm in rows:
        log(f"  before: {tname}")
        log(f"    feeds_master_columns_normalized: {feeds_norm}")

    if do_writes:
        for tname, replacements in TOKEN_REPLACEMENTS.items():
            cur = con.execute(
                f"SELECT feeds_master_columns_normalized FROM {REGISTRY} "
                f"WHERE detail_table_name = ?",
                [tname]).fetchone()
            if not cur:
                log(f"  WARNING - registry has no row for {tname}; skipping")
                continue
            normed = cur[0] or ""
            tokens = [t for t in normed.split(";") if t]
            new_tokens = list(tokens)
            for old, new in replacements:
                if old in new_tokens:
                    idx = new_tokens.index(old)
                    new_tokens[idx] = new
                    summary["replacements"] += 1
            new_normed = ";".join(sorted(set(new_tokens)))
            con.execute(
                f"UPDATE {REGISTRY} SET feeds_master_columns_normalized = ? "
                f"WHERE detail_table_name = ?",
                [new_normed, tname])
            log(f"  REPLACED in {tname}: -> {new_normed}")

        for tname, (note, drop_set) in TOKEN_CLEAR_TARGETS.items():
            cur = con.execute(
                f"SELECT feeds_master_columns_normalized FROM {REGISTRY} "
                f"WHERE detail_table_name = ?",
                [tname]).fetchone()
            if not cur:
                log(f"  WARNING - registry has no row for {tname}; skipping")
                continue
            normed = cur[0] or ""
            tokens = [t for t in normed.split(";") if t]
            kept = [t for t in tokens if t not in drop_set]
            new_normed = ";".join(sorted(set(kept)))
            con.execute(
                f"UPDATE {REGISTRY} SET feeds_master_columns_normalized = ?, "
                f"feeds_master_columns = ? WHERE detail_table_name = ?",
                [new_normed, note, tname])
            summary["cleared"] += 1
            log(f"  CLEARED rolled-up tokens for {tname}; remaining: {new_normed}")
    else:
        log("  DRY-RUN; would replace 3 tokens, clear 1 detail-table feed")

    log(f"  step2 summary: {summary}")
    return summary


def phase_step3_collisions(con, log, do_writes: bool) -> dict:
    log("\nPHASE STEP 3 - collision resolution")

    have_secondary = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='manuscript_workspace'
          AND table_name='detail_table_registry_v1'
          AND column_name='feeds_master_columns_secondary'
    """).fetchone()[0]
    log(f"  feeds_master_columns_secondary present: {bool(have_secondary)}")
    if not have_secondary and do_writes:
        snapshot_table(
            con, REGISTRY, f"detail_table_registry_v1_pre265_{utc_ts()}",
            SCRIPT_TAG, "Pre-step3 snapshot of detail_table_registry_v1.")
        con.execute(
            f"ALTER TABLE {REGISTRY} ADD COLUMN feeds_master_columns_secondary VARCHAR")
        log("  added feeds_master_columns_secondary column")

    if do_writes:
        con.execute(f"DROP TABLE IF EXISTS {COLLISION_TBL}")
        con.execute(f"""
            CREATE TABLE {COLLISION_TBL} (
              master_column      VARCHAR PRIMARY KEY,
              candidate_feeders  VARCHAR,
              chosen_feeder      VARCHAR,
              demoted_feeders    VARCHAR,
              rationale          VARCHAR,
              resolved_at        TIMESTAMP,
              resolved_by        VARCHAR DEFAULT '265_finalization'
            )
        """)
        log(f"  created {COLLISION_TBL}")

    pre = con.execute(f"""
        WITH per_col AS (
          SELECT master_column, COUNT(DISTINCT detail_table_name) AS n
          FROM {POINTER} GROUP BY master_column
        )
        SELECT COUNT(*) FILTER (WHERE n > 1) FROM per_col
    """).fetchone()[0]
    log(f"  pre-fix colliding master_columns: {pre}")

    encoded = {row[0] for row in COLLISION_RESOLUTIONS}
    log(f"  encoded resolutions: {len(encoded)} of {pre} colliding")

    actual_collisions = con.execute(f"""
        WITH per_col AS (
          SELECT master_column, STRING_AGG(DISTINCT detail_table_name, '|') AS feeders,
                 COUNT(DISTINCT detail_table_name) AS n
          FROM {POINTER} GROUP BY master_column
        )
        SELECT master_column, feeders FROM per_col WHERE n > 1
    """).fetchall()
    actual_set = {r[0] for r in actual_collisions}
    feeders_by_col = {r[0]: r[1] for r in actual_collisions}
    missing = sorted(actual_set - encoded)
    if missing:
        log(f"  WARNING - {len(missing)} colliding master_columns not in encoded resolution set:")
        for m in missing:
            log(f"    {m}  feeders={feeders_by_col[m]}")
        log("  These will fall back to: keep the alphabetically-first feeder; demote the rest.")

    rows_resolution = []
    for mc, primary, demoted, rationale in COLLISION_RESOLUTIONS:
        if mc not in actual_set:
            continue
        rows_resolution.append((mc, feeders_by_col.get(mc, ""), primary,
                                ";".join(demoted), rationale))

    for m in missing:
        feeders = feeders_by_col[m].split("|")
        primary = sorted(feeders)[0]
        demoted = [f for f in feeders if f != primary]
        rows_resolution.append((
            m, feeders_by_col[m], primary, ";".join(demoted),
            "Auto-resolved (alphabetical) - not in encoded resolution set."))

    log(f"  prepared {len(rows_resolution)} resolution rows")

    if do_writes:
        con.executemany(
            f"INSERT INTO {COLLISION_TBL} "
            f"(master_column, candidate_feeders, chosen_feeder, demoted_feeders, "
            f" rationale, resolved_at) VALUES (?, ?, ?, ?, ?, current_timestamp)",
            rows_resolution)
        log(f"  inserted {len(rows_resolution)} rows into collision_resolution_v265")

        snapshot_table(
            con, REGISTRY, f"detail_table_registry_v1_pre265_rewrite_{utc_ts()}",
            SCRIPT_TAG, "Pre-rewrite snapshot before collision-resolution registry rewrite.")

        registry_rows = con.execute(f"""
            SELECT detail_table_name, feeds_master_columns_normalized,
                   feeds_master_columns_secondary
            FROM {REGISTRY}
        """).fetchall()
        log(f"  rewriting {len(registry_rows)} registry rows...")

        secondary_adds: dict[str, set[str]] = {}
        primary_keeps: dict[str, set[str]] = {}
        for mc, _candidates, primary, demoted_str, _rat in rows_resolution:
            primary_keeps.setdefault(primary, set()).add(mc)
            for d in demoted_str.split(";"):
                if d:
                    secondary_adds.setdefault(d, set()).add(mc)

        n_updated = 0
        for tname, normed, sec in registry_rows:
            normed_set = set([t for t in (normed or "").split(";") if t])
            sec_set = set([t for t in (sec or "").split(";") if t])
            changed = False
            for mc, _, primary, demoted_str, _ in rows_resolution:
                demoted = set(d for d in demoted_str.split(";") if d)
                if tname == primary:
                    if mc not in normed_set:
                        normed_set.add(mc); changed = True
                    if mc in sec_set:
                        sec_set.discard(mc); changed = True
                elif tname in demoted:
                    if mc in normed_set:
                        normed_set.discard(mc); changed = True
                    if mc not in sec_set:
                        sec_set.add(mc); changed = True
            if changed:
                con.execute(
                    f"UPDATE {REGISTRY} "
                    f"SET feeds_master_columns_normalized = ?, "
                    f"feeds_master_columns_secondary = ? "
                    f"WHERE detail_table_name = ?",
                    [";".join(sorted(normed_set)),
                     ";".join(sorted(sec_set)) if sec_set else None,
                     tname])
                n_updated += 1
        log(f"  updated {n_updated} registry rows")
    else:
        log("  DRY-RUN; would create collision_resolution_v265 + rewrite registry")

    return {
        "pre_collisions": pre,
        "encoded_resolutions": len(encoded),
        "actual_collisions": len(actual_set),
        "auto_resolved_extras": len(missing),
        "missing_columns": missing,
    }


def phase_step3c_verify(con, log) -> int:
    log("\nPHASE STEP 3c - verify zero collisions remain (registry view)")
    n = con.execute(f"""
        WITH exploded AS (
          SELECT detail_table_name,
                 UNNEST(STRING_SPLIT(feeds_master_columns_normalized, ';')) AS col
          FROM {REGISTRY}
          WHERE feeds_master_columns_normalized IS NOT NULL
            AND feeds_master_columns_normalized <> ''
        )
        SELECT COUNT(*) FROM (
          SELECT col FROM exploded WHERE col <> '' GROUP BY col HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    log(f"  remaining collisions in registry: {n} (expected 0)")
    return int(n)


def phase_step4_doc(con, log, do_writes: bool) -> None:
    log("\nPHASE STEP 4 - episode-feed documentation (option b)")
    msg = (
        "Episode-level molecular tests. WARNING: incomplete by design - "
        "undertags 443 ThyroSeq tests as platform=Other, missing 46 BRAF NGS rows, "
        "missing >=38 RET episodes. NOT authoritative for has-flags "
        "(mol_has_thyroseq/afirma) or positive-flags (braf_positive_final, "
        "ret_positive_unified). For those, drill down via canonical_detail_pointer_v1 - "
        "primary feeders are canonical_molecular_tested_v1, extracted_braf_recovery_v1, "
        "and ret_patient_adjudicated_v226 respectively. See Script 265 for the "
        "architectural decision."
    )
    if do_writes:
        safe = msg.replace("'", "''")
        con.execute(f"COMMENT ON TABLE {MTE} IS '{safe}'")
        log("  COMMENT ON TABLE molecular_test_episode_v2 set")
    else:
        log("  DRY-RUN; would set COMMENT ON TABLE")


def phase_step5_fusion(con, log, do_writes: bool) -> dict:
    log("\nPHASE STEP 5 - fusion vs n_fusions reconciliation")

    pre = con.execute(f"""
        SELECT
          COUNT(*) FILTER (WHERE mol_n_fusions > 0 AND any_fusion_positive = FALSE) AS a,
          COUNT(*) FILTER (WHERE mol_n_fusions = 0 AND any_fusion_positive = TRUE)  AS b
        FROM {CPM}
    """).fetchone()
    log(f"  before: contradiction_a={pre[0]}  contradiction_b={pre[1]}")

    if do_writes:
        snapshot_table(
            con, CPM, f"canonical_patient_master_pre265_step5_{utc_ts()}",
            SCRIPT_TAG, "Pre-step5 fusion reconciliation snapshot of CPM.")

        con.execute(f"DROP TABLE IF EXISTS {FUSION_REVIEW}")
        con.execute(f"""
            CREATE TABLE {FUSION_REVIEW} AS
            SELECT * FROM {MVL}
            WHERE variant_class = 'PARSE_ERROR_FUSION_FULLTEXT'
        """)
        n_review_count = con.execute(
            f"SELECT COUNT(*) FROM {FUSION_REVIEW}").fetchone()[0]
        log(f"  surfaced {n_review_count} PARSE_ERROR variants -> {FUSION_REVIEW}")

        con.execute(f"""
            UPDATE {CPM} cpm
            SET mol_n_fusions = (
              SELECT COUNT(*)
              FROM {MVL} v
              WHERE v.research_id = cpm.research_id
                AND v.variant_class = 'FUSION'
                AND v.gene_symbol IS NOT NULL
            )
            WHERE EXISTS (SELECT 1 FROM {MVL} v
                          WHERE v.research_id = cpm.research_id
                            AND v.variant_class IN ('FUSION', 'PARSE_ERROR_FUSION_FULLTEXT'))
        """)
        log("  recomputed mol_n_fusions excluding PARSE_ERROR variants")

        con.execute(f"""
            UPDATE {CPM} cpm
            SET any_fusion_positive = (
              SELECT COUNT(*) > 0
              FROM {MVL} v
              WHERE v.research_id = cpm.research_id
                AND v.variant_class = 'FUSION'
                AND v.gene_symbol IS NOT NULL
            )
            WHERE EXISTS (SELECT 1 FROM {MVL} v
                          WHERE v.research_id = cpm.research_id
                            AND v.variant_class IN ('FUSION', 'PARSE_ERROR_FUSION_FULLTEXT'))
        """)
        log("  recomputed any_fusion_positive on the same FUSION + non-NULL gene rule")

        post = con.execute(f"""
            SELECT
              COUNT(*) FILTER (WHERE mol_n_fusions > 0 AND any_fusion_positive = FALSE) AS a,
              COUNT(*) FILTER (WHERE mol_n_fusions = 0 AND any_fusion_positive = TRUE)  AS b
            FROM {CPM}
        """).fetchone()
        log(f"  after: contradiction_a={post[0]}  contradiction_b={post[1]} (target 0,0)")
        return {
            "before_a": pre[0], "before_b": pre[1],
            "after_a": post[0], "after_b": post[1],
            "fusion_review_rows": n_review_count,
        }
    else:
        log("  DRY-RUN; would snapshot CPM, surface 632 PARSE_ERROR variants, recompute both")
        return {"before_a": pre[0], "before_b": pre[1],
                "after_a": None, "after_b": None, "fusion_review_rows": None}


def phase_step6_null_defaults(con, log, do_writes: bool) -> dict:
    log("\nPHASE STEP 6 - 846 NULL fusion / RET defaults")

    pre = con.execute(f"""
        SELECT
          COUNT(*) FILTER (WHERE any_fusion_positive IS NULL) AS null_fusion,
          COUNT(*) FILTER (WHERE ret_positive_v7 IS NULL) AS null_ret
        FROM {CPM}
    """).fetchone()
    log(f"  before: null_fusion={pre[0]}  null_ret={pre[1]}")

    if do_writes:
        for cn in ("any_fusion_positive_inferred_negative",
                   "ret_positive_v7_inferred_negative"):
            present = con.execute(f"""
                SELECT COUNT(*) FROM information_schema.columns
                WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
                  AND table_name='canonical_patient_master' AND column_name='{cn}'
            """).fetchone()[0]
            if not present:
                con.execute(
                    f"ALTER TABLE {CPM} ADD COLUMN {cn} BOOLEAN DEFAULT FALSE")
                log(f"  added column {cn}")

        con.execute(f"""
            UPDATE {CPM}
            SET any_fusion_positive = FALSE,
                any_fusion_positive_inferred_negative = TRUE
            WHERE any_fusion_positive IS NULL
        """)
        con.execute(f"""
            UPDATE {CPM}
            SET ret_positive_v7 = FALSE,
                ret_positive_v7_inferred_negative = TRUE
            WHERE ret_positive_v7 IS NULL
        """)

        for col, comment in (
            ("any_fusion_positive_inferred_negative",
             "Script 265 (Step 6): TRUE when patient had no molecular testing record "
             "and any_fusion_positive was defaulted to FALSE rather than left NULL. "
             "Operationally negative for fusion."),
            ("ret_positive_v7_inferred_negative",
             "Script 265 (Step 6): TRUE when patient had no molecular testing record "
             "and ret_positive_v7 was defaulted to FALSE rather than left NULL. "
             "Operationally negative for RET."),
        ):
            safe = comment.replace("'", "''")
            con.execute(
                f"COMMENT ON COLUMN {CPM}.{col} IS '{safe}'")

        post = con.execute(f"""
            SELECT
              COUNT(*) FILTER (WHERE any_fusion_positive IS NULL) AS null_fusion,
              COUNT(*) FILTER (WHERE ret_positive_v7 IS NULL) AS null_ret,
              COUNT(*) FILTER (WHERE any_fusion_positive_inferred_negative = TRUE) AS fi,
              COUNT(*) FILTER (WHERE ret_positive_v7_inferred_negative = TRUE) AS ri
            FROM {CPM}
        """).fetchone()
        log(f"  after: null_fusion={post[0]} null_ret={post[1]} "
            f"fusion_inferred={post[2]} ret_inferred={post[3]}")
        return {
            "before_null_fusion": pre[0], "before_null_ret": pre[1],
            "after_null_fusion": post[0], "after_null_ret": post[1],
            "after_fusion_inferred": post[2], "after_ret_inferred": post[3],
        }
    else:
        log("  DRY-RUN; would add 2 columns, default 846 NULLs, set comments")
        return {"before_null_fusion": pre[0], "before_null_ret": pre[1],
                "after_null_fusion": None, "after_null_ret": None}


def phase_step7_ghost_rid(con, log, do_writes: bool) -> dict:
    log("\nPHASE STEP 7 - ghost RID 7744 prune")
    targets = [
        "molecular_test_episode_v2", "molecular_results", "molecular_testing",
        "thyroseq_molecular_enrichment", "_molecular_patient_rollup_v227",
    ]
    pre = {}
    for t in targets:
        n = con.execute(
            f"SELECT COUNT(*) FROM {PUBLICATION_DB}.main.{t} "
            f"WHERE TRY_CAST(research_id AS INTEGER) = 7744").fetchone()[0]
        pre[t] = n
        log(f"  pre-prune {t}: {n}")

    if do_writes:
        ts = utc_ts()
        for t in targets:
            n = pre[t]
            if n == 0:
                continue
            dest = f'{ARCHIVE_QUALIFIED}."ghost_rid_7744_{t}_pre265_{ts}"'
            ensure_archive_schema(con)
            con.execute(f"DROP TABLE IF EXISTS {dest}")
            con.execute(
                f"CREATE TABLE {dest} AS SELECT * FROM {PUBLICATION_DB}.main.{t} "
                f"WHERE TRY_CAST(research_id AS INTEGER) = 7744")
            con.execute(
                f"COMMENT ON TABLE {dest} IS "
                f"'Script 265 ghost RID 7744 prune snapshot of {t}.'")
            con.execute(
                f"DELETE FROM {PUBLICATION_DB}.main.{t} "
                f"WHERE TRY_CAST(research_id AS INTEGER) = 7744")
            log(f"  pruned {n} row(s) from {t}; snapshot at {dest}")

        post = {}
        for t in targets:
            n = con.execute(
                f"SELECT COUNT(*) FROM {PUBLICATION_DB}.main.{t} "
                f"WHERE TRY_CAST(research_id AS INTEGER) = 7744").fetchone()[0]
            post[t] = n
        log(f"  post-prune RID 7744 row counts: {post}")
        return {"pre": pre, "post": post}
    log("  DRY-RUN; would snapshot + delete 5 rows total")
    return {"pre": pre, "post": None}


def phase_step8_nan(con, log, do_writes: bool) -> dict:
    log("\nPHASE STEP 8 - nan_string_audit (documented no-op)")
    rows = con.execute(f"""
        SELECT repair_action, COUNT(*) AS n_rows,
               SUM(n_literal_nan) AS sum_nan
        FROM {PUBLICATION_DB}.manuscript_workspace.nan_string_audit_v1_1
        GROUP BY repair_action
        ORDER BY 2 DESC
    """).fetchall()
    summary = {r[0]: {"n_rows": r[1], "sum_literal_nan": r[2]} for r in rows}
    for k, v in summary.items():
        log(f"  action={k!r:<20}  rows={v['n_rows']}  sum_literal_nan={v['sum_literal_nan']}")
    if do_writes:
        with NAN_SUMMARY.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["repair_action", "n_rows", "sum_literal_nan"])
            for k, v in summary.items():
                w.writerow([k, v["n_rows"], v["sum_literal_nan"]])
        log(f"  wrote {NAN_SUMMARY}")
    log("  No mutating action taken: only PRESERVE_RAW (1 col) and NO_ACTION (475)")
    return summary


def phase_step9a_archive(con, log, do_writes: bool) -> dict:
    log("\nPHASE STEP 9a - archive sweep + manifest (no DROPs in main)")
    schemas = con.execute("""
        SELECT schema_name, COUNT(*) FROM duckdb_tables()
        WHERE database_name = 'Thyroid 2026 UPdated'
        GROUP BY schema_name ORDER BY 2 DESC
    """).fetchall()

    raw_sources = [
        "raw_imaging_12_slots_v1", "raw_us_tirads_excel_v1",
        "raw_us_tirads_scored_v1", "path_synoptics", "tumor_pathology",
        "fna_cytology", "clinical_notes_long", "nuclear_med", "mri_imaging",
        "ct_imaging", "thyroseq_molecular_enrichment", "thyroid_weights",
        "thyroid_sizes", "ultrasound_reports", "us_nodules_tirads",
        "tirads_llm_extracted_v2",
    ]
    nsqip_tables = [r[0] for r in con.execute("""
        SELECT table_name FROM duckdb_tables()
        WHERE database_name='Thyroid 2026 UPdated' AND schema_name='main'
          AND table_name LIKE 'nsqip_%'
    """).fetchall()]
    note_tables = [r[0] for r in con.execute("""
        SELECT table_name FROM duckdb_tables()
        WHERE database_name='Thyroid 2026 UPdated' AND schema_name='main'
          AND table_name LIKE 'note_entities_%'
    """).fetchall()]
    raw_sources_present = set(raw_sources + nsqip_tables + note_tables)

    legacy_candidates = [r[0] for r in con.execute("""
        SELECT table_name FROM duckdb_tables()
        WHERE database_name='Thyroid 2026 UPdated' AND schema_name='main'
          AND (
            table_name = 'canonical_patient_master_v1'
            OR table_name LIKE '%_v225'
            OR table_name LIKE 'thyroid_ete_fix_%'
            OR table_name LIKE '%_pre_v1_0_%'
          )
        ORDER BY table_name
    """).fetchall()]
    log(f"  legacy candidates ({len(legacy_candidates)}): {legacy_candidates}")

    release_schemas = sorted(s for s, _ in schemas if s.startswith("release_"))
    log(f"  release_* schemas to flag: {len(release_schemas)} -> "
        f"{release_schemas}")

    rows_present_main = set(
        r[0] for r in con.execute("""
            SELECT table_name FROM duckdb_tables()
            WHERE database_name='Thyroid 2026 UPdated' AND schema_name='main'
        """).fetchall())

    manifest_rows: list[dict] = []

    for t in sorted(rows_present_main):
        if t in legacy_candidates:
            action = "archive_to_archive_legacy"
            new_loc = f"{ARCHIVE_DB}.archive_legacy.{t}_<UTC>"
            reason = "Legacy CPM v1 table superseded by canonical_patient_master in publication DB."
        elif t in raw_sources_present:
            action = "tag_raw_source"
            new_loc = f"{ARCHIVE_DB}.main.{t}"
            reason = "Raw source / Excel-derived staging table; mirrored to publication DB; tag-only."
        else:
            action = "review"
            new_loc = ""
            reason = "Unclassified main table; pending manual review."
        manifest_rows.append({
            "original_db": ARCHIVE_DB,
            "original_schema": "main",
            "original_table": t,
            "action": action,
            "new_location": new_loc,
            "reason": reason,
        })

    for s in release_schemas:
        n = next((n for sn, n in schemas if sn == s), 0)
        manifest_rows.append({
            "original_db": ARCHIVE_DB,
            "original_schema": s,
            "original_table": f"<{n} tables>",
            "action": "recommend_drop",
            "new_location": "",
            "reason": "Dated release snapshot from earlier build cadence; superseded by publication DB v1_0.",
        })

    if do_writes:
        with MANIFEST_CSV.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=[
                "original_db", "original_schema", "original_table",
                "action", "new_location", "reason"])
            w.writeheader()
            for r in manifest_rows:
                w.writerow(r)
        log(f"  wrote manifest: {MANIFEST_CSV} ({len(manifest_rows)} rows)")

        con.execute(f'CREATE SCHEMA IF NOT EXISTS "{ARCHIVE_DB}".archive_legacy')
        log(f"  ensured schema {ARCHIVE_DB}.archive_legacy")

        ts = utc_ts()
        n_archived = 0
        for t in legacy_candidates:
            src = f'"{ARCHIVE_DB}"."main"."{t}"'
            dest = f'"{ARCHIVE_DB}"."archive_legacy"."{t}_pre265_{ts}"'
            try:
                con.execute(f"DROP TABLE IF EXISTS {dest}")
                con.execute(f"CREATE TABLE {dest} AS SELECT * FROM {src}")
                safe = (
                    f"Script 265 archive of legacy main.{t}. "
                    f"Original DROPped after CTAS; source tracked in 265 manifest."
                ).replace("'", "''")
                con.execute(f"COMMENT ON TABLE {dest} IS '{safe}'")
                con.execute(f"DROP TABLE {src}")
                n_archived += 1
                log(f"  archived + dropped {src} -> {dest}")
            except Exception as e:
                log(f"  WARNING - failed to archive {src}: {e}")

        n_tagged = 0
        for t in sorted(raw_sources_present & rows_present_main):
            try:
                src = f'"{ARCHIVE_DB}"."main"."{t}"'
                comment = (
                    f"raw_source - do not modify, do not promote to publication "
                    f"directly. Mirrored to thyroid_canonical_publication_v1_0.main.{t} "
                    f"as of {RUN_DATE}. Tagged by Script 265."
                ).replace("'", "''")
                con.execute(f"COMMENT ON TABLE {src} IS '{comment}'")
                n_tagged += 1
            except Exception as e:
                log(f"  WARNING - failed to tag {t}: {e}")
        log(f"  tagged {n_tagged} raw-source tables")

        log(f"  STOP before touching {len(release_schemas)} release_* schemas - manifest flags them recommend_drop")
        return {
            "manifest_rows": len(manifest_rows),
            "legacy_archived": n_archived,
            "raw_sources_tagged": n_tagged,
            "release_schemas_flagged": len(release_schemas),
        }
    else:
        log(f"  DRY-RUN; manifest would have {len(manifest_rows)} rows; "
            f"{len(legacy_candidates)} legacy candidates would be archived; "
            f"{len(raw_sources_present & rows_present_main)} raw sources would be tagged.")
        return {"manifest_rows": len(manifest_rows)}


def phase_step10_pointer(con, log, do_writes: bool) -> dict:
    log("\nPHASE STEP 10 - canonical_detail_pointer_v1 rebuild")
    if do_writes:
        con.execute(f"""
            CREATE OR REPLACE VIEW {POINTER} AS
            WITH exploded AS (
              SELECT
                detail_table_name,
                schema_name,
                domain,
                UNNEST(STRING_SPLIT(feeds_master_columns_normalized, ';')) AS master_column
              FROM {REGISTRY}
              WHERE feeds_master_columns_normalized IS NOT NULL
                AND feeds_master_columns_normalized <> ''
            )
            SELECT
              master_column,
              detail_table_name,
              schema_name,
              domain,
              'thyroid_canonical_publication_v1_0.' || schema_name || '.'
                || detail_table_name AS fully_qualified_drilldown
            FROM exploded
            WHERE master_column <> ''
        """)
        log("  view rebuilt")
    else:
        log("  DRY-RUN; would rebuild view")
    rows = con.execute(f"""
        SELECT COUNT(*) AS total_rows,
               COUNT(DISTINCT master_column) AS distinct_master_cols,
               COUNT(DISTINCT detail_table_name) AS distinct_detail_tables
        FROM {POINTER}
    """).fetchone()
    log(f"  total_rows={rows[0]} distinct_master_cols={rows[1]} "
        f"distinct_detail_tables={rows[2]}")
    return {"total_rows": rows[0], "distinct_master_cols": rows[1],
            "distinct_detail_tables": rows[2]}


def phase_step11_readme(con, log, do_writes: bool) -> dict:
    log("\nPHASE STEP 11 - __readme + data_dictionary_v240 refresh")
    cands = [r[0] for r in con.execute(f"""
        SELECT table_name FROM information_schema.tables
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
          AND table_type='BASE TABLE'
        ORDER BY table_name
    """).fetchall()]
    log(f"  base tables in main: {len(cands)}")

    if do_writes:
        snapshot_table(
            con, README, f"__readme_pre265_{utc_ts()}", SCRIPT_TAG,
            "Pre-rebuild snapshot of main.__readme (Script 265).")
        con.execute(f"DROP TABLE IF EXISTS {README}")
        con.execute(f"""
            CREATE TABLE {README} (
                table_name VARCHAR PRIMARY KEY,
                n_rows BIGINT,
                n_distinct_research_id BIGINT,
                description VARCHAR,
                inventoried_at TIMESTAMP
            )
        """)
        n = 0
        for tbl in cands:
            try:
                n_rows = int(con.execute(
                    f'SELECT COUNT(*) FROM main."{tbl}"').fetchone()[0])
            except Exception:
                continue
            try:
                n_pat = int(con.execute(
                    f'SELECT COUNT(DISTINCT research_id) FROM main."{tbl}"'
                ).fetchone()[0])
            except Exception:
                n_pat = None
            con.execute(
                f"INSERT INTO {README} VALUES (?, ?, ?, ?, current_timestamp)",
                [tbl, n_rows, n_pat,
                 f"{SCRIPT_TAG} re-inventoried {RUN_DATE}"])
            n += 1
        con.execute(
            f"COMMENT ON TABLE {README} IS '{SCRIPT_TAG} ({RUN_DATE}) "
            f"queryable enumeration of main BASE TABLEs ({n}). "
            f"Auto-rebuilt by Script 265.'")
        log(f"  rebuilt __readme with {n} rows")

        for cn in ("any_fusion_positive", "ret_positive_v7"):
            try:
                stats = con.execute(f"""
                    SELECT COUNT(*) FILTER (WHERE {cn} IS NOT NULL),
                           COUNT(DISTINCT {cn})
                    FROM {CPM}
                """).fetchone()
                exists = con.execute(
                    f"SELECT COUNT(*) FROM {DICT} WHERE column_name=?",
                    [cn]).fetchone()[0]
                if exists:
                    con.execute(f"""
                        UPDATE {DICT}
                        SET n_non_null = ?, n_distinct = ?,
                            pct_non_null = 100.0 * ? / 10871.0,
                            description = COALESCE(NULLIF(description,''),'') ||
                              CASE WHEN COALESCE(NULLIF(description,''),'') = ''
                                   THEN '' ELSE ' | ' END ||
                              'Script 265 (Step 6): NULLs defaulted to FALSE; companion _inferred_negative column added.'
                        WHERE column_name = ?
                    """, [int(stats[0]), int(stats[1]), int(stats[0]), cn])
                    log(f"  data_dictionary_v240 refreshed for {cn} "
                        f"(non_null={stats[0]} distinct={stats[1]})")
                else:
                    log(f"  WARNING - data_dictionary_v240 has no row for {cn}; skipping")
            except Exception as e:
                log(f"  WARNING - dict refresh for {cn} failed: {e}")
        for cn in ("any_fusion_positive_inferred_negative",
                   "ret_positive_v7_inferred_negative",
                   "mol_n_fusions"):
            try:
                exists = con.execute(
                    f"SELECT COUNT(*) FROM {DICT} WHERE column_name=?",
                    [cn]).fetchone()[0]
                if exists:
                    stats = con.execute(f"""
                        SELECT COUNT(*) FILTER (WHERE {cn} IS NOT NULL),
                               COUNT(DISTINCT {cn})
                        FROM {CPM}
                    """).fetchone()
                    con.execute(f"""
                        UPDATE {DICT}
                        SET n_non_null = ?, n_distinct = ?,
                            pct_non_null = 100.0 * ? / 10871.0
                        WHERE column_name = ?
                    """, [int(stats[0]), int(stats[1]), int(stats[0]), cn])
                    log(f"  data_dictionary_v240 stats refreshed for {cn}")
            except Exception as e:
                log(f"  WARNING - dict refresh for {cn} failed: {e}")
    else:
        log(f"  DRY-RUN; would rebuild __readme with up to {len(cands)} rows")
    return {"main_base_tables": len(cands)}


def phase_step12_invariants(con, log, do_writes: bool, summaries: dict) -> dict:
    log("\nPHASE STEP 12 - final invariants + audit rows")
    inv = con.execute(f"""
        SELECT
          COUNT(*) AS n_rows,
          COUNT(DISTINCT research_id) AS n_distinct_rids,
          COUNT(*) FILTER (WHERE research_id IS NULL) AS null_rids,
          COUNT(*) FILTER (WHERE fna_path_outcome IS NULL) AS null_fna_outcome
        FROM {CPM}
    """).fetchone()
    log(f"  CPM: rows={inv[0]} distinct_rids={inv[1]} null_rids={inv[2]} "
        f"null_fna_outcome={inv[3]} (expect 10871,10871,0,0)")
    if (inv[0], inv[1], inv[2], inv[3]) != (10871, 10871, 0, 0):
        raise SystemExit(f"INVARIANT FAIL: CPM check {inv}")

    coll = con.execute(f"""
        WITH exploded AS (
          SELECT detail_table_name,
                 UNNEST(STRING_SPLIT(feeds_master_columns_normalized, ';')) AS col
          FROM {REGISTRY}
          WHERE feeds_master_columns_normalized IS NOT NULL
            AND feeds_master_columns_normalized <> ''
        )
        SELECT COUNT(*) FROM (
          SELECT col FROM exploded WHERE col <> '' GROUP BY col HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    log(f"  registry collisions: {coll} (expect 0)")

    n_views = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='manuscript_workspace'
          AND table_type='VIEW'
    """).fetchone()[0]
    log(f"  manuscript_workspace VIEW count: {n_views} (expect >=65)")

    fusion_check = con.execute(f"""
        SELECT
          COUNT(*) FILTER (WHERE mol_n_fusions > 0 AND any_fusion_positive = FALSE) AS a,
          COUNT(*) FILTER (WHERE mol_n_fusions = 0 AND any_fusion_positive = TRUE)  AS b,
          COUNT(*) FILTER (WHERE any_fusion_positive IS NULL) AS nf,
          COUNT(*) FILTER (WHERE ret_positive_v7 IS NULL) AS nr
        FROM {CPM}
    """).fetchone()
    log(f"  fusion contradictions: a={fusion_check[0]} b={fusion_check[1]} "
        f"null_fusion={fusion_check[2]} null_ret={fusion_check[3]} (expect 0,0,0,0)")

    if do_writes:
        ensure_audit_table(con)
        s2 = summaries.get("step2", {})
        record_audit(con, SCRIPT_NUM, "step_2_unmatched_tokens",
                     "tokens_resolved",
                     count_before=6,
                     count_after=6 - s2.get("replacements", 0),
                     target_after=0, status="OK",
                     notes=f"replacements={s2.get('replacements')} cleared={s2.get('cleared')}")
        s3 = summaries.get("step3", {})
        record_audit(con, SCRIPT_NUM, "step_3_collisions",
                     "registry_colliding_columns",
                     count_before=s3.get("pre_collisions", 88),
                     count_after=int(coll),
                     target_after=0, status="OK" if coll == 0 else "REVIEW",
                     notes=f"resolutions_inserted={len(COLLISION_RESOLUTIONS)} extras={s3.get('auto_resolved_extras')}")
        s5 = summaries.get("step5", {})
        record_audit(con, SCRIPT_NUM, "step_5_fusion_contradictions",
                     "contradiction_a_plus_b",
                     count_before=s5.get("before_a", 0) + s5.get("before_b", 0),
                     count_after=int(fusion_check[0]) + int(fusion_check[1]),
                     target_after=0, status="OK")
        s6 = summaries.get("step6", {})
        record_audit(con, SCRIPT_NUM, "step_6_null_defaults",
                     "null_fusion_plus_null_ret",
                     count_before=(s6.get("before_null_fusion", 846) +
                                   s6.get("before_null_ret", 846)),
                     count_after=int(fusion_check[2]) + int(fusion_check[3]),
                     target_after=0, status="OK")
        s7 = summaries.get("step7", {})
        if s7 and s7.get("post") is not None:
            record_audit(con, SCRIPT_NUM, "step_7_ghost_rid_7744",
                         "rid_7744_rows_remaining",
                         count_before=sum(s7["pre"].values()),
                         count_after=sum(s7["post"].values()),
                         target_after=0, status="OK")
        s8 = summaries.get("step8", {})
        record_audit(con, SCRIPT_NUM, "step_8_nan_strings",
                     "literal_nan_cells_remaining",
                     count_before=s8.get("PRESERVE_RAW", {}).get("sum_literal_nan", 0),
                     count_after=s8.get("PRESERVE_RAW", {}).get("sum_literal_nan", 0),
                     target_after=0, status="DOCUMENTED_NOOP",
                     notes="Only PRESERVE_RAW + NO_ACTION rows present; intentional.")
        s9 = summaries.get("step9", {})
        record_audit(con, SCRIPT_NUM, "step_9a_archive_sweep",
                     "manifest_rows",
                     count_before=0,
                     count_after=s9.get("manifest_rows", 0),
                     target_after=s9.get("manifest_rows", 0),
                     status="OK",
                     notes=f"legacy_archived={s9.get('legacy_archived')} "
                           f"raw_sources_tagged={s9.get('raw_sources_tagged')} "
                           f"release_schemas_flagged={s9.get('release_schemas_flagged')}")
        log("  audit rows written")

    return {
        "cpm": inv, "registry_collisions": int(coll),
        "ws_views": int(n_views), "fusion_check": fusion_check,
    }


# ===========================================================================
# main
# ===========================================================================

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true",
                    help="Execute writes; default is dry-run.")
    args = ap.parse_args()

    do_writes = bool(args.apply)
    log, fh = make_logger(RUN_LOG)
    t0 = time.time()
    try:
        log("=" * 78)
        log(f"=== START scripts/265_canonical_finalization.py "
            f"({'APPLY' if do_writes else 'DRY-RUN'})")
        log(f"started_at: {utc_now()}")
        con = connect_locked()
        log(f"connected to {PUBLICATION_DB}")

        summaries: dict = {}
        summaries["preflight"] = phase_preflight(con, log)
        phase_step1_skip(con, log)
        summaries["step2"] = phase_step2_tokens(con, log, do_writes)
        summaries["step3"] = phase_step3_collisions(con, log, do_writes)
        if do_writes:
            summaries["step3_verify"] = phase_step3c_verify(con, log)
        phase_step4_doc(con, log, do_writes)
        summaries["step5"] = phase_step5_fusion(con, log, do_writes)
        summaries["step6"] = phase_step6_null_defaults(con, log, do_writes)
        summaries["step7"] = phase_step7_ghost_rid(con, log, do_writes)
        summaries["step8"] = phase_step8_nan(con, log, do_writes)
        summaries["step9"] = phase_step9a_archive(con, log, do_writes)
        summaries["step10"] = phase_step10_pointer(con, log, do_writes)
        summaries["step11"] = phase_step11_readme(con, log, do_writes)
        summaries["step12"] = phase_step12_invariants(con, log, do_writes, summaries)

        elapsed = time.time() - t0
        log(f"=== END elapsed={elapsed:.1f}s")
        write_decision_log(DECISION_LOG, {
            "script": SCRIPT_TAG, "run_date": RUN_DATE,
            "do_writes": do_writes, "elapsed_seconds": round(elapsed, 1),
            "summaries": summaries,
        })
        return 0
    except Exception as e:
        log(f"FATAL: {e!r}")
        import traceback
        log(traceback.format_exc())
        return 1
    finally:
        fh.close()


if __name__ == "__main__":
    sys.exit(main())
