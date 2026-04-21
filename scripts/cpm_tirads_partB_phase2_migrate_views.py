#!/usr/bin/env python3
"""
Part B / Phase 2: Migrate 9 cohort views off CPM TIRADS cols onto cupm_v2.

Strategy: source-replacement, schema-preservation.
- Each rewritten view JOINs main.canonical_us_patient_master_VIEW_v2 (cupm_v2) and
  sources values from cupm_v2 columns, but keeps the legacy CPM column NAMES
  as aliases on the cohort view's SELECT list. This lets downstream consumers
  (notebooks, manuscript scripts) keep working without rename churn.
- Retired metrics (tirads_concordant_count_v12, tirads_mismatch_count_v12,
  tirads_n_sources_v12, tirads_reliability_v12) are DROPPED from m025/m075
  with a view-level comment per Logan's Q1 directive.

Migration order:
  1) cohort_descriptive_full_cohort_v1   (BASE — m050, m053, m064, m076 inherit)
  2) cohort_m011_tirads_fna_genetics_v1
  3) cohort_m025_tirads_performance_v1   (drops 2 retired cols)
  4) cohort_m045_multimodal_risk_v1
  5) cohort_m075_tirads_multi_nodule_v1  (drops 2 retired cols)
  6-9) m050, m053, m064, m076            (auto-inherit; verify only)

Per-view shape verification: row count + distinct RID count before vs after.

Archive: every view's pre-rewrite definition is snapshotted to
"Thyroid 2026 UPdated".cpm_tirads_legacy_20260421.view_def_<name>.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from motherduck_client import MotherDuckClient, MotherDuckConfig  # noqa: E402

DB = "thyroid_canonical_publication_v1_0"
ARCHIVE_DB = '"Thyroid 2026 UPdated"'
ARCHIVE_SCHEMA = "cpm_tirads_legacy_20260421"
OUT = REPO / "scripts" / "output"


def utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def connect():
    return MotherDuckClient(MotherDuckConfig(database=DB)).connect_rw()


# ─────────────────────────────────────────────────────────────────────────────
# View bodies (post-rewrite). Each is the FULL CREATE OR REPLACE VIEW SQL.
# Column-name preservation: where the legacy CPM column name still appears in
# the SELECT list, it's aliased from a cupm_v2 source.
# ─────────────────────────────────────────────────────────────────────────────

VIEW_BODIES: dict[str, str] = {}

VIEW_BODIES["cohort_descriptive_full_cohort_v1"] = """
CREATE OR REPLACE VIEW manuscript_workspace.cohort_descriptive_full_cohort_v1 AS
-- Migrated 2026-04-21: TIRADS columns now sourced from canonical_us_patient_master_VIEW_v2.
-- Legacy column names preserved as aliases for downstream consumer compatibility.
-- Source-of-truth shift: tirads_best_category_v12, tirads_worst_category_v12,
-- tirads_best_score_v12, tirads_nodule_size_max_mm_v12 now reflect cupm_v2
-- derivations (TR rank "TR1"-"TR5", not legacy "TR4_Moderately_Suspicious"
-- vocabulary; per-record nodule size max via GREATEST/size_cm_max fallback).
-- See: CPM TIRADS Part B (2026-04-21).
SELECT
    p.research_id, p.age_at_surgery, p.sex, p.race, p.bmi_combined,
    p.surg_procedure_type, p.surg_hemithyroidectomy, p.surg_total_thyroidectomy,
    p.first_surgery_date, p.n_surgeries, p.op_reoperative_any,
    p.histology_final, p.diagnosis_primary, p.diagnosis_variant, p.is_malignant,
    p.multifocal_flag_path, p.n_tumors,
    p.path_tumor_size_cm AS tumor_size_cm, p.path_tumor_size_cm,
    p.laterality, p.bilateral_disease_flag,
    p.ajcc8_stage_group, p.ajcc8_t_stage, p.ajcc8_n_stage, p.ajcc8_m_stage,
    p.ata_risk_category, p.ata_initial_risk, p.ata_response_category,
    p.macis_score, p.macis_risk_group, p.ages_score, p.ames_risk,
    p.ln_positive_flag, p.ln_total_examined, p.ln_total_positive, p.ln_ratio,
    p.ln_burden_band, p.ln_lateral_dissected,
    p.ln_rollup_total_positive, p.ln_rollup_total_examined,
    p.ln_rollup_central_examined, p.ln_rollup_central_positive, p.ln_rollup_ene,
    p.ete_grade, p.ete_refined_grade, p.gross_ete_flag, p.worst_ete_v10,
    p.margin_status, p.r_class_true, p.closest_margin_mm,
    p.capsular_invasion_refined, p.lvi_grade, p.lvi_ordinal_worst,
    p.vasc_grade, p.vasc_grade_final_v13, p.pni_positive,
    p.syn_frozen_section, p.syn_frozen_section_result, p.syn_carcinoma_on_frozen,
    p.syn_graves, p.syn_hashimoto, p.syn_chronic_thyroiditis,
    p.syn_follicular_adenoma, p.syn_hurthle_cell_change,
    p.syn_multinodular_goiter, p.syn_hyperplastic_nodules,
    p.syn_capsular_invasion_clean, p.syn_lymphatic_invasion_clean,
    p.syn_margin_status_synoptic, p.syn_margin_distance_mm_num,
    p.syn_n_parathyroid_identified, p.syn_parathyroid_in_specimen,
    p.syn_histologic_grade, p.syn_ki67_index,
    p.syn_isthmus_size_cm, p.syn_left_lobe_size_cm, p.syn_right_lobe_size_cm,
    p.syn_total_weight_g, p.syn_left_lobe_weight_g, p.syn_right_lobe_weight_g,
    p.syn_has_second_tumor, p.syn_tumor2_histologic_type, p.syn_tumor2_size_cm,
    p.syn_central_dissection, p.syn_bilateral_neck_dissection,
    p.gland_weight_final_g, p.preop_imaging_size_cm, p.dominant_nodule_size_cm,
    p.bethesda_final, p.bethesda_final_name, p.n_fna_episodes,
    p.fna_path_concordance_category, p.fna_path_concordant, p.cross_fna_concordance,
    -- ── TIRADS columns (migrated to cupm_v2 sources, legacy names preserved) ──
    cupm.tirads_category_at_first_exam       AS tirads_best_category_v12,
    cupm.max_tirads_category_ever            AS tirads_worst_category_v12,
    CAST(SUBSTR(cupm.tirads_category_at_first_exam, 3) AS BIGINT)
                                             AS tirads_best_score_v12,
    cupm.max_nodule_size_mm                  AS tirads_nodule_size_max_mm_v12,
    -- ── end TIRADS ──
    p.mol_platform, p.mol_genes_list, p.mol_has_thyroseq, p.mol_has_afirma,
    p.braf_positive_final, p.ras_positive_final, p.tert_positive_final,
    p.molecular_tested_confirmed, p.molecular_risk_tier, p.mol_n_tests,
    p.para_specimen_included, p.para_incidental_status_refined,
    p.para_abnormality_type, p.para_n_glands_identified,
    p.para_has_pathologic_glands, p.para_removal_intent,
    p.rai_received_reconciled AS rai_received_flag,
    p.rai_max_dose_mci, p.rai_total_cumulative_dose_mci, p.n_rai_episodes,
    p.rai_avid_flag, p.rai_intent_v9,
    p.tg_n_measurements, p.tg_trajectory_class, p.tg_nadir, p.tg_last_value,
    p.tg_rising_flag, p.tg_peak, p.days_first_to_last_tg,
    p.lab_tsh_n_measurements, p.lab_tsh_most_recent,
    p.lab_pth_n_measurements, p.lab_pth_most_recent,
    p.lab_calcium_n_measurements, p.lab_calcium_most_recent,
    p.any_recurrence_flag, p.recurrence_type, p.recurrence_site,
    p.time_to_recurrence_days, p.structural_recurrence_flag,
    p.overall_survival_years, p.vital_status, p.death_occurred,
    p.followup_years, p.followup_category,
    p.comp_hypoparathyroidism_confirmed, p.comp_hypocalcemia_confirmed,
    p.comp_rln_injury_confirmed, p.comp_hematoma_confirmed,
    p.pmhx_nlp_men_syndrome, p.pmhx_nlp_autoimmune_thyroid_hx,
    p.pmhx_nlp_prior_cancer_hx, p.pmhx_nlp_radiation_exposure,
    p.pmhx_nlp_hypothyroidism, p.pmhx_nlp_hyperthyroidism,
    p.pmhx_nlp_family_hx_thyroid, p.pmhx_nlp_family_hx_cancer,
    p.nlp_frozensec_has_data, p.nlp_frozensec_key_finding,
    p.op_nlp_parathyroid_managed, p.op_nlp_parathyroid_autograft,
    p.op_nlp_nerve_monitoring_used, p.op_nlp_reoperative_field,
    p.ajcc8_calculable_flag, p.ata_calculable_flag, p.macis_calculable_flag
FROM main.canonical_patient_master AS p
LEFT JOIN main.canonical_us_patient_master_VIEW_v2 AS cupm USING (research_id)
"""

VIEW_BODIES["cohort_m011_tirads_fna_genetics_v1"] = """
CREATE OR REPLACE VIEW manuscript_workspace.cohort_m011_tirads_fna_genetics_v1 AS
-- Migrated 2026-04-21: TIRADS sources moved to cupm_v2; legacy names preserved.
-- WHERE filter now uses canonical first-exam TIRADS rank as the gate.
SELECT
    p.research_id, p.age_at_surgery, p.sex, p.surg_procedure_type,
    p.is_malignant, p.histology_final,
    p.path_tumor_size_cm AS tumor_size_cm,
    cupm.tirads_category_at_first_exam       AS tirads_best_category_v12,
    cupm.max_tirads_category_ever            AS tirads_worst_category_v12,
    CAST(SUBSTR(cupm.tirads_category_at_first_exam, 3) AS BIGINT)
                                             AS tirads_best_score_v12,
    cupm.max_nodule_size_mm                  AS tirads_nodule_size_max_mm_v12,
    p.bethesda_final, p.n_fna_episodes, p.fna_path_concordance_category,
    p.mol_has_thyroseq, p.mol_has_afirma, p.molecular_tested_confirmed,
    p.molecular_risk_tier, p.braf_positive_final, p.ras_positive_final,
    p.ajcc8_stage_group, p.ata_risk_category, p.ln_positive_flag,
    p.any_recurrence_flag, p.overall_survival_years
FROM main.canonical_patient_master AS p
LEFT JOIN main.canonical_us_patient_master_VIEW_v2 AS cupm USING (research_id)
WHERE cupm.tirads_category_at_first_exam IS NOT NULL
"""

VIEW_BODIES["cohort_m025_tirads_performance_v1"] = """
CREATE OR REPLACE VIEW manuscript_workspace.cohort_m025_tirads_performance_v1 AS
-- Migrated 2026-04-21: TIRADS sources moved to cupm_v2; legacy names preserved.
-- DROPPED per Logan's Q1 directive (concept retired in v2 pipeline; cohort
-- redesigned to use what cupm_v2 provides):
--   tirads_n_sources_v12     (no v2 surrogate)
--   tirads_reliability_v12   (no v2 surrogate)
-- Substituted: tirads_worst_rank_source surfaces the canonical worst-rank
-- derivation source as a nearest-signal "richness" indicator.
SELECT
    p.research_id, p.age_at_surgery, p.sex, p.race,
    cupm.tirads_category_at_last_preop_exam  AS preop_tirads_category,
    cupm.tirads_category_at_first_exam       AS tirads_best_category_v12,
    cupm.max_tirads_category_ever            AS tirads_worst_category_v12,
    CAST(SUBSTR(cupm.tirads_category_at_first_exam, 3) AS BIGINT)
                                             AS tirads_best_score_v12,
    CAST(SUBSTR(cupm.max_tirads_category_ever, 3)      AS BIGINT)
                                             AS tirads_worst_score_v12,
    cupm.tirads_worst_rank_source            AS tirads_worst_rank_source,
    cupm.n_us_exams                          AS n_us_exams,
    p.dominant_nodule_size_cm AS imaging_nodule_size_cm,
    p.dominant_nodule_size_cm,
    p.bethesda_final, p.bethesda_final_name,
    p.histology_final, p.is_malignant,
    p.path_tumor_size_cm AS tumor_size_cm, p.path_tumor_size_cm,
    p.fna_path_concordance_category, p.fna_path_concordant,
    p.surg_procedure_type, p.surg_first_date
FROM main.canonical_patient_master AS p
LEFT JOIN main.canonical_us_patient_master_VIEW_v2 AS cupm USING (research_id)
WHERE cupm.tirads_category_at_last_preop_exam IS NOT NULL
   OR cupm.tirads_category_at_first_exam      IS NOT NULL
"""

VIEW_BODIES["cohort_m045_multimodal_risk_v1"] = """
CREATE OR REPLACE VIEW manuscript_workspace.cohort_m045_multimodal_risk_v1 AS
-- Migrated 2026-04-21: TIRADS sources moved to cupm_v2; legacy names preserved.
SELECT
    p.research_id, p.age_at_surgery, p.sex,
    cupm.tirads_category_at_last_preop_exam  AS preop_tirads_category,
    cupm.tirads_category_at_first_exam       AS tirads_best_category_v12,
    CAST(SUBSTR(cupm.tirads_category_at_first_exam, 3) AS BIGINT)
                                             AS tirads_best_score_v12,
    p.bethesda_final, p.bethesda_final_name,
    p.molecular_tested_confirmed, p.molecular_risk_tier,
    p.histology_final, p.is_malignant,
    p.path_tumor_size_cm AS tumor_size_cm, p.multifocal_flag_path,
    p.ete_grade_final, p.ln_positive_flag,
    p.ajcc8_stage_group, p.ata_risk_category, p.surg_procedure_type,
    p.any_recurrence_flag, p.followup_years, p.surg_first_date
FROM main.canonical_patient_master AS p
LEFT JOIN main.canonical_us_patient_master_VIEW_v2 AS cupm USING (research_id)
WHERE p.bethesda_final IS NOT NULL
  AND p.histology_final IS NOT NULL
  AND (cupm.tirads_category_at_last_preop_exam IS NOT NULL
       OR cupm.tirads_category_at_first_exam   IS NOT NULL)
"""

VIEW_BODIES["cohort_m075_tirads_multi_nodule_v1"] = """
CREATE OR REPLACE VIEW manuscript_workspace.cohort_m075_tirads_multi_nodule_v1 AS
-- Migrated 2026-04-21: TIRADS sources moved to cupm_v2; legacy names preserved.
-- DROPPED per Logan's Q1 directive (per-nodule concordance lives on
-- canonical_us_nodule_v2 now; per-patient counts retired):
--   tirads_concordant_count_v12  (use canonical_us_nodule_v2.acr2017_vs_updated_concordant
--                                 with COUNT(*) FILTER (WHERE flag) for per-patient analysis)
--   tirads_mismatch_count_v12    (use canonical_us_nodule_v2.acr2017_vs_updated_concordant
--                                 with COUNT(*) FILTER (WHERE NOT flag) for per-patient analysis)
SELECT
    p.research_id, p.age_at_surgery, p.sex, p.surg_procedure_type,
    p.is_malignant, p.histology_final,
    p.path_tumor_size_cm AS tumor_size_cm,
    cupm.tirads_category_at_first_exam       AS tirads_best_category_v12,
    cupm.max_tirads_category_ever            AS tirads_worst_category_v12,
    CAST(SUBSTR(cupm.tirads_category_at_first_exam, 3) AS BIGINT)
                                             AS tirads_best_score_v12,
    CAST(SUBSTR(cupm.max_tirads_category_ever, 3)      AS BIGINT)
                                             AS tirads_worst_score_v12,
    cupm.max_nodule_size_mm                  AS tirads_nodule_size_max_mm_v12,
    cupm.n_nodule_records                    AS tirads_n_nodule_records_v12,
    p.bethesda_final, p.n_fna_episodes, p.fna_path_concordance_category,
    p.molecular_tested_confirmed,
    p.ajcc8_stage_group, p.ata_risk_category, p.ln_positive_flag,
    p.any_recurrence_flag, p.overall_survival_years
FROM main.canonical_patient_master AS p
LEFT JOIN main.canonical_us_patient_master_VIEW_v2 AS cupm USING (research_id)
WHERE cupm.tirads_category_at_first_exam IS NOT NULL
"""

# m050, m053, m064, m076 inherit from cohort_descriptive_full_cohort_v1.
# They're not rewritten — they auto-inherit. Phase 4 verifies their shape.
INHERITING_VIEWS = [
    "cohort_m050_tumor_size_volume_v1",
    "cohort_m053_nondiagnostic_fna_v1",
    "cohort_m064_frozen_decision_v1",
    "cohort_m076_ln_surveillance_v1",
]


# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    log: dict = {"phase": 2, "started_at_utc": utc_iso(), "views": {}}
    con = connect()

    # Step 1: Create archive schema
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {ARCHIVE_DB}.{ARCHIVE_SCHEMA}")
    log["archive_schema_created"] = f"{ARCHIVE_DB}.{ARCHIVE_SCHEMA}"

    # Step 2: For each view to be rewritten or that inherits, snapshot the CURRENT view def.
    all_views = list(VIEW_BODIES.keys()) + INHERITING_VIEWS
    for name in all_views:
        defn_row = con.execute(
            """
            SELECT view_definition FROM information_schema.views
            WHERE table_schema='manuscript_workspace' AND table_name=?
            """, [name]
        ).fetchone()
        if not defn_row:
            log["views"][name] = {"status": "NOT_FOUND"}
            continue
        defn = defn_row[0]
        # Pre-snapshot row counts for verification
        pre_n = con.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT research_id) FROM manuscript_workspace.{name}"
        ).fetchone()
        # Pre-column count
        pre_cols_rows = con.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='manuscript_workspace' AND table_name=?
            ORDER BY ordinal_position
            """, [name]
        ).fetchall()
        pre_cols = [r[0] for r in pre_cols_rows]

        # Persist archive row (one row per view; `definition_sql` carries the body)
        archive_table = f'{ARCHIVE_DB}.{ARCHIVE_SCHEMA}."view_def_{name}"'
        con.execute(f'''
            CREATE OR REPLACE TABLE {archive_table} AS
            SELECT
                ? AS view_name,
                ? AS pre_partB_definition_sql,
                ? AS pre_partB_row_count,
                ? AS pre_partB_distinct_rids,
                ? AS pre_partB_column_count,
                now() AS archived_at
        ''', [name, defn, pre_n[0], pre_n[1], len(pre_cols)])

        log["views"][name] = {
            "pre_row_count": pre_n[0],
            "pre_distinct_rids": pre_n[1],
            "pre_column_count": len(pre_cols),
            "pre_columns": pre_cols,
            "archive_table": archive_table.replace('"', ''),
        }

    # Step 3: Apply CREATE OR REPLACE for each rewritten view (in dependency order)
    rewrite_order = [
        "cohort_descriptive_full_cohort_v1",   # base — m050/m053/m064/m076 inherit
        "cohort_m011_tirads_fna_genetics_v1",
        "cohort_m025_tirads_performance_v1",
        "cohort_m045_multimodal_risk_v1",
        "cohort_m075_tirads_multi_nodule_v1",
    ]
    for name in rewrite_order:
        body = VIEW_BODIES[name]
        # Save the new body for archival
        (OUT / f"_partB_phase2_view_defs/{name}.after.sql").parent.mkdir(parents=True, exist_ok=True)
        (OUT / f"_partB_phase2_view_defs/{name}.after.sql").write_text(body + "\n")
        con.execute(body)

    # Step 4: Verify shape per view (rewritten + inheriting)
    for name in all_views:
        post_n = con.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT research_id) FROM manuscript_workspace.{name}"
        ).fetchone()
        post_cols = [
            r[0] for r in con.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_schema='manuscript_workspace' AND table_name=?
                ORDER BY ordinal_position
                """, [name]
            ).fetchall()
        ]
        v = log["views"][name]
        v["post_row_count"]      = post_n[0]
        v["post_distinct_rids"]  = post_n[1]
        v["post_column_count"]   = len(post_cols)
        v["post_columns"]        = post_cols
        v["row_count_changed"]   = (post_n[0] != v["pre_row_count"])
        v["rids_changed"]        = (post_n[1] != v["pre_distinct_rids"])
        v["cols_dropped"]        = sorted(set(v["pre_columns"]) - set(post_cols))
        v["cols_added"]          = sorted(set(post_cols) - set(v["pre_columns"]))

    # Verify retired columns dropped only from m025/m075
    for name, expected_drops in [
        ("cohort_m025_tirads_performance_v1", {"tirads_n_sources_v12", "tirads_reliability_v12"}),
        ("cohort_m075_tirads_multi_nodule_v1", {"tirads_concordant_count_v12", "tirads_mismatch_count_v12"}),
    ]:
        actual = set(log["views"][name]["cols_dropped"])
        # Allow other expected col adds from the rewrite (e.g., tirads_worst_rank_source on m025)
        log["views"][name]["expected_drops_present"] = expected_drops.issubset(actual)

    log["finished_at_utc"] = utc_iso()
    log["status"] = "OK"
    out_path = OUT / "partB_phase2_migrate_views.json"
    out_path.write_text(json.dumps(log, indent=2, default=str))

    # Friendly summary
    print(f"=== Part B Phase 2 done — written: {out_path.relative_to(REPO)} ===")
    print()
    print(f"{'view':50s} {'pre_n':>8s} {'post_n':>8s} {'pre_c':>5s} {'post_c':>6s}  changes")
    print("-" * 120)
    for name in all_views:
        v = log["views"][name]
        chg = []
        if v.get("row_count_changed"):
            chg.append("ROW_COUNT_CHANGED")
        if v.get("cols_dropped"):
            chg.append(f"DROPPED:{','.join(v['cols_dropped'])}")
        if v.get("cols_added"):
            chg.append(f"ADDED:{','.join(v['cols_added'])}")
        chg_s = " ".join(chg) if chg else "no col change"
        print(
            f"{name:50s} {v.get('pre_row_count','-'):>8} {v.get('post_row_count','-'):>8} "
            f"{v.get('pre_column_count','-'):>5} {v.get('post_column_count','-'):>6}  {chg_s}"
        )


if __name__ == "__main__":
    main()
