"""One-off generator for qc_framework_v1/migrations/165_auxiliary_registry_hygiene_20260429.sql.

Run from repo root: .venv/bin/python scripts/_generate_mig165_auxiliary_registry_sql.py
"""
from __future__ import annotations

from collections.abc import Callable

BATCH_ID = "mig_165_auxiliary_registry_hygiene_20260429"
MIG_PATH = "qc_framework_v1/migrations/165_auxiliary_registry_hygiene_20260429.sql"

# main.* auto-na buckets (live MotherDuck classification 2026-04-29)
MAIN_TIER1 = [
    "clinical_notes_long",
    "clinical_note_ln_extracted_v1",
    "path_synoptics",
    "ct_imaging",
    "mri_imaging",
    "nuclear_med",
    "thyroid_sizes",
    "thyroid_weights",
    "note_entities_operative_detail",
    "note_entities_procedures",
    "imaging_exam_master_v1",
]
MAIN_REG_GOV = ["__readme", "data_dictionary_v279"]
MAIN_GOV_AUDIT = [
    "cupm_v2_canonical_backfill_v1",
    "ete_adjudication_v1",
    "patient_completion_oed_path_linkage_v1",
    "nsqip_enrichment",
    "nsqip_patient_summary",
    "specimen_genomic_assay_v1",
    "specimen_master_v1",
    "specimen_source_xref_v1",
    "specimen_tumor_focus_v1",
    "tg_postop_surveillance_windows_v1",
    "tg_timeline_patient_summary_v1",
]

# Leave not_started + CF notes on signoff row only
MAIN_CF = [
    "imaging_fna_linkage_v3",
    "imaging_patient_summary_v1",
    "manuscript_cohort_v1",
    "patient_cross_domain_timeline_v2",
    "recurrence_event_clean_v1",
    "tumor_stage_heterogeneity_v1",
]

WS_ALL = """agent_adjudication_log_v1
archive_candidate_review_v1
archive_move_log_v1
canonical_cleanup_audit_v1
cpm_ajcc_dominant_concordance_v1
cpm_ajcc_dominant_discordance_canonical_v1
cpm_ajcc_dominant_vs_tp_hist1_discordance_v1
cpm_backfill_log_v1
cpm_histologic_classification_audit_v1
cpm_is_malignant_flag_review_v1
cpm_missing_data_provenance_v1
cpm_reconciliation_provenance_v1
cpm_stage_group_manual_review_v1
cpm_tirads_audit_classification_v1
cpm_tirads_canonical_coverage_v1
cpm_tnm_cross_source_disagreements_v1
detail_table_registry_v1
episode_analysis_resolved_v1_dedup
genetics_per_test_discordance_v1
lab_orphan_audit_v1
lab_orphan_cohort_review_v1
lesion_analysis_resolved_v1
ln_crossval_v1
ln_master_rollup_v1
main_schema_keep_list_v1
manuscript_dive_map_v1
manuscript_feasibility_v1
n_surgeries_v1_v2_conflict_v1
nlp_rollup_promotion_audit_v1
object_domain_map_v1
path_tumor_size_chart_review_queue_v1
path_tumor_size_correction_queue_v1
path_tumor_size_multifocal_enumeration_notes_v1
patient_analysis_resolved_v1
pi_review_queue_v1
qc_manual_review_queue_v1
qc_rules_v1
qc_tir03_llm_candidates_v1
qc_usln01_llm_candidates_v1
qc_violations_v1
recurrence_imaging_suspicious_candidates_v1
registry_end_to_end_validation_v1
registry_v2_resolution_audit_v1
registry_v2_unresolved_pointers_v1
schema_reorg_move_log_v1
schema_reorg_orphan_references_v1
script_387_dedup_probe_v1
script_388_archive_move_log_v1
script_389_archive_move_log_v1
tg_orphan_cancer_text_investigation_queue_v1
us_llm_absorption_mapping_v1
us_nodule_conflict_queue_v1
us_raw_index0_conflict_v1
us_raw_index_mismatch_v1
v1_1_finalization_audit_v1
vc_complication_tiering_v1""".strip().splitlines()

WS_CF = {
    "episode_analysis_resolved_v1_dedup",
    "lesion_analysis_resolved_v1",
    "patient_analysis_resolved_v1",
    "ln_master_rollup_v1",
}

WS_REG_GOV = {
    "detail_table_registry_v1",
    "main_schema_keep_list_v1",
    "object_domain_map_v1",
    "registry_end_to_end_validation_v1",
    "registry_v2_resolution_audit_v1",
    "registry_v2_unresolved_pointers_v1",
}


def sql_quote_list(names: list[str]) -> str:
    return ",\n    ".join(f"'{n}'" for n in names)


def emit_na_update(schema: str, tables: list[str], method: str, tag: str) -> str:
    if not tables:
        return ""
    return f"""-- -----------------------------------------------------------------------------
-- {tag}
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1
SET verification_status = 'na',
    verified_by         = 'logan',
    verification_method = '{method}',
    batch_id            = '{BATCH_ID}',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes               = COALESCE(notes, '')
                          || ' | mig_165 auxiliary lane ({tag.lower()}): mass auto-na classification '
                          || '(Lane 53 / mig_165 prompt `CURSOR_PROMPT_mig165_auxiliary_registry_hygiene_20260429.md`).'
WHERE schema_name = '{schema}'
  AND table_name IN (
    {sql_quote_list(tables)}
  )
  AND verification_status = 'not_started';

"""


def emit_cf_notes(
    schema: str, tables: list[str], cf_tag_fn: Callable[[str], str]
) -> str:
    """cf_tag_fn(table_name) -> full CF token string without leading spaces."""
    out = []
    for t in tables:
        cf = cf_tag_fn(t)
        note = (
            f"' | {cf}: analytic / Tier-2 feeder deferred real verification "
            f"(mig_165 Lane 53 classification).'"
        )
        out.append(
            f"""UPDATE main.canonical_table_signoff_registry_v1
SET notes = COALESCE(notes, '')
            || {note}
WHERE schema_name = '{schema}'
  AND table_name = '{t}'
  AND table_status = 'not_started';
"""
        )
    return "\n".join(out)


def main() -> None:
    ws_auto = [t for t in WS_ALL if t not in WS_CF]
    ws_audit = [t for t in ws_auto if t not in WS_REG_GOV]
    ws_reg = [t for t in ws_auto if t in WS_REG_GOV]

    assert len(MAIN_TIER1) + len(MAIN_REG_GOV) + len(MAIN_GOV_AUDIT) + len(MAIN_CF) == 30
    assert len(ws_audit) + len(ws_reg) + len(WS_CF) == 56

    affected_pairs: list[tuple[str, str]] = []
    affected_pairs.extend(("main", t) for t in MAIN_TIER1 + MAIN_REG_GOV + MAIN_GOV_AUDIT)
    affected_pairs.extend(("manuscript_workspace", t) for t in ws_audit + ws_reg)
    affected_pairs.append(("main", "note_entities_llm_presenting_symptoms"))

    values_sql = ",\n    ".join(f"('{s}', '{t}')" for s, t in affected_pairs)

    header = f'''-- =============================================================================
-- Migration 165 — Auxiliary registry hygiene (mass auto-na + CF staging)
-- =============================================================================
-- Date:   2026-04-29 (UTC)
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- Lane:   53 / mig_165
-- Prompt: cursor_prompts/CURSOR_PROMPT_mig165_auxiliary_registry_hygiene_20260429.md
-- batch_id: {BATCH_ID}
--
-- Scope: Registry-only writes ON MotherDuck RW `thyroid_canonical_publication_v1_0`.
-- Path C / Cowork executes APPLY after independent review — **do not RW from agent**.
--
-- Cowork probe corrections vs draft prompt (main-only schema join blind spot):
-- * **85** auxiliary `not_started` rows split **`main` (30)** + **`manuscript_workspace` (56)** —
--   **ALL** rows have physical backing when keyed by **`registry.schema_name`** (0 orphan DELETEs).
-- * Draft cited **53 “stale”** rows — those tables live under **`manuscript_workspace`**, not `main`.
--
-- Live probes (MotherDuck `thyroid_canonical_publication_v1_0`, read-only 2026-04-29):
-- * Gate1 baseline `COUNT(*) WHERE table_status='verified'` on **canonical_table_signoff_registry_v1**
--   = **88** verified tables pre-mig_165.
-- * Expected Gate1 uplift: **+77** → **165** verified tables (**76** existing auxiliary tables flipped +
--   **1** new Tier-1 registration `note_entities_llm_presenting_symptoms`).
-- * **`recurrence_event_clean_v1`** remains **`not_started`** — **CF-mig165-RECURRENCE-EVENT-CLEAN-NEEDS-REAL-VERIFY**
--   (defer real Tier-2 verification to mig_163 lane per prompt §8).
--
-- DELETE blocks: **NONE** — zero registry rows without physical backing after schema-qualified join.
--
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Section A — Pre-snapshots (archive DB — full registry slice for affected objects)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_table_signoff_registry_pre_{BATCH_ID} AS
SELECT *,
       CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig165_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_table_signoff_registry_v1
WHERE (schema_name, table_name) IN (
    SELECT * FROM (VALUES
    {values_sql}
    ) AS v(schema_name, table_name)
);

CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_pre_{BATCH_ID} AS
SELECT *,
       CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig165_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_column_verification_registry_v1
WHERE (schema_name, table_name) IN (
    SELECT * FROM (VALUES
    {values_sql}
    ) AS v(schema_name, table_name)
);

BEGIN TRANSACTION;

'''

    body = ""
    body += emit_na_update("main", MAIN_TIER1, "auto_tier1_raw_mirror_skip", "165b-main-tier1-raw-mirror")
    body += emit_na_update("main", MAIN_REG_GOV, "auto_registry_governance_skip", "165c-main-registry-governance")
    body += emit_na_update(
        "main", MAIN_GOV_AUDIT, "auto_governance_audit_table_skip", "165d-main-governance-audit"
    )
    body += emit_na_update(
        "manuscript_workspace",
        ws_reg,
        "auto_registry_governance_skip",
        "165e-manuscript_workspace-registry-governance",
    )
    body += emit_na_update(
        "manuscript_workspace",
        ws_audit,
        "auto_governance_audit_table_skip",
        "165f-manuscript_workspace-governance-audit",
    )

    body += """-- -----------------------------------------------------------------------------
-- 165g — CF stamps on deferred analytic / Tier-2 tables (column registry untouched)
-- -----------------------------------------------------------------------------
"""
    body += emit_cf_notes(
        "main",
        [
            "imaging_fna_linkage_v3",
            "imaging_patient_summary_v1",
            "manuscript_cohort_v1",
            "patient_cross_domain_timeline_v2",
            "tumor_stage_heterogeneity_v1",
        ],
        lambda tbl: f"CF-mig165-AUX-NEEDS-REAL-VERIFY-{tbl}",
    )
    body += emit_cf_notes(
        "main",
        ["recurrence_event_clean_v1"],
        lambda _tbl: "CF-mig165-RECURRENCE-EVENT-CLEAN-NEEDS-REAL-VERIFY",
    )
    body += emit_cf_notes(
        "manuscript_workspace",
        sorted(WS_CF),
        lambda tbl: f"CF-mig165-AUX-NEEDS-REAL-VERIFY-{tbl}",
    )

    body += """-- -----------------------------------------------------------------------------
-- 165h — Register Tier-1 raw mirror `note_entities_llm_presenting_symptoms` (orphan BASE TABLE)
-- -----------------------------------------------------------------------------
INSERT INTO main.canonical_column_verification_registry_v1
       (schema_name, table_name, column_name, data_type, ordinal_position,
        category, upstream_source, verification_status, verified_by, verified_ts,
        verification_method, batch_id, notes)
SELECT ic.table_schema,
       ic.table_name,
       ic.column_name,
       ic.data_type,
       ic.ordinal_position,
       'source' AS category,
       NULL AS upstream_source,
       'na' AS verification_status,
       'logan' AS verified_by,
       CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS verified_ts,
       'auto_tier1_raw_mirror_skip' AS verification_method,
       '""" + BATCH_ID + """' AS batch_id,
       'mig_165 orphan Tier-1 LLM mirror registration + immediate na classification (Lane 53).'
FROM information_schema.columns AS ic
JOIN information_schema.tables AS it
  ON it.table_catalog = ic.table_catalog
 AND it.table_schema = ic.table_schema
 AND it.table_name = ic.table_name
WHERE ic.table_catalog = 'thyroid_canonical_publication_v1_0'
  AND ic.table_schema = 'main'
  AND ic.table_name = 'note_entities_llm_presenting_symptoms'
  AND it.table_type = 'BASE TABLE'
  AND NOT EXISTS (
        SELECT 1
        FROM main.canonical_column_verification_registry_v1 AS r
        WHERE r.schema_name = ic.table_schema
          AND r.table_name = ic.table_name
          AND r.column_name = ic.column_name
      );

INSERT INTO main.canonical_table_signoff_registry_v1
       (schema_name, table_name, n_columns_total, n_verified, n_not_started, n_failed, n_na,
        table_status, signed_off_ts, signoff_migration, priority_tier, notes)
SELECT sub.schema_name,
       sub.table_name,
       sub.n_columns_total,
       0 AS n_verified,
       0 AS n_not_started,
       0 AS n_failed,
       sub.n_na,
       'verified' AS table_status,
       CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS signed_off_ts,
       '""" + MIG_PATH + """' AS signoff_migration,
       NULL AS priority_tier,
       'mig_165 Tier-1 raw mirror mirror-only classification — all cols na.'
FROM (
  SELECT schema_name,
         table_name,
         COUNT(*) AS n_columns_total,
         SUM(CASE WHEN verification_status = 'na' THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name = 'main'
    AND table_name = 'note_entities_llm_presenting_symptoms'
  GROUP BY 1, 2
) AS sub
WHERE NOT EXISTS (
    SELECT 1 FROM main.canonical_table_signoff_registry_v1 ts
    WHERE ts.schema_name = sub.schema_name AND ts.table_name = sub.table_name
);

"""

    body += """-- -----------------------------------------------------------------------------
-- 165i — Resync canonical_table_signoff_registry_v1 aggregates (mig_159 §159g pattern)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_table_signoff_registry_v1 AS ts
SET n_columns_total   = subq.n_total,
    n_verified        = subq.n_verified,
    n_not_started     = subq.n_not_started,
    n_failed          = COALESCE(subq.n_failed, 0),
    n_na              = subq.n_na,
    table_status      = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed, 0) = 0 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress'
      ELSE 'not_started'
    END,
    signed_off_ts       = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    signoff_migration   = '""" + MIG_PATH + """',
    notes               = COALESCE(ts.notes, '')
                          || ' | mig_165: auxiliary lane AUTO sign-off rollup (verified when na+verified clears queue).'
FROM (
  SELECT schema_name,
         table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status = 'verified' THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status = 'not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status = 'failed' THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status = 'na' THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE (schema_name, table_name) IN (
      SELECT * FROM (VALUES
"""
    body += "      " + values_sql.replace("\n", "\n      ")
    body += """
      ) AS v(schema_name, table_name)
    )
  GROUP BY 1, 2
) AS subq
WHERE ts.schema_name = subq.schema_name
  AND ts.table_name = subq.table_name;

COMMIT;

-- =============================================================================
-- end migration 165 — auxiliary registry hygiene (Lane 53)
-- =============================================================================
"""

    out = header + body
    path = "/Users/ros/THyroid 2026/qc_framework_v1/migrations/165_auxiliary_registry_hygiene_20260429.sql"
    with open(path, "w", encoding="utf-8") as f:
        f.write(out)
    print(f"Wrote {path} ({len(out)} chars)")


if __name__ == "__main__":
    main()
