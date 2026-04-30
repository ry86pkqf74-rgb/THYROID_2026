# Cursor Prompt — mig_203 gate5 → 0 audit allowlist extension + PM signoff registry refresh

**Date:** 2026-04-30
**Lane:** mig_203 / gate5_zero_audit_allowlist_extension
**Batch (proposed):** `mig_203_gate5_zero_audit_allowlist_extension_20260430`
**Predecessor:** mig_160b close-out (`83fa6f1`) — opened CF-mig160b-AUDIT-ALLOWLIST-PATTERN-EXTENSION. mig_188b chain (`51e201a`) added ~10 new `*_resolved` cols on canonical_patient_master that aren't in the registry.
**Posture:** **READ-ONLY scoping + apply skeleton SQL.** No execute against MotherDuck.
**Target DB:** `thyroid_canonical_publication_v1_0`
**Touches:** none (read-only); produces apply SQL + updated audit query template.
**Tool recommendation:** **Cursor Composer** — well-defined SQL authoring; updates audit query template; ~10 new col registry inserts via mechanical INSERT BY NAME.

---

## Background

Currently gate5 = 6 due to two reasons:

1. **Audit allowlist gap** (CF-mig160b-AUDIT-ALLOWLIST-PATTERN-EXTENSION) — the audit query allowlist hardcoded list doesn't include `cpm_built_at`, `gm_path_stage_raw_derived_at`, `path_stage_raw_derived_at`, `resolved_at`, `rollup_built_at`, `gm_rai_date_confidence`. These are correctly typed for their semantics; the audit needs to be smarter.

2. **PM signoff registry stale count** — mig_188b ALTER COLUMN ADD added 10 new `*_resolved` cols on `canonical_patient_master` (`ajcc8_t_stage_resolved`, `ajcc8_n_stage_resolved`, `ajcc8_m_stage_resolved`, `ajcc8_stage_group_resolved`, `ajcc7_t_stage_resolved`, `ajcc7_n_stage_resolved`, `ajcc7_m_stage_resolved`, `ajcc7_stage_group_resolved`, `ajcc_resolution_source`, `ajcc_resolution_confidence`). These don't have rows in `canonical_column_verification_registry_v1` and `canonical_table_signoff_registry_v1.n_columns_total` is still 1,620 (stale; should be 1,630).

mig_203 closes both: extends audit allowlist via runbook update + inserts 10 registry rows for the new resolved cols + bumps PM `n_columns_total`.

---

## Mission

Author 1 SQL apply skeleton with: (1) registry inserts for 10 new resolved cols, (2) PM signoff `n_columns_total` increment, (3) updated 5-gate audit query template that produces gate5=0 with extended allowlist. No data writes.

---

## Required scope

### §1 Audit query allowlist extension

Update the canonical 5-gate audit query template in `qc_framework_v1/queries/cleanliness_audit_v11.sql`:

```sql
-- v11 5-gate audit (updated allowlist; gate5 expected 0 post-mig_203)
WITH verified_tables AS (
  SELECT table_name FROM main.canonical_table_signoff_registry_v1
  WHERE table_status='verified' AND table_name LIKE 'canonical_%'
),
audit_allowlist AS (
  SELECT col_name FROM (VALUES
    -- Original 17
    ('build_ts'),('built_at'),('extracted_at'),('llm_build_ts'),('llm_extracted_at'),
    ('verified_ts'),('signed_off_ts'),('registered_ts'),('updated_at'),('created_at'),
    ('promoted_at'),('completed_at'),('started_at'),('ended_at'),('ingested_at_utc'),
    ('ingestion_date'),('lab_datetime'),
    -- v11 extension (mig_203): explicit audit/build stamps that didn't fit suffix patterns
    ('cpm_built_at'),('rollup_built_at'),('resolved_at'),('reclassified_at')
  ) v(col_name)
)
SELECT
  (SELECT COUNT(*) FROM main.canonical_table_signoff_registry_v1 WHERE table_status='verified') AS gate1,
  (SELECT COUNT(*) FROM main.canonical_table_signoff_registry_v1 WHERE table_status='verified' AND signoff_migration IS NULL) AS gate2,
  (SELECT COUNT(*) FROM main.canonical_table_signoff_registry_v1 t WHERE t.table_status='verified' AND (t.n_verified+t.n_na<>t.n_columns_total OR t.n_not_started<>0 OR COALESCE(t.n_failed,0)<>0)) AS gate3,
  (SELECT COUNT(*) FROM main.canonical_column_verification_registry_v1 r JOIN main.canonical_table_signoff_registry_v1 t USING (schema_name,table_name) WHERE t.table_status='verified' AND r.verification_status='verified' AND (r.verified_by IS NULL OR r.batch_id IS NULL OR r.verification_method IS NULL)) AS gate4,
  (SELECT COUNT(*) FROM information_schema.columns c
     JOIN verified_tables v ON c.table_name=v.table_name
     LEFT JOIN main.canonical_column_verification_registry_v1 r ON r.schema_name='main' AND r.table_name=c.table_name AND r.column_name=c.column_name
   WHERE c.table_catalog='thyroid_canonical_publication_v1_0' AND c.table_schema='main'
     AND c.column_name NOT IN (SELECT col_name FROM audit_allowlist)
     -- v11 suffix-pattern exclusions
     AND NOT regexp_matches(c.column_name, '_built_at$')
     AND NOT regexp_matches(c.column_name, '_derived_at$')
     AND NOT regexp_matches(c.column_name, '_resolved_at$')
     AND NOT regexp_matches(c.column_name, '_confidence$')
     AND c.column_name NOT LIKE '%_status' AND c.column_name NOT LIKE '%_source'
     AND c.column_name NOT LIKE '%_keyword' AND c.column_name NOT LIKE '%_raw'
     AND COALESCE(r.verification_status,'unknown')!='na'
     AND (c.data_type IN ('TIMESTAMP','TIMESTAMP WITH TIME ZONE')
          OR (c.data_type='VARCHAR' AND (regexp_matches(c.column_name,'(^|_)dates?(_|$)') OR regexp_matches(c.column_name,'(^|_)dt(_|$)'))))
  ) AS gate5;
```

### §2 PM new resolved cols registry inserts

Author `qc_framework_v1/migrations/203_gate5_zero_audit_allowlist_extension_20260430.sql`:

```sql
-- LOGAN RATIFIED 2026-04-30; READY FOR COWORK PATH-C APPLY
USE thyroid_canonical_publication_v1_0;

-- §A Insert 10 new resolved-col rows into canonical_column_verification_registry_v1
INSERT INTO main.canonical_column_verification_registry_v1
  (schema_name, table_name, column_name, data_type, verification_status, verification_method, batch_id, verified_by, verified_ts, notes)
VALUES
  ('main','canonical_patient_master','ajcc8_t_stage_resolved','VARCHAR','verified','derived_from_canonical_path_malignant_events_v1_via_mig188b','mig188b_mig184_v2_plus_r1c_with_explicit_T0_20260430','mig_203','CAST(CURRENT_TIMESTAMP AS TIMESTAMP)','mig_188b R1 AJCC + r1c T0 derivation; manuscript SQL prefers *_resolved.'),
  ('main','canonical_patient_master','ajcc8_n_stage_resolved','VARCHAR','verified','derived_from_canonical_path_malignant_events_v1_via_mig188b','mig188b_mig184_v2_plus_r1c_with_explicit_T0_20260430','mig_203','CAST(CURRENT_TIMESTAMP AS TIMESTAMP)','mig_188b N1a/N1b split using PM cnln_img_*_present + lateral_neck_dissected.'),
  ('main','canonical_patient_master','ajcc8_m_stage_resolved','VARCHAR','verified','derived_from_canonical_path_malignant_events_v1_via_mig188b','mig188b_mig184_v2_plus_r1c_with_explicit_T0_20260430','mig_203','CAST(CURRENT_TIMESTAMP AS TIMESTAMP)','mig_188b M0 default unless M1 evidence.'),
  ('main','canonical_patient_master','ajcc8_stage_group_resolved','VARCHAR','verified','derived_from_canonical_path_malignant_events_v1_via_mig188b','mig188b_mig184_v2_plus_r1c_with_explicit_T0_20260430','mig_203','CAST(CURRENT_TIMESTAMP AS TIMESTAMP)','mig_188b §G AJCC8 stage_group with MTC N1a→III / N1b→IVA + DTC age 55 split.'),
  ('main','canonical_patient_master','ajcc7_t_stage_resolved','VARCHAR','verified','derived_from_canonical_path_malignant_events_v1_via_mig188b','mig188b_mig184_v2_plus_r1c_with_explicit_T0_20260430','mig_203','CAST(CURRENT_TIMESTAMP AS TIMESTAMP)','mig_188b AJCC7 mapping: T3b→T3, T0→T0 pass-through.'),
  ('main','canonical_patient_master','ajcc7_n_stage_resolved','VARCHAR','verified','derived_from_canonical_path_malignant_events_v1_via_mig188b','mig188b_mig184_v2_plus_r1c_with_explicit_T0_20260430','mig_203','CAST(CURRENT_TIMESTAMP AS TIMESTAMP)','mig_188b AJCC7 N copy-through.'),
  ('main','canonical_patient_master','ajcc7_m_stage_resolved','VARCHAR','verified','derived_from_canonical_path_malignant_events_v1_via_mig188b','mig188b_mig184_v2_plus_r1c_with_explicit_T0_20260430','mig_203','CAST(CURRENT_TIMESTAMP AS TIMESTAMP)','mig_188b AJCC7 M copy-through.'),
  ('main','canonical_patient_master','ajcc7_stage_group_resolved','VARCHAR','verified','derived_from_canonical_path_malignant_events_v1_via_mig188b','mig188b_mig184_v2_plus_r1c_with_explicit_T0_20260430','mig_203','CAST(CURRENT_TIMESTAMP AS TIMESTAMP)','mig_188b §G AJCC7 stage_group with MTC N1a→III / N1b→IVA + DTC age 45 split.'),
  ('main','canonical_patient_master','ajcc_resolution_source','VARCHAR','verified','set_by_mig188b_resolution_token','mig188b_mig184_v2_plus_r1c_with_explicit_T0_20260430','mig_203','CAST(CURRENT_TIMESTAMP AS TIMESTAMP)','mig_188b: identifies which derivation rule produced the *_resolved values.'),
  ('main','canonical_patient_master','ajcc_resolution_confidence','VARCHAR','verified','set_by_mig188b_confidence_classification','mig188b_mig184_v2_plus_r1c_with_explicit_T0_20260430','mig_203','CAST(CURRENT_TIMESTAMP AS TIMESTAMP)','mig_188b: high/medium/uncalculable_or_pending.');

-- §B Bump PM n_columns_total + n_verified
UPDATE main.canonical_table_signoff_registry_v1
SET n_columns_total = n_columns_total + 10,
    n_verified = n_verified + 10
WHERE table_name = 'canonical_patient_master';

-- §C Verify gate3 still 0 after the bump
SELECT COUNT(*) AS gate3_violations
FROM main.canonical_table_signoff_registry_v1 t
WHERE t.table_status='verified'
  AND (t.n_verified+t.n_na<>t.n_columns_total OR t.n_not_started<>0 OR COALESCE(t.n_failed,0)<>0);

-- §D Provenance row insert
INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
  (run_id, started_at, ended_at, phases_applied, critical_findings_cleared, high_findings_cleared, med_findings_cleared, held_for_adjudication)
VALUES
  ('mig_203_gate5_zero_audit_allowlist_extension_20260430',
   CAST(CURRENT_TIMESTAMP AS TIMESTAMP), CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
   'pm_resolved_cols_registry_inserts_signoff_bump_audit_allowlist_v11',
   'CF-mig160b-AUDIT-ALLOWLIST-PATTERN-EXTENSION',
   '10_pm_resolved_cols_registered',
   'audit_query_v11_template_authored',
   'none');

-- §E Run v11 5-gate audit (expect 172/0/0/0/0)
WITH verified_tables AS (SELECT table_name FROM main.canonical_table_signoff_registry_v1 WHERE table_status='verified' AND table_name LIKE 'canonical_%'),
audit_allowlist AS (SELECT col_name FROM (VALUES ('build_ts'),('built_at'),('extracted_at'),('llm_build_ts'),('llm_extracted_at'),('verified_ts'),('signed_off_ts'),('registered_ts'),('updated_at'),('created_at'),('promoted_at'),('completed_at'),('started_at'),('ended_at'),('ingested_at_utc'),('ingestion_date'),('lab_datetime'),('cpm_built_at'),('rollup_built_at'),('resolved_at'),('reclassified_at')) v(col_name))
SELECT
  (SELECT COUNT(*) FROM main.canonical_table_signoff_registry_v1 WHERE table_status='verified') AS gate1,
  (SELECT COUNT(*) FROM main.canonical_table_signoff_registry_v1 WHERE table_status='verified' AND signoff_migration IS NULL) AS gate2,
  (SELECT COUNT(*) FROM main.canonical_table_signoff_registry_v1 t WHERE t.table_status='verified' AND (t.n_verified+t.n_na<>t.n_columns_total OR t.n_not_started<>0 OR COALESCE(t.n_failed,0)<>0)) AS gate3,
  (SELECT COUNT(*) FROM main.canonical_column_verification_registry_v1 r JOIN main.canonical_table_signoff_registry_v1 t USING (schema_name,table_name) WHERE t.table_status='verified' AND r.verification_status='verified' AND (r.verified_by IS NULL OR r.batch_id IS NULL OR r.verification_method IS NULL)) AS gate4,
  (SELECT COUNT(*) FROM information_schema.columns c JOIN verified_tables v ON c.table_name=v.table_name LEFT JOIN main.canonical_column_verification_registry_v1 r ON r.schema_name='main' AND r.table_name=c.table_name AND r.column_name=c.column_name WHERE c.table_catalog='thyroid_canonical_publication_v1_0' AND c.table_schema='main' AND c.column_name NOT IN (SELECT col_name FROM audit_allowlist) AND NOT regexp_matches(c.column_name,'_built_at$') AND NOT regexp_matches(c.column_name,'_derived_at$') AND NOT regexp_matches(c.column_name,'_resolved_at$') AND NOT regexp_matches(c.column_name,'_confidence$') AND c.column_name NOT LIKE '%_status' AND c.column_name NOT LIKE '%_source' AND c.column_name NOT LIKE '%_keyword' AND c.column_name NOT LIKE '%_raw' AND COALESCE(r.verification_status,'unknown')!='na' AND (c.data_type IN ('TIMESTAMP','TIMESTAMP WITH TIME ZONE') OR (c.data_type='VARCHAR' AND (regexp_matches(c.column_name,'(^|_)dates?(_|$)') OR regexp_matches(c.column_name,'(^|_)dt(_|$)'))))) AS gate5_v11;
```

### §3 Audit/report

Author `qc_framework_v1/reports/mig_203_gate5_zero_audit_allowlist_extension_20260430.md`:
- §1 problem statement
- §2 fix description
- §3 expected post-state: gate1=172, gate2=0, gate3=0, gate4=0, gate5_v11=**0**, PM 1,606 v / 24 na / 0 / 1,630 ✓
- §4 future-proofing — pattern: when adding `*_resolved` cols via ALTER COLUMN ADD, always append registry inserts in the same batch + bump signoff counts.

### §4 Mark READY

Header: `-- LOGAN RATIFIED 2026-04-30; READY FOR COWORK PATH-C APPLY`

---

## Governance reminders

- Read-only investigation only. No `query_rw`.
- Author = `Logan Glosser <logan.glosser@gmail.com>`.
- Registry inserts mutate `canonical_column_verification_registry_v1` and `canonical_table_signoff_registry_v1`; pre-snapshot affected rows before apply.

---

## Deliverables

1. `qc_framework_v1/migrations/203_gate5_zero_audit_allowlist_extension_20260430.sql`
2. `qc_framework_v1/queries/cleanliness_audit_v11.sql`
3. `qc_framework_v1/reports/mig_203_gate5_zero_audit_allowlist_extension_20260430.md`

Commit message: `qc: mig_203 gate5 → 0 audit allowlist extension + PM signoff registry refresh (10 *_resolved cols registered; n_columns_total 1620→1630; CF-mig160b-AUDIT-ALLOWLIST-PATTERN-EXTENSION CLOSED)`

---

End of prompt.
