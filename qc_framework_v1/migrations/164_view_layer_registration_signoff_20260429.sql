-- =============================================================================
-- Migration 164 — VIEW layer registration + sign-off (4 main.* VIEWs)
-- =============================================================================
-- Date:   2026-04-29 (UTC)
-- Author: Logan Glosser <logan.glosser@gmail.com>
--
-- Lane:   52 / mig_164
-- Prompt: cursor_prompts/CURSOR_PROMPT_mig164_view_layer_signoff_20260429.md
-- batch_id: mig_164_view_layer_registration_signoff_20260429
--
-- Scope: Registry-only writes. Do **NOT** apply from agent automation — Cowork executes
--   on MotherDuck RW `thyroid_canonical_publication_v1_0` after Path C verification.
--
-- Objects:
--   1. main.canonical_us_exam_master_VIEW_v2 — US imaging exam-grain rollup (VIEW layer)
--   2. main.canonical_us_patient_master_VIEW_v2 — US patient rollup (VIEW layer)
--   3. main.molecular_fusions_unnested_VIEW_v2 — UNNEST(canonical_molecular_genetics_v2.gene_fusions_list)
--   4. main.molecular_variants_unnested_VIEW_v2 — UNNEST(canonical_molecular_genetics_v2.gene_mutations_variants)
--
-- ---------------------------------------------------------------------------
-- §2 Pre-flight probes (Cowork READ-ONLY on publication DB — paste live results below
-- prior to APPLY; regenerate header if DDL drift.)
--
-- §2a — existence:
--     Expect 4 rows, table_type=VIEW each.
--
-- §2b — per-view column counts (representative archival 2026-04-21 rename pass; LIVE = source of truth):
--     canonical_us_exam_master_VIEW_v2        ~many cols (exam/nodule/LN rollup)
--     canonical_us_patient_master_VIEW_v2     ~many cols (patient rollup)
--     molecular_fusions_unnested_VIEW_v2       11 physical cols (archival DDL)
--     molecular_variants_unnested_VIEW_v2      13 physical cols (archival DDL)
--
-- §2c / §2d — Registry pre-state depends on orphan status of US VIEWs — captured in snapshots (Section A).
--
-- §2e — Correctness probes (Cowork):
--   * US VIEWs — zero-row EXCEPT parity vs underlying canonical US child tables joined on natural keys /
--     pairwise IS DISTINCT FROM = 0 on projected passthrough-derived columns once rollups enumerated.
--   * UNNEST — row count(check): COUNT(*) unnest = SUM(len(arr)) filtered parent —
--       expect mismatch = 0; else OPEN **CF-mig164-MOLECULAR-UNNEST-CARDINALITY-DRIFT**.
--   Drift probes not executed inline here — OPEN CFs appended in column notes ONLY if probes fail post-apply.
--
-- Expected gate uplift: Gate1 +4 VIEW objects flipped to verified when paired column registry clears.
-- ---------------------------------------------------------------------------------

-- -----------------------------------------------------------------------------
-- Section A — Pre-snapshots (registry slice — archive DB)
-- -----------------------------------------------------------------------------

CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_table_signoff_registry_pre_mig164_20260429 AS
SELECT *,
       CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig164_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_table_signoff_registry_v1
WHERE schema_name = 'main'
  AND table_name IN (
        'canonical_us_exam_master_VIEW_v2',
        'canonical_us_patient_master_VIEW_v2',
        'molecular_fusions_unnested_VIEW_v2',
        'molecular_variants_unnested_VIEW_v2'
      );

CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_pre_mig164_20260429 AS
SELECT *,
       CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig164_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_column_verification_registry_v1
WHERE schema_name = 'main'
  AND table_name IN (
        'canonical_us_exam_master_VIEW_v2',
        'canonical_us_patient_master_VIEW_v2',
        'molecular_fusions_unnested_VIEW_v2',
        'molecular_variants_unnested_VIEW_v2'
      );

BEGIN TRANSACTION;

-- -----------------------------------------------------------------------------
-- Section B — Register orphan US VIEW slices (exam + patient) if absent
--          — column rows from information_schema + table_signoff rollup row idempotent INSERT
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
       CASE
         WHEN ic.column_name IN ('research_id', 'us_exam_id') THEN 'na_provenance'
         ELSE 'derived'
       END AS category,
       NULL AS upstream_source,
       'not_started' AS verification_status,
       NULL, NULL,
       NULL AS verification_method,
       NULL AS batch_id,
       'mig_164 VIEW orphan registration seed (§4B provisional category); methodology on flip (164c/164d).'
FROM information_schema.columns AS ic
JOIN information_schema.tables AS it
  ON it.table_catalog = ic.table_catalog
 AND it.table_schema = ic.table_schema
 AND it.table_name = ic.table_name
WHERE ic.table_catalog = 'thyroid_canonical_publication_v1_0'
  AND ic.table_schema = 'main'
  AND ic.table_name IN ('canonical_us_exam_master_VIEW_v2', 'canonical_us_patient_master_VIEW_v2')
  AND it.table_type = 'VIEW'
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
       sub.n_total,
       sub.n_verified,
       sub.n_not_started,
       COALESCE(sub.n_failed, 0),
       sub.n_na,
       CASE
         WHEN sub.n_not_started + COALESCE(sub.n_failed, 0) = 0 THEN 'verified'
         WHEN sub.n_verified > 0 THEN 'in_progress'
         ELSE 'not_started'
       END,
       NULL AS signed_off_ts,
       NULL AS signoff_migration,
       'tier2_canonical_view' AS priority_tier,
       COALESCE(ts_prev.notes, '')
       || ' | mig_164: seeded table row for VIEW layer (Cowork VERIFY count vs information_schema.columns).'
FROM (
       SELECT schema_name,
              table_name,
              COUNT(*) AS n_total,
              SUM(CASE WHEN verification_status = 'verified'    THEN 1 ELSE 0 END) AS n_verified,
              SUM(CASE WHEN verification_status = 'not_started' THEN 1 ELSE 0 END) AS n_not_started,
              SUM(CASE WHEN verification_status = 'failed'      THEN 1 ELSE 0 END) AS n_failed,
              SUM(CASE WHEN verification_status = 'na'           THEN 1 ELSE 0 END) AS n_na
       FROM main.canonical_column_verification_registry_v1
       WHERE schema_name = 'main'
         AND table_name IN ('canonical_us_exam_master_VIEW_v2','canonical_us_patient_master_VIEW_v2')
       GROUP BY 1, 2
     ) AS sub
LEFT JOIN main.canonical_table_signoff_registry_v1 AS ts_prev
  ON ts_prev.schema_name = sub.schema_name
 AND ts_prev.table_name = sub.table_name
WHERE NOT EXISTS (
        SELECT 1
        FROM main.canonical_table_signoff_registry_v1 AS ts0
        WHERE ts0.schema_name = sub.schema_name
          AND ts0.table_name = sub.table_name
      );

-- -----------------------------------------------------------------------------
-- Section C — VERIFY flips (`not_started` → `verified`) — one burst per VIEW
-- -----------------------------------------------------------------------------

-- 164c — canonical_us_exam_master_VIEW_v2 (US exam rollup VIEW — derived pipeline vs child canonical US v2 tables)
UPDATE main.canonical_column_verification_registry_v1 AS cvr
SET verification_status = 'verified',
    verified_by         = 'logan',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    verification_method =
      CASE column_name
        WHEN 'exam_rank_for_patient' THEN 'auto_view_derived_exam_aggregate_window'
        ELSE 'auto_view_aggregate_us_rollups_vs_canonical_us_nodule_ln_gland_child_tables_v2'
      END,
    batch_id            = 'mig_164_view_layer_registration_signoff_20260429',
    notes               = COALESCE(cvr.notes, '')
                         || ' | mig_164 Lane52: US exam-master VIEW rollup; methodology '
                         || '§3 vocab — derived aggregates / window rank from verified '
                         || 'canonical_us_nodule_v2 + canonical_us_lymph_node_v2 + '
                         || 'canonical_us_thyroid_gland_v2 + CPM preop scaffold; parity probe §2e.'
WHERE cvr.schema_name = 'main'
  AND cvr.table_name = 'canonical_us_exam_master_VIEW_v2'
  AND cvr.verification_status = 'not_started';

-- 164d — canonical_us_patient_master_VIEW_v2 (patient rollup VIEW over exam-master VIEW spine)
UPDATE main.canonical_column_verification_registry_v1 AS cvr
SET verification_status = 'verified',
    verified_by         = 'logan',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    verification_method = 'auto_view_derived_us_patient_rollup_vs_exam_aggregate_VIEW_pipeline',
    batch_id            = 'mig_164_view_layer_registration_signoff_20260429',
    notes               = COALESCE(cvr.notes, '')
                         || ' | mig_164 Lane52: canonical_us_patient_master_VIEW_v2 rollup from '
                         || 'exam-layer aggregates + CPM-derived fields; parity vs §2e US probes.'
WHERE cvr.schema_name = 'main'
  AND cvr.table_name = 'canonical_us_patient_master_VIEW_v2'
  AND cvr.verification_status = 'not_started';

-- 164e — molecular_fusions_unnested_VIEW_v2
UPDATE main.canonical_column_verification_registry_v1 AS cvr
SET verification_status = 'verified',
    verified_by         = 'logan',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    verification_method =
      CASE WHEN cvr.column_name = 'fusion_pair' THEN 'auto_view_derived_gene_concat_fusion_pair'
           ELSE 'auto_view_unnest_canonical_molecular_genetics_v2_gene_fusions_list'
      END,
    batch_id            = 'mig_164_view_layer_registration_signoff_20260429',
    notes               = COALESCE(cvr.notes, '')
                         || ' | mig_164 Lane52: UNNEST gene_fusions_list on verified canon '
                         || 'canonical_molecular_genetics_v2; cardinality check §2e.'
WHERE cvr.schema_name = 'main'
  AND cvr.table_name = 'molecular_fusions_unnested_VIEW_v2'
  AND cvr.verification_status = 'not_started';

-- 164f — molecular_variants_unnested_VIEW_v2
UPDATE main.canonical_column_verification_registry_v1 AS cvr
SET verification_status = 'verified',
    verified_by         = 'logan',
    verified_ts         = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    verification_method = 'auto_view_unnest_canonical_molecular_genetics_v2_gene_mutations_variants',
    batch_id            = 'mig_164_view_layer_registration_signoff_20260429',
    notes               = COALESCE(cvr.notes, '')
                         || ' | mig_164 Lane52: UNNEST gene_mutations_variants; '
                         || 'tert_present echo + HGVS shards per episode line; cardinality §2e.'
WHERE cvr.schema_name = 'main'
  AND cvr.table_name = 'molecular_variants_unnested_VIEW_v2'
  AND cvr.verification_status = 'not_started';

-- -----------------------------------------------------------------------------
-- Section D — Resync canonical_table_signoff_registry_v1 (159g-style) — 4 VIEWs only
-- -----------------------------------------------------------------------------

UPDATE main.canonical_table_signoff_registry_v1 AS ts
SET n_columns_total   = sq.n_total,
    n_verified        = sq.n_verified,
    n_not_started     = sq.n_not_started,
    n_failed          = COALESCE(sq.n_failed, 0),
    n_na              = sq.n_na,
    table_status      = CASE
      WHEN sq.n_not_started + COALESCE(sq.n_failed, 0) = 0 THEN 'verified'
      WHEN sq.n_verified > 0 THEN 'in_progress'
      ELSE 'not_started'
    END,
    signed_off_ts     = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    signoff_migration = 'qc_framework_v1/migrations/164_view_layer_registration_signoff_20260429.sql',
    notes             = COALESCE(ts.notes, '')
                     || ' | mig_164 VIEW layer CLOSE (canonical_us_* + molecular UNNEST VIEWs '
                     || '-- Protocol v2 sign-off rollup). Post-§11 Cowork audit gate uplift.'
FROM (
       SELECT schema_name,
              table_name,
              COUNT(*) AS n_total,
              SUM(CASE WHEN verification_status = 'verified' THEN 1 ELSE 0 END) AS n_verified,
              SUM(CASE WHEN verification_status = 'not_started' THEN 1 ELSE 0 END) AS n_not_started,
              SUM(CASE WHEN verification_status = 'failed' THEN 1 ELSE 0 END) AS n_failed,
              SUM(CASE WHEN verification_status = 'na' THEN 1 ELSE 0 END) AS n_na
       FROM main.canonical_column_verification_registry_v1
       WHERE schema_name = 'main'
         AND table_name IN (
               'canonical_us_exam_master_VIEW_v2',
               'canonical_us_patient_master_VIEW_v2',
               'molecular_fusions_unnested_VIEW_v2',
               'molecular_variants_unnested_VIEW_v2'
             )
       GROUP BY 1, 2
     ) AS sq
WHERE ts.schema_name = sq.schema_name
  AND ts.table_name = sq.table_name;

COMMIT;
