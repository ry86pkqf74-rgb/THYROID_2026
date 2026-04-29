-- =============================================================================
-- Migration 164b — VIEW layer registration for 2 molecular UNNEST VIEWs (mig_164 gap)
-- =============================================================================
-- Date:   2026-04-29 (UTC)
-- Author: Cowork (Path-C-applied directly per §6.3 — registry-only Cowork-authored)
--
-- Reason: mig_164 §B INSERT only registered the 2 canonical_us_*_VIEW_v2 VIEWs
--   (its WHERE clause filtered to those names). It assumed the 2 molecular UNNEST
--   VIEWs were already in both registries, but Cowork live-MD pre-flight 2026-04-29
--   confirmed they were NOT — both `molecular_fusions_unnested_VIEW_v2` and
--   `molecular_variants_unnested_VIEW_v2` had 0 rows in either registry. mig_164's
--   164e/164f UPDATEs therefore hit 0 rows; only US VIEWs flipped to verified
--   (gate1 165 → 167 instead of 165 → 169 as the queue plan called for).
--
-- This file closes the gap with the same INSERT-then-flip pattern, registering 24
-- molecular UNNEST VIEW columns (11 fusions + 13 variants) plus 2 table-signoff
-- rows, then flipping all 24 col rows to verified using the methodology strings
-- from mig_164 §164e/§164f. Net gate1: 167 → 169.
--
-- Pre-snapshot stored at:
--   "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_*_pre_mig164b_20260429
-- (will be empty since neither registry has rows for these tables, but markers preserved)
--
-- Pre-flight (Cowork live 2026-04-29):
--   * molecular_fusions_unnested_VIEW_v2  : 11 phys cols, 0 in col-reg, 0 in table-reg
--   * molecular_variants_unnested_VIEW_v2 : 13 phys cols, 0 in col-reg, 0 in table-reg
--
-- EFFECT: ~6 query_rw calls. No data writes; no schema changes; no PM impact.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Section A — Pre-snapshots (will be empty by design)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_table_signoff_registry_pre_mig164b_20260429 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig164b_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_table_signoff_registry_v1
WHERE schema_name='main' AND table_name IN ('molecular_fusions_unnested_VIEW_v2','molecular_variants_unnested_VIEW_v2');

CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_pre_mig164b_20260429 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig164b_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_column_verification_registry_v1
WHERE schema_name='main' AND table_name IN ('molecular_fusions_unnested_VIEW_v2','molecular_variants_unnested_VIEW_v2');


-- -----------------------------------------------------------------------------
-- Section B — Register molecular UNNEST VIEW columns (11 fusions + 13 variants)
-- -----------------------------------------------------------------------------
INSERT INTO main.canonical_column_verification_registry_v1
       (schema_name, table_name, column_name, data_type, ordinal_position,
        category, upstream_source, verification_status, verified_by, verified_ts,
        verification_method, batch_id, notes)
SELECT ic.table_schema, ic.table_name, ic.column_name, ic.data_type, ic.ordinal_position,
       CASE WHEN ic.column_name IN ('research_id') THEN 'na_provenance' ELSE 'derived' END,
       NULL, 'not_started', NULL, NULL, NULL, NULL,
       'mig_164b VIEW orphan registration seed (mig_164 gap closure); methodology on flip below.'
FROM information_schema.columns ic
JOIN information_schema.tables it
  ON it.table_catalog=ic.table_catalog AND it.table_schema=ic.table_schema AND it.table_name=ic.table_name
WHERE ic.table_catalog='thyroid_canonical_publication_v1_0'
  AND ic.table_schema='main'
  AND ic.table_name IN ('molecular_fusions_unnested_VIEW_v2','molecular_variants_unnested_VIEW_v2')
  AND it.table_type='VIEW'
  AND NOT EXISTS (
    SELECT 1 FROM main.canonical_column_verification_registry_v1 r
    WHERE r.schema_name=ic.table_schema AND r.table_name=ic.table_name AND r.column_name=ic.column_name
  );


-- -----------------------------------------------------------------------------
-- Section B2 — Insert table-signoff rows for the 2 molecular UNNEST VIEWs
-- -----------------------------------------------------------------------------
INSERT INTO main.canonical_table_signoff_registry_v1
       (schema_name, table_name, n_columns_total, n_verified, n_not_started, n_failed, n_na,
        table_status, signed_off_ts, signoff_migration, priority_tier, notes)
SELECT sub.schema_name, sub.table_name, sub.n_total, sub.n_verified, sub.n_not_started,
       COALESCE(sub.n_failed,0), sub.n_na,
       CASE WHEN sub.n_not_started + COALESCE(sub.n_failed,0) = 0 THEN 'verified'
            WHEN sub.n_verified > 0 THEN 'in_progress'
            ELSE 'not_started' END,
       NULL, NULL, 'tier2_canonical_view',
       ' | mig_164b: seeded table row for molecular UNNEST VIEW layer (mig_164 gap closure).'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified' THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed' THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na' THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main'
    AND table_name IN ('molecular_fusions_unnested_VIEW_v2','molecular_variants_unnested_VIEW_v2')
  GROUP BY 1,2
) sub
WHERE NOT EXISTS (
  SELECT 1 FROM main.canonical_table_signoff_registry_v1 ts0
  WHERE ts0.schema_name=sub.schema_name AND ts0.table_name=sub.table_name
);


-- -----------------------------------------------------------------------------
-- Section C — Flip molecular_fusions_unnested_VIEW_v2 cols to verified (mig_164 §164e methodology)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1 cvr
SET verification_status='verified',
    verified_by='logan',
    verified_ts=CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    verification_method = CASE WHEN cvr.column_name='fusion_pair'
                               THEN 'auto_view_derived_gene_concat_fusion_pair'
                               ELSE 'auto_view_unnest_canonical_molecular_genetics_v2_gene_fusions_list' END,
    batch_id='mig_164b_view_layer_molecular_unnest_register_20260429',
    notes=COALESCE(cvr.notes,'') || ' | mig_164b: UNNEST gene_fusions_list on verified canonical_molecular_genetics_v2; cardinality check §2e (mig_164 gap closure).'
WHERE cvr.schema_name='main'
  AND cvr.table_name='molecular_fusions_unnested_VIEW_v2'
  AND cvr.verification_status='not_started';


-- -----------------------------------------------------------------------------
-- Section D — Flip molecular_variants_unnested_VIEW_v2 cols to verified (mig_164 §164f methodology)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_column_verification_registry_v1 cvr
SET verification_status='verified',
    verified_by='logan',
    verified_ts=CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    verification_method='auto_view_unnest_canonical_molecular_genetics_v2_gene_mutations_variants',
    batch_id='mig_164b_view_layer_molecular_unnest_register_20260429',
    notes=COALESCE(cvr.notes,'') || ' | mig_164b: UNNEST gene_mutations_variants; tert_present echo + HGVS shards per episode line; cardinality §2e (mig_164 gap closure).'
WHERE cvr.schema_name='main'
  AND cvr.table_name='molecular_variants_unnested_VIEW_v2'
  AND cvr.verification_status='not_started';


-- -----------------------------------------------------------------------------
-- Section E — Resync table-signoff registry (mig_159 §159g pattern)
-- -----------------------------------------------------------------------------
UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total=sq.n_total, n_verified=sq.n_verified, n_not_started=sq.n_not_started,
    n_failed=COALESCE(sq.n_failed,0), n_na=sq.n_na,
    table_status=CASE WHEN sq.n_not_started+COALESCE(sq.n_failed,0)=0 THEN 'verified'
                      WHEN sq.n_verified>0 THEN 'in_progress' ELSE 'not_started' END,
    signed_off_ts=CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    signoff_migration='qc_framework_v1/migrations/164b_view_layer_molecular_unnest_register_20260429.sql',
    notes=COALESCE(ts.notes,'') || ' | mig_164b: molecular UNNEST VIEW signoff (mig_164 gap closure).'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified' THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed' THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na' THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main'
    AND table_name IN ('molecular_fusions_unnested_VIEW_v2','molecular_variants_unnested_VIEW_v2')
  GROUP BY 1,2
) sq
WHERE ts.schema_name=sq.schema_name AND ts.table_name=sq.table_name;


-- -----------------------------------------------------------------------------
-- Section F — Post-state probes (commented)
-- -----------------------------------------------------------------------------
-- F1: gate1 should be 167 → 169 after this migration
-- SELECT COUNT(*) FROM main.canonical_table_signoff_registry_v1 WHERE table_status='verified';
-- Expect: 169.
-- F2: 24 col rows now verified across 2 molecular UNNEST VIEWs
-- SELECT table_name, COUNT(*) FILTER (WHERE verification_status='verified') AS n_verified
-- FROM main.canonical_column_verification_registry_v1
-- WHERE table_name IN ('molecular_fusions_unnested_VIEW_v2','molecular_variants_unnested_VIEW_v2')
-- GROUP BY 1;
-- Expect: 11 + 13 = 24.
-- End mig_164b.
