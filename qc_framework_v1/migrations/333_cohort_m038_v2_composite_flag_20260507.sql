-- =============================================================================
-- mig_333 — manuscript_workspace.cohort_m038_massive_goiter_v2
--           Adds pre-computed composite massive-goiter flag columns to the
--           existing M038 cohort base view (v1) so the audit can isolate the
--           2,501-patient massive arm without analysis-time filter replication.
--
-- Date:       2026-05-07
-- Lane:       audit-gap closure — post_mig_086_manuscript_reconciliation_20260507.md §2.11
-- Depends:    mig_255 (cohort_m038_massive_goiter_v1, ~129 cols, 10,871 rows)
-- Author:     Cowork (Claude, Cursor Agent)
--
-- CONTEXT:
--   post_mig_086 audit (2026-05-07) returned CANNOT_VERIFY for M038 because
--   the query `SELECT massive_goiter_flag, COUNT(*) ... GROUP BY 1` raised
--   "column not found" — the v1 view (mig_251/mig_255) was designed to keep
--   computed exposure flags at analysis-query time rather than baked into the
--   view (per mig_251 design notes).  For audit verification and downstream
--   re-use, the composite flag should live in the view.
--
-- NOTE ON ECMO:
--   The manuscript_inventory.md entry for M038 incorrectly labels it as
--   "ECMO Support for Massive Goiter Surgery".  M038 is actually:
--   "Massive Goiter at a Tertiary Referral Center: A Composite-Definition
--   Descriptive Cohort of 2,501 Patients".  There is no ECMO column, ECMO
--   analysis, or ECMO data source in M038.  The inventory title is a
--   labeling error; no ECMO filter is applied here.  See manuscript_inventory.md
--   fix in this same commit.
--
-- SCOPE:
--   CREATE OR REPLACE VIEW manuscript_workspace.cohort_m038_massive_goiter_v2.
--   v1 is preserved (backward compatible for existing analysis scripts).
--   Zero CPM mutations.  Zero DML on any canonical table.
--
-- NEW COLUMNS (appended after all existing v1 columns):
--   is_massive_w          BOOL  gland_weight_final_g >= 100 g (W-axis)
--   is_massive_s          BOOL  CT or MRI substernal extension (S-axis)
--   is_massive_a          BOOL  CT airway compromise/deviation/narrowing (A-axis)
--   is_massive_composite  BOOL  W OR S OR A  (primary composite exposure)
--   gland_weight_known    BOOL  gland_weight_final_g IS NOT NULL
--
-- LOCKED NUMBERS (pre-verified 2026-05-01 in validation report):
--   COUNT(*)                          = 10,871  (full cohort)
--   COUNT(is_massive_composite=TRUE)  =  2,501  (massive arm)
--   COUNT(gland_weight_known=TRUE)    =  9,130  (weight-known analytic subset)
--
-- VERIFY (post-apply):
--   SELECT
--     COUNT(*) AS n_total,                                          -- expect 10871
--     SUM(CASE WHEN is_massive_composite THEN 1 ELSE 0 END) AS n_massive,   -- expect 2501
--     SUM(CASE WHEN gland_weight_known   THEN 1 ELSE 0 END) AS n_wt_known,  -- expect 9130
--     SUM(CASE WHEN is_massive_w         THEN 1 ELSE 0 END) AS n_w,         -- expect 1429
--     SUM(CASE WHEN is_massive_s         THEN 1 ELSE 0 END) AS n_s,         -- expect 1047
--     SUM(CASE WHEN is_massive_a         THEN 1 ELSE 0 END) AS n_a          -- expect 1440
--   FROM manuscript_workspace.cohort_m038_massive_goiter_v2;
-- =============================================================================

USE thyroid_canonical_publication_v1_0;

CREATE OR REPLACE VIEW manuscript_workspace.cohort_m038_massive_goiter_v2 AS
SELECT
  -- All columns from v1 (research_id through biochemical_recurrence_flag)
  v.*,

  -- ------------------------------------------------------------------
  -- COMPOSITE MASSIVE-GOITER FLAG  (mig_333, 2026-05-07)
  --
  -- Exposure definitions per M038 outline §3 Methods + §2 DEFINITION BRIEF:
  --   W (weight): gland_weight_final_g >= 100 g
  --   S (substernal): CT or MRI substernal extension
  --   A (airway): CT tracheal deviation OR narrowing OR airway compromise
  --   Composite (primary): W OR S OR A
  --
  -- COALESCE(... , FALSE) treats NULL source columns as absent / not documented;
  -- consistent with the verification SQL in M038_DEFINITION_READY_FOR_WRITING_BRIEF.md
  -- ------------------------------------------------------------------
  COALESCE(v.gland_weight_final_g >= 100, FALSE)                                         AS is_massive_w,
  COALESCE(v.ct_substernal_extension_any OR v.mri_substernal_any, FALSE)                 AS is_massive_s,
  COALESCE(
      v.ct_tracheal_deviation_any
      OR v.ct_tracheal_narrowing_any
      OR v.ct_airway_compromise_any,
      FALSE
  )                                                                                       AS is_massive_a,
  COALESCE(v.gland_weight_final_g >= 100, FALSE)
      OR COALESCE(v.ct_substernal_extension_any OR v.mri_substernal_any, FALSE)
      OR COALESCE(
             v.ct_tracheal_deviation_any
             OR v.ct_tracheal_narrowing_any
             OR v.ct_airway_compromise_any,
             FALSE)                                                                       AS is_massive_composite,
  (v.gland_weight_final_g IS NOT NULL)                                                   AS gland_weight_known

FROM manuscript_workspace.cohort_m038_massive_goiter_v1 AS v;

-- =============================================================================
-- Post-apply verification (run as separate session after CREATE OR REPLACE):
-- =============================================================================
--
-- Primary check — arm counts must match locked manuscript numbers:
--   SELECT
--     COUNT(*)                                                          AS n_total,     -- 10871
--     SUM(CASE WHEN is_massive_composite THEN 1 ELSE 0 END)           AS n_massive,    -- 2501
--     SUM(CASE WHEN gland_weight_known   THEN 1 ELSE 0 END)           AS n_wt_known,   -- 9130
--     SUM(CASE WHEN is_massive_w         THEN 1 ELSE 0 END)           AS n_w,          -- 1429
--     SUM(CASE WHEN is_massive_s         THEN 1 ELSE 0 END)           AS n_s,          -- 1047
--     SUM(CASE WHEN is_massive_a         THEN 1 ELSE 0 END)           AS n_a,          -- 1440
--     SUM(CASE WHEN is_massive_composite AND NOT gland_weight_known
--              THEN 1 ELSE 0 END)                                      AS n_massive_wt_unknown
--   FROM manuscript_workspace.cohort_m038_massive_goiter_v2;
--
-- Cross-tab sanity (8-cell, from definition brief — weight-known subset only):
--   SELECT
--     is_massive_w AS e1, is_massive_s AS e2, is_massive_a AS e3,
--     COUNT(*) AS n,
--     SUM(CASE WHEN any_confirmed_complication_flag THEN 1 ELSE 0 END) AS n_comp
--   FROM manuscript_workspace.cohort_m038_massive_goiter_v2
--   WHERE gland_weight_known
--   GROUP BY e1, e2, e3 ORDER BY e1 DESC, e2 DESC, e3 DESC;
--   -- Row (T,T,T): expect n=179
--   -- Row (F,F,F): expect n=7647
--
-- =============================================================================
-- signoff_migration row (apply after verification):
-- =============================================================================
--   INSERT INTO signoff_migration.mig_333 VALUES (
--     'mig_333',
--     'manuscript_workspace.cohort_m038_massive_goiter_v2',
--     '2026-05-07',
--     'Cowork',
--     'PASS',
--     'n_massive=2501 MATCH; n_w=1429, n_s=1047, n_a=1440; gand_weight_known=9130'
--   );
-- =============================================================================
-- End mig_333
-- =============================================================================
