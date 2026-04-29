# Cursor Prompt — mig_174b: cnln_img_laterality multi-label apply (Option A: per-side BOOLEANs)

**Date:** 2026-04-29
**Lane:** 67 / mig_174b
**Batch:** `mig_174b_cnln_img_laterality_per_side_boolean_20260429`
**Predecessor design:** mig_174a (committed `955801f`) — read-only token-level audit
**Logan ratification:** 2026-04-29 — **Option A** chosen (per-side BOOLEAN columns; preserve raw VARCHAR as legacy)
**Posture:** SQL-only authoring; commit + push. **DO NOT execute against MotherDuck** — Cowork applies via Path C.
**Target DB:** `thyroid_canonical_publication_v1_0`
**Target table:** `main.canonical_patient_master`

---

## Mission

Reform the multi-label semicolon-delimited VARCHAR columns into per-side BOOLEAN columns for clean analyst access. The mig_168 audit showed values like `'left; bilateral'`, `'null; bilateral'` (literal string `'null'` as a token), `'right; bilateral; left'`, with whitespace + casing drift.

**Logan's directive: Option A** — per-side BOOLEANs added; raw VARCHAR preserved as legacy for audit.

---

## Scope (3 columns)

1. **Primary target:** `cnln_img_laterality` (VARCHAR, multi-label)
2. **Verify structurally similar (probe before applying same pattern):** `lateral_levels_v10`, `ene_levels_v9`

For each of the 3 columns, run the **token-level enumeration probe** first (mig_174a-style read-only):

```sql
WITH tokens AS (
  SELECT
    column_name,
    research_id,
    TRIM(LOWER(t)) AS token_norm
  FROM (
    SELECT 'cnln_img_laterality' AS column_name, research_id, UNNEST(string_split(cnln_img_laterality, ';')) AS t
    FROM main.canonical_patient_master WHERE cnln_img_laterality IS NOT NULL
    UNION ALL
    SELECT 'lateral_levels_v10', research_id, UNNEST(string_split(lateral_levels_v10, ';')) AS t
    FROM main.canonical_patient_master WHERE lateral_levels_v10 IS NOT NULL
    UNION ALL
    SELECT 'ene_levels_v9', research_id, UNNEST(string_split(ene_levels_v9, ';')) AS t
    FROM main.canonical_patient_master WHERE ene_levels_v9 IS NOT NULL
  )
)
SELECT column_name, token_norm, COUNT(*) AS n_appearances, COUNT(DISTINCT research_id) AS n_pts
FROM tokens GROUP BY 1, 2 ORDER BY 1, 3 DESC;
```

Embed the live result in the SQL header so Cowork can verify token coverage.

---

## Required canonical token mapping for `cnln_img_laterality`

Derived from mig_174a token enumeration. Token normalization: trim whitespace → lowercase → drop literal `'null'` and empty strings → map to canonical token.

| Raw token (lowercase, trimmed) | Canonical token | Action |
|---|---|---|
| `left` | `left` | mark `cnln_img_left_present = TRUE` |
| `right` | `right` | mark `cnln_img_right_present = TRUE` |
| `central` | `central` | mark `cnln_img_central_present = TRUE` |
| `bilateral` | `bilateral` | mark `cnln_img_bilateral_present = TRUE` |
| `lateral_neck`, `lateral neck`, `lateral` | `lateral_neck` | mark `cnln_img_lateral_neck_present = TRUE` |
| `null`, `nan`, `none`, `n/a`, `unknown` | (drop) | sentinel — do not mark any flag |
| `` (empty after trim) | (drop) | empty token from `; ;` artifacts |

If your live token enumeration finds ANY token not in this list, **HALT** and emit a `CF-mig174b-UNHANDLED-TOKEN-<token>` carry-forward — do not silently default to FALSE. Emit one CF row per unhandled token.

---

## Apply SQL skeleton (commit `qc_framework_v1/migrations/174b_cnln_img_laterality_per_side_boolean_20260429.sql`)

```sql
-- =============================================================================
-- Migration 174b — cnln_img_laterality multi-label apply (Option A per-side BOOLEANs)
-- =============================================================================
-- Date: 2026-04-29 (UTC)
-- Author: <Cursor agent name> + Logan Glosser <logan.glosser@gmail.com>
-- Predecessor: mig_174a design (`955801f`)
-- batch_id: mig_174b_cnln_img_laterality_per_side_boolean_20260429
--
-- EFFECT:
--   * ALTER TABLE canonical_patient_master ADD COLUMN ×5 BOOLEAN (left/right/central/bilateral/lateral_neck)
--   * UPDATE populates the new BOOLEANs from cnln_img_laterality token parse
--   * REGISTRY rows registered as not_started → flipped to verified post-apply
--   * Raw cnln_img_laterality VARCHAR PRESERVED as legacy
--
-- =============================================================================

-- §A — Pre-snapshot
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_pre_mig174b_cnln_laterality_20260429 AS
SELECT research_id, cnln_img_laterality,
       CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig174b_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master
WHERE cnln_img_laterality IS NOT NULL;

-- §B — Add 5 per-side BOOLEAN columns (default FALSE; NULL = "no laterality data")
ALTER TABLE main.canonical_patient_master ADD COLUMN cnln_img_left_present BOOLEAN;
ALTER TABLE main.canonical_patient_master ADD COLUMN cnln_img_right_present BOOLEAN;
ALTER TABLE main.canonical_patient_master ADD COLUMN cnln_img_central_present BOOLEAN;
ALTER TABLE main.canonical_patient_master ADD COLUMN cnln_img_bilateral_present BOOLEAN;
ALTER TABLE main.canonical_patient_master ADD COLUMN cnln_img_lateral_neck_present BOOLEAN;

-- §C — Populate via token parse (NULL stays NULL when raw is NULL)
WITH tokens AS (
  SELECT research_id,
         TRIM(LOWER(t)) AS tok
  FROM (
    SELECT research_id, UNNEST(string_split(cnln_img_laterality, ';')) AS t
    FROM main.canonical_patient_master
    WHERE cnln_img_laterality IS NOT NULL
  )
), tok_clean AS (
  SELECT research_id, tok
  FROM tokens
  WHERE tok NOT IN ('null', 'nan', 'none', 'n/a', 'unknown', '')
), per_pt AS (
  SELECT research_id,
         BOOL_OR(tok = 'left') AS has_left,
         BOOL_OR(tok = 'right') AS has_right,
         BOOL_OR(tok = 'central') AS has_central,
         BOOL_OR(tok = 'bilateral') AS has_bilateral,
         BOOL_OR(tok IN ('lateral_neck', 'lateral neck', 'lateral')) AS has_lateral_neck
  FROM tok_clean
  GROUP BY 1
)
UPDATE main.canonical_patient_master pm
SET cnln_img_left_present = COALESCE(per_pt.has_left, FALSE),
    cnln_img_right_present = COALESCE(per_pt.has_right, FALSE),
    cnln_img_central_present = COALESCE(per_pt.has_central, FALSE),
    cnln_img_bilateral_present = COALESCE(per_pt.has_bilateral, FALSE),
    cnln_img_lateral_neck_present = COALESCE(per_pt.has_lateral_neck, FALSE)
FROM per_pt
WHERE pm.research_id = per_pt.research_id;

-- §D — Register the 5 new cols in canonical_column_verification_registry_v1
-- (initial status='not_started', then flip below in §E once derivation confirmed)
INSERT INTO main.canonical_column_verification_registry_v1
       (schema_name, table_name, column_name, data_type, ordinal_position,
        category, upstream_source, verification_status, verified_by, verified_ts,
        verification_method, batch_id, notes)
SELECT 'main', 'canonical_patient_master', col_name, 'BOOLEAN',
       (SELECT MAX(ordinal_position) FROM main.canonical_column_verification_registry_v1
         WHERE schema_name='main' AND table_name='canonical_patient_master') + ROW_NUMBER() OVER (ORDER BY col_name),
       'derived', NULL, 'not_started', NULL, NULL, NULL, NULL,
       'mig_174b: per-side BOOLEAN derived from cnln_img_laterality token parse (Option A; legacy VARCHAR preserved).'
FROM (VALUES
  ('cnln_img_left_present'),
  ('cnln_img_right_present'),
  ('cnln_img_central_present'),
  ('cnln_img_bilateral_present'),
  ('cnln_img_lateral_neck_present')
) v(col_name)
WHERE NOT EXISTS (
  SELECT 1 FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_patient_master' AND column_name=v.col_name
);

-- §E — Flip the 5 new cols to verified (cohort-uniformity sweep included as note)
UPDATE main.canonical_column_verification_registry_v1
SET verification_status='verified',
    verified_by='logan',
    verified_ts=CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    verification_method='derivation_vs_cnln_img_laterality_token_parse',
    batch_id='mig_174b_cnln_img_laterality_per_side_boolean_20260429',
    notes=COALESCE(notes,'')
          || ' | mig_174b: token-parse-derived from raw cnln_img_laterality. '
          || 'Cohort uniformity per-col: <embed live T/F/N counts here from §C post-state>. '
          || 'CF-mig174b-COHORT-UNIFORM-* informational.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND column_name IN (
    'cnln_img_left_present','cnln_img_right_present','cnln_img_central_present',
    'cnln_img_bilateral_present','cnln_img_lateral_neck_present'
  );

-- §F — Append CF on the legacy raw cnln_img_laterality col
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | mig_174b: legacy multi-label VARCHAR; PREFER per-side BOOLEAN columns '
            || '(cnln_img_<left|right|central|bilateral|lateral_neck>_present) for analytic use. '
            || 'Raw VARCHAR retained for audit/provenance; token parsing logic preserves all non-sentinel tokens.'
WHERE schema_name='main' AND table_name='canonical_patient_master' AND column_name='cnln_img_laterality';

-- §G — Resync canonical_table_signoff_registry_v1 row for canonical_patient_master
UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total = subq.n_total,
    n_verified      = subq.n_verified,
    n_not_started   = subq.n_not_started,
    n_failed        = COALESCE(subq.n_failed, 0),
    n_na            = subq.n_na,
    table_status    = CASE WHEN subq.n_not_started+COALESCE(subq.n_failed,0)=0 THEN 'verified'
                           WHEN subq.n_verified>0 THEN 'in_progress' ELSE 'not_started' END,
    signed_off_ts   = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes = COALESCE(ts.notes,'') || ' | mig_174b: +5 per-side BOOLEAN cols from cnln_img_laterality multi-label parse.'
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified' THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed' THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na' THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  WHERE schema_name='main' AND table_name='canonical_patient_master'
  GROUP BY 1,2
) subq
WHERE ts.schema_name=subq.schema_name AND ts.table_name=subq.table_name;

-- §H — Post-state probes (commented; Cowork runs after apply)
-- H1: 5 new cols populated correctly
-- SELECT cnln_img_laterality,
--        cnln_img_left_present, cnln_img_right_present, cnln_img_central_present,
--        cnln_img_bilateral_present, cnln_img_lateral_neck_present, COUNT(*)
-- FROM main.canonical_patient_master
-- WHERE cnln_img_laterality IS NOT NULL
-- GROUP BY 1,2,3,4,5,6 ORDER BY 7 DESC LIMIT 25;

-- H2: NULL-respecting probe — when raw is NULL, all 5 should be NULL
-- SELECT COUNT(*) FROM main.canonical_patient_master
-- WHERE cnln_img_laterality IS NULL
--   AND (cnln_img_left_present IS NOT NULL OR cnln_img_right_present IS NOT NULL OR
--        cnln_img_central_present IS NOT NULL OR cnln_img_bilateral_present IS NOT NULL OR
--        cnln_img_lateral_neck_present IS NOT NULL);
-- Expect: 0.

-- =============================================================================
-- end mig_174b
-- =============================================================================
```

---

## Conditional sister-lane: lateral_levels_v10 / ene_levels_v9

After the `cnln_img_laterality` work, run the same token enumeration probe on `lateral_levels_v10` and `ene_levels_v9`. If they have similar semicolon-delimited multi-label structure with cleanly-enumerable canonical tokens, propose a **mig_174c** prompt extending the same pattern. If their structure is materially different (e.g., level numbers like `2,3,4` instead of side names), surface the difference + propose a separate design lane (`mig_174d`).

DO NOT mix lateral_levels / ene_levels into mig_174b — keep them separate apply lanes.

---

## Governance reminders

- Read-only probes only in this Cursor session; commit + push SQL artifact and the token-enumeration result document. Do **NOT** call any RW tool.
- Cowork executes the apply via Path C with pre-snapshot verification.
- If you encounter ANY token not in the canonical mapping, HALT and emit `CF-mig174b-UNHANDLED-TOKEN-<token>` — do not silently coerce to FALSE.
- Linting: ensure `BEGIN TRANSACTION;` / `COMMIT;` are NOT used inside the SQL (DuckDB MCP wrapper applies one statement per call).
- Author = `Logan Glosser <logan.glosser@gmail.com>` for all commits.

---

## Deliverables

1. `qc_framework_v1/migrations/174b_cnln_img_laterality_per_side_boolean_20260429.sql` — apply SQL with embedded live token-enumeration result in §A header
2. `qc_framework_v1/reports/mig_174b_token_enumeration_20260429.md` — read-only token enumeration result (every distinct trimmed-lowercased token + count + n_pts) for all 3 candidate columns
3. Optional: `cursor_prompts/CURSOR_PROMPT_mig174c_lateral_levels_v10_apply_20260429.md` if `lateral_levels_v10` is structurally similar enough to extend the same pattern

Commit with message `qc: mig_174b cnln_img_laterality per-side BOOLEAN apply (Option A — Logan-ratified 2026-04-29)` and push to origin/main.

---

End of prompt.
