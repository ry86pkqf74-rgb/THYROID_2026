# Cursor handoff: canonical_molecular_genetics_v2 completeness pass

**Recommendation: do this in Cursor.** Cowork's BQ MCP is read-only. This needs:
- Canonical-layer writes (new rows + audit columns + episode-id rebuild)
- Possibly new source-table parser passes
- Skill version bump to v2.3.0 (minor — schema/coverage change)

Builds on the audit at `studies/proposal_2to4cm_extent_molecular_20260326/elicit_expansion_20260509/GENETICS_COMPLETENESS_AUDIT_20260513.md`.

---

## Goal

Resolve three findings from the 2026-05-13 Cowork genetics-completeness audit:
1. Canonical layer covers only 1,151/10,862 (10.6%) of patients with source-table molecular content
2. 65% of canonical rows have NO test date
3. `molecular_episode_id` is broken (4 distinct values across 1,384 rows)

Plus a parse-status escalation pass on the 76% non-ok rows.

## Hard rules (from `.cowork/skills/thyroid-integration/SKILL.md`)
1. No PHI in Airtable / Linear / committed code. Aggregate counts and pattern classes only.
2. Append-only. Snapshot `canonical_molecular_genetics_v2` to `pub_archive.canonical_molecular_genetics_v2_pre_completeness_pass_<YYYYMMDD>` before MERGE.
3. Log DFL row BEFORE the canonical edit (`appJYOnUb7KrHKwpV.tblsiYKJtKcktkzze`, `target_type=column`, `change_type=data_correction`).
4. Append new MFL row `MFL-<YYYYMMDD>-EXT2-4-CANONICAL-COMPLETENESS-PASS` linked to EXT2-4 (`rec1GJyrmKdKxjlaY`).
5. File `VC-MOL-COVERAGE-001` (new) at severity `high`. Link to EXT2-4.
6. File the Notable Finding `NF-<YYYYMMDD>-canonical-molecular-coverage-gap` in the Airtable Notable Findings table.
7. Mandatory pre-bump verified-state check before bumping `thyroid-integration` to v2.3.0.

## Phase-by-phase plan

### Phase 1 — Size the orphan-with-real-test population (read-only diagnostic, ~10 min)

Classify the 9,711 source-table orphans by whether they contain a strong named-platform signal in source text:

```sql
WITH orphan_pts AS (
  SELECT DISTINCT research_id
  FROM `thyroid-canonical-pub-2026.pub_canonical.thyroseq_molecular_enrichment`
  WHERE (pathology_raw IS NOT NULL OR mutation_raw IS NOT NULL)
    AND research_id NOT IN (
      SELECT DISTINCT research_id
      FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_molecular_genetics_v2`
    )
),
sig AS (
  SELECT
    o.research_id,
    REGEXP_CONTAINS(LOWER(IFNULL(e.pathology_raw,'')),
      r'(afirma gec|afirma gsc|afirma gene expression|thyroseq v[23]|risk of malignancy ~?\d{1,3}|thyroseq.*positive|thyroseq.*negative)')
    AS strong_signal,
    e.molecular_platform,
    LOWER(IFNULL(e.gep_norm, ''))  AS gep_norm_lc
  FROM orphan_pts o
  LEFT JOIN `thyroid-canonical-pub-2026.pub_canonical.thyroseq_molecular_enrichment` e USING (research_id)
)
SELECT
  COUNT(DISTINCT research_id) AS n_orphans,
  COUNTIF(strong_signal) AS n_strong_signal,
  COUNTIF(LENGTH(IFNULL(molecular_platform,''))>0) AS n_with_platform_string,
  COUNTIF(gep_norm_lc IN ('afirma','thyroseq','quest diagnostics','thyroseq v3','thyroseq (v2)')) AS n_with_recognized_gep_norm
FROM sig;
```

Capture the numbers. Expected ranges:
- `n_orphans` ≈ 9,711
- `n_strong_signal` ≈ 100–500 (these are the real missed commercial tests — category C from the audit)
- `n_with_platform_string` ≈ 500–2,000 (broader category — needs review)

If `n_strong_signal` < 100: the orphan recovery has small manuscript impact; document and proceed to Phase 2.
If `n_strong_signal` 100–500: targeted manual review + canonical-row creation. Recommended path.
If `n_strong_signal` > 500: parser-rebuild needed; this is a bigger project than a single Cursor session.

### Phase 2 — Date backfill (the highest-impact, lowest-risk fix)

Add audit column `resolved_test_date_source STRING` to `canonical_molecular_genetics_v2`.

Backfill `resolved_test_date` from these sources in priority order:

```sql
-- Build a staging table with proposed dates
CREATE OR REPLACE TABLE `pub_workspace.cmg_date_backfill_staging_<YYYYMMDD>` AS
SELECT
  g.molecular_episode_id,
  g.research_id,
  g.report_source_table,
  -- Priority 1: native or already-resolved
  g.test_date_native,
  g.resolved_test_date,
  -- Priority 2: FNA linkage
  fna.fna_date_resolved AS proposed_date_from_fna,
  -- Priority 3: surgery linkage (use 14 days before surgery as test-date proxy)
  DATE_SUB(op.resolved_surgery_date, INTERVAL 14 DAY) AS proposed_date_from_surgery,
  -- Priority 4: imported_at as fallback (file upload date)
  DATE(SAFE_CAST(e.imported_at AS TIMESTAMP)) AS proposed_date_from_imported_at,
  -- Decision: pick the highest-priority non-NULL
  COALESCE(
    g.resolved_test_date,
    g.test_date_native,
    fna.fna_date_resolved,
    DATE_SUB(op.resolved_surgery_date, INTERVAL 14 DAY),
    DATE(SAFE_CAST(e.imported_at AS TIMESTAMP))
  ) AS proposed_resolved_date,
  CASE
    WHEN g.resolved_test_date IS NOT NULL OR g.test_date_native IS NOT NULL THEN 'native'
    WHEN fna.fna_date_resolved IS NOT NULL THEN 'fna_linkage'
    WHEN op.resolved_surgery_date IS NOT NULL THEN 'surgery_linkage_minus_14d'
    WHEN e.imported_at IS NOT NULL THEN 'imported_at_fallback'
    ELSE 'unresolvable'
  END AS proposed_resolved_date_source
FROM `pub_canonical.canonical_molecular_genetics_v2` g
LEFT JOIN `pub_canonical.canonical_fna_events_v1` fna
  ON fna.fna_event_id = CAST(g.fna_episode_id AS STRING)
LEFT JOIN `pub_canonical.canonical_operative_events_v1` op
  ON op.surgery_episode_id = g.surgery_episode_id
LEFT JOIN `pub_canonical.thyroseq_molecular_enrichment` e
  ON e.research_id = g.research_id;
```

MERGE-update `canonical_molecular_genetics_v2.resolved_test_date` ONLY where current value is NULL AND `proposed_resolved_date IS NOT NULL`. Set `resolved_test_date_source` on every row (including those already populated, mark as `native`).

Acceptance: `frac_with_date` ≥ 90% (currently 35%). LLM extraction from `report_text_ref` can be a Phase 2.5 if 90% is still missed.

### Phase 3 — Rebuild `molecular_episode_id`

Replace the current broken column with a deterministic hash:

```sql
UPDATE `pub_canonical.canonical_molecular_genetics_v2` g
SET molecular_episode_id_v2 =
  FARM_FINGERPRINT(CONCAT(
    g.research_id,
    '|',
    CAST(IFNULL(g.resolved_test_date, DATE '1900-01-01') AS STRING),
    '|',
    IFNULL(g.platform, '_unknown'),
    '|',
    IFNULL(g.report_source_table, '_unknown')
  ))
WHERE TRUE;
```

(`molecular_episode_id_v2` is a new column; preserve the old one for audit.)

Add `test_dedup_key`:
```sql
test_dedup_key = FARM_FINGERPRINT(CONCAT(
  research_id, '|',
  CAST(IFNULL(resolved_test_date, DATE '1900-01-01') AS STRING), '|',
  IFNULL(platform, '_unknown')
))
```

Multi-canonical-row patients with the same `test_dedup_key` → flag as duplicate. The 16 guard-cleanup rows already identified (see `guard_rows_16_decision_matrix.csv`) will all share their Afirma companion's `test_dedup_key`. Add this check to the acceptance criteria.

### Phase 4 — Orphan recovery (conditional on Phase 1 outcome)

If Phase 1 found `n_strong_signal` between 100 and 500:
- Pull the source-text rows for those patients
- Run the existing parsers (ThyroSeq detailed, Afirma result-field) on them
- Create new canonical rows with `band_source = 'orphan_recovery_<YYYYMMDD>'`
- Verify against histology to bound the manuscript impact (does the orphan-recovered cohort shift Table 3 v3?)

If Phase 1 found `n_strong_signal` > 500: STOP, escalate as `NF-<YYYYMMDD>-canonical-coverage-major-gap` at severity `publishable`, and propose a multi-week canonical-builder rebuild as a separate workstream.

### Phase 5 — Parse-status escalation pass

For all canonical rows where:
- `parse_status IN ('partial','no_detailed_block','minimal','empty_block')`
- AND `report_text_length > 0`
- AND `overall_result_class IS NULL`

Re-run the parser stack (mig_321 ROM% fallback + mig_323 Afirma `result`-field + new ROM-band inference for ranges). Add `parse_status_v2` audit column tracking the new exit point. Don't overwrite the old `parse_status` — preserve for audit.

### Phase 6 — Verification

```sql
-- Coverage check (post-fix)
SELECT
  ROUND(SAFE_DIVIDE(COUNTIF(resolved_test_date IS NOT NULL), COUNT(*)),3) AS frac_with_date,
  ROUND(SAFE_DIVIDE(COUNTIF(molecular_episode_id_v2 IS NOT NULL), COUNT(*)),3) AS frac_with_episode_id_v2,
  COUNT(DISTINCT molecular_episode_id_v2) AS n_distinct_episodes_v2,
  COUNT(*) AS n_rows
FROM `pub_canonical.canonical_molecular_genetics_v2`;
```

Acceptance criteria:
- `frac_with_date` ≥ 0.90
- `n_distinct_episodes_v2` ≥ 0.95 × `n_rows` (most rows should be unique tests)
- 0 patients with 3+ rows sharing the same `test_dedup_key` (legitimate dual-platform pairs are OK; triples or higher are duplicates)
- Pre-merge snapshot row count matches pre-state

### Phase 7 — Manuscript package refresh (if Phase 4 found real orphans)

Only required if orphan recovery materially changed the Table 3 v3 numbers. Otherwise skip — the EXT2-4 v3 package stays as-is.

If required:
- Re-run `sql/04b_table3_v2_actual_reported_call.sql`
- Update `build_table3_v2_actual_call.py`, `build_figures_v2.py`, `build_manuscript_docx.js` headline numbers
- Rebuild `manuscript_v3_draft.docx` and the zip
- Move v3 → `superseded_v3/` with a SUPERSEDED_NOTE; produce a v4 package

### Phase 8 — Skill version bump

After Phases 1–6 pass acceptance: bump `thyroid-integration` v2.2.0 → v2.3.0 with CHANGELOG entry documenting the coverage pass.

## Acceptance criteria

- [ ] `frac_with_date` ≥ 0.90 on `canonical_molecular_genetics_v2`
- [ ] `molecular_episode_id_v2` populated on ≥ 95% of rows; ≥ 0.95 × n_rows distinct values
- [ ] `test_dedup_key` populated on 100% of rows; 0 patients with 3+ same-key rows
- [ ] Pre-merge snapshot at `pub_archive.canonical_molecular_genetics_v2_pre_completeness_pass_<YYYYMMDD>`
- [ ] No-regression check: 0 pre-existing non-NULL `resolved_test_date` values changed
- [ ] DFL row, MFL row, VC-MOL-COVERAGE-001 row, Notable Finding row all present and consistent
- [ ] Skill version bumped to v2.3.0 with CHANGELOG entry
- [ ] If Phase 7 ran: v3 deliverables in `superseded_v3/`, v4 deliverables in parent folder
- [ ] EXT2-4 lifecycle still Active

## When this is done, hand back to Cowork for

- Pre-submission cohort decision (item 1 from the prior next-session list) — depends on the date backfill being complete to support era stratification claims
- Final manuscript prose pass on the v4 numbers if Phase 7 ran
- Any Notable-Finding escalation in Airtable
