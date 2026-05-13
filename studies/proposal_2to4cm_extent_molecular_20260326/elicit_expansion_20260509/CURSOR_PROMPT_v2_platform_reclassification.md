# Cursor handoff v2: platform-mislabeling correction + Afirma rescue path

**Recommendation: do this in Cursor.** Cowork's BQ MCP is read-only and this requires another canonical-layer MERGE plus a Verification Check. Builds directly on the v1 Cursor handoff (parser fallback work; complete) — see `MFL-20260509-EXT2-4-PARSER-FIX-REFRESH` (`recRImNEcxZYbRYnQ`) and `VC-MOL-PARSE-001` (`rec6xTvsRN6KHqqGa`).

**Context: why we are NOT just tightening the ThyroSeq parser further.**
After the Cursor v1 ThyroSeq parser fix landed (band coverage 83.8% global, 94% on the manuscript subset), Cowork ran a second-look diagnostic on the remaining 141 `manual_review` rows. The dominant finding is not a regex gap — it is **platform mislabeling at the canonical layer**: roughly 56/141 of these rows have `canonical_molecular_genetics_v2.platform = 'ThyroSeq'` but the source `pub_canonical.thyroseq_molecular_enrichment.gep_norm` (or the legacy `pub_canonical.molecular_testing.thyroseq_afirma` / `genetic_test`) clearly identifies the test as Afirma or Quest Diagnostics in-house. The reason these rows are in `manual_review` is that the ThyroSeq parser cannot find a ThyroSeq band — because it isn't a ThyroSeq test.

For these mislabeled-Afirma rows, the actual reported Afirma call is sitting in `pub_canonical.molecular_testing.result` in unambiguous patterns: `Suspicious (10% ROM)`, `Suspicious (>50% ROM)`, `Benign`, etc. The current canonical layer does not capture these calls because (a) the platform field routes the row to the ThyroSeq parser and (b) the `result` field is not part of the ThyroSeq parser input.

## Goal
1. **Reclassify** mislabeled rows in `canonical_molecular_genetics_v2` using a clear, deterministic source-of-truth waterfall (`gep_norm` → `thyroseq_afirma` → `genetic_test` keywords). Snapshot before, MERGE only when canonical disagrees with source-of-truth, never overwrite cases where canonical and source agree.
2. **Rescue** Afirma calls for the reclassified rows (and any pre-existing Afirma rows with `overall_result_class IS NULL`) by extracting the binary call + numeric ROM% from `molecular_testing.result` using a small new regex set. Add `band_source = 'afirma_result_field'`.
3. **Refresh** the manuscript v2 → v3 package so the user finally has consistent docx + tables + figures + zip with the corrected numbers.
4. **Decide** whether the 95% global coverage gate is now met → bump skill to v2.2.0 if yes; otherwise document remaining gap and defer.

## Hard rules (NON-NEGOTIABLE — same as the prior handoff)
1. No PHI in Airtable, Linear, or any committed code/log. The `result` column is a structured short field (~10–25 chars) but treat its raw values as PHI-adjacent — never quote a specific patient's `result` string in any audit trail; only quote pattern classes (`'suspicious_with_rom'`, `'suspicious_no_rom'`, etc.).
2. Append-only. Snapshot to `pub_archive.canonical_molecular_genetics_v2_pre_platform_reclass_<YYYYMMDD>` before MERGE.
3. **Log Data Feedback Log row BEFORE the canonical edit** (`tblsiYKJtKcktkzze`). `target_type=column`, `change_type=data_correction`. Reference the existing VC.
4. **File / update Verification Check** `VC-MOL-PLATFORM-001` (new) for the platform reclassification. Severity = high (it changes diagnostic-performance numbers in an active Drafting manuscript). Link to `EXT2-4` (`rec1GJyrmKdKxjlaY`).
5. **Update `VC-MOL-PARSE-001`** (`rec6xTvsRN6KHqqGa`) post-fix — describe what fraction of `manual_review` was rescued via reclassification vs ROM-text re-parse.
6. **Append a new MFL row** `MFL-<YYYYMMDD>-EXT2-4-PLATFORM-RECLASS-REFRESH` linked to `EXT2-4`. Builds on the audit chain: `MFL-20260509-EXT2-4-ELICIT-EXPANSION` → `…-TABLE3-CORRECTION` → `…-FULL-PACKAGE-v2` → `…-PARSER-FIX-REFRESH` → THIS ROW.
7. **Skill version bump**: only after Phase 4 verification AND Phase 5 manuscript refresh complete; bump `thyroid-integration` v2.1.0 → **v2.2.0** with full CHANGELOG entry citing both the parser fix (prior session) and this reclassification.

## Step-by-step plan

### Phase 1 — verify the diagnosis (read-only, ~15 min)
1.1 Confirm the platform-mislabel count by re-running the cross-tab Cowork captured (counts by `gep_norm_class` × Bethesda). Expected ranges: gep_norm_afirma ~56±5, gep_norm_thyroseq ~46±5, gep_norm_quest ~16±2, other/null ~23±5. If the numbers diverge by >10, stop and surface.
1.2 Sample 5–10 reclassification candidates and read `molecular_testing.thyroseq_afirma`, `genetic_test`, `genetic_test_performed`, `result` columns alongside `thyroseq_molecular_enrichment.gep_norm`, `gep_raw`. Build the **source-of-truth waterfall** rule:
  - **Tier 1**: `thyroseq_molecular_enrichment.gep_norm` ∈ {`afirma`, `afirma; afirma`} → reclassify to `Afirma`.
  - **Tier 2**: `thyroseq_molecular_enrichment.gep_norm` LIKE `%thyroseq%` → keep `ThyroSeq`.
  - **Tier 3**: `thyroseq_molecular_enrichment.gep_norm` LIKE `%quest%` → reclassify to `Other` (Quest Diagnostics in-house panel; not Afirma or ThyroSeq).
  - **Tier 4**: fall back to `molecular_testing.thyroseq_afirma` / `genetic_test` keywords (`afirma` / `thyroseq` / `quest`).
  - **Tier 5**: if no source signal, leave platform unchanged but flag with `platform_reclass_status = 'unresolved'`.
1.3 Spot-check that the 506 rows already classified by the v1 parser fix (band_source ∈ {`reported_text`, `numeric_rom_inferred`}) have the **correct** platform per the same waterfall. If any have `platform = 'ThyroSeq'` but `gep_norm = 'afirma'`, those need reclassification too — but the band/ROM the parser extracted will be invalid (it would be the Afirma binary call routed through the ThyroSeq parser). Surface the count; expect this to be small (≤10) but treat it carefully.

### Phase 2 — Afirma call extractor for `molecular_testing.result`
Add a small focused module (e.g., `molecular_consolidation_20260421/afirma_result_field_parser.py` or extend the existing parser). Patterns to match (case-insensitive):

```python
import re

AFIRMA_RESULT_PATTERNS = [
    # Suspicious with explicit ROM%
    (r'\bsuspicious\s*\(\s*(\d{1,3})\s*%\s*rom\s*\)', 'positive', 'point'),
    # Suspicious with bounded ROM%
    (r'\bsuspicious\s*\(\s*>\s*(\d{1,3})\s*%\s*rom\s*\)', 'positive', 'lower'),
    (r'\bsuspicious\s*\(\s*>\s*(\d{1,3})\s*%\s*\)', 'positive', 'lower'),
    (r'\bsuspicious\s*\(\s*(\d{1,3})\s*%\s*\)', 'positive', 'point'),
    # Suspicious alone
    (r'\bsuspicious\b', 'positive', None),
    # Common typo
    (r'\bsuspicous\b', 'positive', None),
    # Benign / Negative
    (r'\bbenign\b', 'negative', None),
    (r'^negative\b', 'negative', None),  # at start of string
    # Positive (rarer; usually GSC + Xpression Atlas mutation positive)
    (r'\bpositive\b', 'positive', None),
    # Non-diagnostic
    (r'\b(insufficient|inadequate|cancelled|non[- ]diagnostic)\b', 'non_diagnostic', None),
]

def parse_afirma_result(result_text: str) -> dict:
    """Parse Afirma `molecular_testing.result` field into overall_result_class + ROM%."""
    if not result_text:
        return {'overall_result_class': None, 'rom_percent_point': None,
                'rom_percent_low': None, 'band_source': None}
    s = result_text.strip().lower()
    for pat, cls, rom_kind in AFIRMA_RESULT_PATTERNS:
        m = re.search(pat, s)
        if m:
            out = {'overall_result_class': cls, 'band_source': 'afirma_result_field',
                   'rom_percent_point': None, 'rom_percent_low': None}
            if rom_kind == 'point' and m.lastindex:
                out['rom_percent_point'] = float(m.group(1))
            elif rom_kind == 'lower' and m.lastindex:
                out['rom_percent_low'] = float(m.group(1))
                # For >X% ROM, leave point null but record the lower bound
            return out
    return {'overall_result_class': None, 'rom_percent_point': None,
            'rom_percent_low': None, 'band_source': None}
```

Add unit tests covering each pattern (Suspicious with `(15% ROM)`, with `(>50% ROM)`, alone, the typo, Benign, Negative-with-suffix, Inadequate, Cancelled, empty string, malformed).

**Critical rule**: this parser handles **Afirma-only**. Do not run it on rows that source-of-truth says are ThyroSeq. ThyroSeq has different vocabulary (LOW/INTERMEDIATE/HIGH bands, not Suspicious/Benign).

### Phase 3 — combined MERGE pipeline (`scripts/mig_322_platform_reclass_and_afirma_rescue.py`)
3.1 Snapshot: `CREATE OR REPLACE TABLE pub_archive.canonical_molecular_genetics_v2_pre_platform_reclass_<YYYYMMDD> AS SELECT * FROM pub_canonical.canonical_molecular_genetics_v2`.
3.2 Build a staging table `pub_workspace.canonical_molecular_genetics_v2_platform_reclass_staging_<YYYYMMDD>` with columns: `molecular_episode_id`, `research_id`, `current_platform`, `proposed_platform`, `proposed_platform_source` (which tier of the waterfall fired), `proposed_overall_result_class`, `proposed_rom_percent_point`, `proposed_rom_percent_low`, `proposed_band_source`. Run the reclassification waterfall + Afirma call extractor against ALL `canonical_molecular_genetics_v2` rows (not just the 141 manual_review — also re-check the 506 already-classified rows for platform consistency, per Phase 1.3).
3.3 Validate: emit a diff report (`scripts/output/mig_322_diff_report_<YYYYMMDD>.md`) summarizing:
  - n_rows_proposed_platform_change
  - For platform changes: split by `current_platform → proposed_platform`
  - n_rows_proposed_call_change (where current call was NULL/unclassified and Afirma rescue produced one)
  - n_rows_with_pre_existing_call_that_disagrees_with_proposal (this should be 0 or near-0; if nontrivial, STOP)
3.4 MERGE into `pub_canonical.canonical_molecular_genetics_v2` with these guards:
  - UPDATE `platform` only when (a) `proposed_platform != current_platform`, (b) `proposed_platform_source IS NOT NULL`, AND (c) row currently has `band_source IS NULL` OR `band_source = 'numeric_rom_inferred'` (do NOT silently change platform on rows with `band_source = 'reported_text'` from the prior parser run — those need a manual decision; surface them in the diff report).
  - UPDATE `overall_result_class`, `rom_percent_point`, `rom_percent_low`, `band_source` only when current value is NULL.
  - Add audit columns: `platform_reclass_applied_at TIMESTAMP`, `platform_reclass_source STRING`, `platform_reclass_run_id STRING`. (Existing `band_backfill_*` columns from mig_321 stay.)
3.5 Verify post-MERGE invariants: total row count unchanged, `pub_archive` snapshot is row-identical to pre-state.

### Phase 4 — verification (mandatory before skill bump)
4.1 Coverage:
```sql
WITH base AS (SELECT * FROM `pub_canonical.canonical_molecular_genetics_v2`)
SELECT
  platform,
  COUNT(*) AS n_total,
  COUNTIF(overall_result_class IS NOT NULL OR rom_descriptor IS NOT NULL) AS n_classified,
  ROUND(SAFE_DIVIDE(COUNTIF(overall_result_class IS NOT NULL OR rom_descriptor IS NOT NULL), COUNT(*)), 3) AS frac_classified
FROM base
WHERE platform IN ('Afirma','ThyroSeq')
GROUP BY platform;
```
Assertion: `frac_classified ≥ 0.95` for both Afirma AND ThyroSeq.

4.2 No-regression:
```sql
SELECT COUNT(*) AS n_regressed
FROM `pub_archive.canonical_molecular_genetics_v2_pre_platform_reclass_<YYYYMMDD>` pre
JOIN `pub_canonical.canonical_molecular_genetics_v2` post USING (molecular_episode_id)
WHERE (pre.overall_result_class IS NOT NULL AND pre.overall_result_class != post.overall_result_class)
   OR (pre.rom_descriptor IS NOT NULL AND pre.rom_descriptor != post.rom_descriptor);
```
Assertion: `n_regressed = 0`.

4.3 Platform-vs-source-of-truth consistency:
```sql
WITH joined AS (
  SELECT g.molecular_episode_id, g.platform, e.gep_norm
  FROM `pub_canonical.canonical_molecular_genetics_v2` g
  LEFT JOIN `pub_canonical.thyroseq_molecular_enrichment` e USING (research_id)
)
SELECT
  COUNTIF(platform = 'ThyroSeq' AND LOWER(gep_norm) LIKE '%afirma%') AS n_thyroseq_with_afirma_source,
  COUNTIF(platform = 'Afirma' AND LOWER(gep_norm) LIKE '%thyroseq%') AS n_afirma_with_thyroseq_source
FROM joined;
```
Assertion: both columns ≤ 5 (residual ambiguous rows acceptable; large counts indicate the waterfall didn't fire).

### Phase 5 — manuscript refresh (THIS time complete it; the prior Cursor session left it half-done)
5.1 Re-run `studies/proposal_2to4cm_extent_molecular_20260326/elicit_expansion_20260509/sql/04b_table3_v2_actual_reported_call.sql` against the updated canonical layer. Capture the new aggregate counts for both Afirma AND ThyroSeq.
5.2 Update the hardcoded `cells = [...]` block in `build_table3_v2_actual_call.py` (in BOTH locations: the parent folder AND the `manuscript_v2_package_20260509/` staging folder). Re-run.
5.3 Update `forest_rows = [...]` in `build_figures_v2.py` with the new headline numbers, re-run to refresh `figures/fig2_*.png|.pdf` and `figures/fig3_*.png|.pdf`.
5.4 Update the headline-summary tables embedded in `build_manuscript_docx.js` (Table 1 row counts may shift slightly because some former-ThyroSeq are now Afirma; Table 3 v3 row counts will shift more substantially). Re-run to produce `manuscript_v3_draft.docx`.
5.5 Move v2 deliverables (`manuscript_v2_draft.docx`, `manuscript_v2_package_20260509.zip`, the prior Table 3 v2 CSVs) into `superseded_v2/` if not already there. Add a `SUPERSEDED_NOTE_v2_to_v3.md` documenting the v2 → v3 reclassification reason and pointing to the new MFL row.
5.6 **Clean up the macOS-sync duplicate**: there is a stray `manuscript_v2_package_20260509 2/` (note trailing space + 2) folder in the elicit_expansion_20260509/ directory; delete it after confirming it's a duplicate.
5.7 Build `manuscript_v3_package_<YYYYMMDD>.zip` containing the refreshed docx + tables + figures + sql + a refreshed `README_PACKAGE.md`. Save to the same study folder.
5.8 Verify number consistency: extract text from the new docx and grep for the new headline numbers; confirm they match the new CSVs and the new exec summary §2 (which also needs to be rewritten with the new Afirma + ThyroSeq counts).

### Phase 6 — Airtable + Linear sync
6.1 Append `MFL-<YYYYMMDD>-EXT2-4-PLATFORM-RECLASS-REFRESH` (manuscript link to `rec1GJyrmKdKxjlaY`). Pre-state cites the prior MFL chain; post-state cites the new diagnostic-performance numbers.
6.2 Update the existing Data Feedback Log row from the prior session (`rec9zlFG8mH2j1DTn`) with a follow-up note OR create a new DFL row for this reclassification — depends on scope (recommend new row since it's a different fix).
6.3 File `VC-MOL-PLATFORM-001` (new) at severity `high`, lifecycle `In QA`. The daily sync will spawn a Linear issue.
6.4 Update `VC-MOL-PARSE-001` (`rec6xTvsRN6KHqqGa`) lifecycle to `Verified` if Phase 4 thresholds passed.
6.5 Update Notable Findings:
  - `reccqcuz80A9k7FWJ` (data coverage): refresh evidence_summary with the post-reclassification numbers.
  - `recKi7cbVad976age` (manuscript impact): if Afirma sens/spec shifts by >5 percentage points after reclassification, escalate severity to `publishable`.

### Phase 7 — skill version bump (LAST)
After Phase 4 assertions pass AND Phase 5 manuscript refresh completes:
- `.cowork/skills/thyroid-integration/SKILL.md`: bump `version: 2.1.0` → `2.2.0`.
- `.cowork/skills/thyroid-integration/references/CHANGELOG.md`: convert the existing `v2.2.0 — PENDING` entry into a final entry documenting (a) the prior parser-fallback work, (b) this platform-reclassification + Afirma-result-field rescue, (c) verified-state coverage numbers (post-fix Afirma frac_classified, post-fix ThyroSeq frac_classified), and (d) the new MFL row.

## Acceptance criteria
- [ ] `frac_classified ≥ 0.95` for BOTH Afirma and ThyroSeq in `canonical_molecular_genetics_v2`.
- [ ] Pre-platform-reclass snapshot present at `pub_archive.canonical_molecular_genetics_v2_pre_platform_reclass_<YYYYMMDD>`.
- [ ] No-regression check returns `n_regressed = 0`.
- [ ] Platform-vs-source-of-truth consistency check returns ≤ 5 residual rows in each direction.
- [ ] `manuscript_v3_draft.docx` exists with refreshed headline numbers; numbers reconcile across docx + Table 3 v3 CSV + executive summary + cohort flow.
- [ ] `superseded_v2/` contains the v2 deliverables; the stray `manuscript_v2_package_20260509 2/` macOS-sync duplicate is removed.
- [ ] DFL, MFL, VC-MOL-PLATFORM-001, updated VC-MOL-PARSE-001 all present and consistent.
- [ ] Skill version is at v2.2.0 with a final CHANGELOG entry citing both the parser fix and the reclassification.
- [ ] EXT2-4 lifecycle still `Active` at end of run.

If any acceptance criterion fails, STOP and surface the diff. Do not bump the skill on a partial fix; if frac_classified is between 0.90 and 0.95, document the residual gap as `VC-MOL-PARSE-002` and consider deferring the bump or adopting an explicit lower threshold.

## When this is done, hand back to Cowork for
- Final manuscript prose pass (the v3 numbers may shift the framing in §2 of the executive summary materially; co-author-ready prose review fits Cowork).
- Cohort-definitional reconciliation between EXT2-4 v1 (DuckDB N=558) and the BQ v3 (N=400 preop 2–4 cm with corrected platform classification) — this is the pre-submission step.
