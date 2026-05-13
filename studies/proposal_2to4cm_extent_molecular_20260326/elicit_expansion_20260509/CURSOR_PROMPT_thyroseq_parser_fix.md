# Cursor handoff: ThyroSeq parser gap fix + canonical refresh + manuscript update

**Recommendation: do this in Cursor, not in Cowork.** Cowork's BigQuery MCP is read-only; this task needs canonical-layer writes (or a `_v3` rebuild), and the institutional pipeline / skill version bump / CHANGELOG / verified-state check workflow lives in the repo where Cursor operates.

Copy the prompt block below into Cursor (or your Cursor-agent CLI of choice).

---

## PROMPT TO CURSOR AGENT (copy from here ↓)

You are operating inside the THYROID_2026 repo (path: `/Users/loganglosser/THYROID_2026`). Read `CLAUDE.md` and `.cowork/skills/thyroid-integration/SKILL.md` before doing anything. Run the Session Opening Protocol from the skill (verify Airtable + Linear + BigQuery connectors; identify target manuscript = EXT2-4; check lifecycle = Active; check last 24h Issue Ledger). Then proceed.

### Goal
Fix the ThyroSeq band-assignment gap in `thyroid-canonical-pub-2026.pub_canonical.canonical_molecular_genetics_v2` so that **every ThyroSeq row with a parseable report has a non-null `rom_descriptor` and `overall_result_class`**, then rebuild downstream artifacts (Table 3 v2, manuscript v2 package, executive summary) so the EXT2-4 Elicit-expansion can cite a tighter performance estimate.

### Concrete, verified problem statement (captured 2026-05-09 by Cowork)
- **647 ThyroSeq rows** in `canonical_molecular_genetics_v2` have `rom_descriptor IS NULL` AND `overall_result_class NOT IN ('positive','negative')`.
- **186 of those 647** are linked to surgical patients with Bethesda III/IV cytology (the manuscript's evaluable set).
- **97/186** of those (and 230/647 overall) **already have a numeric `rom_percent_point` populated** — the band can be inferred deterministically from numeric ROM% without any re-parsing.
- The remaining **~417 rows have report text** in `report_text_ref` (mean length 708 chars), `report_source_table` ∈ {`thyroseq_molecular_enrichment` (435), `molecular_testing` (212)}, but the parser's `parse_status` is `partial` (310) / `ok` but no band (187) / `no_detailed_block` (118) / `minimal` (8) / `empty_block` (2). For these the report text must be re-parsed.
- **Source files**:
  - Existing migration: `scripts/mig_320_thyroseq_parser_fix.py` (was MotherDuck-targeted; needs adaptation to BQ).
  - Parser module: `molecular_consolidation_20260421/thyroseq_detailed_parser.py` (handles the report-text → fields mapping; lines 161–216 already extract `rom_percent_point` correctly when the text matches; lines 251, 331–332, 406, 439 are exit points that never set `rom_descriptor` and never try a numeric-ROM fallback).
  - Manuscript-side artifacts to refresh after the rebuild: `studies/proposal_2to4cm_extent_molecular_20260326/elicit_expansion_20260509/`.

### Hard rules (NON-NEGOTIABLE — from `.cowork/skills/thyroid-integration/SKILL.md`)
1. **No PHI in Airtable, Linear, or any committed code/log.** `research_id` only. Report text excerpts logged anywhere = Claude-summarized 1–2 sentences max, never raw.
2. **Append-only.** No `DELETE` or `DROP` against `canonical_molecular_genetics_v2`. Build a new immutable version (`canonical_molecular_genetics_v3` or `canonical_molecular_genetics_v2_with_band_backfill`) or use `MERGE … WHEN MATCHED THEN UPDATE` with explicit `band_backfill_applied_at` audit columns. Never overwrite the existing rows in place without a snapshot.
3. **Log before edit.** Append a row to **Data Feedback Log** (`appJYOnUb7KrHKwpV.tblsiYKJtKcktkzze`) BEFORE touching BQ. `target_type = column`, `change_type = data_correction`, summarize the request and planned action. Then edit. Then update the row's `after_value`. Field IDs in `references/airtable_ids.md`.
4. **File the Verification Check** in THYROID_DATA_REGISTRY (Verification Checks table) as `VC-MOL-PARSE-001` BEFORE the rebuild. Severity = `medium`. Linked manuscripts = EXT2-4. The daily sync will spawn a Linear issue automatically.
5. **Skill version bump after the rebuild succeeds**: `thyroid-integration` v2.0.0 → v2.1.0 (minor — schema/pipeline change). MANDATORY pre-bump verified-state check per the skill's `Skill version bumps — required pre-checks` section: `SELECT … FROM INFORMATION_SCHEMA.TABLE_STORAGE` and `COUNTIF(rom_descriptor IS NOT NULL)` against the new table; assert ≥ 95% of ThyroSeq rows now have a band. Append CHANGELOG entry.
6. **Manuscript-Locked check on EXT2-4** before any folder writes: lifecycle should still be `Active` (it was as of 2026-05-09 13:00 UTC). If it has advanced to `Manuscript-Locked`, STOP and require explicit unlock.
7. **No PHI in commit messages or Linear comments**.

### Step-by-step plan

#### Phase 1 — diagnose (read-only; ≤30 min)
1.1 Verify the diagnosis by re-running this query and confirming the 647 / 230 / 417 numbers within ±5%:
```sql
SELECT
  COUNT(*) AS n_unclassified,
  COUNTIF(rom_percent_point IS NOT NULL) AS n_with_numeric_rom_inferable,
  COUNTIF(rom_percent_point IS NULL AND report_text_length > 0) AS n_need_reparse,
  COUNTIF(report_text_ref IS NULL OR report_text_length = 0) AS n_no_text
FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_molecular_genetics_v2`
WHERE platform = 'ThyroSeq'
  AND rom_descriptor IS NULL
  AND (overall_result_class NOT IN ('positive','negative') OR overall_result_class IS NULL);
```
1.2 Pull a representative sample of 20 unclassified rows including `report_text_ref`, the joined raw text from `pub_raw.thyroseq_molecular_enrichment` and `pub_raw.molecular_testing`, and the existing `rom_percent_point` / `gene_mutations_status`. Look at the actual report text to confirm the patterns the parser is missing. **Do not log report text into Airtable, Linear, or any committed file.**
1.3 Read the existing parser at `molecular_consolidation_20260421/thyroseq_detailed_parser.py`. Identify which exit points produce `parse_status` ∈ {`partial`, `ok` without band, `no_detailed_block`, `minimal`} without setting `rom_descriptor`.

#### Phase 2 — parser fixes (`molecular_consolidation_20260421/thyroseq_detailed_parser.py`)
Add two layered fallbacks to the ThyroSeq path:

**Fallback A: numeric-ROM → band inference.** When the parser otherwise has `rom_percent_point` but no `rom_descriptor`, infer the band:
- ROM ≤ 5 → `LOW`
- 5 < ROM ≤ 10 → `INTERMEDIATE-LOW`
- 10 < ROM ≤ 30 → `INTERMEDIATE-LOW` (Sonic v3 ROM band documentation)
- 30 < ROM < 50 → `INTERMEDIATE`
- 50 ≤ ROM < 70 → `INTERMEDIATE-HIGH`
- 70 ≤ ROM < 90 → `INTERMEDIATE-HIGH` *(prefer HIGH only if explicit "HIGH" found in text and ROM ≥ 80)*
- ROM ≥ 90 → `HIGH`

(Validate these thresholds against the populated ROM ranges in current `canonical_molecular_genetics_v2` rows where both fields exist:
```sql
SELECT rom_descriptor, MIN(rom_percent_point) AS lo, APPROX_QUANTILES(rom_percent_point,4)[OFFSET(2)] AS median, MAX(rom_percent_point) AS hi, COUNT(*) AS n
FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_molecular_genetics_v2`
WHERE platform = 'ThyroSeq' AND rom_descriptor IS NOT NULL AND rom_percent_point IS NOT NULL
GROUP BY rom_descriptor ORDER BY median;
```
Adjust the thresholds above to match the empirical band ranges if they disagree.)

Set `overall_result_class` from the inferred band:
- `LOW`, `INTERMEDIATE-LOW` → `negative`
- `INTERMEDIATE` → leave as `intermediate` (third category — do NOT pool)
- `INTERMEDIATE-HIGH`, `HIGH` → `positive`

Add an audit column to the parser output: `band_source ∈ {'reported_text','numeric_rom_inferred','manual_review'}`. The default for parser-extracted bands is `reported_text`; numeric-ROM fallback rows get `numeric_rom_inferred`.

**Fallback B: full-text band scan when DETAILED RESULTS block is missing.** For the ~118 `no_detailed_block` cases, scan the full report text for the strings `RISK OF MALIGNANCY`, `ROM`, `LOW`, `INTERMEDIATE-LOW`, `INTERMEDIATE`, `INTERMEDIATE-HIGH`, `HIGH` in proximity (±50 chars). If found, set `rom_descriptor` and `band_source = 'reported_text'`. If still no band but a numeric ROM% pattern (`\d{1,3}\s*%`) appears within 100 chars of "MALIGNANCY", apply Fallback A.

Add unit tests covering: (i) a clean LOW report (3% ROM), (ii) an INTERMEDIATE report (50%), (iii) a HIGH report (95%), (iv) a `no_detailed_block` report where only the numeric appears in a comment block, (v) a fully unparseable report → `rom_descriptor` stays NULL and `band_source = 'manual_review'`.

#### Phase 3 — canonical-layer rebuild on BigQuery (NOT MotherDuck)
The existing `scripts/mig_320_thyroseq_parser_fix.py` writes to MotherDuck. Adapt it (or write `scripts/mig_321_thyroseq_band_backfill_bq.py`) to:
3.1 Pull the 647 unclassified ThyroSeq rows + their report text via a single `bq query` join: `canonical_molecular_genetics_v2` ⨝ `pub_raw.thyroseq_molecular_enrichment` ⨝ `pub_raw.molecular_testing` on `report_text_ref`.
3.2 Run them through the updated parser locally (Python).
3.3 Build a new staging table `pub_workspace.canonical_molecular_genetics_v2_band_backfill_20260509` with the original `genomic_assay_id`, the new `rom_descriptor`, `rom_percent_point`, `overall_result_class`, `band_source`, `parser_version_post_fix`. Validate counts.
3.4 `MERGE` from the staging table into `canonical_molecular_genetics_v2` using `genomic_assay_id` as the join key, ONLY when `rom_descriptor IS NULL` on the target side (idempotent; never overwrites existing band assignments). Add audit columns: `band_backfill_applied_at TIMESTAMP`, `band_backfill_source STRING`, `band_backfill_run_id STRING`.
3.5 Snapshot the pre-merge table to `pub_archive.canonical_molecular_genetics_v2_pre_band_backfill_20260509` before the MERGE (per the never-delete rule).

#### Phase 4 — verification (mandatory before skill version bump)
4.1 ThyroSeq band coverage check:
```sql
SELECT
  COUNTIF(rom_descriptor IS NOT NULL) / COUNT(*) AS frac_with_band,
  COUNTIF(overall_result_class IN ('positive','negative','intermediate')) / COUNT(*) AS frac_classified,
  COUNTIF(band_source = 'numeric_rom_inferred') AS n_inferred_from_numeric,
  COUNTIF(band_source = 'reported_text') AS n_from_text,
  COUNTIF(band_source = 'manual_review') AS n_still_unresolved,
  COUNT(*) AS n_thyroseq
FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_molecular_genetics_v2`
WHERE platform = 'ThyroSeq';
```
Assertion: `frac_with_band ≥ 0.95`. If not, surface the diff and stop.

4.2 No-overwrite check:
```sql
-- Should be 0: rows where pre-existing band changed
SELECT COUNT(*) FROM `pub_archive.canonical_molecular_genetics_v2_pre_band_backfill_20260509` AS pre
JOIN `pub_canonical.canonical_molecular_genetics_v2` AS post USING (genomic_assay_id)
WHERE pre.rom_descriptor IS NOT NULL
  AND pre.rom_descriptor != post.rom_descriptor;
```

4.3 Numeric-ROM consistency check on `'numeric_rom_inferred'` rows: median ROM% should match the band the inference assigned (LOW band median ROM% < 10; HIGH band median ROM% ≥ 90).

#### Phase 5 — manuscript package refresh
5.1 Re-run the Bethesda III/IV diagnostic-performance query in `studies/proposal_2to4cm_extent_molecular_20260326/elicit_expansion_20260509/sql/04b_table3_v2_actual_reported_call.sql` against the refreshed canonical layer. Capture the new aggregate counts.
5.2 Update the hardcoded `cells = [...]` block in `studies/proposal_2to4cm_extent_molecular_20260326/elicit_expansion_20260509/build_table3_v2_actual_call.py` with the new counts. Re-run it. The output `tables/table3_v2_diagnostic_performance_actual_reported_call.csv` and `tables/table3_v2_rom_pct_descriptive_stats.csv` must regenerate.
5.3 Re-run `build_figures_v2.py` to refresh `figures/fig2_forest_diagnostic_performance.{png,pdf}` and `figures/fig3_rom_pct_distribution.{png,pdf}` against the new numbers.
5.4 Re-run `build_manuscript_docx.js manuscript_v3_draft.docx figures/` to produce `manuscript_v3_draft.docx`. Update the `forest_rows` and `cells` numbers in the headline-tables blocks of the docx builder. Note in the manuscript text: "ThyroSeq band coverage refreshed YYYY-MM-DD; previously 165 unclassified Bethesda III/IV evaluable patients reduced to N." (insert actual N).
5.5 Move the prior `manuscript_v2_draft.docx`, `manuscript_v2_package_20260509.zip`, `tables/table3_v2_*.csv`, and `figures/fig2_*` / `figures/fig3_*` into `studies/proposal_2to4cm_extent_molecular_20260326/elicit_expansion_20260509/superseded_v2/` with a `SUPERSEDED_NOTE.md` explaining the supersession (point to MFL row).
5.6 Build `manuscript_v3_package_<YYYYMMDD>.zip` containing the refreshed docx + tables + figures + sql + README_PACKAGE.md (updated headline numbers). Save to the same study folder.

#### Phase 6 — Airtable + Linear sync
6.1 Append a new MFL row `MFL-<YYYYMMDD>-EXT2-4-PARSER-FIX-REFRESH` (manuscript link to EXT2-4 record `rec1GJyrmKdKxjlaY`) with before/after summary. Reference the upstream Data Feedback Log row from Phase 1.
6.2 Update VC-MOL-PARSE-001 with `lifecycle = Verified`, attach the verification query results.
6.3 Daily sync will move the linked Linear issue to `Pending Auto-Close` automatically; verify it appears.
6.4 File two Notable Findings rows (if not already filed):
  - `NF-<YYYYMMDD>-thyroseq-band-coverage-recovered` — informational/data-coverage; document the % uplift.
  - `NF-<YYYYMMDD>-ext24-thyroseq-perf-stabilized` — hypothesis-generating if the new sens/spec materially shift; clinically actionable only if it changes a guideline-relevant claim.

#### Phase 7 — skill version bump (LAST)
After Phase 4 assertions pass and Phases 5–6 complete:
- `.cowork/skills/thyroid-integration/SKILL.md`: bump `version: 2.0.0` → `2.1.0`
- `.cowork/skills/thyroid-integration/references/CHANGELOG.md`: append entry like:
  ```
  ## v2.1.0 (YYYY-MM-DD)
  - canonical_molecular_genetics_v2 ThyroSeq band-coverage backfill: rom_descriptor coverage <baseline%> → ≥95%; <N> rows inferred from numeric ROM%, <N> from full-text scan, <N> still unresolved (band_source = manual_review). Pre-merge snapshot at pub_archive.canonical_molecular_genetics_v2_pre_band_backfill_<YYYYMMDD>. Verification: COUNTIF(rom_descriptor IS NOT NULL) / COUNT(*) ≥ 0.95 confirmed. EXT2-4 manuscript v3 package regenerated. MFL row: MFL-<YYYYMMDD>-EXT2-4-PARSER-FIX-REFRESH.
  ```

### Sanity checks before you start
- **Authoritative manuscript record**: Airtable Manuscripts `rec1GJyrmKdKxjlaY` (code `EXT2-4`, status `Drafting`, lifecycle `Active`).
- **Existing MFL chain to extend**: `recDdqL9CDf4iZPQZ` → `rec2RAsAFehw1zEHV` → `rechvFk8Fweo8cc9U` (this work appends a 4th row).
- **BigQuery project**: `thyroid-canonical-pub-2026`; primary dataset `pub_canonical`; staging `pub_workspace`; immutable archive `pub_archive`.
- **Do not modify** `studies/proposal_2to4cm_extent_molecular_20260326/manuscript_submission_v1.md`, `abstract_structured_v1.md`, or any other v1-era prose. The v3 refresh is additive.
- **Do not** run any DELETE statements. Use MERGE + snapshot.
- **Do not** mutate `manuscript_cohort_v1` itself (that's a downstream rollup; if any change there is needed, surface a separate Verification Check first).

### Acceptance criteria
- [ ] `rom_descriptor` populated on ≥ 95% of ThyroSeq rows in `canonical_molecular_genetics_v2`.
- [ ] `band_source` audit column populated on every backfilled row.
- [ ] Pre-merge snapshot present at `pub_archive.canonical_molecular_genetics_v2_pre_band_backfill_<YYYYMMDD>`.
- [ ] `manuscript_v3_draft.docx` regenerates with new numbers; headline cells reconcile across docx + Table 3 v2 CSV + executive summary + cohort flow.
- [ ] No previously-non-null `rom_descriptor` rows changed value.
- [ ] Data Feedback Log row, MFL row, VC-MOL-PARSE-001 row all present and consistent.
- [ ] Skill version bumped to 2.1.0; CHANGELOG entry written; verified-state check evidence pasted into the CHANGELOG.
- [ ] Original v2 deliverables moved to `superseded_v2/` with a clear note.
- [ ] EXT2-4 lifecycle still `Active` at end of run.

If any acceptance criterion fails, STOP and surface the diff. Do not bump the skill version on a partial fix.

---

(End of Cursor prompt.)

## When this is done, what to bring back to Cowork

After Cursor finishes, hand back to Cowork (this assistant) for:
- Co-author review prep (if the new numbers materially change the manuscript's framing).
- Cross-check against the v1 EXT2-4 cohort definitional reconciliation (n=558 DuckDB vs n=400 BQ preop 2–4 cm; the v3 numbers may be the right basis for a single unified cohort definition).
- Optional decision-curve analysis or NRI/IDI on the refreshed Table 3 v3.
