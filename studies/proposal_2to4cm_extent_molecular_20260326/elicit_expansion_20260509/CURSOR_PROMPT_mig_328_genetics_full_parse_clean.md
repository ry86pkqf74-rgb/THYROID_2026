# Cursor handoff: mig_328 — full genetics parse + ThyroSeq-contamination cleanup on canonical_molecular_genetics_v2

**Recommendation: do this in Cursor.** Requires canonical-layer MERGE writes (NULL contaminated fields + reparser pass on ~230 rows). Logan asked for "fix all issues, even if minor, to be as accurate as possible" and specifically flagged: **"There shouldn't be inferred ROM% from thyroseq on afirma reporting."** Cowork-side read-only audit confirmed the contamination is real, scoped, and fixable.

Skill version bump v2.4.0 → **v2.4.1** (patch — additive cleanup + parser-tail recovery; no schema changes).

## Background — what the Cowork audit found

Two distinct problems on `thyroid-canonical-pub-2026.pub_canonical.canonical_molecular_genetics_v2` (state at 2026-05-14, post-mig_327):

### Problem 1 — Afirma platform contamination from ThyroSeq parser (Logan-flagged)

mig_323 (platform reclassification, commit 5e7e1cd) reassigned `platform = 'Afirma'` on **~158 rows whose `platform_raw = 'ThyroSeq'`** because their canonical report text identified them as Afirma. **The Afirma binary call was correctly extracted from `molecular_testing.result` and stored in `overall_result_class`.** But the **`rom_descriptor`, `rom_percent_*`, and `rom_description` fields populated by the earlier ThyroSeq parser (mig_320/mig_321) were left in place on those rows** — so we now have Afirma rows carrying ThyroSeq-style band labels (`LOW`/`INTERMEDIATE`/`INTERMEDIATE-LOW`/`INTERMEDIATE-HIGH`/`HIGH`) and ThyroSeq-inferred numeric ROM%.

Scope (verified 2026-05-14):

| Field | Afirma rows with value (active) | Of which `platform_raw='ThyroSeq'` |
|---|---:|---:|
| `rom_descriptor` IS NOT NULL | 98 | **94 (95.9%)** |
| any `rom_percent_*` IS NOT NULL | 146 | **138 (94.5%)** |
| `rom_description` IS NOT NULL | 58 | **53 (91.4%)** |

Within the v4 manuscript cohort (n=765): **10 of the 47 Afirma rows** carry contaminated fields (6 with rom_descriptor, 9 with rom_percent_*, 5 with rom_description).

Specific rid examples (from Cowork audit):
- **rid 2130** — `rom_percent_point = 599`, `rom_percent_raw = '599%'` — OCR error (impossible ROM% value).
- **rids 1184, 9539, 9592** — `band_backfill_source = 'numeric_rom_inferred'` (ThyroSeq parser inferred ROM% from text that's actually Afirma).
- **rid 9539** — description text says "Afirma returned benign with 4% ROM" but the stored `rom_percent_point = 35` (inferred from a parenthetical "30-40%" that was actually about Hurthle cell adenoma probability, NOT the Afirma ROM).

### Problem 2 — Parser-tail (unrecovered labels + ROM%, even where source text exists)

Coverage on **active** canonical_molecular_genetics_v2 rows:

| Platform | n active | Label coverage | Numeric ROM% coverage |
|---|---:|---|---|
| ThyroSeq | 703 | 89.9% (632 rom_descriptor) | 77.4% (544 any ROM, 52.9% numeric range) |
| Afirma | 582 | 98.3% (572 overall_result_class) | 25.1% (146; expected — Afirma is binary-by-design) |
| NGS_unspecified | 67 | 0% | 4% |
| Other | 19 | 5% | 0% |

**Recoverable parser-tail (rows with `report_text_length > 0` but missing the field):**

| Platform | report_source_table | n rows | Missing label (text exists) | Missing ROM% (text exists) |
|---|---|---:|---:|---:|
| ThyroSeq | molecular_testing | 110 | 50 | 110 |
| ThyroSeq | thyroseq_molecular_enrichment | 51 | 11 | 49 |
| ThyroSeq | extracted_braf_recovery_v1 | 5 | 4 | 5 |
| ThyroSeq | ret_patient_adjudicated_v226 | 2 | 2 | 2 |
| Afirma | molecular_testing | 2 (no label) | 2 | n/a (binary by design) |
| Afirma | thyroseq_molecular_enrichment | 6 (no label) | 6 | n/a |
| Afirma | ret_patient_adjudicated_v226 | 1 (no label) | 1 | n/a |
| Afirma | extracted_braf_recovery_v1 | 2 (no label) | 2 | n/a |

**Truly unparseable (out of scope — document only):** 67 NGS_unspecified + 19 Other + 4 NGS rows in extracted_braf_recovery_v1. These are not commercial-test reports (BRAF-recovery extractions, generic NGS panels, Quest in-house, etc.) and have no commercial-test band reporting by design.

## Hard rules

1. **Snapshot first.** `pub_archive.canonical_molecular_genetics_v2_pre_mig328_<YYYYMMDD>` before any MERGE.
2. **No PHI in Airtable, Linear, or committed code.** Bridge tables and audit CSVs use research_id + structured fields only.
3. **DFL row pre-edit** (`appJYOnUb7KrHKwpV.tblsiYKJtKcktkzze`). `change_type=data_correction`. Reference `VC-MOL-PLATFORM-002` and `VC-MOL-PARSE-002`.
4. **MFL row post-edit** (`MFL-<YYYYMMDD>-EXT2-4-MIG328-FULL-PARSE-CLEAN`) linked to EXT2-4 (`rec1GJyrmKdKxjlaY`).
5. **No regression.** Verify post-merge that:
   - No previously-`native` resolved_test_date_source changed
   - All previously-set `overall_result_class` values on Afirma rows are unchanged (only ROM/descriptor cleanup)
   - All ThyroSeq rom_descriptor values that survive the contamination filter are unchanged
6. **Skill version bump** v2.4.0 → v2.4.1.

## Arm A — Afirma platform contamination cleanup

### Decision matrix

For Afirma rows where `platform_raw LIKE '%ThyroSeq%'` (n=158 reclassified by mig_323):

| Field | Action | Rationale |
|---|---|---|
| `overall_result_class` | **KEEP** | Extracted from Afirma's own result text by mig_323 (`band_backfill_source = 'afirma_result_field'`). |
| `rom_descriptor` (`LOW`/`INT*`/`HIGH`) | **NULL** | ThyroSeq band concept; Afirma reports binary suspicious/benign, not bands. |
| `rom_percent_*` | **NULL** if `band_backfill_source = 'numeric_rom_inferred'` OR if `rom_percent_point > 100` (OCR garbage). Otherwise **KEEP** with note that source is Afirma Xpression Atlas. |
| `rom_description` | **NULL** if the text is ThyroSeq-style (mentions "ThyroSeq GC", "DNA copy number alterations of Hurthle cell type", "molecular profile is associated with X% probability"). Otherwise KEEP. |
| `band_backfill_source`, `band_backfill_applied_at`, `band_backfill_run_id` | Append `mig_328_afirma_contamination_clean` to `band_backfill_source`, refresh applied_at and run_id. |

### Implementation (Path A)

```sql
-- Stage 1: snapshot
CREATE TABLE `pub_archive.canonical_molecular_genetics_v2_pre_mig328_<YYYYMMDD>`
AS SELECT * FROM `pub_canonical.canonical_molecular_genetics_v2`;

-- Stage 2: identify contamination rows
CREATE OR REPLACE TABLE `pub_workspace.mig328_afirma_contamination_<YYYYMMDD>` AS
SELECT
  molecular_episode_id,
  research_id,
  platform,
  platform_raw,
  overall_result_class,
  rom_descriptor,
  rom_percent_point,
  rom_percent_low,
  rom_percent_high,
  rom_percent_raw,
  rom_description,
  band_backfill_source,
  CASE
    WHEN band_backfill_source = 'numeric_rom_inferred' THEN 'null_all_rom_fields'
    WHEN rom_percent_point > 100 THEN 'null_all_rom_fields_ocr_garbage'
    WHEN rom_descriptor IS NOT NULL THEN 'null_descriptor_only'
    ELSE 'no_action'
  END AS contamination_action,
  CASE
    WHEN rom_description LIKE '%ThyroSeq GC%'
      OR rom_description LIKE '%DNA copy number alterations%'
      OR rom_description LIKE '%molecular profile is associated with%'
      THEN TRUE
    ELSE FALSE
  END AS rom_description_is_thyroseq_style
FROM `pub_canonical.canonical_molecular_genetics_v2`
WHERE platform = 'Afirma'
  AND (platform_raw LIKE '%ThyroSeq%' OR platform_raw LIKE '%thyroseq%')
  AND (platform_reclass_status IS NULL
       OR platform_reclass_status NOT IN ('superseded_by_afirma_row','non_diagnostic_cancelled','non_diagnostic'))
  AND (rom_descriptor IS NOT NULL
       OR rom_percent_point IS NOT NULL
       OR rom_percent_low IS NOT NULL
       OR rom_percent_high IS NOT NULL
       OR rom_percent_raw IS NOT NULL
       OR rom_description IS NOT NULL);

-- Stage 3: MERGE the cleanup
MERGE `pub_canonical.canonical_molecular_genetics_v2` T
USING `pub_workspace.mig328_afirma_contamination_<YYYYMMDD>` S
  ON T.molecular_episode_id = S.molecular_episode_id
WHEN MATCHED THEN UPDATE SET
  rom_descriptor = NULL,
  rom_percent_point = CASE
    WHEN S.contamination_action IN ('null_all_rom_fields','null_all_rom_fields_ocr_garbage') THEN NULL
    ELSE T.rom_percent_point
  END,
  rom_percent_low = CASE
    WHEN S.contamination_action IN ('null_all_rom_fields','null_all_rom_fields_ocr_garbage') THEN NULL
    ELSE T.rom_percent_low
  END,
  rom_percent_high = CASE
    WHEN S.contamination_action IN ('null_all_rom_fields','null_all_rom_fields_ocr_garbage') THEN NULL
    ELSE T.rom_percent_high
  END,
  rom_percent_raw = CASE
    WHEN S.contamination_action IN ('null_all_rom_fields','null_all_rom_fields_ocr_garbage') THEN NULL
    ELSE T.rom_percent_raw
  END,
  rom_description = CASE
    WHEN S.rom_description_is_thyroseq_style THEN NULL
    ELSE T.rom_description
  END,
  band_backfill_source = CONCAT(COALESCE(T.band_backfill_source,''), '|mig_328_afirma_contamination_clean'),
  band_backfill_applied_at = CURRENT_TIMESTAMP(),
  band_backfill_run_id = 'mig_328_<YYYYMMDD>_<run_id>';
```

Acceptance:
- All Afirma rows have `rom_descriptor IS NULL` post-merge (Afirma doesn't have ThyroSeq bands).
- No Afirma row has `rom_percent_point > 100`.
- 4 rows with `band_backfill_source = 'numeric_rom_inferred'` have all rom_percent_* nulled.
- `overall_result_class` count unchanged.

## Arm B — ThyroSeq parser-tail recovery

Re-run the ThyroSeq parser on the ~168 rows with reported_text but missing `rom_descriptor` or numeric ROM%. Use the same parser as `mig_321_thyroseq_parser_fix.py` (in `scripts/`) but force a recovery pass against:

```sql
SELECT
  molecular_episode_id,
  research_id,
  platform,
  report_source_table,
  report_text_ref,
  report_text_length,
  rom_descriptor,
  rom_percent_point
FROM `pub_canonical.canonical_molecular_genetics_v2`
WHERE platform = 'ThyroSeq'
  AND (platform_reclass_status IS NULL
       OR platform_reclass_status NOT IN ('superseded_by_afirma_row','non_diagnostic_cancelled','non_diagnostic'))
  AND report_text_length > 0
  AND (rom_descriptor IS NULL OR rom_percent_point IS NULL);
```

For each row, fetch the underlying source text (via `report_text_ref`):
1. **Hard pattern pass first** — exact regexes for `(low|intermediate|high)\s*(risk|positive|grade)` and `~?\s*\d{1,3}\s*%` near "ROM" / "probability" / "risk of malignancy" / "GC results" markers.
2. **If hard-pattern fails**, fall back to an LLM extraction (Claude Haiku via `window.cowork.askClaude` or a Cursor-local model) with a tight prompt: "Extract the ThyroSeq risk band (LOW, INT-LOW, INTERMEDIATE, INT-HIGH, HIGH, or not_classifiable) AND any numeric ROM% (single value or range) from the following ThyroSeq report. Return JSON: `{band: string, rom_point: number|null, rom_low: number|null, rom_high: number|null}`. If neither is clearly stated, return all nulls."
3. **Reject** any LLM-extracted band that doesn't appear verbatim in the source text (no hallucinations). Apply the same guard as mig_325.

Expected recovery: 5–15 ThyroSeq label rows + 30–80 ThyroSeq ROM% rows. The 4 `extracted_braf_recovery_v1` rows are likely truly unparseable (BRAF-recovery extractions don't carry full reports) — document but don't force.

Snapshot diffs to `scripts/output/mig_328_thyroseq_parser_tail_recovery_<YYYYMMDD>.csv` with columns: molecular_episode_id, research_id, before_band, after_band, before_rom_pt, after_rom_pt, extraction_method (hard_pattern | llm), source_text_excerpt_first_200_chars.

## Arm C — Afirma label-missing recovery (11 rows total)

The 11 active Afirma rows missing `overall_result_class` all have reported_text. Re-run the Afirma rescue (same logic as mig_323 Stage 4) against:

```sql
SELECT
  molecular_episode_id,
  research_id,
  platform,
  report_source_table,
  report_text_ref,
  overall_result_class
FROM `pub_canonical.canonical_molecular_genetics_v2`
WHERE platform = 'Afirma'
  AND (platform_reclass_status IS NULL
       OR platform_reclass_status NOT IN ('superseded_by_afirma_row','non_diagnostic_cancelled','non_diagnostic'))
  AND overall_result_class IS NULL
  AND report_text_length > 0;
```

For each, look for canonical Afirma call patterns:
- `(?i)\b(suspicious|benign|positive|negative|no result|cancelled)\b` within Afirma result context
- Map: suspicious/positive → `suspicious` or `positive`; benign/negative → `negative`; no result/cancelled → `no_result`.

If no clear match, leave NULL and document in the residual CSV. Expected recovery: 5–9 rows.

## Arm D — NGS_unspecified / Other / extracted_braf_recovery_v1 (document only)

For the 67 NGS_unspecified + 19 Other + 4 NGS rows in `extracted_braf_recovery_v1`:
- Confirm via spot-check (sample 5 rows from each bucket): are these BRAF-recovery extractions, Quest in-house panels, and generic NGS findings without commercial-test band reporting?
- If yes: no canonical-layer write. Document as "truly not recoverable from source — no commercial-test reporting structure" in the mig_328 audit CSV and in the manuscript-impact section below.
- If any are actually parseable commercial tests, flag them to Cowork for a separate decision.

## Phase Verification

```sql
-- Post-merge coverage check
SELECT
  platform,
  COUNT(*) AS n_active,
  COUNTIF(overall_result_class IS NOT NULL) AS n_with_call,
  COUNTIF(rom_descriptor IS NOT NULL) AS n_with_descriptor,
  COUNTIF(rom_descriptor IS NOT NULL AND platform = 'Afirma') AS n_contamination_remaining,
  COUNTIF(rom_percent_point > 100) AS n_impossible_rom,
  COUNTIF(rom_percent_point IS NOT NULL OR rom_percent_low IS NOT NULL OR rom_percent_high IS NOT NULL) AS n_with_numeric_rom
FROM `pub_canonical.canonical_molecular_genetics_v2`
WHERE platform_reclass_status IS NULL
   OR platform_reclass_status NOT IN ('superseded_by_afirma_row','non_diagnostic_cancelled','non_diagnostic')
GROUP BY platform;
```

Acceptance criteria:
- [ ] `n_contamination_remaining` for Afirma = **0**
- [ ] `n_impossible_rom` = **0**
- [ ] ThyroSeq label coverage ≥ **94%** (from 89.9% baseline)
- [ ] ThyroSeq ROM% coverage ≥ **85%** (from 77.4% baseline)
- [ ] Afirma label coverage ≥ **99%** (from 98.3%)
- [ ] V4 cohort Afirma contamination = 0 (was 10/47)
- [ ] Snapshot at `pub_archive.canonical_molecular_genetics_v2_pre_mig328_<YYYYMMDD>`
- [ ] No regression: 0 previously-set `overall_result_class` values changed on ThyroSeq rows

## Airtable + skill bump

- DFL pre-edit `DFL-MIG328-<YYYYMMDD>-FULL-PARSE-CLEAN`
- MFL post-edit `MFL-<YYYYMMDD>-EXT2-4-MIG328-FULL-PARSE-CLEAN` linked to EXT2-4 (`rec1GJyrmKdKxjlaY`)
- Open new VC-MOL-PLATFORM-002 ("Afirma rows carry ThyroSeq-pipeline-inferred rom_descriptor / ROM% / rom_description after mig_323 reclassification") and close as Verified/Resolved after merge
- Update VC-MOL-PARSE-002 fix_action with the mig_328 parser-tail recovery details (does not re-open the VC; just extends the audit chain)
- Skill v2.4.0 → v2.4.1; CHANGELOG entry

## Manuscript impact (for Cowork to handle after Cursor returns)

The Afirma contamination cleanup does NOT change Table 3 v4 cells because:
- Table 3 uses `overall_result_class` (binary call) for Afirma → unchanged
- Table 3 does not use rom_descriptor for Afirma

§2 ROM% descriptive validation table will lose ~10–15 Afirma rows that were inappropriately included in the ThyroSeq ROM% distribution table — those rows shouldn't have been there in the first place. Cowork to re-derive `table3_v4_rom_pct_descriptive_stats.csv` after merge.

The ThyroSeq parser-tail recovery may add 5–15 patients to the Table 3 v4 cells. If a ThyroSeq row's rom_descriptor goes from NULL → a band, that row becomes eligible for the 2×2 if the patient is in v4 cohort with B3/B4 cytology. Re-run `04b_table3_v4_actual_reported_call.sql` after merge.

## When done, hand back to Cowork for

- Re-run `04b_table3_v4_actual_reported_call.sql` and refresh Table 3 v4 cells in the executive summary §2
- Refresh `table3_v4_rom_pct_descriptive_stats.csv` (drop the contaminated Afirma rows from the ThyroSeq ROM% panel)
- Update §2 caveat (b) to reflect new coverage ≥94%/85% on the manuscript-relevant subset
- Open and Verify VC-MOL-PLATFORM-002
- Optional: file a Notable Finding documenting the platform-reclassification audit methodology (publishable as a data-engineering disclosure if the parent manuscript needs methodologic transparency)
