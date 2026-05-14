# Genetics testing completeness audit — `canonical_molecular_genetics_v2`

**Date:** 2026-05-13
**Author:** Cowork (Logan request: "be sure that all genetics testing and their dates are in the canonical database and all the reports in cases where a patient had multiple are fully parsed out")
**Source layer:** `thyroid-canonical-pub-2026.pub_canonical.*`

## TL;DR — three serious findings

| # | Finding | Magnitude | Manuscript impact |
|---|---|---|---|
| 1 | **Canonical layer misses ~89% of patients with molecular evidence in source tables.** | 9,711 of 10,862 patients with content in `thyroseq_molecular_enrichment` and/or `molecular_testing` are **not** in `canonical_molecular_genetics_v2`. | Possibly large. Could expand the Bethesda III/IV evaluable cohorts substantially if a meaningful fraction are real commercial tests. |
| 2 | **65% of canonical rows have NO test date.** | 903/1,384 rows have neither `test_date_native` nor `resolved_test_date`. The `thyroseq_molecular_enrichment` source contributes 0% date population (425 rows, all dateless). | Any temporal analysis (era stratification, pre/post-surgery sequencing, follow-up windows) is fragile until dates are backfilled. |
| 3 | **`molecular_episode_id` is broken.** | Only 4 distinct values (1, NULL, 2, 3) across 1,384 rows. NULL on 525 rows; value `1` on 791 rows. | Can't reliably identify distinct tests-per-patient. The "212 multi-test patients" finding mixes genuine multi-test cases (e.g., rid 8729 with both ThyroSeq v3 and Afirma) with duplicate canonical rows representing the SAME test routed through two source tables. |

## Full audit table

### Row-level coverage of `canonical_molecular_genetics_v2`

| Metric | Value | Notes |
|---|---:|---|
| Total rows | 1,384 | |
| Distinct patients | 1,151 | |
| Distinct `molecular_episode_id` | **3 (+ NULL)** | broken; should be ~1,400+ |
| Patients with 1 row | 939 (82%) | |
| Patients with 2 rows | 191 (17%) | |
| Patients with 3 rows | 21 (2%) | |
| Max rows for one patient | 3 | |
| Multi-test patients with ≥1 row missing date | 201/212 (95%) | |

### Date completeness

| Field | n populated | % |
|---|---:|---:|
| `test_date_native` | 481 | 35% |
| `resolved_test_date` | 481 | 35% |
| Both NULL | 903 | **65%** |

### Date completeness by source table × platform

| `report_source_table` | Platform | n | n with date | % with date |
|---|---|---:|---:|---:|
| `molecular_testing` | Afirma | 425 | 297 | **70%** |
| `molecular_testing` | ThyroSeq | 434 | 184 | 42% |
| `thyroseq_molecular_enrichment` | ThyroSeq | 275 | 0 | **0%** |
| `thyroseq_molecular_enrichment` | Afirma | 150 | 0 | **0%** |
| `thyroseq_molecular_enrichment` | Other | 18 | 0 | 0% |
| `extracted_braf_recovery_v1` | NGS_unspecified | 38 | 0 | 0% |
| `ret_patient_adjudicated_v226` | NGS_unspecified | 29 | 0 | 0% |
| `extracted_braf_recovery_v1` | ThyroSeq / Afirma | 8 | 0 | 0% |
| `ret_patient_adjudicated_v226` | ThyroSeq / Afirma | 7 | 0 | 0% |

**The `thyroseq_molecular_enrichment`-sourced rows have 0% dates because that source table has no date column.** This is structural, not a parser failure — needs an upstream date-linkage strategy (link via `fna_episode_id` → `canonical_fna_events_v1.fna_date_resolved`, or via `surgery_episode_id` → `canonical_operative_events_v1.resolved_surgery_date`, or via the upload date `imported_at`).

### Parse-status quality

| `parse_status` | n | % |
|---|---:|---:|
| `ok` | 331 | 24% |
| `partial` | 508 | 37% |
| `no_detailed_block` | 297 | 21% |
| `minimal` | 185 | 13% |
| `empty_block` | 63 | 5% |
| NULL | 0 | 0% |

**Only 24% are fully parsed (`ok`).** The remaining 76% extracted *something* (band, mutation, ROM%) but the parser flagged the report structure as incomplete.

### Call (result) completeness

| Field | n populated | % |
|---|---:|---:|
| `overall_result_class` | 1,208 | 87% |
| `rom_descriptor` | 745 | 54% |
| `rom_percent_point` (numeric) | 678 | 49% |
| Completely uncalled (all three NULL) | 159 | 11% |

### Source-table orphans (patients with molecular content but NO canonical row)

| Source | Patients with content | Orphaned from canonical |
|---|---:|---:|
| `thyroseq_molecular_enrichment` | 10,862 | **9,711 (89%)** |
| `molecular_testing` | 10,023 | **8,874 (89%)** |
| Either source (deduped) | ≥10,862 | **9,711** |
| `canonical_molecular_genetics_v2` | — | 1,151 |

## What "9,711 orphans" actually means — three possibilities

The orphan number is large, but most of the 9,711 patients are almost certainly NOT missed commercial tests. The source tables are designed to capture *any molecular evidence* (including thin LLM-extracted phrases from clinical notes). Three categories likely:

**a. Vague clinical-note mentions, not real commercial tests** (probably the majority)
> "patient reports prior thyroid biopsy with molecular testing several years ago, results unavailable" — gets a row in `thyroseq_molecular_enrichment` with `pathology_raw` containing the phrase, `gep_norm = NULL`, no `mutation_raw`, no flags. Should NOT be in canonical_molecular_genetics_v2 (it's not a test, it's a mention).

**b. Patients with content in the source tables but classifying their data as "not a commercial molecular test"** (probably a meaningful fraction)
> e.g., genetic syndrome screening (MEN2, FAP), pharmacogenomics, oncology panels on lymph-node metastases sent to non-thyroid molecular labs. These have molecular content but they're not Afirma/ThyroSeq/Quest commercial thyroid molecular tests.

**c. Genuine missed commercial Afirma/ThyroSeq tests** (probably the smallest fraction but the most manuscript-relevant)
> Real Afirma GSC or ThyroSeq v3 reports stored as text in `molecular_testing.detailed_findings` or `thyroseq_molecular_enrichment.pathology_raw` where the canonical builder's regex/parser couldn't recognize them as commercial tests. **These need a Cursor handoff to recover.**

The first audit step is to **size category (c)** vs (a)+(b). The criterion: is there an explicit named-platform call (`afirma gec`, `afirma gsc`, `thyroseq v3`, `risk of malignancy`) in the source text? If yes, it's a missed test. Diagnostic query below.

## Recommended next steps (in priority order)

### Priority 1 — Size the orphan-with-real-test population (CHEAP DIAGNOSTIC)

Run the following on BQ to classify the 9,711 orphans:

```sql
WITH orphan_pts AS (
  SELECT DISTINCT research_id
  FROM `pub_canonical.thyroseq_molecular_enrichment`
  WHERE (pathology_raw IS NOT NULL OR mutation_raw IS NOT NULL)
    AND research_id NOT IN (
      SELECT DISTINCT research_id FROM `pub_canonical.canonical_molecular_genetics_v2`
    )
)
SELECT
  COUNT(*) AS n_orphan_pts,
  COUNTIF(REGEXP_CONTAINS(LOWER(IFNULL(e.pathology_raw,'')),
    r'(afirma gec|afirma gsc|afirma gene expression|thyroseq v[23]|thyroseq.*positive|thyroseq.*negative|risk of malignancy)')) AS n_orphan_with_strong_signal
FROM orphan_pts o
JOIN `pub_canonical.thyroseq_molecular_enrichment` e USING (research_id);
```

If `n_orphan_with_strong_signal` is small (<200), category (c) is small and the manuscript impact of recovering them is bounded. If it's large (>500), the manuscript needs to wait until the canonical layer is rebuilt.

### Priority 2 — Date backfill (HIGH-IMPACT, MEDIUM EFFORT)

The 903 dateless rows can mostly be rescued by joining to `canonical_fna_events_v1` and `canonical_operative_events_v1` via `fna_episode_id` / `surgery_episode_id`:
- If `fna_episode_id IS NOT NULL`: use `canonical_fna_events_v1.fna_date_resolved` as the molecular test date proxy (≤ 4 weeks after FNA is typical).
- If `surgery_episode_id IS NOT NULL`: use `canonical_operative_events_v1.resolved_surgery_date - 14 days` as a conservative back-estimate.
- Fall back to `thyroseq_molecular_enrichment.imported_at` (file upload date — coarse but better than NULL).
- LLM-extract dates from `report_text_ref` where the text contains date patterns (`MM/DD/YYYY` is in the leading line of most reports per the 16 guard rows we just analyzed).

Add `resolved_test_date_source ∈ {native, fna_linkage, surgery_linkage, imported_at_fallback, llm_extracted}` audit column.

### Priority 3 — Rebuild `molecular_episode_id` (HIGH-IMPACT, LOW EFFORT)

Generate a deterministic episode_id from a hash of `(research_id, resolved_test_date, platform, report_source_table)` so every distinct test gets a unique non-NULL identifier. Preserves audit trail; backfills the broken column.

### Priority 4 — Resolve canonical-row duplicates for single-test patients

Of the 212 multi-canonical-row patients, some have 2 rows from a SINGLE molecular test (e.g., the 15 from the mig_323 guard cleanup), others have 2 rows from 2 genuine tests (rid 8729). Add a `test_dedup_key` audit column = hash of `(research_id, resolved_test_date_within_30d, normalized_specimen_id)`. Patients with multiple canonical rows that share the same `test_dedup_key` are likely duplicates and should be flagged for review.

### Priority 5 — Parse-status escalation pass

For the 1,053 rows with `parse_status ∈ {partial, no_detailed_block, minimal, empty_block}` and **non-zero report_text_length**, run a v2 parser pass that:
- Tries the Afirma `result`-field parser (from the mig_323 work) on any row whose source is `molecular_testing` and platform routing matches.
- Tries the ROM%-band inference fallback (from mig_321) for any ThyroSeq row with a recoverable ROM%.
- Logs which exit point set each row's call so the next audit can be even more targeted.

## Manuscript impact

If priorities 1+2+3 are addressed:
- ThyroSeq B3+B4 evaluable cohort could grow from 226 to potentially 300+ (depending on category-c orphan count).
- Afirma B3+B4 evaluable cohort could grow from 91 to potentially 120+.
- Era stratification (pre-2015 vs 2015+) becomes more reliable because dates are non-NULL.
- The "165 → 17 not-classifiable" finding will need a refresh — likely lands higher than 17 once orphans are pulled in.

If only priorities 2+3 are addressed (no orphan recovery), the impact is methodological (cleaner audit trail) but the manuscript headlines won't move much.

## Audit-trail recommendation

This audit should be filed as a **Notable Finding** at severity `hypothesis_generating`:
- `finding_id`: `NF-2026-05-13-canonical-molecular-coverage-gap`
- `title`: "canonical_molecular_genetics_v2 has 10.6% patient coverage of source-table molecular evidence; date completeness 35%; molecular_episode_id broken"
- `severity`: `hypothesis_generating` (could escalate to `publishable` if orphan recovery materially shifts manuscript results)
- `domain`: data_quality
- `linked_manuscripts`: EXT2-4

## Outputs of this audit

- `GENETICS_COMPLETENESS_AUDIT_20260513.md` (this file)
- `CURSOR_PROMPT_canonical_molecular_completeness.md` (handoff for priorities 1–5)
- VC-MOL-PARSE-002 already closed (this audit confirmed the within-canonical band coverage; the broader-scope coverage gap is separate)
