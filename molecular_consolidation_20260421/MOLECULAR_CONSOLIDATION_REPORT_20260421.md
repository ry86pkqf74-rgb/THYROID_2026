# Molecular / Genetics Table Consolidation — 2026-04-21

**Database:** `thyroid_canonical_publication_v1_0` (MotherDuck)
**Owner:** Logan Glosser (LGLOSSE@emory.edu)
**Run:** 2026-04-21

## Outcome

13 fragmented molecular tables consolidated into **2 canonical tables + 2 flat views**.
All legacy tables archived to Parquet *and* renamed in-place with a
`_DEPRECATED_20260421` suffix (kept ~30 days for live rollback).

## New artefacts (in MotherDuck)

| Object                              | Rows  | Distinct patients | Grain                                                     |
|-------------------------------------|------:|------------------:|-----------------------------------------------------------|
| `molecular_genetics_test_v2`        | 1,384 | 1,151             | 1 row per real ThyroSeq / Afirma / NGS test episode       |
| `molecular_genetics_from_notes_v2`  | 1,738 |   605             | NLP-derived genetics facts from clinical notes            |
| `molecular_variant_flat_v2` (view)  |   936 |   568             | UNNEST of `gene_mutations_variants`                       |
| `molecular_fusion_flat_v2`  (view)  |    60 |    46             | UNNEST of `gene_fusions_list`                             |

`molecular_genetics_test_v2` retains:
- 7 DETAILED RESULTS sub-fields (specimen_adequacy, gene_mutations, gene_fusions, cna, gep, parathyroid, medullary)
- Header fields: `test_result_summary`, `rom_descriptor`, `rom_percent_low/high/point`, `rom_description`
- Nested `gene_mutations_variants STRUCT[]` and `gene_fusions_list STRUCT[]`
- Afirma pass-throughs: `afirma_braf_result`, `afirma_mtc_result`, `afirma_tert_c228t_result`, `afirma_tert_c250t_result`, `afirma_retptc_result`
- **Structured episode flags carried forward** so signals from short / OCR-degraded
  reports and notes-derived rollups are not lost: `braf_flag`, `braf_variant`,
  `ras_flag`, `ras_subtype`, `ret_flag`, `ret_fusion_flag`, `tert_flag`,
  `ntrk_flag`, `eif1ax_flag`, `tp53_flag`, `pax8_pparg_flag`, `cna_flag`,
  `fusion_flag`, `loh_flag`, `alk_flag`, `high_risk_marker_flag`,
  `inadequate_flag`, `cancelled_flag`, `overall_result_class`.
- Provenance: `report_text_source`, `report_text_length`, `report_text_ref`,
  `report_source_table`, `parser`, `parse_status`, `n_fields_parsed`,
  `built_at`, `builder_version='v3_2026-04-21'`.

## Parser

- `molecular_consolidation_20260421/thyroseq_detailed_parser.py`
- Handles ThyroSeq V3 GC and Afirma GSC reports.
- Heavy OCR normalization for "Gene mutations / fusions / Copy number alterations / Gene expression profile / Parathyroid / Medullary–C cells" headers.
- Header parser pulls TEST RESULT, ROM% (`<3%`, `~3-5%`, `~80%`, etc.) and INTERPRETATION prose.
- Variant extractor recognizes 50+ thyroid-relevant gene tokens, c. / p. notation, and AF%.

## Text-source priority cascade

For each episode the parser chooses the longest available text in this order:

| Priority | Source column                                      | Episodes used |
|---------:|-----------------------------------------------------|--------------:|
| 1        | `enrichment.pathology_raw` (from `thyroseq_molecular_enrichment`) | 1,076 |
| 2        | `testing.detailed_findings` (from `molecular_testing`)            |   167 |
| 3        | `episode.detailed_findings_raw` (from `molecular_test_episode_v2`)|    71 |
| 4        | synthesized from short fields (`mutation`, `result`, etc.)        |     7 |
| n/a      | empty (no text anywhere)                                          |    63 |

When the parser found no variants but the episode's structured flag was TRUE
(e.g. `braf_flag=TRUE` with no parsed BRAF variant), a minimal variant entry
is synthesized with `source_call='episode_flag:<colname>'`.
**647 of 1,384 episodes were augmented this way (770 variants + 40 fusions).**

## Verification (Step 8)

| Check | Result |
|-------|--------|
| 8a row counts | master 1,384 / 1,151 patients; notes 1,738 / 605; variant_flat 936; fusion_flat 60 |
| 8b parse_status | ok 26.8%, partial 12.5%, minimal 13.4%, empty 4.6%, no_detailed_block 42.7% |
| 8c platform/parser | ThyroSeq×thyroseq 833, Afirma×afirma 414, NGS_unspec 82 |
| 8d ROM coverage | 448 numeric, 239 descriptor, 337 description (of 854 ThyroSeq rows) |
| 8e TERT | 79 positive total (14 C228T + 1 C250T + 63 from flag + 1 OCR drift) |
| 8f top variants | BRAF p.V600E=329, RET=177, NRAS=168, TERT=78, HRAS=42, NRAS p.Q61R=34 |
| 8g legacy parity | 738 mutation positives (vs legacy 703 distinct IDs); BRAF 337 vs legacy 376 (90%) |
| 8j Afirma | 212 BRAF / 221 MTC / 38 TERT-C228T / 143 RET-PTC results extracted |
| 8k notes | 1,600 present-gene mentions + 138 negated |

The remaining 39-patient gap on BRAF is expected: legacy `canonical_molecular_tested_v1.braf_positive_canonical` blends formal-report flags with notes-derived BRAF detections, while the new design intentionally keeps notes facts in `molecular_genetics_from_notes_v2`.

## Archive

`/Users/ros/THyroid 2026/archive/molecular_legacy_20260421/`

Contains all 13 source tables as ZSTD-compressed Parquet plus:

- `MANIFEST.json`     — table list with row counts and sizes
- `RENAMES.json`      — ALTER TABLE history
- `DROPPED_VIEWS.json`— captured definitions of any views dropped before rename (none)

Total archive size ≈ **2.6 MB**.

## Deprecated tables (kept in DB ~30 days for rollback)

```
genetics_per_patient_master_v1_DEPRECATED_20260421     10,871
molecular_results_DEPRECATED_20260421                  10,861
thyroseq_molecular_enrichment_DEPRECATED_20260421      10,861
molecular_testing_DEPRECATED_20260421                  10,861
molecular_test_episode_v2_DEPRECATED_20260421          10,650
analysis_molecular_subset_v1_DEPRECATED_20260421       10,025
genetics_per_test_master_v1_DEPRECATED_20260421         1,742
note_entities_genetics_DEPRECATED_20260421              1,738
molecular_variant_long_DEPRECATED_20260421              1,640
canonical_molecular_tested_v1_DEPRECATED_20260421       1,286
molecular_code_crosswalk_DEPRECATED_20260421               44
molecular_assay_dictionary_DEPRECATED_20260421              4
molecular_ingestion_runs_DEPRECATED_20260421                1
```

## Rollback

In-DB rollback (within the 30-day window):

```sql
ALTER TABLE genetics_per_patient_master_v1_DEPRECATED_20260421
    RENAME TO genetics_per_patient_master_v1;
-- ... repeat for each
DROP VIEW  IF EXISTS molecular_variant_flat_v2;
DROP VIEW  IF EXISTS molecular_fusion_flat_v2;
DROP TABLE IF EXISTS molecular_genetics_from_notes_v2;
DROP TABLE IF EXISTS molecular_genetics_test_v2;
```

Restore from Parquet (after permanent drop):

```python
import duckdb, glob
con = duckdb.connect("md:thyroid_canonical_publication_v1_0")
for p in glob.glob("/Users/ros/THyroid 2026/archive/molecular_legacy_20260421/*.parquet"):
    name = p.split("/")[-1].replace(".parquet", "")
    con.execute(f"CREATE TABLE {name} AS SELECT * FROM read_parquet('{p}')")
```

## Repo files added

| Path | Purpose |
|------|---------|
| `molecular_consolidation_20260421/thyroseq_detailed_parser.py` | Parser module (ThyroSeq + Afirma) |
| `molecular_consolidation_20260421/01_env_check.py`             | Step 1 sanity script |
| `molecular_consolidation_20260421/07_archive_legacy.py`        | Step 3 Parquet archive |
| `molecular_consolidation_20260421/08_build_master.py`          | Steps 4–7 build master + notes + views |
| `molecular_consolidation_20260421/09_verify.py`                | Step 8 verification queries |
| `molecular_consolidation_20260421/10_deprecate.py`             | Step 9 deprecation rename |
| `molecular_consolidation_20260421/MOLECULAR_CONSOLIDATION_REPORT_20260421.md` | This report |
