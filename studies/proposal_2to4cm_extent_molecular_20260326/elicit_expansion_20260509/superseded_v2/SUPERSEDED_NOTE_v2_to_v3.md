# SUPERSEDED: v2 → v3 reclassification

## What changed

The v2 manuscript package (2026-05-09) was superseded by v3 on 2026-05-09 after
**mig_323 platform reclassification + Afirma rescue** was applied to
`pub_canonical.canonical_molecular_genetics_v2`.

## Root cause of v2 errors

In v2, approximately 170 rows in `canonical_molecular_genetics_v2` had
`platform = 'ThyroSeq'` but their canonical source (`thyroseq_molecular_enrichment.gep_norm`)
clearly identified the underlying test as **Afirma** or **Quest Diagnostics** in-house panel.
Because the ThyroSeq band parser was applied to Afirma tests, 141 of these rows produced
`overall_result_class = NULL` and `rom_descriptor = NULL`, inflating the ThyroSeq
"not-classifiable" count to 165 and deflating the Afirma evaluable cohort.

## What mig_323 did

- **191 platform changes** applied (ThyroSeq→Afirma: 158; ThyroSeq→Other: 18;
  NGS_unspecified→Afirma/ThyroSeq: 15).
- **148 Afirma call extractions** from `molecular_testing.result` field using
  `molecular_consolidation_20260421/afirma_result_field_parser.py`.
- **16 rows flagged** (reported_text guard — pre-existing mig_321 calls, require
  manual platform confirmation; NOT auto-applied).
- **Snapshot**: `pub_archive.canonical_molecular_genetics_v2_pre_platform_reclass_20260513`.

## v2 → v3 number changes

| Metric | v2 | v3 |
|---|---|---|
| Afirma B3+B4 all sizes n (2x2) | 76 | 91 |
| Afirma sensitivity | 89.4% [77.4–95.4] | 90.4% [79.4–95.8] |
| Afirma specificity | 17.2% [7.6–34.5] | 20.5% [10.8–35.5] |
| Afirma NPV | 50.0% [23.7–76.3] | 61.5% [35.5–82.3] |
| ThyroSeq B3+B4 all sizes n (2x2) | 104 | 226 |
| ThyroSeq sensitivity | 67.3% [53.4–78.8] | 69.7% [60.5–77.6] |
| ThyroSeq specificity | 61.8% [48.6–73.5] | 63.2% [54.2–71.4] |
| ThyroSeq B3+B4 2-4cm n | 19 | 31 |
| ThyroSeq not-classifiable n | 165 | 17 |
| Afirma B3+B4 2-4cm n | 4 | 5 |

## Audit trail

- **DFL**: `DFL-20260509-EXT2-4-PLATFORM-RECLASS` (`recKXrfsM9jtzM0zG`)
- **MFL**: `MFL-20260509-EXT2-4-PLATFORM-RECLASS-REFRESH` (current session)
- **VC-MOL-PLATFORM-001**: `recPnjqNfMaE1AS9H` (lifecycle: In QA)
- **VC-MOL-PARSE-001**: `rec6xTvsRN6KHqqGa` (updating to Verified post-fix)
- **VC-MOL-PARSE-002**: to be filed for ThyroSeq coverage near-miss (90.4% < 95% target)

## Files in this superseded_v2 folder

- `SUPERSEDED_NOTE.md` — original v1 → v2 supersession note (Table 3 derived call fix)
- `SUPERSEDED_NOTE_v2_to_v3.md` — this file (v2 → v3 platform reclassification fix)
- `manuscript_v2_package_20260509_snapshot/` — full v2 package snapshot

**Do NOT cite any numbers from the v2 package for diagnostic-performance claims.**
The corrected v3 package is at the parent elicit_expansion_20260509/ directory.
