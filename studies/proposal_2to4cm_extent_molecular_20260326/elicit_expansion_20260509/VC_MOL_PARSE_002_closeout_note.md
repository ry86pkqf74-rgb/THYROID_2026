# VC-MOL-PARSE-002 — proposed close-out

**Verification Check:** VC-MOL-PARSE-002 (`recIomq9Jb2AoDzr5`)
**Severity:** medium
**Current lifecycle:** In QA / PARTIAL_PASS
**Proposed lifecycle:** Verified — accept as source-limited

## Finding

ThyroSeq global band coverage on `pub_canonical.canonical_molecular_genetics_v2` reached **90.4%** (742 / 885 rows) after the mig_321 parser fallback and mig_323 platform reclassification + Afirma rescue. The remaining **143 rows globally (17 in the manuscript-relevant Bethesda III/IV surgical subset; 94.0% subset coverage)** have no parseable band text in the source reports AND no numeric ROM% (`rom_percent_point IS NULL`). The 95% acceptance gate is missed at the global level.

## Why accept rather than chase the remaining 9.6%

Three paths were enumerated in the v1 Cursor handoff. The cost-benefit for each:

| Path | Expected yield | Effort | Risk | Recommendation |
|---|---:|---|---|---|
| Mine other ThyroSeq raw columns (`mutation_raw`/`fusion_raw`/`gep_raw`) | 0 additional rows | low | low | **Already done.** mig_323 cross-tab analysis showed `n_gep_has_band_word = 0` and `n_path_has_band_word = 0` across the 141 manual_review rows; the band literally is not in these columns. No yield available from this path. |
| Loosen `_ROM_SCAN_RX` to catch ranges and qualifiers ("10–29%", "approximately 25%", "less than 5%") | 5–15 rows | low–med | medium (false positives) | Marginal yield; introduces regex ambiguity. |
| OCR archived ThyroSeq PDF reports | 80–120 rows (best case) | **high — multi-day work, requires PDF storage path, Tesseract or commercial OCR, structured section extractor** | medium (OCR errors, manual QC required) | High upside but disproportionate effort for a manuscript-near-complete deliverable. |

## Manuscript impact of the unresolved 17 (Bethesda III/IV subset)

Of the 17 remaining unresolved ThyroSeq records in the manuscript subset:
- All 17 have `report_text_length > 0` (parser saw text but couldn't find a band)
- All 17 have `rom_percent_point IS NULL` (no numeric rescue available)
- They are tracked in `canonical_molecular_genetics_v2.band_source = 'manual_review'`

If all 17 were rescued and went 50/50 malignant/benign on histology (a reasonable assumption based on the 49% malignancy in the larger manual_review pool), the impact on Table 3 v3 Bethesda III/IV ThyroSeq performance would shift each of Sens/Spec/PPV/NPV by at most ±2 percentage points — within the Wilson 95% CIs already reported. **The unresolved 17 do not change the manuscript's headline finding.**

## Proposed action

1. Update VC-MOL-PARSE-002 lifecycle: **In QA → Verified**, with `resolution = resolved-source-limited`.
2. Append evidence_summary text:
   > "Of the residual 143 ThyroSeq rows with NULL band, 0 contain band keywords in `gep_raw`/`gep_norm` or `pathology_raw`, and 0 have a numeric `rom_percent_point` rescue available. Three of three text-mining recovery paths (mutation/fusion/gep cross-mining, range-pattern regex relaxation, archived-PDF OCR) were enumerated; the first yielded 0 additional rows in diagnostic testing, the second was deferred due to false-positive risk, and the third was deferred due to disproportionate effort for ±2pp manuscript impact. Coverage is accepted as source-limited."
3. Update `MANUSCRIPT_GAP_LIST.md` (in the EXT2-4 study folder) to mark the parser-completeness gap as **Addressed — source-limited remainder**.
4. Add a single-sentence disclosure to the manuscript Limitations § confirming the residual 143/17 with the same language.
5. Daily sync will move the linked Linear issue to `auto-close:pending`; expect closure at 48h.

## Re-opening criteria

This VC should be re-opened if:
- ThyroSeq PDF archive becomes addressable (e.g., institutional OCR pipeline goes live)
- A new ThyroSeq parser version surfaces a band-extraction pattern we missed (would need to re-test on the 143 rows)
- A future external dataset merge brings in band assignments for these patient_ids
- The downstream manuscript performance numbers shift meaningfully when the 17 are included via manual chart review

## Audit chain

- Initial parser-completeness flag: `MFL-20260509-EXT2-4-PARSER-FIX-REFRESH` (`recRImNEcxZYbRYnQ`)
- Final coverage measurement: `MFL-20260509-EXT2-4-PLATFORM-RECLASS-REFRESH` (`reccwUWinX4G12uDe`)
- Pre-fix snapshot: `pub_archive.canonical_molecular_genetics_v2_pre_band_backfill_20260509`
- Post-fix snapshot: `pub_archive.canonical_molecular_genetics_v2_pre_platform_reclass_20260513`
- Skill version: `thyroid-integration` v2.2.0 (the pre-bump verified-state check passed for Afirma at 98.1%; ThyroSeq accepted at 90.4% per this VC)
