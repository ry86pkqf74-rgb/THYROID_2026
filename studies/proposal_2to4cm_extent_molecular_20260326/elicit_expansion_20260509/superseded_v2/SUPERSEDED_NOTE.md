# SUPERSEDED — v2 manuscript package (ThyroSeq band gap present)

This folder contains the EXT2-4 Elicit-expansion manuscript **v2** package
produced on 2026-05-09 **before** the ThyroSeq band-assignment gap was repaired.

## What was wrong in v2

`canonical_molecular_genetics_v2` had 647 ThyroSeq rows with `rom_descriptor IS NULL`,
including an estimated 165 Bethesda III/IV evaluable patients that therefore landed in
`unknown_or_excluded` in Table 3. This inflated the "not classifiable" count and
suppressed the diagnostic-performance estimates (sensitivity, specificity, NPV, PPV).

## The fix

Repair: `mig_321_thyroseq_band_backfill_bq.py` (run 2026-05-09).
Parser: `thyroseq_detailed_parser.py` v4 — added Fallback A (numeric ROM→band) and
Fallback B (full-text scan). Band results:
- reported_text = 150 rows
- numeric_rom_inferred = 356 rows
- manual_review (still unresolvable) = 141 rows

Result: ThyroSeq `unknown_or_excluded` in Bethesda III/IV evaluable set: **165 → 17**.

## Coverage gate note

Post-merge `frac_with_band` = **83.8%** (742/885 ThyroSeq rows). This is below the
95% target in VC-MOL-PARSE-001. The 141 unresolvable rows have no parseable text AND no
numeric `rom_percent_point`. Skill version bump to v2.2.0 is BLOCKED until the coverage
gate passes (requires additional data, e.g. full-text OCR or upstream test-report PDFs).

## Where to find the v3 package

The corrected v3 package is in the parent folder (`manuscript_v2_package_20260509/`
subdirectory, now rebuilt with v3 data). All Table 3 CSVs and figures have been
regenerated.

**Do NOT cite files in this superseded_v2/ folder for diagnostic-performance claims.**
Preserved per the project's append-only / never-delete rule.

Corresponding MFL row: `MFL-20260509-EXT2-4-PARSER-FIX-REFRESH` (to be logged in
THYROID_MANUSCRIPT Manuscript Feedback Log).
