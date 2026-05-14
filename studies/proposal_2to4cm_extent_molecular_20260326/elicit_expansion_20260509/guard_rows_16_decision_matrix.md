# 16 reported_text guard rows — decision matrix and recommended Cursor handoff

**Source:** Cursor mig_323 diff report (`scripts/output/mig_323_diff_report_20260513.md` §"Rows flagged (reported_text guard — NOT auto-applied)").
**Verified against:** `pub_canonical.canonical_molecular_genetics_v2` + `thyroseq_molecular_enrichment` + `molecular_testing` raw fields, 2026-05-13.

## Headline finding from the diagnostic pull

**15 of the 16 ThyroSeq rows are parser hallucinations.** They share a common pattern:
- Patient actually had an Afirma test (the report text shows the Afirma GSC binary call: "benign", "suspicious", or a 4%/75% ROM%)
- The ThyroSeq parser nonetheless found enough mutation/marker text in the same report (BRAF/RAS/MTC/parathyroid keywords, all of which Afirma+Xpression-Atlas reports include) to assemble a fake "LOW" or "HIGH" ThyroSeq band
- mig_323 correctly created the Afirma row alongside but, per the reported_text guard, did not delete the ThyroSeq row

Each patient therefore has **TWO canonical rows** — one fabricated ThyroSeq (with `band_source = 'reported_text'`) and one correct Afirma (created by mig_323). Both currently sit in `canonical_molecular_genetics_v2` and both currently feed the Table 3 v3 denominators.

**The single exception is research_id 8729**, a genuine dual-platform case: the patient had ThyroSeq v3 done on one specimen (5.8 cm left nodule, HRAS-positive, INTERMEDIATE-HIGH band, ~70% ROM) **and** Afirma done on a different specimen. Both rows are real. No change needed.

**The other exception with a twist is 5724** — a cancelled ThyroSeq v2 test. The ThyroSeq band is fabricated (parser saw the mutation-test menu but the test was not run), and there's no actual Afirma result either. Both rows should be downgraded to non_diagnostic.

## Decision summary (full per-row matrix at `guard_rows_16_decision_matrix.csv`)

| Decision class | n | Action |
|---|---:|---|
| `fabricated_thyroseq_real_afirma_benign` | 6 | Mark ThyroSeq row as `superseded_by_afirma_row`; the existing Afirma row is correct (or needs ORC corrected from 'other' to 'negative'). |
| `fabricated_thyroseq_real_afirma_suspicious` | 5 | Mark ThyroSeq row as `superseded_by_afirma_row`; Afirma row's 'suspicious' is correct. |
| `fabricated_thyroseq_real_afirma_HIGH_RISK_or_positive` | 2 | Mark ThyroSeq row as `superseded_by_afirma_row`; Afirma row's 'positive'/'suspicious' is correct (BRAF or HRAS context). |
| `fabricated_thyroseq_real_afirma_no_result` | 1 | Mark ThyroSeq row as superseded; downgrade Afirma row to 'non_diagnostic' (low follicular content). |
| `fabricated_thyroseq_test_was_cancelled` | 1 | Mark BOTH rows as non_diagnostic/cancelled (no real molecular result on either platform). |
| `fabricated_thyroseq_real_quest_in_house_panel` | 1 | Reclassify ThyroSeq row to `platform='Other'`; drop rom_descriptor (Quest panels do not report bands). |
| `genuine_dual_platform_keep_both` | 1 | No change (research_id 8729). |
| **Total** | **16** | |

## Manuscript impact (back-of-envelope)

If all 15 fabricated ThyroSeq rows are removed from the ThyroSeq denominator and the corresponding Afirma rows are kept, the v3 Bethesda III/IV evaluable cohorts shift by at most:
- ThyroSeq: 226 → ~211 (−7%)
- Afirma: 91 → +5 to +8 (depending on whether some currently-not-classifiable Afirma rows get a correctness boost)

The Wilson 95% CIs on Table 3 already bracket the magnitude of this shift; **the v3 headlines should not change materially**. Updated numbers can be verified against BQ after the cleanup mutation runs.

## Recommended Cursor handoff prompt

**Scope:** small fix; bundle with the prior mig_323 audit-column conventions; bumps `thyroid-integration` to v2.2.1 (patch).

```
Reference: mig_323 reported_text guard residual cleanup.
Source-of-truth: studies/proposal_2to4cm_extent_molecular_20260326/elicit_expansion_20260509/guard_rows_16_decision_matrix.csv

Goal:
1. For each of the 15 'fabricated_thyroseq_*' rows in the matrix, UPDATE the ThyroSeq
   row in pub_canonical.canonical_molecular_genetics_v2:
     - Set platform_reclass_status = 'superseded_by_afirma_row' (new audit column)
     - Set rom_descriptor = NULL (the band was fabricated)
     - Set overall_result_class = 'superseded'
     - Preserve original values in pub_archive snapshot
2. For the rows the matrix flags 'update Afirma row ORC from other to negative'
   (research_ids 8233, 9991, 10174, 10699, 10926, 10939), UPDATE the companion
   Afirma row's overall_result_class accordingly with band_source='afirma_result_field'.
3. For research_id 5724, downgrade BOTH rows to non_diagnostic.
4. For research_id 11156, reclassify the ThyroSeq row to platform='Other' and drop
   the rom_descriptor.
5. NO CHANGE to research_id 8729 (genuine dual-platform).

Hard rules:
- Snapshot pub_canonical.canonical_molecular_genetics_v2 to pub_archive before MERGE.
- Append new MFL row MFL-YYYYMMDD-EXT2-4-GUARD-CLEANUP linked to EXT2-4 (rec1GJyrmKdKxjlaY).
- Append new DFL row linked to the prior DFL chain.
- Update VC-MOL-PLATFORM-001 lifecycle: PARTIAL_PASS -> Verified.
- After MERGE, re-run sql/04b_table3_v2_actual_reported_call.sql, capture new cell
  counts, update build_table3_v2_actual_call.py + build_figures_v2.py +
  build_manuscript_docx.js with refreshed numbers. Rebuild manuscript_v3_draft.docx
  and manuscript_v3_package_20260509.zip.
- Bump thyroid-integration v2.2.0 -> v2.2.1 with CHANGELOG entry.

Acceptance criteria:
- 15 ThyroSeq rows have platform_reclass_status='superseded_by_afirma_row' and
  rom_descriptor=NULL.
- 6 Afirma companion rows have overall_result_class='negative' (was 'other').
- BOTH 5724 rows are 'non_diagnostic'.
- The 11156 row is platform='Other'.
- 8729 unchanged.
- Pre-merge snapshot is row-identical to pre-state.
- VC-MOL-PLATFORM-001 lifecycle = Verified.
- manuscript_v3_draft.docx regenerated with refreshed numbers; number-consistency
  spot-check passes across docx + Table 3 v3 CSV + executive summary.
```

## Audit chain

The cleanup builds on the existing chain:
`MFL-20260509-EXT2-4-ELICIT-EXPANSION` →
`...-TABLE3-CORRECTION` →
`...-FULL-PACKAGE-v2` →
`...-PARSER-FIX-REFRESH` →
`...-PLATFORM-RECLASS-REFRESH` →
**`...-GUARD-CLEANUP` (this work, when applied)**

The mig_323 diff report's "Pre-existing call disagreements (INSPECT BEFORE APPLY)" section listed an additional 48 rows where mig_323 auto-overwrote pre-existing band assignments (mostly `current=intermediate → proposed=positive` from numeric_rom_inferred). Those are separate from the 16 reported_text guard rows and have already been applied; this matrix concerns only the 16 still-pending rows.
