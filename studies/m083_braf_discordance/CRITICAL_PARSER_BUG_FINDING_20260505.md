# 🚨 CRITICAL — M083 ThyroSeq "false-negative" finding is a parser bug, not biology

> Cowork audit, 2026-05-05. **Do NOT publish M083 with the current finding.** The 99-patient ThyroSeq false-negative pattern reflects a parsing pipeline gap, not actual ThyroSeq detection failure.

---

## What I claimed earlier (wrong)

After mig_319 verification I wrote that ThyroSeq systematically undercalls BRAF V600E vs Afirma in the Emory cohort, with a 99/159 (62.3%) false-negative rate. The claim was based on the cohort flat's `thyroseq_braf` field, which reads `canonical_molecular_genetics_v2.braf_flag` for ThyroSeq-platform records.

## What's actually happening

The 99 patients flagged as `thyroseq_braf='negative'` have ThyroSeq records where:

| Layer | What was captured | What was missed |
|---|---|---|
| Top-level result | `gene_mutations_status='Positive'` for **96/99** | — |
| Variant block | — | **`braf_flag` defaulted to false on 99/99** |
| Variant-long extraction | — | **0 BRAF records in `readonly_share.molecular_variant_long` for any of the 99** |
| `braf_variant` | NULL on 99/99 | — |

**ThyroSeq detected something positive on 96 of 99 patients** (the report header parsed cleanly), but **the variant-level detail block was never extracted**, so `braf_flag` stayed false at every downstream layer.

### Two pipeline defects

| Defect | n records of 99 | Fingerprint |
|---|---:|---|
| **A. Wrong-parser routing** | 30 | `parser='afirma'` despite `platform='ThyroSeq'` |
| **B. Variant-block skip** | 69 | `parser='thyroseq'` AND `parse_status='no_detailed_block'` |

Defect A is a script-level routing error (the Afirma parser was applied to ThyroSeq reports). Defect B means the ThyroSeq parser ran successfully on the report header but bailed before extracting the variant table — a parser-coverage gap.

## Why this matters

The cohort flat (`cohort_m083_braf_dual_platform_discordance_v1`) is a faithful read of the canonical layer. The defect is **upstream** of the cohort flat. So:

- mig_319 is **technically correct** — it built the view from canonical fields per the prompt.
- The cohort flat **accurately reports** `braf_flag=false` for all 99.
- The **interpretation** ("ThyroSeq under-detects BRAF") is wrong because it confuses parser-output absence with detection-result absence.

If this manuscript shipped with the current finding, it would be a **retraction-class error** — claiming a clinical-platform difference when the difference is in the data pipeline.

## What ThyroSeq actually called

Unknown, until the parser is fixed and re-run. The raw text is in `canonical_molecular_genetics_v2.report_text_ref` (pointer to `enrichment.pathology_raw`) for all 99 records. The parser needs to:

1. **Routing fix** (Defect A): when `platform='ThyroSeq'`, always invoke the ThyroSeq parser, never the Afirma parser. Add a regression test that asserts `parser=platform` parity for 100% of records in a sentinel set.
2. **Variant-block extraction** (Defect B): when `parse_status='no_detailed_block'`, re-attempt extraction with a regex/structural fallback that looks for the typical ThyroSeq mutation-table format (e.g. `BRAF V600E (positive, AF XX%)` per the synthetic test file).

## Cost of fix

Zero new lab work. The raw report text is already in MotherDuck. A re-parse pass + downstream rebuild of `canonical_molecular_genetics_v2.braf_flag` + `molecular_variant_long.gene_symbol='BRAF'` rows is cheap. mig_320 cursor prompt drafted.

## Acceptance gate after mig_320

The Cortex/parser fix should be considered acceptable when:

```sql
-- Among the 99 patients with gene_mutations_status='Positive' and the historical false-negative pattern
WITH affected AS (
  SELECT research_id
  FROM manuscript_workspace.cohort_m083_braf_dual_platform_discordance_v1
  WHERE afirma_braf='positive' AND thyroseq_braf='negative' AND path_braf_status='positive'
)
SELECT
  COUNT(DISTINCT a.research_id) AS n_affected,
  COUNT(DISTINCT CASE WHEN cmg.parse_status NOT IN ('no_detailed_block','partial')
                       AND cmg.parser='thyroseq' THEN a.research_id END) AS n_repaired,
  COUNT(DISTINCT CASE WHEN cmg.braf_flag THEN a.research_id END) AS n_now_braf_pos
FROM affected a
JOIN main.canonical_molecular_genetics_v2 cmg
  ON CAST(cmg.research_id AS VARCHAR) = a.research_id
WHERE cmg.platform='ThyroSeq';
-- Acceptance: n_repaired = 99 (all routing + parse-block defects fixed)
-- Then: n_now_braf_pos is the real ThyroSeq BRAF-positive count among these
-- The interpretation "ThyroSeq under-calls BRAF" is only valid if n_now_braf_pos < 99
```

After mig_320 lands, mig_319's cohort_m083 view should be **rebuilt** (CREATE OR REPLACE) so the discordance cells reflect post-parser-fix reality. Cohort N should remain 167; per-cell counts will shift.

## Status update

| Carry-forward | Old state | New state |
|---|---|---|
| `CF-M083-STUB` | CLOSED via mig_319 | Stays closed (view shape is fine) |
| **`CF-M083-PARSER-BUG`** | — | **NEW OPEN** — owner cursor (mig_320) |
| `M083 publication readiness` | apparent | **BLOCKED** until mig_320 |

The earlier handoff doc `MIG_319_VERIFICATION_AND_HEADLINE_FINDING_20260505.md` should be considered superseded for the headline finding. Cohort-shape verification (167 rows × 31 cols, path coverage 99.4%) remains valid.
