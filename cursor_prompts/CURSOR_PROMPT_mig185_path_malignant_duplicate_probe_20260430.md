# Cursor Prompt — mig_185 canonical_path_malignant_events_v1 duplicate row probe + dedupe scoping

**Date:** 2026-04-30
**Lane:** mig_185 / path_malignant_duplicate_dedupe_scoping
**Batch (proposed):** `mig_185_path_malignant_dedupe_scoping_20260430`
**Trigger:** Logan flagged rid 2480 having 2 identical path-malignant rows during R1 size CSV review. Cowork verified 533 duplicate (rid, surgery_episode_id, tumor_ordinal) tuples across the table (6,689 total events vs 6,156 distinct = ~8% duplication).
**Posture:** **READ-ONLY scoping + skeleton SQL.** No execute against MotherDuck.
**Target DB:** `thyroid_canonical_publication_v1_0`
**Touches:** none (read-only)

---

## Mission

Identify the structural cause of the 533 duplicate rows on `canonical_path_malignant_events_v1`, classify each duplication pattern, and propose a Logan-ratifiable dedupe rule. Logan ratifies before any apply.

**Live MD probe (Cowork 2026-04-30):**
- 6,689 total rows in `canonical_path_malignant_events_v1`
- 6,156 distinct `(research_id, surgery_episode_id, tumor_ordinal)` tuples
- **533 duplicate events** (excess rows beyond distinct grain)
- rid 2480 example: 2 identical rows — same `(surgery_episode_id=1, tumor_ordinal=1, primary_histology='PTC', histology_variant='microcarcinoma', size=NULL, ete='x', t_stage=NULL)`

---

## Required scope

### §1 Duplicate-pattern classification

Bucket the 533 duplicates by what differs (or doesn't) between paired rows:

```sql
WITH grain_counts AS (
  SELECT research_id, surgery_episode_id, tumor_ordinal, COUNT(*) AS n_rows
  FROM main.canonical_path_malignant_events_v1
  GROUP BY 1, 2, 3
  HAVING COUNT(*) > 1
)
SELECT n_rows AS rows_per_grain, COUNT(*) AS n_grains_with_this_count, SUM(n_rows) AS total_event_rows
FROM grain_counts GROUP BY 1 ORDER BY 1;

-- Then for each grain with >1 row, classify:
-- A. fully_identical — every column matches (true duplicate)
-- B. differs_in_audit_only — only build_ts / synoptic_row_ix / extracted_at differ
-- C. differs_in_synoptic_row_ix — different source row but same finding
-- D. differs_clinically — different size / stage / ete (genuine multi-event)
```

### §2 Trace to upstream Script 361 build

Read `scripts/361_*` (the SELECT * + malignancy filter that built `canonical_path_malignant_events_v1` from CTC pre361). Identify whether:
- (a) The dedupe key was missing from the build (likely — Script 361 was a faithful-copy lane)
- (b) The upstream CTC pre361 archive already had duplicates
- (c) A later script (363, 365, etc.) introduced them

### §3 Investigate `synoptic_row_ix` as a tiebreaker

Per `reference_synoptic_row_ix.md`, `synoptic_row_ix` is a Script 108 pandas-load-order global index — should be unique-ish per source row. Check whether duplicate event rows have different `synoptic_row_ix` (indicating two source synoptic rows mapped to the same logical event).

### §4 Propose dedupe rule for Logan ratification

Three plausible rules:

| Rule | Approach | Pros | Cons |
|---|---|---|---|
| R-A | Drop fully-identical duplicates only (Bucket A) | Safest; lossless | May not address all 533 |
| R-B | Dedupe by (rid, surg_ep, tumor_ord, synoptic_row_ix) keeping MAX(build_ts) | Addresses build-copy duplicates | May lose multi-source events |
| R-C | Dedupe by (rid, surg_ep, tumor_ord) keeping MAX completeness score (non-null col count) | Aggressive consolidation | Loses provenance |

### §5 Author placeholder skeleton apply SQL

`qc_framework_v1/migrations/185_path_malignant_dedupe_TBD_20260430.sql` (placeholder; not for execution):
- §A pre-snapshot
- §B per-rule dedupe variants (clearly marked `-- LOGAN MUST RATIFY RULE BEFORE EXECUTION`)
- §C registry note appendix on affected tier-2 cols

### §6 Audit/report

`qc_framework_v1/reports/mig_185_path_malignant_duplicate_scoping_20260430.md`:
- §1 533-duplicate classification breakdown (Buckets A/B/C/D with counts)
- §2 Script 361 lineage trace
- §3 synoptic_row_ix analysis
- §4 R-A/R-B/R-C rule recommendation
- §5 sample 5 rids per bucket for Logan spot-check (CSV at `exports/mig185_dedupe_scoping_20260430/duplicate_examples.csv`)
- §6 downstream impact: how do canonical_invasion_events_v1, canonical_us_lymph_node_patient_rollup_v2, etc. handle the duplicates currently?

---

## Governance reminders

- Read-only investigation only. No `query_rw`. Author = `Logan Glosser <logan.glosser@gmail.com>`.

---

## Deliverables

1. `qc_framework_v1/migrations/185_path_malignant_dedupe_TBD_20260430.sql`
2. `qc_framework_v1/reports/mig_185_path_malignant_duplicate_scoping_20260430.md`
3. `exports/mig185_dedupe_scoping_20260430/duplicate_examples.csv`

Commit message: `qc: mig_185 canonical_path_malignant_events_v1 duplicate scoping (533 dups; pending Logan dedupe rule ratification)`

---

End of prompt.
