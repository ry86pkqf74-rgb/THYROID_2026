# Imaging Linkage Truth Sync — 2026-03-14

**Script**: `scripts/94_pipeline_gap_closure.py` (Workstream B)  
**Date**: 2026-03-14

---

## Objective

Correct documentation errors about `imaging_fna_linkage_v3`, populate
`fna_episode_master_v2.linked_imaging_nodule_id` from the linkage table,
and formally retire `imaging_nodule_long_v2` as a feature source in favor of
`imaging_nodule_master_v1`.

---

## Documentation Correction

Prior documentation (including AGENTS.md, `docs/post_maturation_gap_audit_20260313.md`)
incorrectly stated `imaging_fna_linkage_v3` had **0 rows**.

**Actual state** (verified 2026-03-14 against live MotherDuck):
- `imaging_fna_linkage_v3`: **9,024 rows** (not 0)

The confusion originated from `imaging_fna_linkage_v2` (a different table) and/or
`fna_episode_master_v2.linked_imaging_nodule_id` being 0 (which is the backfill column,
not the linkage table itself).

---

## Linkage Quality Summary

### imaging_fna_linkage_v3 (score_rank = 1, best candidate per FNA episode)

| Tier | Score Band | N episodes | N patients |
|---|---|---|---|
| high_confidence | ≥ 0.65 | **646** | ~580 |
| plausible | 0.45 – 0.65 | **3,883** | ~2,100 |
| weak | 0.013 – 0.45 | **3,395** | ~1,800 |
| unlinked | 0.0 | **3** | 3 |

Average linkage score: **0.441** (plausible range overall)

### Composite Linkage Score Components
Per `docs/analysis_resolved_layer.md`:
- Temporal proximity (0.50 weight)
- Laterality concordance (0.30 weight)  
- Size compatibility (0.20 weight, where available)
- Minus ambiguity penalty (0.0/0.1/0.2 for 1/2/3+ candidates)

---

## Backfill Executed

### `fna_episode_master_v2.linked_imaging_nodule_id`

| | Before | After |
|---|---|---|
| Rows with imaging link | **0** | **1,454** |
| % of FNA episodes linked | 0% | **16.2%** (of ~8,978 episodes) |

**Backfill rule**: `score_rank = 1 AND CAST(linkage_score AS FLOAT) >= 0.45`
(plausible or better, best candidate only)  
**Join key**: `(research_id, fna_episode_id)` — joining on `fna_episode_id` alone
causes cartesian product (~47M rows); composite key is required.

### Score-stratified breakdown of backfilled rows
- High confidence (≥ 0.65): **646** of the 1,454
- Plausible (0.45 – 0.65): **808** of the 1,454

---

## Canonical Imaging Tables: Hierarchy Clarification

### Use `imaging_nodule_master_v1` (19,891 rows, 3,439 patients) ✅
Populated from `raw_us_tirads_excel_v1` (Phase 12 structured US Excel ingestion).  
Columns available: `tirads_reported`, `tirads_acr_recalculated`, `composition`,
`echogenicity`, `shape`, `margins`, `calcifications`, `length_mm`, `width_mm`,
`height_mm`, `max_dimension_cm`, `laterality`, `suspicious_flag`.

### Retire `imaging_nodule_long_v2` (10,866 rows) as feature source ❌
**ALL feature columns are 0/NULL**:
- `size_cm_max`: 0
- `composition`: 0
- `linked_fna_episode_id`: 0
- `suspicious_node_flag`: FALSE for all

Root cause: V2 ImagingNoduleExtractor was defined and tested but its outputs were
**never materialized to MotherDuck**. The schema exists but data was never populated.

**Action**: Any analytics code that queries `imaging_nodule_long_v2` for feature data
should switch to `imaging_nodule_master_v1` or `extracted_tirads_validated_v1`.

### Per-Patient Summary: `imaging_patient_summary_v1` (3,439 patients)
Columns: `tirads_worst_category`, `tirads_best_category`, `multifocal_flag`,
`bilateral_disease_flag`, `n_nodule_records`, `nodule_size_max_mm`.

---

## Tables Created/Updated This Sprint

| Table | Action | Rows |
|---|---|---|
| `fna_episode_master_v2.linked_imaging_nodule_id` | Backfilled | 1,454 |
| `imaging_fna_linkage_summary_v1` | Created | 4 (tier breakdown) |

---

## Recommendations

1. **AGENTS.md update**: Correct `imaging_fna_linkage_v3` = 9,024 rows (not 0);
   `fna_episode_master_v2.linked_imaging_nodule_id` = 1,454 (not 0)
2. **Dashboard rewire**: Any Imaging/Nodule V2 dashboard tab that reads
   `imaging_nodule_long_v2.size_cm_max` should be redirected to
   `imaging_nodule_master_v1.max_dimension_cm`
3. **Analytics alert**: `imaging_fna_linkage_v3` plausible-tier links (n=3,883)
   include some false-positive temporal matches; use `score >= 0.65` for
   publication-grade analyses; `score >= 0.45` is acceptable for exploratory work
4. **Missing imaging data**: 7,432 FNA episodes (83.8%) still unlinked — reflects
   genuine absence of structured US imaging for a large fraction of the cohort,
   not a linkage failure
