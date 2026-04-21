# CPM TIRADS Part B — delivery summary (2026-04-21)

**Status:** complete. All seven phases pushed to `origin/main`.
**Architecture:** Option C-soft (Logan's clarification 2026-04-21). Canonical
TIRADS lives on the `canonical_us_*_v2` surface at the right grain — patient
on `cupm_v2`, exam on `cuem_v2`, nodule on `cunc_v2`. Consumers JOIN with
aggregation where needed.

## Headline result

```
main.canonical_patient_master:
  columns:        1585 → 1532   (delta -53 TIRADS columns)
  rows:           10871 → 10871 (unchanged)
  distinct RIDs:  10871 → 10871 (unchanged)
  TIRADS-keyword cols on live CPM (post Part B):
    - 5 nlp_tirads_*   (out-of-scope per Part A + Part B prompt)
    - 0 non-NLP TIRADS (target met)
```

## Commit chain (all on `origin/main`)

| commit  | scope |
|---------|---|
| `c488f32` | pre-B Phase 1 — recon (cupm_v2 view + cunc_v2 schema sanity) |
| `759c5ab` | pre-B Phase 2 — built `main.cupm_v2_canonical_backfill_v1` (10871 rows) |
| `ed15f25` | pre-B Phase 3-4 — replaced cupm_v2 view (19 → 28 cols); shape verified |
| `c7ac6d5` | pre-B Phase 5-6 — coverage audit re-run; 0 gap_ABORT; QA bundle |
| `f599165` | Part B Phase 1 — coverage re-confirmed (30/8/6/6/2/1, 0 gap_ABORT) |
| `8239fbf` | Part B Phase 2 — migrated 9 cohort views to cupm_v2 |
| `0db2db6` | Part B Phase 3 recon — reader/writer triage + STOP gate |
| `934d52f` | Part B Phase 3 — 9 active reader scripts surgically migrated |
| `89ebc65` | Part B Phase 4 — 33 scripts frozen to `scripts/frozen/` |
| `a1b2fac` | Part B Phase 5 — archived CPM + dropped 53 TIRADS columns |
| `f264525` | Part B Phase 6 — QA bundle (10/10 checks pass) |

## What changed by destination

### `main.canonical_patient_master` (live)
- 53 TIRADS columns dropped via per-column `ALTER TABLE DROP COLUMN`.
- Row count unchanged (10,871). Schema slim by design.

### `main.canonical_us_patient_master_v2` (cupm_v2 view)
- Now carries 28 columns (was 19). The 9 added in pre-B:
  - 7 ports from CPM (rename-on-move): `imaging_laterality_rollup_v2`,
    `pathology_vs_imaging_laterality_concordant_v2`,
    `tumor_pathology_laterality_v2`, `any_fna_recommended_report_ever`,
    `any_fna_recommended_report_source`, `tirads_worst_rank_ever`,
    `tirads_worst_rank_source`.
  - 2 computed from `cunc_v2`: `max_nodule_size_mm`, `n_nodule_records`.
- Backed by `main.cupm_v2_canonical_backfill_v1` (snapshot of the 7 ports,
  10871 rows × 7 ported cols + 2 metadata cols).

### `manuscript_workspace.cohort_*` (9 views migrated)
- 5 rewritten to JOIN `cupm_v2`, source TIRADS values from canonical, and
  preserve legacy column names as aliases:
  - `cohort_descriptive_full_cohort_v1`
  - `cohort_m011_tirads_fna_genetics_v1` (3474 → 3286, −188 RIDs)
  - `cohort_m025_tirads_performance_v1` (3474 → 3377, −97 RIDs; dropped
    `tirads_n_sources_v12`, `tirads_reliability_v12`)
  - `cohort_m045_multimodal_risk_v1`     (1192 → 1167, −25 RIDs)
  - `cohort_m075_tirads_multi_nodule_v1` (3474 → 3286, −188 RIDs; dropped
    `tirads_concordant_count_v12`, `tirads_mismatch_count_v12`)
- 4 inheriting views unchanged in shape (auto-pick up the rewritten base):
  - `cohort_m050_tumor_size_volume_v1`
  - `cohort_m053_nondiagnostic_fna_v1`
  - `cohort_m064_frozen_decision_v1`
  - `cohort_m076_ln_surveillance_v1`

> **Manuscript authors: cohort N shifts on m011/m025/m045/m075 reflect the
> stricter canonical denominator (only patients with US data). Re-pull N's
> after rebasing onto post-Part-B CPM and update methods/results text.**

### `"Thyroid 2026 UPdated".cpm_tirads_legacy_20260421` (archive schema)
- `canonical_patient_master_pre_partB` — full pre-drop CPM (10871 rows × 1585 cols)
- `view_def_<name>` × 9 — pre-rewrite definitions of all migrated/inheriting cohort views

### Workspace retention (2-week)
- `manuscript_workspace.cpm_tirads_audit_classification_v1` (Part A audit, 32 rows)
- `manuscript_workspace.cpm_tirads_canonical_coverage_v1` (Part B Phase 1 / pre-B Phase 5, 53 rows)

The 19 `manuscript_workspace.cpm_tirads_audit_sample_*_v1` tables from Part A
were dropped in Phase 5 per spec.

### `scripts/frozen/` (new directory)
- 33 `.py` files moved via `git mv` (history preserved); each carries a
  FROZEN header documenting the cleanup operation, canonical replacement,
  and archive restore path.
- `README.md` lists all 33 entries with the manuscript-N callout.
- Special headers:
  - `221_tirads_v2_integration.py`, `221b_suspicious_ln_reextraction.py` —
    `# NEW TARGET ON REFRESH: main.cupm_v2_canonical_backfill_v1` (per Logan)
  - `48_build_analysis_resolved_layer.py`, `50_multinodule_imaging.py` —
    `# CAT B: script uses TIRADS-shaped names as local CTAS aliases;
    schema migration requires internal alias rename, not just column substitution`

### `scripts/` (active reader migrations)
- 9 scripts surgically edited (Path Z scope):
  - 5 manuscript probe-list scripts (56, 57, 58, 59, 61, 62)
  - `209_nlp_entity_crossvalidation.py` — TIRADS NLP cross-validation
    block neutered (signature preserved; `[WARN]` print on the no-op path)
  - `213_data_dictionary.py` — legacy TIRADS regex retired
  - `280_synoptic_rollup_rebuild.py` — `KNOWN_UNDOCUMENTED_CPM_COLS` emptied
- `scripts/272_canonical_cleanup_phase1.py` — left as-is per Logan's
  "ALTER TABLE only, no TIRADS projection" exemption.

## QA — 10/10 checks pass

`qa/qa_script_cpm_tirads_partB.json`:

| check | result |
|---|---|
| 01 CPM row count unchanged (10871) | ✓ |
| 02 0 non-NLP TIRADS cols on CPM | ✓ |
| 03 0 pathology_vs_imaging_laterality_concordant* on CPM | ✓ |
| 04 9 cohort views resolve at Phase 2 baseline counts | ✓ |
| 05 git grep for legacy column regex returns empty | ✓ |
| 06 0 gap_ABORT in coverage table | ✓ |
| 07 archive integrity (10871 × 1585) | ✓ |
| 08 9 view_def_* tables in archive schema | ✓ |
| 09 33 frozen scripts; FROZEN headers + special headers present | ✓ |
| 10 canonical spot check (≥50% population on 33 mapped cols) | ✓ |

## Provenance + restore path

If a future manuscript needs to reconstruct any dropped CPM TIRADS column,
the canonical sources are:

1. **`main.cupm_v2_canonical_backfill_v1`** — 7 port cols (laterality + worst_rank + fna_recommended_report rollups). Frozen snapshot from CPM 2026-04-21.
2. **`main.canonical_us_patient_master_v2`** — patient-level rollups (TIRADS category, points, first/last preop, max nodule size, etc.).
3. **`main.canonical_us_nodule_v2`** — per-nodule grain for anything `cupm_v2` doesn't already aggregate.
4. **`"Thyroid 2026 UPdated".cpm_tirads_legacy_20260421.canonical_patient_master_pre_partB`** — full pre-drop CPM (10871 × 1585) for forensic restore.

To resurrect a frozen script:
1. Read its FROZEN header for the canonical replacement.
2. Confirm the upstream schema still supports the script's logic.
3. Rewrite to use `cupm_v2` / `cuem_v2` / `cunc_v2` per the header guidance.
4. `git mv` back to `scripts/`, removing the FROZEN header.
