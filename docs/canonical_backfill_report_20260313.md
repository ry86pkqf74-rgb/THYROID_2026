# Canonical Episode Backfill Report — 2026-03-13

## Objective

Close remaining canonical propagation failures identified in the final
verification report (`docs/final_repo_verification_20260313.md`). Values
already solved in sidecar/refined/linkage tables were not yet backfilled into
the canonical episode layer.

## Scope

| # | Target | Source |
|---|--------|--------|
| 1 | `rai_treatment_episode_v2.dose_mci` | `extracted_rai_dose_refined_v1` |
| 2 | `molecular_test_episode_v2.ras_flag` | `extracted_ras_patient_summary_v1` |
| 3 | `molecular_test_episode_v2.linked_fna_episode_id` | `fna_molecular_linkage_v3` (score_rank=1) |
| 4 | `fna_episode_master_v2.linked_molecular_episode_id` | `fna_molecular_linkage_v3` (score_rank=1) |
| 5 | `rai_treatment_episode_v2.linked_surgery_episode_id` | `pathology_rai_linkage_v3` (score_rank=1) |
| 6 | `imaging_nodule_long_v2.linked_fna_episode_id` | `imaging_fna_linkage_v3` (score_rank=1) |

Additionally: added `episode_analysis_resolved_v1_dedup` and
`manuscript_cohort_v1` to `scripts/26_motherduck_materialize_v2.py`
MATERIALIZATION_MAP.

## Results — Before / After

| Column | Before | After | Delta | Fill Rate |
|--------|--------|-------|-------|-----------|
| `rai_treatment_episode_v2.dose_mci` | 55 / 1,857 | 371 / 1,857 | **+316** | 3.0% → 20.0% |
| `molecular_test_episode_v2.ras_flag=TRUE` | 0 / 10,126 | 325 / 10,126 | **+325** | 0% → 3.2% |
| `molecular_test_episode_v2.linked_fna_episode_id` | 0 / 10,126 | 639 / 10,126 | **+639** | 0% → 6.3% |
| `fna_episode_master_v2.linked_molecular_episode_id` | 0 / 59,620 | 689 / 59,620 | **+689** | 0% → 1.2% |
| `rai_treatment_episode_v2.linked_surgery_episode_id` | 0 / 1,857 | 19 / 1,857 | **+19** | 0% → 1.0% |
| `imaging_nodule_long_v2.linked_fna_episode_id` | 0 / 10,866 | 0 / 10,866 | +0 | 0% (source table empty) |

**Total cells backfilled: 1,988**

## Ambiguity Review

45 linkage candidates had `n_candidates > 1` in V3 linkage tables:
- `fna_molecular`: 38 rows (multiple molecular tests per FNA)
- `pathology_rai`: 7 rows (multiple RAI episodes per surgery)

These are routed to `canonical_backfill_ambiguity_review` for manual review.
Only the top-ranked candidate (score_rank=1) was propagated.

## Notes

- **Imaging -> FNA linkage**: `imaging_fna_linkage_v3` had 0 rows at time
  of this report. ~~_[UPDATED 2026-03-15]_~~ **imaging_fna_linkage_v3 was
  subsequently rebuilt to 9,024 rows** (2,072 patients; 652 high-confidence;
  3,048 analysis-eligible) once `imaging_nodule_master_v1` was populated from
  the TIRADS Excel. Canonical value: **9,024 rows**. See
  `docs/imaging_nodule_master_repair_20260313.md` for rebuild details.

- **RAI dose join**: Used deterministic `(research_id, resolved_rai_date)`
  match with ROW_NUMBER dedup on `source_reliability DESC` for the single
  duplicate (research_id=7332).

- **RAS flag**: 292 patients are RAS-positive in `extracted_ras_patient_summary_v1`.
  325 molecular episode rows were updated (some patients have >1 molecular test row).

- **MATERIALIZATION_MAP**: `episode_analysis_resolved_v1_dedup` (9,368 rows)
  and `manuscript_cohort_v1` (10,871 rows) now included in the standard
  materialization refresh path.

## Validation

- `scripts/67_database_hardening_validation.py --md`: **CONDITIONALLY_READY**
  (unchanged from pre-backfill; coverage gaps are pre-existing)
- `py_compile`: PASS
- `mypy --ignore-missing-imports`: PASS (0 errors)
- `pyflakes`: PASS (0 warnings)

## Artifacts

- Script: `scripts/70_canonical_backfill.py`
- Export: `exports/canonical_backfill_20260313_0911/`
  - `manifest.json` — full audit with before/after per step
  - `ambiguity_review.csv` — 45 multi-candidate linkage rows
  - `sample_*.csv` — sample backfilled values per column
