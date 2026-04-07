# THYROID_2026 — Multimodal contract strict release gate

This document defines **fail-closed** conditions for promoting or signing off builds that include **`128_multimodal_contract_mm_v1.py`** and **`129_imaging_fna_linkage_mm_v1.py`**. For day-to-day commands and schema boundaries, see [`docs/multimodal_contract_runbook.md`](multimodal_contract_runbook.md).

## When strict mode applies

- **Script 128:** `--strict-release`
- **Script 129:** `--strict-release` (release runs only; omit for dev exploration on incomplete catalogs)

CI / workflow examples:

| Workflow | Behavior |
|----------|----------|
| [`.github/workflows/ci.yml`](../.github/workflows/ci.yml) `multimodal-md-contract-gate` (manual `workflow_dispatch`) | Runs **129** then **128** with `--strict-release` on MotherDuck; uploads JSON artifacts. |
| [`.github/workflows/motherduck_episode_pipeline.yml`](../.github/workflows/motherduck_episode_pipeline.yml) | Optional `multimodal_strict_release: true` adds `--strict-release` to **129** and **128**; uploads `imaging_fna_linkage_gate.json` + `multimodal_release_gate.json`. |

## Exact fail conditions (128 — `assert_strict_release_passes`)

The process **exits with an error** (non-zero) if any of the following hold:

1. **Bootstrap used** — Any entry in `bootstrapped_upstream` (upstream logical name mapped to `{schema}._bootstrap_*` stubs). That happens only when **`--allow-bootstrap-dev`** was used with missing native tables. **`--strict-release` cannot be combined with `--allow-bootstrap-dev`.**
2. **Required upstream columns** — `validate_upstream_schema_for_strict` cannot `DESCRIBE` a resolved upstream relation or is missing any column listed in `UPSTREAM_REQUIRED_COLUMNS` in [`scripts/mm_contract_upstream.py`](../scripts/mm_contract_upstream.py).
3. **Non-empty blocking validation tables** — Any row count &gt; 0 in:

   - `val_contract_required_join_keys_mm_v1`
   - `val_nodes_invariant_mm_v1`
   - `val_multitumor_expansion_mm_v1`
   - `val_side_lobe_mismatch_mm_v1`
   - `val_preop_temporal_order_mm_v1`
   - `val_ambiguous_multimodal_linkage_mm_v1`
   - `val_imaging_fna_contract_blockers_mm_v1`

   (`val_imaging_fna_contract_blockers_mm_v1` mirrors **`review_queue_imaging_fna_mm_v1`** — any imaging–FNA review row is a strict blocker.)

## Exact fail conditions (129 — strict release)

With **129 `--strict-release`**:

1. **Missing core tables** — Raises if `imaging_nodule_master_v1`, `fna_episode_master_v2`, or **`tumor_episode_master_v2`** is not available. *Rationale:* without `tumor_episode_master_v2`, 129 historically omitted first-surgery preop filtering (`preop_filter = TRUE`), which is unsuitable for release.
2. **`status != ok` on MotherDuck** — If MotherDuck returns `blocked_missing_fna_episode_master_v2` (or any non-ok status) instead of raising, strict mode **`RuntimeError`** s after `run()` so CI fails.

Without `--strict-release`, local file mode still **raises** if `fna_episode_master_v2` is missing; MotherDuck may return a blocked payload for observability.

## Default fail-closed upstream resolution (no bootstrap)

If **`--allow-bootstrap-dev` is not passed**, `ensure_upstream_sources` **raises** when any of these native tables are missing:

`linkage_master_v1`, `mrn_crosswalk_v1`, `operative_episode_detail_v2`, `tumor_episode_master_v2`, `fna_episode_master_v2`, `molecular_test_episode_v2`, `imaging_nodule_master_v1`, `event_date_audit_v2`, `patient_cross_domain_timeline_v2`, `preop_surgery_linkage_v3`, `surgery_pathology_linkage_v3`, `fna_molecular_linkage_v3`, `pathology_rai_linkage_v3`.

Core tables `operative_episode_detail_v2`, `tumor_episode_master_v2`, `molecular_test_episode_v2`, `imaging_nodule_master_v1` are required **before** that list is evaluated.

## Artifact contract (`multimodal_release_gate_v1`)

Emitted by 128 with `--emit-ci-artifact`. Important top-level keys:

- `strict_release.requested` / `strict_release.pass` — When `requested` is true, `pass` is true only if **blocker_total == 0** and **bootstrapped_upstream** is empty.
- `release_validation_metrics` — Counts and breakdowns (ambiguous linkage, laterality, node invariants, temporal violations, review queue by reason, imaging–FNA audit snapshot).
- `review_queue_deltas` — Present when `--prior-gate-artifact` was supplied; compares current `review_queue_by_reason` to the prior artifact.

## Artifact contract (`imaging_fna_linkage_mm_v1_gate_v1`)

Emitted by 129 with `--emit-ci-artifact`. Includes `status`, `before`/`after` counts, `audit`, `review_queue_by_reason`, `strict_release`.

## Operational note

Live MotherDuck may carry thousands of imaging–FNA **review_queue** rows; strict release will **fail** until queues are cleared or linkage rules/adjudication change. That is intentional: **`val_imaging_fna_contract_blockers_mm_v1` must be empty** for strict PASS. Treat high counts as **data/process debt**, not as “warnings-only.”
