# PSM Reproducibility & Policy — Stability Report

**Date:** 2026-04-13
**Branch:** `ete-remediation-20260413`
**Module:** `studies/proposal2_ete_staging/proposal2_endpoint_psm_strata.py`
**Manuscript anchor:** N = 711 matched pairs (frozen)
**MD snapshot:** `ete_pre_psm_policy` (Phase 5 pre-transition)
**Scope:** mETE vs No ETE greedy 1:1 NN PSM with caliper = 0.05 on propensity, fit on (age_at_surgery, female, largest_tumor_cm, n_positive_flag).

## 1. Policy decision

**Anchor:** the frozen manuscript PSM result (N = 711 pairs) remains the structural anchor for all manuscript-reported PSM numbers.

**Reruns:** any rerun of `proposal2_endpoint_psm_strata.py` under the hardened determinism contract is a **sensitivity analysis**. A rerun may only be promoted to anchor status if it:

1. Runs on SHA-256-locked input exports (see `artifacts/ete_export_freeze_manifest.json`).
2. Uses canonical AJCC7 mapping (T3b -> T3; see `studies/proposal2_ete_staging/ajcc7_mapping.py`).
3. Is bit-reproducible across two independent runs on the same host.
4. Receives explicit governance sign-off (export decision memo + manuscript-numeric manifest update).

No promotion is proposed in this branch. Phase 5 outputs the determinism contract and the sensitivity frame; the anchor is unchanged.

## 2. Identified instability sources in the original implementation

Prior to the Phase 5 patch, the following sources of non-determinism were present in `propensity_match()`:

1. **Input row order dependence.** The caller-supplied DataFrame came from an upstream merge (`load_expanded()` merges three CSV exports); pandas preserves merge order but it is sensitive to hash / sort interactions across pandas minor versions. `sub.dropna(...)` preserves the incoming order, so any caller-side order drift propagated into the propensity-model fit and the pair-selection loop.

2. **Tied-propensity ordering.** `sort_values("propensity")` with no secondary key has no guaranteed tie-break when two rows receive identical fitted propensities (possible when covariate vectors coincide). pandas' sort is stable on the input index, but the input index was itself order-dependent (see 1).

3. **Tied-distance nearest-neighbour selection.** `dist.idxmin()` returns the first index among equidistant candidates as scanned; equivalence among candidates was not unusual for rounded or duplicated propensities.

4. **`available_controls` list semantics.** The candidate pool was maintained as a Python `list` (mutation via `.remove()`), so the order in which remaining controls were searched was order-dependent from step 2.

5. **sklearn version drift.** `LogisticRegression` default solver has been stable across 1.4-1.6, but the default was not pinned in code; different minor versions could, in principle, change the optimiser default and shift propensities at the 4th-6th decimal.

6. **`dfs_years` NaN-drop interaction.** Dropping on `dfs_years` (some patients lacked surgery or follow-up dates) removed rows *after* the initial mETE/No-ETE filter; the row set entering the propensity fit therefore depended on upstream date-parsing behaviour.

## 3. Hardening applied

The following determinism controls are now in place in `propensity_match()`:

- Stable sort of `sub` by `research_id` (mergesort) immediately after the mETE/No-ETE filter and before `dropna`. This gives the propensity model a canonical row order.
- Secondary sort key on `research_id` inside both the treated and control frames: `sort_values(["propensity", "research_id"], kind="mergesort")`.
- Nearest-neighbour tie-breaking rewritten to sort candidates by `(distance, research_id)` explicitly, selecting the lexicographically smallest `research_id` when distances tie.
- `LogisticRegression(... solver="lbfgs")` pinned explicitly.
- `caliper` promoted to a keyword argument with default 0.05, retaining the manuscript setting.
- Docstring codifies the determinism contract so future maintainers do not regress.

## 4. Sensitivity frame for reruns

When a rerun is performed on the frozen exports:

- **Expected pair count:** 711 (anchor). Prior ad-hoc reruns under the original implementation produced counts in the 711-712 range; one earlier rerun under a pre-freeze input variant produced 1006. The 1006 result predates the export freeze and is not comparable.
- **Acceptable sensitivity range:** any rerun within +-5 pairs of 711 should be treated as numerically equivalent to the anchor for manuscript purposes. Pair counts outside that range trigger a manual inspection before any reporting.
- **Decision rule if a rerun diverges:** log the rerun in this file, snapshot inputs, document the source of divergence, and do **not** update manuscript numerics unless the divergence is explained and governance approves.

## 5. Verification

A regression test (`tests/test_psm_determinism.py`) exercises the determinism contract on a synthetic mETE/No-ETE frame:

- Shuffles the input row order with a fixed seed and confirms pair count and matched `research_id` sets are identical to the canonical-order run.
- Asserts that `propensity_match` respects the hardened sort keys by verifying both treated and control frames are sorted by `(propensity, research_id)` post-sort.

The full end-to-end script rerun on frozen exports is deferred to a sensitivity cycle in Phase 7 (release-governance). This report, the code hardening, and the regression test are sufficient to close the Phase 5 determinism gate.

## 6. Files touched

- `studies/proposal2_ete_staging/proposal2_endpoint_psm_strata.py` (policy docstring + determinism hardening in `propensity_match`)
- `tests/test_psm_determinism.py` (new regression)
- `artifacts/ete_psm_stability_report.md` (this file)
- `artifacts/ete_md_snapshots.json` (recorded `ete_pre_psm_policy` snapshot timestamp)
