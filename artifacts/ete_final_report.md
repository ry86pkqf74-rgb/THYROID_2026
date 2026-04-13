# ETE remediation — final report

**Date:** 2026-04-13
**Branch:** `ete-remediation-20260413`
**Base:** `main` (sync SHA `4795d42d`)
**Commits (6):** `5f5aa4b`, `68964d2`, `2490e1d`, `34c6d17`, `4e0579d`, `e0690a3`
**Tests:** 12/12 passing

## Verdicts on the four questions

### Q1. Revise the ETE manuscript from a validated source of truth?

**VERDICT: YES — drafted.**

A master draft has been written at `manuscripts/ete_ajcc8_202603/manuscript_ete_master.md` using only frozen numerics that trace to the Phase-3 export freeze (`artifacts/ete_export_freeze_manifest.json`) and the Phase-6 numeric manifest (`artifacts/ete_manuscript_numeric_manifest.json`, 28 pinned claims). Every quantitative statement in the draft has a `source_ref` in the manifest; every frozen source artifact has a SHA-256 hash that is asserted in CI (`tests/test_ete_manifest_integrity.py::test_frozen_source_shas_match_manifest`).

### Q2. Rerun ETE analysis on refreshed data if gates pass?

**VERDICT: NO rerun executed this branch. Sensitivity frame in place.**

Per user selection (Branch A = frozen) the anchor remains the frozen manuscript bundle. The PSM determinism contract (`studies/proposal2_ete_staging/proposal2_endpoint_psm_strata.py` + `tests/test_psm_determinism.py`) makes any *future* rerun deterministic and sensitivity-labeled. A rerun may be promoted to anchor only after passing the gates enumerated in `artifacts/ete_psm_stability_report.md` §1.

### Q3. Is a "fresh fully updated live-database reanalysis" claim allowed?

**VERDICT: NO.**

Enumerated blockers from `artifacts/ete_release_governance_status.md`:

1. No export refresh has been executed on this branch.
2. PSM anchor remains the frozen 711-pair result.
3. RO-share gate BLOCKED (MotherDuck MCP does not expose service-account / share SQL).
4. `scripts/96_release_manifest.py` has not been regenerated for this branch.

The policy is codified in `artifacts/ete_manuscript_numeric_manifest.json` (`policy.live_reanalysis_claim_allowed: false`) and guarded in CI by `test_psm_anchor_policy_is_encoded`.

### Q4. Is AJCC7 T3b -> T3 correctly unified?

**VERDICT: YES.**

A canonical mapping module `studies/proposal2_ete_staging/ajcc7_mapping.py` centralises the rule. Both executable code paths (`proposal2_ete_analysis.py::derive_ajcc7_t_stage`, `proposal2_expanded_cohort.py::derive_ajcc7`) delegate to it. The diff artifact (`artifacts/ete_ajcc7_diff.md`) shows 276 tumor-1-level row shifts T4a -> T3 on `ptc_full.csv` (N=2844); audit-level count 346 is documented. A regression guard `test_no_stale_t3b_to_t4a_in_executable_paths` scans the executable tree for the stale pattern on every CI run.

## What was delivered (per-phase)

| Phase | Deliverable | Artifact |
|-------|-------------|----------|
| 0 | Session manifest | `artifacts/ete_fix_session_manifest.md` |
| 1 | Repo inventory + file map | `artifacts/ete_repo_inventory.md`, `ete_file_map.json` |
| 2 | MD object map + zero-copy clone + snapshots | `artifacts/ete_md_object_map.json`, `ete_md_snapshots.json` |
| 3 | Export-source decision (Branch A, frozen) + SHA-256 freeze | `artifacts/ete_export_decision.md`, `ete_export_freeze_manifest.json` |
| 4 | Canonical AJCC7 mapping + tests + diff | `ajcc7_mapping.py`, `tests/test_ajcc7_mapping.py`, `artifacts/ete_ajcc7_diff.md` |
| 5 | PSM determinism hardening + policy + tests | patched `proposal2_endpoint_psm_strata.py`, `tests/test_psm_determinism.py`, `artifacts/ete_psm_stability_report.md` |
| 6 | Manuscript master + numeric manifest | `manuscripts/ete_ajcc8_202603/manuscript_ete_master.md`, `artifacts/ete_manuscript_numeric_manifest.json` |
| 7 | Release-governance status + RO-share gate | `artifacts/ete_release_governance_status.md` |
| 8 | CI + manifest-integrity tests + test report | `.github/workflows/ete_remediation.yml`, `tests/test_ete_manifest_integrity.py`, `artifacts/ete_test_report.md` |
| 9 | Final report (this file) | `artifacts/ete_final_report.md` |

## MotherDuck snapshot ledger

All snapshots live in the scratch database `thyroid_ete_fix_20260413` (zero-copy clone of `Thyroid 2026 (DUCKLAKE)`, 30-day retention). Named snapshots: `ete_pre_export_decision`, `ete_pre_ajcc7_unification`, `ete_pre_psm_policy`, `ete_pre_manuscript_packaging`. Metadata in `artifacts/ete_md_snapshots.json`.

## Outstanding follow-ups (non-blocking)

1. **Service-account + RO share** for `thyroid_ete_fix_20260413` (org-admin UI; see `ete_release_governance_status.md` §RO-share gate).
2. **Missing `scripts/95_environment_promotion.py`** — open a follow-up issue to either implement or retire the reference.
3. **Reverse-merge to `main`** after PR review; the branch contains six commits and no force-push events.
4. **Future PSM rerun cycle** (if triggered) must land through a separate governance-approved branch, not this one.

## Acceptance summary

- Six atomic commits, linear history.
- 12/12 tests green.
- All four questions answered with explicit verdicts above.
- No "fresh live reanalysis" claim introduced.
- Frozen numerics preserved; AJCC7 corrected on the executable path; PSM determinism enforced; manuscript draft assembled from the same frozen bundle.
