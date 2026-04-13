# ETE remediation — test report

**Date:** 2026-04-13
**Branch:** `ete-remediation-20260413`
**Runner:** local (macOS, Python 3.14.2, pytest 9.0.3)
**CI workflow:** `.github/workflows/ete_remediation.yml`

## Result

All 12 ETE-remediation tests pass.

```
tests/test_ete_manifest_integrity.py ....  5 passed
tests/test_ajcc7_mapping.py          .... 4 passed
tests/test_psm_determinism.py        ...  3 passed

12 passed in 3.94s
```

## Coverage by gate

| Gate | Test | Purpose |
|------|------|---------|
| AJCC7 canonical mapping | `test_t3b_maps_to_t3_not_t4a` | Unit mapping correctness |
| AJCC7 | `test_t4a_t4b_passthrough_and_microscopic_ete` | Pass-through + microscopic ETE rule |
| AJCC7 | `test_young_patient_overall_stage_is_i_or_ii_only` | Age rule for overall stage |
| AJCC7 | `test_no_stale_t3b_to_t4a_in_executable_paths` | Repo-wide guard against regressions |
| PSM determinism | `test_propensity_match_is_row_order_invariant` | Row-shuffle invariance |
| PSM determinism | `test_propensity_match_respects_stable_sort_keys` | Hardened sort behaviour |
| PSM determinism | `test_propensity_match_caliper_is_keyword` | Caliper monotonicity + kwarg shape |
| Manuscript manifest | `test_manifest_schema_is_recognized` | Schema version pinned |
| Manuscript manifest | `test_manifest_has_claims` | Minimum claim count |
| Manuscript manifest | `test_frozen_source_shas_match_manifest` | Frozen artifacts still SHA-match the manifest |
| Manuscript manifest | `test_psm_anchor_policy_is_encoded` | Policy block has required fields |
| Manuscript manifest | `test_anchor_claims_are_marked` | Anchor claims carry `policy: anchor` |

## CI configuration

`.github/workflows/ete_remediation.yml` runs on:

- push to `ete-remediation-**` or `main`
- pull_request touching any ETE-relevant path
- `workflow_dispatch`

The workflow installs the minimal deps required by the three test modules (pandas, numpy, scikit-learn, scipy, pytest, tabulate) and runs them in sequence. It deliberately does **not** pull the heavy plotting/statistics stack (seaborn, lifelines, statsmodels, yaml) — the PSM test module stubs those out of `proposal2_endpoint_psm_strata.py`'s top-level imports to keep the CI job fast and minimal.

## What is NOT tested (by design)

- End-to-end rerun of `proposal2_endpoint_psm_strata.py::main` on frozen exports. This is deferred to a governance-approved sensitivity cycle; the PSM anchor is the frozen 711-pair result.
- Heavy plotting / Kaplan-Meier figure generation.
- MotherDuck integration. Snapshot provenance is tracked in `artifacts/ete_md_snapshots.json` but not exercised by CI.
- Service-account / RO-share gate. Blocked by MCP surface; tracked in `artifacts/ete_release_governance_status.md`.
