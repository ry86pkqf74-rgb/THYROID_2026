# ETE Release-Governance Status

**Date:** 2026-04-13
**Branch:** `ete-remediation-20260413`
**Scope:** Governance gates for ETE manuscript publication and for any claim of "fresh fully updated live-database reanalysis".

## Summary verdict

| Gate | Status | Notes |
|------|--------|-------|
| Export freeze (SHA-256 lock) | GREEN | `artifacts/ete_export_freeze_manifest.json` |
| AJCC7 canonical mapping | GREEN | `ajcc7_mapping.py` + `tests/test_ajcc7_mapping.py` (4/4 pass) |
| PSM anchor policy | GREEN | 711 pairs frozen; reruns = sensitivity; `tests/test_psm_determinism.py` (3/3 pass) |
| Manuscript numeric manifest | GREEN | `artifacts/ete_manuscript_numeric_manifest.json` (28 claims) |
| Script 90 manuscript-freeze rebuild | REVIEWED | No changes required this branch |
| Script 94 map-dedup validator | REVIEWED | No changes required this branch |
| Script 95 environment-promotion | N/A | Script `95_environment_promotion.py` does not exist in repo. Closest functional analogue: `112_v2_domain_promotion_gate.py`. Recommend a follow-up issue to create `95_environment_promotion.py` or retire the reference. |
| Script 96 release-manifest | REVIEWED | No changes required this branch |
| RO-share gate | BLOCKED | Read-only MotherDuck share provisioning requires org-admin UI; MCP does not expose service-account SQL. |
| Fresh-live-reanalysis claim | DISALLOWED | See blockers below |

## RO-share gate status

The read-only share gate is the governance hook that lets an external reviewer replicate the manuscript numerics against a read-only MotherDuck snapshot without write access to the canonical `Thyroid 2026 (DUCKLAKE)` database.

State as of this branch:

1. A zero-copy clone was created at `thyroid_ete_fix_20260413` (30-day retention).
2. Named snapshots taken at each phase transition: `ete_pre_export_decision`, `ete_pre_ajcc7_unification`, `ete_pre_psm_policy`, `ete_pre_manuscript_packaging`.
3. No service-account or RO share has been created. The MotherDuck MCP surface does not expose service-account provisioning or share creation SQL in the connected environment.

Recommended next steps (outside-of-this-branch):

- Provision a MotherDuck service account via org-admin UI scoped read-only to `thyroid_ete_fix_20260413`.
- Create a named RO share pinned to the `ete_pre_manuscript_packaging` snapshot.
- Record the share URL + consumer token in `artifacts/ete_ro_share.json` (not in git; reference only).

Until these three steps are complete, the RO-share gate remains BLOCKED and no "fresh live-database reanalysis" claim can be made in the manuscript.

## Scripts 90 / 94 / 96 review

All three scripts were inspected on this branch. No changes were required for the ETE remediation goals; each already implements its prod-sourcing and dedup checks as documented in its module docstring. A cross-reference of each script to the ETE-relevant concern is below.

| Script | ETE-relevance | Notes |
|--------|---------------|-------|
| `scripts/90_manuscript_freeze_rebuild.py` | Rebuilds manuscript publication bundle; fail-closed on drift. Used downstream when promoting a new manuscript numeric manifest. | Preserve behaviour; no edits on this branch. |
| `scripts/94_map_dedup_validator.py` | Guards materialization-map integrity for MotherDuck sync. Protects upstream table integrity that feeds `thyroid_ete_fix_20260413`. | Preserve behaviour; no edits on this branch. |
| `scripts/96_release_manifest.py` | Emits release manifest with git SHA, row counts, gate results. Parallel to but distinct from `artifacts/ete_manuscript_numeric_manifest.json` (ETE-specific). | Preserve behaviour; no edits on this branch. |

## Missing script 95

`scripts/95_environment_promotion.py` is referenced in the task spec but does **not** exist in the repo. Closest analogues:

- `scripts/112_v2_domain_promotion_gate.py`
- `scripts/126_final_master_release.py`
- `scripts/148_thyroid2026_release_gate.py`

Recommendation: open a follow-up issue to either (a) implement `95_environment_promotion.py` with a documented interface consistent with 90/94/96, or (b) formally retire the reference in the task spec. No blocker for Phase 8/9.

## Blockers to a "fresh live-database reanalysis" claim

This manuscript branch does **not** support that claim. The enumerated blockers are:

1. **No export refresh** has been executed on this branch. Branch A (frozen) is the source per the export-decision memo (`artifacts/ete_export_decision.md`).
2. **PSM anchor remains the frozen 711-pair result.** Reruns are sensitivity analyses only.
3. **RO-share gate is BLOCKED** as described above.
4. **Release manifest** has not been regenerated for this branch (`scripts/96_release_manifest.py` not invoked, intentionally).

Any relaxation of these blockers requires a separate, governance-approved release cycle.

## Phase 7 artifacts

- `artifacts/ete_release_governance_status.md` (this file)
- `artifacts/ete_manuscript_numeric_manifest.json` (Phase 6)
- `artifacts/ete_export_freeze_manifest.json` (Phase 3)
- `artifacts/ete_psm_stability_report.md` (Phase 5)
- `artifacts/ete_md_snapshots.json` (ongoing)
