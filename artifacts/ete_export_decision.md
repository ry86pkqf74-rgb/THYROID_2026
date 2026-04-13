# ETE Export Source-of-Truth Decision

**Decision:** Branch A — **FROZEN**.
**Date:** 2026-04-13.
**Decision maker:** automated remediation session under Logan Glosser PAT, per user-provided default "Branch A (Recommended)".

## Chosen source of truth

The three frozen CSV exports at `exports/ptc_full.csv`, `exports/recurrence_full.csv`, `exports/imaging_correlation.csv`, all mtime 2026-03-10 20:30, SHA-256 locked in `ete_export_freeze_manifest.json`.

## Rationale

1. The manuscript revision packet (`manuscripts/ete_ajcc8_202603/MANUSCRIPT_REVISION_PACKET_20260326.md`) explicitly anchors every reported number to the 2026-03-10 exports and to 711 PSM matched pairs.
2. A refreshed live rerun is currently blocked by four items from the task framing: export-source decision, AJCC7 unification, PSM policy, and release-governance gate. Only the first three are being resolved in-session; the governance tail work is scheduled for Phase 7 but must be independently signed off before a live-reanalysis claim is permissible.
3. The MotherDuck RO share `thyroid_research_ro_v2` does contain candidate canonical tables (`manuscript_cohort_v1` @ 10,871 rows; `analysis_cancer_cohort_v1` @ 4,136; `analysis_recurrence_subset_v1` @ 1,946; `extracted_ete_subgraded_v1` @ 3,558; `imaging_patient_summary_v1` @ 6,126; `patient_refined_master_clinical_v12` @ 12,886), so Branch B is **feasible** but not currently authorized.
4. Schema spot-check of the three frozen exports shows they are export-shaped (`research_id` primary key, patient-level one row, AJCC8 columns, histology) and not directly shaped like any single canonical MD table — Branch B would require a deliberate export-rebuild script, not a simple table swap.

## What downstream work this authorizes

- Phase 4 AJCC7 unification operating on the frozen CSV inputs.
- Phase 5 PSM determinism work operating on the frozen CSV inputs. Reruns labeled "**frozen export rerun**"; never "live reanalysis".
- Phase 6 manuscript packaging sourcing only the 711-pair frozen structural result as anchor, with any PSM rerun result explicitly labeled sensitivity.

## What this does NOT authorize

- A claim of "fresh fully updated live-database reanalysis" in any manuscript, blog post, talk, or release note.
- Regeneration of the three CSV exports from MotherDuck without a separate, explicitly-approved Branch B session with its own promotion gate.
- Replacing the 711-pair structural PSM anchor in the manuscript without a Branch B rerun plus full release-governance green.

## Open follow-up (deferred to Phase 7 or a later session)

- Create dedicated `ete_fix_rw` service account.
- Stand up a read-only share gate test.
- Build a Branch-B rebuild script (candidate: `scripts/155_ete_export_rebuild_from_md.py`) but do **not** run it in this session.
