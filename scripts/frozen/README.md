# `scripts/frozen/` — frozen scripts catalog

Scripts in this directory are intentionally inert. They reference
schema state that no longer exists on the live database (typically
columns that were dropped during a publication-stable cleanup).

Each script carries a FROZEN header at the top documenting:
  - the freeze date and the cleanup operation
  - the canonical replacement (where applicable)
  - the archive location for restoration if needed

**Do NOT run scripts in this directory against the live database.**
If a use case resurrects, unfreezing requires:
  1. Reviewing the FROZEN header for replacement guidance.
  2. Confirming the upstream schema still supports the script's logic.
  3. Updating the script to use canonical post-cleanup tables.
  4. `git mv` back to `scripts/`, removing the FROZEN header.

---

## CPM TIRADS Part B (2026-04-21) — 33 scripts

Architecture: Option C-soft. canonical_patient_master no longer carries TIRADS
columns. Canonical TIRADS lives on canonical_us_*_v2 surface (cupm_v2 patient,
cuem_v2 exam, cunc_v2 nodule grain).

- `207_canonical_master_expansion.py` — frozen 2026-04-21 — CPM TIRADS Part B — expanded canonical_patient_master_v1 with _v12 / _v271 / preop / max_tirads* TIRADS columns
- `265_canonical_finalization.py` — frozen 2026-04-21 — CPM TIRADS Part B — finalized CPM with legacy TIRADS columns (best/worst/_v12/combined)
- `271_tirads_imaging_finalization.py` — frozen 2026-04-21 — CPM TIRADS Part B — wrote tirads_*_points_v271 and laterality rollups to CPM
- `271a_fix_concordance_three_valued.py` — frozen 2026-04-21 — CPM TIRADS Part B — wrote pathology_vs_imaging_laterality_concordant 3-valued patch to CPM
- `271b_laterality_normalization.py` — frozen 2026-04-21 — CPM TIRADS Part B — wrote *_v271b laterality rollups to CPM
- `273_registry_curation.py` — frozen 2026-04-21 — CPM TIRADS Part B — registry curation that read/wrote CPM TIRADS columns
- `204_canonical_master_assembly.py` — frozen 2026-04-21 — CPM TIRADS Part B — wrote preop_tirads_best/worst/category to CPM during master assembly
- `205_canonical_consolidation.py` — frozen 2026-04-21 — CPM TIRADS Part B — wrote preop_tirads_* + tirads_*_combined to CPM during consolidation
- `221_tirads_v2_integration.py` — frozen 2026-04-21 — CPM TIRADS Part B — main producer of the tirads_v2_* family on CPM (13 cols)
- `221b_suspicious_ln_reextraction.py` — frozen 2026-04-21 — CPM TIRADS Part B — wrote tirads_v2_any_fna_recommended_report* + suspicious_ln_on_us to CPM
- `221c_rollup_threevalue_patch.py` — frozen 2026-04-21 — CPM TIRADS Part B — patched 3-valued tirads_v2_any_* rollups on CPM
- `252_recompute_max_tirads.py` — frozen 2026-04-21 — CPM TIRADS Part B — recomputed max_tirads_ever / imaging_tirads_worst / preop_tirads_best on CPM
- `301_canonical_us_patient_master_v1.py` — frozen 2026-04-21 — CPM TIRADS Part B — old US v1 patient-master writer; superseded by canonical_us_patient_master_v2 pipeline
- `328_tirads_v2_gap_a_cast_fix.py` — frozen 2026-04-21 — CPM TIRADS Part B — Gap-A patch on tirads_v2_* family on CPM
- `329_tirads_v2_gap_b_report_reroll.py` — frozen 2026-04-21 — CPM TIRADS Part B — Gap-B patch on tirads_v2_any_fna_recommended_report on CPM
- `368_cpm_us_cutover_to_v2.py` — frozen 2026-04-21 — CPM TIRADS Part B — the literal CPM US v2 cutover script that materialized the 6 *_v2 cols on CPM
- `prompt6_348_older_masters.py` — frozen 2026-04-21 — CPM TIRADS Part B — writer of tirads_v2_worst_rank + any_fna_recommended_report from older rollup tables (caught by Part B Phase 3 grep, missed by Logan's initial 6-script list)
- `375_cpm_column_cleanup_and_audit.py` — frozen 2026-04-21 — CPM TIRADS Part B — ALTER TABLE RENAME COLUMN of imaging_updated_tirads_category_cpm_v2_v2 -> imaging_updated_tirads_category_cpm_v2 (already executed; CPM-mutating, frozen for safety)
- `48_build_analysis_resolved_layer.py` — frozen 2026-04-21 — CPM TIRADS Part B — built patient_analysis_resolved_v1 with TIRADS-shaped column aliases sourced from extracted_tirads_validated_v1
- `50_multinodule_imaging.py` — frozen 2026-04-21 — CPM TIRADS Part B — built multi_exam CTE with max_tirads_ever / worst_tirads_category as LOCAL aliases on a derived rollup
- `259_final_verification_lock.py` — frozen 2026-04-21 — CPM TIRADS Part B — one-shot final verification audit (max_tirads_ever undercount check); already executed
- `264_final_acceptance_addendum.py` — frozen 2026-04-21 — CPM TIRADS Part B — one-shot final acceptance audit; already executed
- `277_canonical_cleanup_phase7_verification.py` — frozen 2026-04-21 — CPM TIRADS Part B — one-shot Phase 7 cleanup verification; already executed
- `336_final_main_audit.py` — frozen 2026-04-21 — CPM TIRADS Part B — one-shot final main audit (per-column NULL counts on CPM); already executed
- `phase1_dryrun_probe.py` — frozen 2026-04-21 — CPM TIRADS Part B — one-shot Phase 1 dry-run sizing probe; already executed
- `preflight.py` — frozen 2026-04-21 — CPM TIRADS Part B — one-shot canonical-cleanup preflight; already executed
- `schema_recon.py` — frozen 2026-04-21 — CPM TIRADS Part B — one-shot canonical-cleanup schema reconnaissance; already executed
- `run_feasibility.py` — frozen 2026-04-21 — CPM TIRADS Part B — one-shot multimodal-prediction feasibility study; already executed
- `228_registry_backfill.py` — frozen 2026-04-21 — CPM TIRADS Part B — registry backfill metadata script with TIRADS column refs in source-table comments
- `246_canonical_us_nodule_characteristics.py` — frozen 2026-04-21 — CPM TIRADS Part B — canonical US nodule characteristics builder with TIRADS column refs in docstring
- `369_us_v2_views_and_registry.py` — frozen 2026-04-21 — CPM TIRADS Part B — US v2 views/registry builder with TIRADS column refs in view definitions
- `prompt6_353_completion_audit.py` — frozen 2026-04-21 — CPM TIRADS Part B — Prompt 6 completion audit with TIRADS column refs in audit metadata
- `prompt6_353_repoint_orphan_view.py` — frozen 2026-04-21 — CPM TIRADS Part B — Prompt 6 orphan-view repoint with TIRADS column refs in header
