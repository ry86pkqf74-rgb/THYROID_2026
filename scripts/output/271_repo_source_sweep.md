# Script 271 — Repo Source-Code Sweep (Step 7)

Run: 2026-04-17 / `scripts/271_tirads_imaging_finalization.py --step 7`

The DB-side sweep found **0** deprecated tables left in the canonical `main` schema (clean). This document captures the remaining repo-source references that name now-renamed / now-dropped objects, with a triage classification per the runbook rule:

> "For each hit: update to current canonical name, or annotate with a TODO if it's an analysis script that needs human decision."

---

## 1. `thyroid_canonical_publication_v1\b` (without trailing `_0`)

**Hits in active code:** 0
**Status:** CLEAN — every reference in repo uses the full `_v1_0` canonical name.

## 2. `thyroid_ete_fix_20260413` (old DB name)

**Hits anywhere:** 0
**Status:** CLEAN — old DB name fully retired.

## 3. `canonical_patient_master_v1\b` (wrong table name; current is `canonical_patient_master`)

**Active scripts referencing the literal `_v1` suffix:**

| File | Disposition |
|------|-------------|
| scripts/200_canonical_diagnosis_standardization.py | TODO — historical canonical-build script (era 200-219) |
| scripts/201_canonical_survival_followup.py | TODO — historical |
| scripts/202_canonical_molecular_tested.py | TODO — historical |
| scripts/203_canonical_recurrence.py | TODO — historical |
| scripts/204_canonical_master_assembly.py | TODO — historical, this is where v1 was first authored |
| scripts/205_canonical_consolidation.py | TODO — historical |
| scripts/206_fleet_nlp_validate_upload.py | TODO — historical |
| scripts/207_canonical_master_expansion.py | TODO — historical |
| scripts/208_ln_master_integration.py | TODO — historical |
| scripts/209_nlp_entity_crossvalidation.py | TODO — historical |
| scripts/210_database_audit_backup.py | TODO — historical |
| scripts/211_canonical_gap_fill.py | TODO — historical |
| scripts/212_nlp_entity_rollup.py | TODO — historical |
| scripts/213_data_dictionary.py | TODO — historical |
| scripts/214_final_canonical_integration.py | TODO — historical |
| scripts/215_deep_nlp_entity_integration.py | TODO — historical |
| scripts/216_data_gap_resolution.py | TODO — historical |
| scripts/216b_llm_extraction.py | TODO — historical |
| scripts/217_lab_recovery_ln_integration.py | TODO — historical |
| scripts/218_followup_recovery.py | TODO — historical |
| scripts/219_imaging_gap_resolution.py | TODO — historical |
| scripts/221a_death_date_integration.py | TODO — historical |
| scripts/221b_final_gap_resolution.py | TODO — historical |
| scripts/archive/221_final_gap_resolution.py | OK — already in archive/ |
| scripts/archive/221_final_database_consolidation.py | OK — already in archive/ |

**Recommendation:** Leave historical scripts as-is. They executed once during the canonical build (era 200-219) and are not intended to re-run. If any of these are ever re-run, the operator must port them to the current naming.

## 4. `imaging_nodule_long_v2` (table was dropped before 271)

| File | Disposition |
|------|-------------|
| scripts/233_canonical_finalization.py | TODO — historical canonical finalization era |
| scripts/238_populate_serial_imaging_us.sql | TODO — Script 238 reconstructs dominant flag on the fly. Now superseded by inm_v1.dominant_nodule_flag (Step 6). |
| scripts/237_document_fna_size_gap.sql | TODO — historical |
| scripts/49_enhanced_linkage_v3.py | TODO — historical |
| scripts/20_enriched_patient_timeline_v3.sql | TODO — historical |
| scripts/97_repo_truth_sync.py | TODO — historical |
| scripts/24_reconciliation_review_v2.py | TODO — historical |
| scripts/95_episode_linkage_repair.py | TODO — historical |
| scripts/130_md_materialize_multimodal_upstream.py | TODO — historical materialization path |
| scripts/223_ingest_and_publish.py | TODO — publication-era script |
| scripts/223_publish_canonical.py | TODO — publication-era script |
| scripts/22_canonical_episodes_v2_views.sql | TODO — historical |
| scripts/22_canonical_episodes_v2.py | TODO — historical |
| scripts/29_validation_engine.py | TODO — historical |
| scripts/56_pre_manuscript_audit.py | TODO — historical audit |
| scripts/70_canonical_backfill.py | TODO — historical |
| scripts/75_dataset_maturation.py | TODO — historical |
| scripts/78_final_hardening.py | TODO — historical |
| scripts/94_pipeline_gap_closure.py | TODO — historical |
| scripts/97_episode_linkage_audit.py | TODO — historical |
| scripts/98_multi_surgery_artifact_linkage_audit.py | TODO — historical |
| scripts/99_comprehensive_final_verification.py | TODO — historical |
| scripts/101_review_ops.py | TODO — historical |
| scripts/129_imaging_fna_linkage_mm_v1.py | TODO — historical |
| scripts/_fix_missing_v2_tables.py | TODO — recovery script; verify before re-running |
| llm_extraction/extraction_audit_engine_v5.py | TODO — older audit engine version |
| app/imaging_nodule_dashboard.py | **TODO (PRIORITY)** — Streamlit dashboard. Verify it does not query imaging_nodule_long_v2; if it does, switch to inm_v1 + dominant_nodule_flag. |
| app/patient_timeline_explorer.py | **TODO (PRIORITY)** — same. |
| app/qa_workbench.py | **TODO (PRIORITY)** — same. |

**Backup:** `archive_pub_v1_0` retains the dropped table (per pre-271 history).

## 5. `imaging_nodule_size_cm_v11` (dropped this script — Step 2)

| File | Disposition |
|------|-------------|
| scripts/207_canonical_master_expansion.py | TODO — original author of column; historical |
| llm_extraction/extraction_audit_engine_v9.py | TODO — older audit engine; historical |
| scripts/271_tirads_imaging_finalization.py | OK — this script (Step 2 drops it) |
| data_dictionary.csv / data_dictionary.md | Will be refreshed in Step 8 / next dictionary build |
| AGENTS.md | TODO — update operator notes if needed |
| studies/proposal_multimodal_prediction_20260318/schema_inventory.md | OK — historical study artifact (timestamped) |
| studies/canonical_cleanup_20260417/cpm_cols_pre.txt | OK — pre-state snapshot |

## 6. `tirads_worst_score_v12` (relabeled as legacy in Step 4 — NOT dropped)

These references continue to work; the column still exists, just with a more accurate COMMENT.

| File | Disposition |
|------|-------------|
| scripts/207_canonical_master_expansion.py | TODO — annotate that it returns category code, not points |
| scripts/265_canonical_finalization.py | TODO — same |
| llm_extraction/extraction_audit_engine_v10.py | TODO — same |

**Going forward** — analysis scripts should prefer `tirads_worst_points_v271` for ACR-points-based analysis.

## 7. `data_dictionary_v240` (now `data_dictionary_v266a`)

Many hits — most in `scripts/output/*.log` (historical logs; do not touch). Active scripts:

| File | Disposition |
|------|-------------|
| scripts/236_canonical_finalization.py | TODO — historical |
| scripts/240_ln_staging_cleanup.py | TODO — created the v240 dictionary; historical |
| scripts/247_canonical_v1_0_lock.py | TODO — verify next dictionary refresh path |
| scripts/250_registry_pointer_rebuild.py | TODO |
| scripts/251_drilldown_eviction_audit.py | TODO |
| scripts/252_recompute_max_tirads.py | TODO |
| scripts/254_rebuild_n_fna_episodes.py | TODO |
| scripts/255_rebuild_rai_tg_rollups.py | TODO |
| scripts/256_rebuild_confirmed_complication_flag.py | TODO |
| scripts/257_clean_house_sweep.py | TODO |
| scripts/259_final_verification_lock.py | TODO |
| scripts/260_hydrate_fna_links.py | TODO |
| scripts/262_drop_ras_v7_align_dtype.py | TODO |
| scripts/263_bethesda_semantic_decision.py | TODO |
| scripts/264_final_acceptance_addendum.py | TODO |
| scripts/265_canonical_finalization.py | TODO |
| scripts/266_preflight.py | TODO |
| scripts/266a_dictionary_and_feeder_registration.py | **TODO (PRIORITY)** — actively maintains the dictionary; align with v266a name |
| scripts/266a_discovery.py | TODO |
| scripts/266c_wide_format_slots_and_renames.py | TODO |
| scripts/270b_phase_a_step_2_registry.py | TODO |

**Recommendation:** Active dictionary-maintenance scripts (`266a_dictionary_and_feeder_registration.py`, `247_canonical_v1_0_lock.py`) should be patched at the next dictionary refresh; everything else is era-bound and already executed.

---

## Summary

- **DB-side sweep:** 0 deprecated tables remaining in `main` schema (clean).
- **Repo-side sweep:** Active code references to dropped/renamed canonical objects exist in **historical scripts** (era 200-265). The currently-active dashboards (`app/*.py`) and dictionary maintenance script (`266a_*`) are flagged PRIORITY for the next operator pass.
- **No mass-edit performed** — historical finalization scripts are intentionally left frozen as built. This document is the living TODO list.
