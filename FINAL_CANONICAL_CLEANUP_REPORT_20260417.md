# FINAL CANONICAL CLEANUP REPORT — 2026-04-17

_Run id: `canonical_cleanup_20260417` | ended: 2026-04-17T10:44:11.809077+00:00_

Database: **`thyroid_canonical_publication_v1_0`**
Archive destination: **`"Thyroid 2026 UPdated".archive_pub_v1_0`** (no moves needed this run)

## Top-line

- CPM invariants: rows 10,871 / distinct 10,871 (expected 10,871 / 10,871) PASS
- main schema objects: **115** (all LIVE; 0 ARCHIVE__/DEPRECATED__/md_/empty)
- manuscript_workspace VIEW count: **67** (was 65 pre-cleanup; +2 added in Phase 4: `imaging_nodule_master_clean_v1`, `path_tumor_size_invariant_v1`) PASS
- archive_pub_v1_0 table count (read-only sanity): 182
- Replay assertions: ALL PASS

## Phase-by-phase outcomes

See `studies/canonical_cleanup_20260417/phase{1,2_3,4}_decision_log.json` for per-step rowcounts and decisions.

### Findings cleared

**CRITICAL** (4):
- PART2 §1.1 max_tirads_ever — verified canonical (no rebuild needed; storage type drift noted)
- PART2 §2.1 (Batch 2) orphan research_ids — VC s236 promotion + CPM backfill (Phase 1.1+1.2)
- PART2 §3.1 rai_max_dose_mci — rebuilt 27 episode-driven + 27 v9-fallback (Phase 1.5)
- PROMPT_18 2.1 VC cross-ref — same fix as PART2 §2.1

**HIGH** (8):
- PART2 §2.1 n_fna_episodes — verified canonical (already fixed by prior scripts)
- PART2 §2.2 worst_bethesda_num — provenance verified (672 already populated)
- PART2 §3.3 Tg counts — verified canonical (0 mismatches across 2,721 patients)
- PART2 §3.4 Tg peak/nadir — verified canonical (0 mismatches)
- PART2 §5.3 any_confirmed_complication_flag — rebuilt as BOOL_OR(phenotype) (Phase 1.8)
- PROMPT_18 3.1 lateral ND — 119 -> 336 (+217); structured_or_nlp synonym added (Phase 1.3)
- PROMPT_18 6 hypopara permanence — 14 reset to FALSE; 4 contradictions queued (Phase 2)

**MED** (8):
- PART2 §1.2 / §2.3 orphan placeholder rows — 3 us-nodule placeholders confirmed (HOLD)
- PART2 §1.4 n_us_exams provenance — COMMENT applied (Phase 4.5)
- PART2 §1.5 imaging exam_date completeness — exam_date_quality + clean_v1 view (Phase 4.2)
- PART2 §2.8 FNA date drift — COMMENT applied
- PART2 §3.5 / §3.6 RAI date fallback — COMMENT applied (`rai_first_date` only; `rai_last_date` absent on CPM)
- PART2 §4.2 multifocal ghost TRUEs — 344 downgraded; 559 NLP-corroborated preserved (Phase 4.1)
- PART2 §4.3 path_tumor_size_cm semantics — COMMENT + invariant view (80 violators surfaced)
- PART2 §5.4 LN counts — NOT explicitly addressed in this run (no candidate query supplied in prompt); flagged as deferred MED follow-up

## New CPM columns added

- `cpm_built_at`
- `comp_hypopara_permanent_source`
- `lateral_neck_dissected_structured_or_nlp`
- (Phase 1.8 was a no-op for `comp_hematoma_confirmed`, `comp_seroma_confirmed`, `comp_chyle_leak_confirmed`, `comp_wound_infection_confirmed` — already present.)

## HELD FOR ADJUDICATION (require Logan's decision)

1. **Hypopara contradictions (4 patients)**: `9765, 7487, 6447, 10743` queued in `manuscript_workspace.cpm_hypopara_adjudication_queue_v1`. Prompt cited 2 (9765, 7487); 6447 and 10743 newly identified by same rule (CPM permanent=TRUE vs phenotype `confirmed_transient`).
2. **Tg-lab orphans (403 patients)** in `manuscript_workspace.lab_orphan_audit_v1` — all 403 classified `likely_non_cancer` (zero evidence in fna/tem/stl/path/inm). Prompt cited 537; live found 403 (Tg lab table count drifted 76,971 -> 74,258).
3. **us_nodules_tirads placeholders (3 rids)**: `2332, 2445, 7744` — confirmed not in CPM, 0 us_nodule rows. DELETE recommendation pending Logan approval.
4. **path_tumor_size_cm invariant violators (80 patients)**: `manuscript_workspace.path_tumor_size_invariant_v1` lists rids where `path_tumor_size_cm > tumor_size_cm_max` (semantic violation; prompt expected 0).
5. **Phase 4.6 ajcc8_t_stage rename HELD**: 9 manuscript_workspace cohort views reference bare `ajcc8_t_stage`. Migration proposal in `studies/canonical_cleanup_20260417/ajcc8_t_stage_view_migration_proposal.md`. Run `scripts/274b_canonical_cleanup_phase4_6_rename.py` only AFTER applying the proposed CREATE OR REPLACE VIEW statements.

## Objects renamed / moved / dropped

- **None.** Phase 5 inventory found 0 ARCHIVE__/DEPRECATED__/md_/empty objects in main; all 115 objects are LIVE-classified. The canonical surface is already clean (historical archives reside in `archive_pub_v1_0`, 182 tables).

## Objects left KEEP_REVIEW and why

- None (the inventory script's enriched LIVE detection — view substring + Python source substring — covered all 115 objects).

## Build provenance

- `main.canonical_patient_master.cpm_built_at` populated for all 10,871 rows.
- `manuscript_workspace.cpm_reconciliation_provenance_v1` row inserted with run_id `canonical_cleanup_20260417`, phases applied, findings cleared, holds.

## Files of record

- `studies/canonical_cleanup_20260417/preflight.json`
- `studies/canonical_cleanup_20260417/drift_report.md`
- `studies/canonical_cleanup_20260417/cpm_cols_pre.txt`
- `studies/canonical_cleanup_20260417/phase1_decision_log.json`
- `studies/canonical_cleanup_20260417/phase1_run.log`
- `studies/canonical_cleanup_20260417/phase1_6_tg_drift_audit.md`
- `studies/canonical_cleanup_20260417/phase2_3_decision_log.json`
- `studies/canonical_cleanup_20260417/phase4_decision_log.json`
- `studies/canonical_cleanup_20260417/phase4_1_multifocal_preflight.json`
- `studies/canonical_cleanup_20260417/ajcc8_t_stage_migration_needed.csv`
- `studies/canonical_cleanup_20260417/ajcc8_t_stage_view_migration_proposal.md`
- `studies/canonical_cleanup_20260417/phase5_inventory.json`
- `studies/canonical_cleanup_20260417/phase6_decision_log.json`
- `studies/canonical_cleanup_20260417/verification.md`

