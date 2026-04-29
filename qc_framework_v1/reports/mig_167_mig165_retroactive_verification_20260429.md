# mig_167 — Retroactive Path-C verification of mig_165

**Lane:** 55 / mig_167<br>
**Batch audited:** `mig_165_auxiliary_registry_hygiene_20260429`<br>
**SQL authored:** `qc_framework_v1/migrations/167_mig165_retroactive_verification_20260429.sql`<br>
**Posture:** read-only MotherDuck audit; authored notes-only registry SQL for Logan/Cowork apply.

## Cardinality reconcile

| Probe | Observed | Assessment |
|---|---:|---|
| Column-registry rows with `batch_id LIKE 'mig_165%'` | 1,306 cols | PASS |
| Distinct auto-`na` tables in column registry | 77 tables | PASS: 76 mass auto-na + 1 new registration |
| CF-only `not_started` table-registry rows | 10 tables | PASS: all are analytic / Tier-2 deferrals |
| Schema/status post-state | main: 97 verified, 6 not_started, 1 in_progress; manuscript_workspace: 68 verified, 4 not_started | Expected split |

Method histogram for `mig_165`: `auto_governance_audit_table_skip` = 57 tables / 707 cols; `auto_tier1_raw_mirror_skip` = 12 / 530; `auto_registry_governance_skip` = 8 / 69.

## Ten CF-only `not_started` tables

| schema.table | rows | classification | evidence |
|---|---:|---|---|
| main.imaging_fna_linkage_v3 | 9,911 | VALID-DEFERRED | linkage/candidate fields (`nodule_id`, `fna_episode_id`, `day_gap`, candidate counts) |
| main.imaging_patient_summary_v1 | 6,126 | VALID-DEFERRED | patient imaging rollup (`max_tirads_ever`, dominant size, bilateral/multifocal flags) |
| main.manuscript_cohort_v1 | 10,871 | VALID-DEFERRED | 151-column manuscript analytic composite |
| main.patient_cross_domain_timeline_v2 | 61,055 | VALID-DEFERRED | cross-domain event timeline (`event_type`, `domain`, `episode_id`) |
| main.recurrence_event_clean_v1 | 1,946 | VALID-DEFERRED | recurrence event table with source priority and structural/biochemical flags |
| main.tumor_stage_heterogeneity_v1 | 8,422 | VALID-DEFERRED | AJCC heterogeneity / dominant tumor rollup |
| manuscript_workspace.episode_analysis_resolved_v1_dedup | 9,368 | VALID-DEFERRED | episode-level resolved analytic table with linkage/confidence fields |
| manuscript_workspace.lesion_analysis_resolved_v1 | 11,851 | VALID-DEFERRED | lesion-level resolved analytic table |
| manuscript_workspace.ln_master_rollup_v1 | 4,273 | VALID-DEFERRED | lymph-node rollup with counts/ratios/levels |
| manuscript_workspace.patient_analysis_resolved_v1 | 10,871 | VALID-DEFERRED | patient-level resolved analytic composite |

No CF-only table should have been auto-`na`'d under the mig_165 rules.

## Sample-of-five sanity check by auto-na bucket

Deterministic sample used `ROW_NUMBER() OVER (PARTITION BY verification_method ORDER BY md5(schema_name || '.' || table_name))`.

| method | sampled tables | assessment |
|---|---|---|
| `auto_governance_audit_table_skip` | `manuscript_workspace.qc_manual_review_queue_v1`, `lab_orphan_cohort_review_v1`, `main.specimen_master_v1`, `recurrence_imaging_suspicious_candidates_v1`, `genetics_per_test_discordance_v1` | Mostly queues/audit/satellite evidence. No immediate status correction in notes-only lane. |
| `auto_registry_governance_skip` | `registry_v2_unresolved_pointers_v1`, `main.__readme`, `object_domain_map_v1`, `registry_v2_resolution_audit_v1`, `detail_table_registry_v1` | Governance/registry metadata bucket is plausible. |
| `auto_tier1_raw_mirror_skip` | `thyroid_weights`, `clinical_notes_long`, `thyroid_sizes`, `ct_imaging`, `imaging_exam_master_v1` | Four are plausible raw/source mirrors. `imaging_exam_master_v1` is **not** a plain raw mirror; it contains analytic per-exam rollup columns (`n_nodules`, `max_tirads`, `has_suspicious_nodule`, `largest_nodule_cm`). |

## New registration check

`main.note_entities_llm_presenting_symptoms` has 23 column-registry rows, all `verification_status='na'`, all `verification_method='auto_tier1_raw_mirror_skip'`, and all 23 match `information_schema.columns`. Classification upheld.

## Net assessment

mig_165's **10 CF-only deferrals are correct**. The schema split is correct (`main` vs `manuscript_workspace`), and the new presenting-symptoms raw mirror registration is correct.

One **follow-up CF** is opened for `main.imaging_exam_master_v1`: the table was auto-`na`'d under `auto_tier1_raw_mirror_skip`, but live schema shows it is an analytic per-exam rollup rather than a raw Tier-1 mirror. Per the user's notes-only constraint, mig_167 does **not** flip statuses; it appends `CF-mig167-MIG165-MISCLASSIFIED-AUTO-NA-imaging_exam_master_v1` to column/table registry notes and queues future real verification/status reconsideration.

## Open CFs

- `CF-mig167-MIG165-MISCLASSIFIED-AUTO-NA-imaging_exam_master_v1`
- `CF-mig167-VALID-DEFER-imaging_fna_linkage_v3`
- `CF-mig167-VALID-DEFER-imaging_patient_summary_v1`
- `CF-mig167-VALID-DEFER-manuscript_cohort_v1`
- `CF-mig167-VALID-DEFER-patient_cross_domain_timeline_v2`
- `CF-mig167-VALID-DEFER-recurrence_event_clean_v1`
- `CF-mig167-VALID-DEFER-tumor_stage_heterogeneity_v1`
- `CF-mig167-VALID-DEFER-episode_analysis_resolved_v1_dedup`
- `CF-mig167-VALID-DEFER-lesion_analysis_resolved_v1`
- `CF-mig167-VALID-DEFER-ln_master_rollup_v1`
- `CF-mig167-VALID-DEFER-patient_analysis_resolved_v1`
- `CF-mig167-MIG165-PRESENTING-SYMPTOMS-REGISTRATION-CHECK`