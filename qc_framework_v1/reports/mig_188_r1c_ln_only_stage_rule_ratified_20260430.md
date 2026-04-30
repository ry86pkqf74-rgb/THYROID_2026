# mig_188 — r1c LN-only stage rule RATIFIED (Logan prior-thy carry-forward)

**Author:** Logan Glosser `<logan.glosser@gmail.com>`  
**Date:** 2026-04-30  
**Lane:** `mig_188` / `r1c_ln_only_stage_rule_RATIFIED`  
**Batch:** `mig_188_mig184_v2_plus_r1c_ln_only_stage_rule_apply_20260430`  
**Apply skeleton:** `qc_framework_v1/migrations/188_mig184_v2_plus_r1c_ln_only_stage_rule_apply_20260430.sql`  
**Governance:** Cursor lane authored artifacts only — **no MotherDuck DDL/DML executed** from this session.

---

## §1 Logan-ratified prior-thy rule (verbatim)

Apply **per patient-event**:

1. **Strong prior-thy evidence** (`pshx_nlp_prior_thyroidectomy=TRUE` OR EXISTS another `canonical_path_malignant_events_v1` row at this rid with non-null size): **UPSTAGE** by carrying the prior-event T-stage forward. Use `path_tumor_size_cm` from PM (or the maximum non-null size from any other event on same rid) for T derivation; combine with current N from this event row; compute stage_group via existing AJCC §G branches. Set `t_resolution_source='prior_thy_recurrence_T_from_prior_path'`. **~41 patients** target.
2. **No prior-thy evidence** (no NLP signal AND no other event with size AND `path_tumor_size_cm` is NULL): **LEAVE NULL**. Set `t_stage_resolved=NULL`, `stage_group_resolved=NULL`, `t_resolution_source='no_primary_at_this_surgery_pT0_unstaged'`. **~6 patients** target.
3. **Ambiguous: PM `path_tumor_size_cm` populated but no other event with size** (~25 patients): **GENERATE LOGAN-REVIEW CSV**. Default to NULL pending review. Set `t_resolution_source='ambiguous_pm_size_only_logan_pending'`.

Stage_group consequences (already correct in mig_184_v2 §G):

- DTC age ≥55 + N1 → Stage II (regardless of N1a vs N1b)
- DTC age <55 → Stage I (M0) / II (M1)
- **MTC + N1a M0 → Stage III**
- **MTC + N1b M0 → Stage IVA**

---

## §2 Column-existence pre-flight (repo-grounded; live MD probe deferred)

MotherDuck `information_schema` was **not queried** from this Cursor lane (`READ-ONLY scoping`, **no MotherDuck execution**).

Cross-check against repo inventories:

| Table | Column list status |
|-------|---------------------|
| `canonical_path_malignant_events_v1` | `ln_involved`, `ln_examined`, `nodal_disease_positive_count`, `nodal_disease_total_count`, `size_greatest_dimension_cm`, `tumor_size_cm_per_surgery`, `primary_histology`, `histology_variant`, `extrathyroidal_extension`, `gross_ete`, `t_stage_ajcc8`, `n_stage_ajcc8`, `m_stage_ajcc8`, `t_stage_ajcc7`, `n_stage_ajcc7`, `m_stage_ajcc7` — referenced throughout mig_184_v2 / mig_185 artifacts; **none flagged missing**. |
| `canonical_invasion_events_v1` | `invasion_type`, `evidence_qualifier`, `finding_status`, `research_id`, `linked_surgery_episode_id` — used in mig_184_v2 §D. |
| `canonical_patient_master` | `age_at_surgery`, `histologic_types_all`, `histology_final`, `path_tumor_size_cm`, `tumor_size_cm_dominant`, `tumor_size_cm_max`, `pshx_nlp_prior_thyroidectomy` — listed in `studies/canonical_cleanup_20260417/cpm_cols_pre.txt` / data_dictionary artifacts. |
| `canonical_patient_master` LN split §E/G | `cnln_img_lateral_neck_present`, `cnln_img_left_present`, `cnln_img_right_present`, `cnln_img_bilateral_present`, `cnln_img_central_present`, `cnln_img_levels_mentioned`, `cnln_surg_levels_mentioned` — **`qc_framework_v1/migrations/174b_cnln_img_laterality_per_side_boolean_20260429.sql` ADD COLUMN** (confirm deployed on publication MD before Path-C apply). |
| `canonical_patient_master` | `lateral_neck_dissected_structured_or_nlp` — **present in AGENTS.md / canonical cleanup narrative** but **absent from aged `cpm_cols_pre.txt` snapshot** → **VERIFY live MD** before apply. |

**Rewiring:** none required from repo evidence; only **live-catalog confirmation** for `cnln_img_*` booleans + `lateral_neck_dissected_structured_or_nlp`.

---

## §3 Dependent-VIEW scan

- **`manuscript_workspace.canonical_path_malignant_events_v1_ajcc_flag`** (`qc_framework_v1/migrations/20_ajcc01_03_staging_integrity.sql`): reads legacy `t_stage_ajcc8` / calculability flags — **unaffected** by additive `*_resolved` columns.
- **`manuscript_workspace.canonical_path_malignant_events_v1_size_flag`** (`16_path12_size_disagreement.sql`): reads size columns — **unaffected**.
- **Risk pattern:** any view whose SQL body referenced **`SELECT *`** from `canonical_path_malignant_events_v1` at CREATE time would gain shadow columns client-side — grep suggests explicit column lists on flagged views.

**Live enumeration:** Cowork Path-C should run  
`SELECT table_schema, table_name FROM information_schema.view_table_usage WHERE ...`  
for diligence after ADD COLUMN wave.

---

## §4 §D injection site + §G NULL-T / MTC spot-check

### §D injection

The **main `CASE` expression for `t_stage_ajcc8_resolved`** in mig_184_v2 §D is the injection surface. mig_188 inserts **§D-prime** branches:

- **Immediately after** airway/extrathyroidal **T4a** detection **and before** microscopic-ETE text branching — so **NULL event-size** rows **cannot be swallowed** by the microscopic-ETE inner `CASE` returning NULL without evaluating Logan buckets.

Resolution metadata (`ajcc_resolution_source` / confidence) parallels the same bucket ordering.

### §G NULL T with non-null N

When `t8` IS NULL after rollup (`arg_max` yields NULL if all events NULL), **every §G branch requires concrete `t8`/`t7` literals or defaults** → execution falls through to **`ELSE NULL`** for `sg8`/`sg7`. **Stage group stays NULL** (not error).

### MTC + N1 (r1c exemplars from adjudication CSV)

Example research_ids appearing under `review_size_unavailable`: **423**, **2018**, **4015** (`exports/mig184_r1_adjudication_20260430/r1c_size_unavailable_residual_121events.csv`).

Tracing §G (`stage_component='MTC'`):

- **`t8 IN ('T1','T1a','T1b','T2','T3','T3a','T3b') AND n8='N1a'` → `III`**
- **`t8 IN (...same...) AND n8 IN ('N1','N1b')` → `IVA`** (Logan-flagged upstage vs understaged lumped N1)

Once §D-prime supplies **`t8` from prior path size** for bucket-1 rows, **`n8`** still comes from §E event LN logic + §G PM N split exactly as mig_184_v2.

---

## §5 Row-level disposition counts & drift cohort delta (expectations)

**CSV rows:** `exports/mig188_r1c_disposition_20260430/*.csv` ship **headers only** from Cursor lane; populate via migration **§K** SELECT shells post-apply.

**Expected movement vs mig_184_v2 §J baseline** (HEAD hypothesis `6edb881`):

| Metric | mig_184_v2 baseline (informative) | Post mig_188 expectation |
|--------|-----------------------------------|---------------------------|
| `size_residual_logan_pending` events | ~82 | Drops by **upstage bucket (~41)** + **ambiguous (~25)** becoming explicitly labeled sources (still NULL `t_stage` for ambiguous until Logan CSV adjudication completes numeric anchor choice). Remaining hard residual approaches **~6** unstaged-only after excluding ambiguous row-label semantics. |
| `prior_thy_recurrence_T_from_prior_path` | 0 | **~41 events** |
| `no_primary_at_this_surgery_pT0_unstaged` | 0 | **~6 events** |
| `ambiguous_pm_size_only_logan_pending` | 0 | **~25 events** |
| `paired_pm_ajcc8_stage_group` / drift | baseline from §J | **Non-null `ajcc8_stage_group_resolved` rises** for patients gaining **`t8`** via bucket 1 (~41 patient-level uplifts conditional on rollup dominance). |

Exact counts require Path-C execution of §J probes post-apply.

---

## §6 Unblocking checklist — Cowork Path-C apply

1. **Live MD column verification** for §E/G LN booleans (`cnln_img_*`, `lateral_neck_dissected_structured_or_nlp`).
2. Confirm whether **mig_184_v2 already applied**: §B/C `ADD COLUMN IF NOT EXISTS` + snapshots — mig_188 uses **`mig188_pre_snapshot_*`** table names to avoid clobbering mig184-only snapshots.
3. Apply **`188_mig184_v2_plus_r1c_ln_only_stage_rule_apply_20260430.sql`** as single scripted batch (or staged §A→§J per governance).
4. Run **§J** probes including **`path_event_t_resolution_source`** distribution.
5. Export disposition CSVs via **§K** commented `COPY` shells; Logan completes **`ambiguous_pm_only`** review columns.
6. Update **`canonical_patient_master` row invariant** tooling (`cpm_built_at`, provenance insert validation per AGENTS.md).

---

## Deliverables map

| Artifact | Path |
|----------|------|
| Combined apply SQL | `qc_framework_v1/migrations/188_mig184_v2_plus_r1c_ln_only_stage_rule_apply_20260430.sql` |
| This report | `qc_framework_v1/reports/mig_188_r1c_ln_only_stage_rule_ratified_20260430.md` |
| Disposition CSV stubs + manifest | `exports/mig188_r1c_disposition_20260430/` |
