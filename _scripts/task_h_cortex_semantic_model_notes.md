# Task H — Cortex Analyst semantic model notes
# m025_m044_v1.yaml

**File:** `_semantic_models/m025_m044_v1.yaml`
**Created:** 2026-05-06
**Closes:** Task H (CURSOR_PARALLEL_TASKS_20260506.md)
**Related:** `CORTEX_ANALYST_BIND_WALKTHROUGH_20260505.md`

---

## What this YAML does

Defines a **Cortex Analyst semantic model** that lets Logan ask natural-language
questions over two manuscripts — M025 (TI-RADS) and M044 (ETE/AJCC8) — without
writing SQL. Cortex Analyst translates the NL question → SQL → results.

The model covers three tables:

| Logical name | SF mirror target | MotherDuck source |
|---|---|---|
| `m025_tirads_cohort` | `THYROID_VALIDATION.PUBLIC.COHORT_M025_TIRADS_PERFORMANCE_V1_FLAT` | `manuscript_workspace.cohort_m025_tirads_performance_v1` |
| `m044_ete_cohort` | `THYROID_VALIDATION.PUBLIC.COHORT_M044_AJCC_ETE_V1_FLAT` | `manuscript_workspace.cohort_m044_ajcc_ete_v1` |
| `canonical_patient_master` | `THYROID_VALIDATION.PUBLIC.CANONICAL_PATIENT_MASTER` | `main.canonical_patient_master` |

---

## Natural-language questions the YAML can answer

### M025 TI-RADS performance

| Category | Example question | Key columns used |
|---|---|---|
| ROM by TI-RADS | "What is the ROM by TI-RADS category?" | `tirads_worst_category`, `is_malignant` |
| ROM cross-tab | "ROM at each TI-RADS × Bethesda combination?" | `tirads_worst_category`, `bethesda_category`, `is_malignant` |
| Cohort size | "How many patients are in the M025 cohort?" | `n_patients` measure |
| Sex breakdown | "How many male vs female patients are in each TR category?" | `tirads_worst_category`, `sex` |
| Race breakdown | "What is the ROM by race and TI-RADS category?" | `tirads_worst_category`, `race`, `is_malignant` |
| FNA usage | "What fraction of TR4/TR5 patients had a pre-op FNA?" | `tirads_worst_category`, `n_patients_with_fna` |
| FNA concordance | "How does FNA-pathology concordance vary by TI-RADS?" | `tirads_worst_category`, `fna_path_concordance` |
| Nodule size | "What is the mean nodule size in TR4 vs TR5 patients?" | `tirads_worst_category`, `mean_dominant_nodule_size_cm` |
| Temporal trends | "How has the ROM in TR4 patients changed by year of surgery?" | `surgery_date`, `tirads_worst_category`, `is_malignant` |
| Procedure split | "What fraction of malignant TR5 patients had total thyroidectomy?" | `tirads_worst_category`, `is_malignant`, `surg_procedure_type` |

### M044 ETE grading and recurrence

| Category | Example question | Key columns used |
|---|---|---|
| Prevalence by grade | "How many patients have gross vs microscopic ETE?" | `ete_grade_final`, `n_patients`, `gross_ete_pct` |
| Recurrence by grade | "What is the recurrence rate by ETE grade?" | `ete_grade_final`, `any_recurrence_flag` |
| Structural recurrence | "Structural recurrence rate in gross ETE vs absent?" | `ete_grade_final`, `structural_recurrence_flag` |
| Stage distribution | "What is the AJCC stage distribution by ETE grade?" | `ete_grade_final`, `ajcc8_stage_group` |
| T-stage × ETE | "How many patients have T3b by ETE grade?" | `ete_grade_final`, `ajcc8_t_stage` |
| ATA risk × ETE | "What fraction of gross-ETE patients are ATA high-risk?" | `ete_grade_final`, `ata_risk_category` |
| RAI receipt | "Is RAI receipt higher in gross vs microscopic ETE?" | `ete_grade_final`, `rai_received_flag` |
| LN positivity | "LN-positive rate by ETE grade?" | `ete_grade_final`, `ln_positive_flag` |
| LVI × ETE | "How does LVI grade co-distribute with ETE grade?" | `ete_grade_final`, `lvi_grade` |
| Histology split | "Recurrence by ETE grade within PTC patients?" | `ete_grade_final`, `histology_final`, `any_recurrence_flag` |
| Follow-up | "Mean follow-up by ETE grade?" | `ete_grade_final`, `mean_followup_years` |
| Tumor size | "Mean tumor size by ETE grade?" | `ete_grade_final`, `mean_tumor_size_cm` |

### Cross-table (M044 + CPM enrichment)

| Example question | Tables joined |
|---|---|
| "Recurrence by ETE grade, stratified by BRAF mutation?" | `m044_ete_cohort` JOIN `canonical_patient_master` on research_id |
| "Does TERT positivity modify the ETE-recurrence relationship?" | `m044_ete_cohort` JOIN `canonical_patient_master` |
| "BRAF rate among M025 malignant TR4/TR5 patients?" | `m025_tirads_cohort` JOIN `canonical_patient_master` |

---

## What this YAML CANNOT answer (scope limits)

| Out-of-scope question | Why / what to use instead |
|---|---|
| Per-nodule ROM (the M025 nodule-level model) | Use `m025_nodule_level` (separate YAML already bound) |
| Survival curves / Kaplan-Meier | Cortex Analyst generates SQL aggregates; KM requires R/lifelines computation outside Snowflake |
| AUC / ROC values | Multi-step statistic; use pre-computed tables (`m025_auc_summary` if exposed separately) |
| Any M032, M037, M038 questions | Out of scope for this YAML; those cohort tables not included |
| Raw note text | PHI; not in any Snowflake mirror |
| Longitudinal Tg trajectories | Requires `longitudinal_lab_canonical_v1`; not in this model |

---

## Column provenance notes

### M025 cohort
- `tirads_worst_category_v12` — Phase 12 structured source (ACR+Excel); replaces `preop_tirads_category` for publication
- `bethesda_final_name` — resolved Bethesda from `extracted_fna_bethesda_v1` via `v_fna_episode_bethesda_resolved_v1`
- `is_malignant` — from CPM `is_malignant` (10,871-patient spine); surgical cohort enrichment
- `fna_path_concordance_category` — computed by `extracted_fna_path_concordance_v1`

### M044 cohort
- `ete_grade_final` — canonical cleanup Phase 4.6 + 266b adjudicated grade; absent/microscopic/gross
- `gross_ete_flag` — BOOLEAN; TRUE only for gross (pT3b+); microscopic ETE does NOT set this flag
- `ajcc8_t_stage` — post-mig_313 corrected column (was `ajcc8_t_stage_corrected` pre-Phase 4.6 rename)
- `ajcc8_m_stage` — post-mig_313 M-stage corruption fix; M1=114 (2.84%), was 1,816 (45%) before fix
- `any_recurrence_flag` — CPM field; 1,986/10,871 patients; 88.8% have unresolved event dates (see AGENTS.md)
- `ata_risk_category` — M044 cohort-specific derivation; prefer over `canonical_patient_master.ata_initial_risk`

### CPM enrichment table
- `braf_positive_final` — Phase 11 composite (NGS + NLP-confirmed + ThyroSeq); 546 positive / 10,871
- `tert_positive_v9` — Phase 9 expanded; 96 positive (was 1 pre-Phase 5)
- `ras_positive_final` — Phase 11 composite; 292 positive
- `comp_vc_paralysis_confirmed` — mapped to `comp_rln_confirmed` synonym in this YAML; Tier 1+2 confirmed

---

## SF mirror creation (pre-bind requirement)

Before binding this YAML in Snowsight, Logan must create the three SF flat tables.
Follow the pattern from `snowflake_trial/scripts/load_m025_nodule_level_to_sf.py`:

```bash
# 1. Export cohort tables from MotherDuck → CSV
# 2. Load to Snowflake via snow sql + PUT + COPY INTO
# Target tables (use Snowflake uppercasing convention):
#   THYROID_VALIDATION.PUBLIC.COHORT_M025_TIRADS_PERFORMANCE_V1_FLAT
#   THYROID_VALIDATION.PUBLIC.COHORT_M044_AJCC_ETE_V1_FLAT
#   THYROID_VALIDATION.PUBLIC.CANONICAL_PATIENT_MASTER
```

Scripts to create: `snowflake_trial/scripts/load_m025_patient_to_sf.py`,
`snowflake_trial/scripts/load_m044_ete_to_sf.py` (mig_312).

---

## Bind walkthrough (abbreviated)

1. Upload `_semantic_models/m025_m044_v1.yaml` to stage:
   ```
   @THYROID_VALIDATION.PUBLIC.CORTEX_ANALYST_STAGE/m025_m044_v1.yaml
   ```
2. Snowsight → AI & ML → Cortex Analyst → + Semantic model → Use existing file
3. Display name: `M025+M044 patient-level (TI-RADS + ETE)`
4. Smoke test (M025): `"What is the ROM by worst TI-RADS category?"`
5. Smoke test (M044): `"What is the recurrence rate by ETE grade?"`
6. Smoke test (cross-table): `"Recurrence by ETE grade, stratified by BRAF mutation?"`

See `CORTEX_ANALYST_BIND_WALKTHROUGH_20260505.md` for full step-by-step.

---

## Carry-forward for mig_312

This YAML is Task H output; it is **not** deployed. The binding requires:
1. SF mirror tables created (see above)
2. Manual Snowsight bind step
3. Smoke tests pass against locked manuscript numbers
4. Signoff row in `main.signoff_migration` for mig_312
