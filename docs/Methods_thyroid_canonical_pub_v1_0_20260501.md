---
title: Methods — Thyroid canonical publication v1.0 (Lane M refresh 2026-05-01)
database: thyroid_canonical_publication_v1_0 (MotherDuck)
refresh_batch: mig_234_table1_csv_refresh_20260501
---

## 1. Data sources

This retrospective cohort combines structured fields extracted from the institutional electronic health record (EHR), synoptic surgical pathology workbooks, laboratory feeds, radiology-derived ultrasound characteristics, and chart-derived NLP pipelines coordinated under the Thyroid 2026 research registry. All analytic-ready tables referenced below live in the MotherDuck database **`thyroid_canonical_publication_v1_0`** (`main` schema + derivative **`semantic_publication`** manuscript-safe projections + selected **`manuscript_workspace`** verification views).

The canonical patient spine is **`main.canonical_patient_master`** (10,871 distinct `research_id` rows per publication governance). Manuscript-facing aggregates for Tables 1–5 were regenerated on **2026-05-01** using **`semantic_publication.vw_*_safe_VIEW_v1`** views built in Lane G (**mig_223**) together with Lane LN lymph-node safe surfaces (**mig_225–226**) and recurrence-safe filtering (**mig_213 semantics reflected in `vw_recurrence_safe_VIEW_v1`**). CSV outputs are frozen alongside this document under `manuscript_outputs/v1_0_20260501/` via **`qc_framework_v1/scripts/build_mig234_lane_m_table1_refresh.py`** + SQL fragments in **`qc_framework_v1/manuscript/mig234_lane_m/`**.

## 2. Cohort definition

We retained adults meeting malignant cohort inclusion after sequential exclusions documented in **`cohort_flow_v1_0_20260501.csv`** (built from `qc_framework_v1/manuscript/mig234_lane_m/cohort_flow_v15.sql`). After filtering to **`is_malignant=TRUE`** CPM rows and removing indeterminate-only (NIFTP/UMP-only) patients, **4,022** malignant patients remain with complete histology or AJCC8 T-stage resolution and non-null last-contact fields — matching step **5** in the cohort-flow export. The final analytic row (**step 9**) intersects those patients with **≥1** row on **`semantic_publication.vw_path_malignant_tumor_safe_VIEW_v1`**, which — on the live MotherDuck refresh executed for mig_234 — also totals **4,022** patients (no additional loss versus step 5 in this snapshot).

**Reproducibility anchors**

- **`semantic_publication.release_manifest_v1`** row **`release_id='pub_v1_0_20260430'`** documents the Lane G semantic freeze (patient/tumor/recurrence/molecular/US/lab row counts + QC gate snapshot + **`git_commit_hash`** captured at mig_223 apply time).
- Lane M refresh stamps **`manuscript_workspace.cpm_reconciliation_provenance_v1`** with **`run_id='mig_234_table1_refresh_v15'`** (see **`qc_framework_v1/migrations/234_table1_csv_refresh_20260501.sql`**) after CSV regeneration.
- Parquet mirror for offline replication: **`parquet_export/pub_v1_0_20260430/`** (mig_230 export lane — align manifest checksums before trusting stale clones).

## 3. Pathology adjudication

Structured malignant pathology events were harmonized into **`main.canonical_path_malignant_events_v1`**, with manuscript-safe deduplication exposed as **`main.canonical_path_malignant_events_dedup_VIEW_v1`** (Lane B **mig_212**). On the refreshed semantic publication surface this corresponds to **5,944** tumor-level rows among **4,022** distinct malignant patients on **`semantic_publication.vw_path_malignant_tumor_safe_VIEW_v1`**. Tumor-row exclusions remove internal duplicate-grain artifacts (`is_source_distinct_duplicate_grain=TRUE`) and **borderline/benign-with-staging quarantine** rows (**mig_229**, **n = 27 rows**) representing FTUMP / follicular adenoma records erroneously carrying AJCC N/M positivity.

Histology labels for lymph-node attribution QC leverage **`main.histology_vocab_normalization_map_v1`** (**104 rows**, Lane **mig_224**) prior to rolling summaries into **`manuscript_workspace.vw_ln_histology_attribution_VIEW_v1`**.

## 4. TIRADS adjudication (ACR 2017)

Ultrasound nodule manuscript denominators are defined through four mutually referenced **`manuscript_workspace`** cohort views (**mig_219 / mig_221**):

| View | Manuscript purpose |
|------|--------------------|
| **`vw_us_nodule_tirads_strict_acr2017_VIEW_v1`** | Strict analytic cohort requiring **`acr2017_feature_points_complete=TRUE`** (descriptor-complete rows suitable for direct ACR point reproducibility). |
| **`vw_us_nodule_tirads_any_reported_VIEW_v1`** | Broad denominator — any reported TI-RADS signal ingested into the characteristics spine (feeds **`semantic_publication.vw_us_nodule_safe_VIEW_v1`**). |
| **`vw_us_nodule_tirads_reported_not_fully_parsed_VIEW_v1`** | Descriptor-incomplete reporting (`acr2017_feature_points_complete=FALSE`) — **not** interchangeable with “derived ACR missing.” |
| **`vw_us_nodule_tirads_unresolved_or_excluded_VIEW_v1`** | Excluded/unresolved documentation cohort for Methods transparency. |

**mig_232 follow-on:** **`vw_us_nodule_tirads_derived_acr_missing_VIEW_v1`** narrows to **`acr2017_feature_points_complete=FALSE`** **AND** NULL derived points/category (~7.3k rows — CF-mig219 reconciliation). Methods statements that describe “missing derived ACR category/points” must cite this **narrow** view; statements about incomplete descriptors must cite **`…reported_not_fully_parsed…`**.

## 5. Lymph node analysis

Publication LN denominators roll up from **`manuscript_workspace.vw_ln_surgery_publication_safe_VIEW_v1`** (per-surgery safe LN examined/positive counts with ambiguity/denominator-conflict flags) into **`manuscript_workspace.vw_ln_patient_publication_safe_VIEW_v1`** (patient grain SSOT with **`ln_crossval_status`** vs `canonical_patient_master` LN rollup fields).

Histology-attributed LN positivity summaries for exploratory tumor-type-specific claims join through **`manuscript_workspace.vw_ln_histology_attribution_VIEW_v1`**. Confidence strata follow Lane LN assessment plan (**concordant**, **`discordant_with_cpm`**, **`cpm_only_null`**, **`safe_only_null`**) with denominator-source priority documented under Open Question 3 of **`qc_framework_v1/reports/lymph_nodes_histology_assessment_plan_20260430.md`**.

Table 3 aggregates (`Table_3_LN_summary_safe_v1_0_20260501.csv`) summarize **`ln_crossval_status`** and positive-count buckets using **`vw_ln_patient_publication_safe_VIEW_v1`** exclusively.

## 6. Recurrence and survival

Structured recurrence synthesis lives in **`main.canonical_recurrence_resolved_v1`**. Manuscript-safe recurrence rows (**excluding implausible-date quarantine**) surface via **`semantic_publication.vw_recurrence_safe_VIEW_v1`** (`WHERE is_implausible_date_quarantine=FALSE` applied during mig_223 projection).

Follow-up duration and vital status use **`main.canonical_survival_followup_v1`** as the survival SSOT (**Protocol v2 / mig_123 lineage**): **`last_known_alive_date`** (`DATE`), **`vital_status_current`**, **`days_from_first_surgery_to_last_contact`**. **`Table_4_recurrence_survival_v1_0_20260501.csv`** left-joins recurrence-safe rows to survival for manuscript-ready patient-grain exports.

## 7. Molecular genetics

Molecular testing episodes derive from **`main.canonical_molecular_genetics_v2`** and publish through **`semantic_publication.vw_molecular_safe_VIEW_v1`**, retaining adjudication flags including **`is_patient_level_only_evidence`** (Lane D **mig_214** semantics). **`Table_5_molecular_distribution_v1_0_20260501.csv`** stratifies episode counts by platform + evidence-class flag.

## 8. Statistical analysis surfaces & reproducibility checklist

1. Regenerate CSV bundle:  
   `.venv/bin/python qc_framework_v1/scripts/build_mig234_lane_m_table1_refresh.py`
2. Optional provenance stamp (mutates `cpm_reconciliation_provenance_v1`): rerun with **`--apply-provenance`** after validating counts.
3. Confirm **`semantic_publication.release_manifest_v1`** metrics still align with expectation after upstream edits.
4. Record executing **`git rev-parse HEAD`** in supplemental appendices whenever CSV hashes change.

## 9. Limitations

1. **Multi-surgery tumor-size aggregation (`CF tumor_size_cm_max`):** `canonical_patient_master.tumor_size_cm_max` may under-represent later completion surgeries — manuscript tumor-size sensitivity analyses should consider **`GREATEST(path_tumor_size_cm, tumor_size_cm_max)`** or approved correction-queue values (**`path_tumor_size_correction_queue_v1`**).

2. **LN metastasis array sparsity (`CF-LN-METS-ARRAY-EMPTY-2801`):** ~2,801 of ~2,847 LN-positive patients lack structured histology-attribution arrays needed for refined tumor-subtype LN summaries — restrict claims accordingly.

3. **Recurrence dating:** Quarantine handles implausible anchors but residual **`unresolved_date`** recurrence burden persists — time-to-event analyses must document censoring rules tied to `canonical_recurrence_resolved_v1`.

4. **Semantic projection lag:** `vw_patient_master_safe_VIEW_v1` intentionally exposes a curated column subset — supplemental joins to **`canonical_patient_master`** for operative timing (`rai_first_date`, multifocal path aggregates reconciled against tumor rows, etc.) are documented explicitly in **`table_1_cohort_demographics_v15.sql`** and mirrored in this Methods note.

5. **External reproducibility:** Analysts without MotherDuck access should consume **`parquet_export/pub_v1_0_20260430/semantic_publication/`** Parquet shards plus this CSV refresh; always verify checksums in **`parquet_export/pub_v1_0_20260430/_MANIFEST.md`**.

---

### Artifact index (Lane M)

| Artifact | Role |
|----------|------|
| `qc_framework_v1/manuscript/mig234_lane_m/table_*_v15.sql` | Executable SELECT definitions |
| `qc_framework_v1/migrations/234_table1_csv_refresh_20260501.sql` | Provenance DDL |
| `qc_framework_v1/scripts/build_mig234_lane_m_table1_refresh.py` | CSV exporter |
| `manuscript_outputs/v1_0_20260501/*.csv` | Submission-ready tables |
| `docs/Methods_thyroid_canonical_pub_v1_0_20260501_REFERENCES.bib` | BibTeX stubs |
