<!-- READY FOR LOGAN MANUSCRIPT REFINEMENT -->
# Methods section starter (mig_195)

**Author template:** Cursor (Lane mig_195). **Voice / final scope:** Logan Glosser.  
**Target evidence:** `thyroid_canonical_publication_v1_0` after mig_188b → mig_186b → mig_185b → mig_187.

---

## Data sources

We studied patients in a single-institution thyroid surgery and oncology cohort at Emory University, with data spanning **[start year]–[end year]** ([cite institutional IRB / registry charter]).

Structured and semi-structured sources included synoptic and narrative pathology, operative notes, ultrasound and cross-sectional imaging reports, laboratory feeds (e.g., thyroglobulin and related analytes), structured complications and NSQIP-linked surgical quality data, medication and problem-list NLP layers, and molecular pathology where available. Clinical notes underwent tiered NLP extraction with manual adjudication queues where noted in the publication quality framework.

Analytic datasets were materialized in the publication MotherDuck lakehouse (`thyroid_canonical_publication_v1_0`), including **[N]** canonical tables at gate verification with **[N_verified]** patient-master columns verified under Protocol v2; cohort spine **`canonical_patient_master`** contains **10,871** distinct `research_id` rows. Tiering convention: **[expand — Tier 1 raw / Tier 2 canonical events / Tier 3 patient rollup]**.

---

## Cohort definition

**Inclusion:** Adult patients **[refine age rule]** who underwent **[ Thyroid surgery / thyroid cancer care]** at **[site]** and meet the analytic spine: `canonical_patient_master.is_malignant = TRUE` **and** `research_id` appears in **`canonical_path_malignant_events_v1`** after application of the NIFTP/uncertain-malignant-potential exclusion policy (**mig_186b**), i.e., at least one malignant-classified path event remains on the publication path-malignant event table.

**Exclusion:**

- Patients flagged **not malignant** on the canonical patient master (`is_malignant` false or null per operational rulebook — align with CONSORT SQL step 2).
- Patients with **only** indeterminate-classified path lesions (**NIFTP** / uncertain malignant potential / related WHO 2017-era entities) moved to **`canonical_path_indeterminate_events_v1`** without a retained malignant path event (**mig_186b** landing table; preserved for sensitivity analyses).
- Patients lacking **both** a resolved primary histology field (`histology_final`) **and** **`ajcc8_t_stage_resolved`** (operational “no histology + no T-stage” screen — confirm with Logan whether to relax or replace with registry-eligibility flag).
- Patients lacking **`last_contact_date`** when follow-up-bearing analyses require a terminal contact anchor.

Refer to **`qc_framework_v1/manuscript/cohort_flow_diagram.sql`** for reproducible step counts. Final analytic **N** must match Table 1 denominator unless a documented override is approved.

---

## Variable definitions

- **AJCC 8th edition staging:** Patient-level **`ajcc8_*_resolved`** columns (T, N, M, stage group) supersede legacy stored staging where both exist, following the Logan-ratified R1 rollup (**mig_184_v2** / **`mig188b_mig184_v2_plus_r1c_with_explicit_T0_20260430`**) including explicit **`T0`** handling for LN-only / no-primary-at-index-surgery scenarios.

- **T0:** Used where no in-surgical primary tumor qualifies for pT assignment but nodal or distant disease documentation supports staging convention per **[internal rule memo / AJCC citation]**.

- **Histology:** `histology_final` and auxiliary `histologic_types_all` drive manuscript buckets (**PTC / FTC / MTC / ATC / mixed DTC / other**). Multi-component cancers: default severity ladder **[MTC > PTC > FTC; ATC highest]** — replace bracketed text with ratified pathology policy.

- **Recurrence:** Structural or registry-confirmed recurrence events per **`canonical_recurrence_v1`** grain; align `any_recurrence_flag` on the patient master with event-tier definitions in the supplementary appendix (**CF / registry rows per mig_122 family**).

- **Survival / follow-up:** Time from **first definitive thyroid cancer–related surgery** (`first_surgery_date`) to **last structured contact** (`last_contact_date`) and/or vital status from **`canonical_survival_followup_v1`** (**`last_known_alive_date`**, **`vital_status_current`**). Normalize TIMESTAMP vs DATE per **mig_121 / clinical_date_retype** guidance when joining.

- **Tumor size:** Primary display `path_tumor_size_cm` with `tumor_size_cm_max` / queue corrections where multi-surgery maxima matter (**path_tumor_size_correction_queue_v1**).

- **Margins / R-class:** `r_class_true`, `margin_status_true`, `margin_involved_any` — avoid deprecated `margin_r_class`.

- **RAI:** `rai_first_date IS NOT NULL` as pragmatic “ever treated” screen; refine with episode-grain **`rai_treatment_episode_v2`** if receipt vs planning must be separated.

---

## Statistical approach

- **Descriptive:** Median (25th, 75th percentiles) for continuous variables; counts (percentages) for categorical variables unless imputation protocol dictates otherwise. Denominators equal the analytic cohort **N** unless a variable-specific missingness column is pre-specified.

- **Survival:** Kaplan–Meier estimates with log-rank tests for univariable comparisons; Cox proportional hazards regression for multivariable models, with **[Schoenfeld / log-log]** diagnostics for proportionality; consider **[restricted mean survival time]** where appropriate.

- **Sensitivity analyses:** NIFTP/UMP retain in **`canonical_path_indeterminate_events_v1`** supports re-inclusion sensitivity; duplicate tumor-grain rows flagged **`is_source_distinct_duplicate_grain`** require **COUNT(DISTINCT …)** tumor accounting (**mig_185b**).

- **[Additional methods placeholders]:** competing risks; propensity; missing data (MICE); cluster bootstrap — fill per SAP.

---

## Data quality notes (starter bullets)

- **533** source-distinct duplicate event rows may exist at shared `(research_id, surgery_episode_id, tumor_ordinal)` grain on **`canonical_path_malignant_events_v1`** with **`is_source_distinct_duplicate_grain = TRUE`**; do not dedupe without clinical sign-off (**mig_185b**).
- **220** NIFTP/UMP path events were excluded from the malignant event table but preserved in **`canonical_path_indeterminate_events_v1`** (**mig_186b**).
- **121** LN-NLP-only ultrasound exam identifiers were seeded with deterministic IDs in the exam-master extension (**mig_187** R-A); cite in imaging linkage limitations if relevant.
- Carry-forward (**CF**) annotations live in the verification registry and `manuscript_appendix_candidates` (**mig_190** family); migrate bullet list into supplemental appendix during polish.

---

_End of starter — Logan replaces bracketed placeholders, tightens denominators, and aligns journal style._
