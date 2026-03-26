# Study design editorial — Bethesda III/IV molecular utilization manuscript

**Role:** Study design editor notes for the next manuscript, aligned with `METHODS_RESULTS_SKELETON.md` and linkage audit.  
**Constraints honored:** Descriptive (non-causal); Bethesda III/IV primary; Bethesda V secondary/exploratory; scaffold reused; explicit separation of all-eligible, tested, and operated subsets; surgery-restricted ROM must not dominate the primary narrative.

---

## 1. Primary manuscript question (best framing)

**Among adults with resolved **Bethesda III (AUS/FLUS) or Bethesda IV (FN/SFN)** cytology in `manuscript_cohort_v1`, how often did patients receive **preoperative ThyroSeq or Afirma**, how did that frequency vary by **calendar period** and **nodule size stratum**, and—among those tested—how were **molecular result categories** distributed by **platform**?**

*Rationale:* This question matches the recoverable analytic path (patient-level temporal join to `molecular_test_episode_v2`; see `SCHEMA_LINKAGE_AUDIT.md`), centers the clinically contested indeterminate zone (III/IV), and keeps **utilization and result mix** as the main contribution rather than **rate of malignancy (ROM) among operated patients**.

---

## 2. Primary and secondary cohorts (exact definitions)

| Role | Cohort | Inclusion (logical definition) |
|------|--------|--------------------------------|
| **Primary — all-eligible (utilization denominator)** | Bethesda **III or IV** only | `manuscript_cohort_v1`; `fna_bethesda_final` ∈ **{3, 4}**; apply the same adult / data-quality rules the manuscript already uses for the institutional cohort spine (as documented in the MotherDuck views—do **not** require surgery or final histology for this denominator). |
| **Primary — tested (result-mix denominator)** | Subset of all-eligible | Same as above, plus ≥1 preoperative ThyroSeq or Afirma row in `molecular_test_episode_v2` per existing rules (**latest** qualifying test **on or before** `surg_first_date` where surgery exists; for patients **without** surgery, define preoperative as **on or before first thyroid surgery date if applicable**, or use an explicit alternative **prespecified** in methods—e.g. “any test dated within the indeterminate-management episode window”—*must be one rule, stated once*). |
| **Secondary / sensitivity — operated + histology (ROM & extent)** | Bethesda III/IV subset | All-eligible **and** non-missing `surg_first_date` **and** non-null `histology_final` (current operated logic, restricted to **Bethesda 3 or 4**). |
| **Exploratory — Bethesda V** | Parallel rows / supplement | Same three shells (**all-eligible V**, **tested V**, **operated+V histology**) reported **secondary**: appendix or supplement tables, or clearly labeled exploratory panels—not pooled with III/IV for the primary abstract conclusion. |

*Note:* Refresh *N* for each shell from `sql/01_views_and_cohort.sql` after adding explicit Bethesda 3/4 vs 5 splits; audit snapshot totals in `SCHEMA_LINKAGE_AUDIT.md` describe the **operated III–V** view (*N* = 641) and manuscript Bethesda 3–5 counts for cross-check only.

---

## 3. Denominator logic (exact)

1. **Utilization % (primary):**  
   - **Denominator:** count of patients in **primary all-eligible Bethesda III/IV** cohort.  
   - **Numerator:** patients with **preoperative** ThyroSeq or Afirma (platform filter and date fields as in skeleton: `platform` ∈ {ThyroSeq, Afirma}, resolved/native test date, temporal rule relative to first surgery when surgery is present).

2. **Result distribution % (primary, among tested):**  
   - **Denominator:** tested patients within **Bethesda III** and separately **Bethesda IV** (column % **within** Bethesda stratum among tested), matching current **Table 2** structure.  
   - **Numerator:** mapped **Benign / Suspicious / Malignant / Inconclusive** via `mol_result_class_map_v1`.

3. **ROM % (secondary only):**  
   - **Denominator:** **operated + histology** subset (Bethesda III/IV primary; Bethesda V exploratory).  
   - **Numerator / malignancy flag:** keyword logic on `histology_final` as in skeleton, with sensitivity checks noted in limitations.  
   - **Never-tested operated** row remains a **descriptive comparator** under strong selection (see §4)—reported **after** primary utilization, not as the paper’s headline.

---

## 4. Why the current operated-only analytic core should be revised

The current spine (`indeterminate_molecular_cohort_v1`, *N* = 641) requires **first surgery date** and **`histology_final`**. That:

- **Truncates the eligible population** to patients who underwent surgery with documented final histology, mixing **indeterminate cytology management** with **referral to surgery** and **pathology ascertainment**.
- **Enriches for malignancy** relative to all Bethesda III/IV patients under care (illustrated by very high ROM in never-tested operated patients in **Table 3**), so **testing intensity and ROM cannot be read as independent “real-world” quantities** on the same denominator.
- **Makes utilization appear marginal** if readers implicitly compare to the wrong population (operated-only denominators versus institution-wide Bethesda III/IV prevalence).
- **Conflicts with the primary clinical question** for Bethesda III/IV—**who gets tested and what results look like**—which requires a denominator that includes **non-operated** and **not-yet-operated** pathways.

**Revision:** Keep the operated cohort as the **prespecified secondary/sensitivity** shell for ROM and surgery extent, but move **primary** inference to **all-eligible Bethesda III/IV** (and **tested** subset for result mix).

---

## 5. Bethesda V — primary or secondary?

**Secondary / exploratory only.**  

Pool Bethesda V with III/IV **only** for global technical summaries if needed (e.g. total institutional test volume), but **do not** let Bethesda V define the **primary** question, abstract, or first table—**suspicious-for-malignancy** cytology has a different risk architecture and management default; per user constraint, **Bethesda III/IV stay primary**; **V** in supplement or labeled exploratory panels.

---

## 6. Final table list

| # | Content | Primary / secondary |
|---|---------|---------------------|
| **Flow / cohort summary** | Counts: manuscript Bethesda III/IV **all-eligible → tested → operated+histology**; optional parallel line for Bethesda V (supplement). | Primary (flow table or CONSORT-style **Supplemental Table 1**) |
| **Table 1 / 1a** | Utilization by **surgery year** (or management year if redefined for non-operated—prefer **two timelines** in methods: surgery-year for operated shell; **cohort index date** for all-eligible if needed) × **Bethesda III vs IV**; Bethesda V appendix. | Primary (III/IV); V exploratory |
| **Table 1b–c** | Size and era stratifications (existing exports: `table1_utilization_by_size.md`, `table1_utilization_by_era.md` pattern). | Primary (restrict to III/IV for main text cells) |
| **Table 2** | Platform × result bucket, **tested only**, column % within Bethesda; **III/IV** main; **V** supplement. | Primary (III/IV) |
| **Table 3** | ROM by platform × result; **operated+histology only**; **never-tested** comparator explicitly **selection-prone**; stratified **III/IV** primary, **V** exploratory. | Secondary |
| **Optional supplemental** | `tumor_pathology.histology_1_type` sensitivity for malignancy flag; `date_status` for molecular dates. | Supplement |

---

## 7. Final figure list

| # | File / concept | Role |
|---|----------------|------|
| **Figure 1** | `outputs/plots/utilization_trend_by_year.png` (or successor): **Bethesda III/IV** preoperative testing % over time; Bethesda V **appendix figure** or dashed overlay—not the only story. | Primary |
| **Figure 2** | `outputs/plots/stacked_result_by_platform.png`: result mix **among tested**, **III/IV** focus. | Primary |
| **Figure 3 (supplement / exploratory)** | Sankey (`outputs/sankey_edges.csv`): optional **III/IV-only** main; full III–V in supplement if crowded. | Secondary / exploratory |

---

## 8. Limitations language (non-causal; copy-ready)

This **retrospective, single-institution** descriptive study **quantifies documented care patterns** extracted from a linked lakehouse; it **does not** support causal inference about whether molecular testing **changes** surgical intensity, histologic outcomes, or malignancy risk. **Selection into testing** and **selection into surgery** are expected; **preoperative testing status** and **rate of malignancy among operated patients** **coexist** in charts but **must not** be read as treatment effects. Molecular test timestamps have **incomplete day-level precision** for part of the record; linkage between FNA episodes and molecular rows is **partial at episode grain**, so utilization uses **patient-level temporal logic** as prespecified. Malignancy on final pathology is **algorithmically summarized from text** and should be **interpreted descriptively**, with **confirmation against structured pathology** where available. Results **may not** generalize beyond this health system, EHR implementation, or era.

---

## Implementation note

SQL/views should gain explicit **`bethesda_primary_group`** flags (3/4 vs 5) and three **cohort shells** while **reusing** existing variable definitions (`mol_result_class_map_v1`, preop test rule, ROM keyword). Update table exports and figure scripts to **subset III/IV** for main text and **migrate V** to supplement paths.
