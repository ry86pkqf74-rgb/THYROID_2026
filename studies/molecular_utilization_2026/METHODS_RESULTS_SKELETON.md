# Methods & Results — Molecular Utilization (ThyroSeq vs Afirma, Bethesda III–V)

**Design:** Retrospective descriptive study using the institutional THYROID_2026 lakehouse (`thyroid_research_2026`, tag `v2026.03.13`). No formal hypothesis tests; counts and percentages only.

**Study design (editorial).** The **primary** manuscript is **Bethesda III/IV–centric**: utilization and result-mix denominators should be defined on **all-eligible** and **tested** cohort shells **without** requiring surgery or `histology_final`. **Bethesda V** and **operated+histology** subsets are **secondary/exploratory** (ROM, procedure mix); the prior operated-only spine (`indeterminate_molecular_cohort_v1`, *N* = 641 Bethesda III–V with surgery and histology) remains a **prespecified sensitivity** shell—not the sole primary population. See **`STUDY_DESIGN_EDITORIAL.md`** for exact cohort shells, denominator logic, table/figure list, and non-causal limitations language.

## Methods (skeleton)

**Cohort.** *Legacy operated-and-histology row (sensitivity / ROM shell):* Adults were included if they appeared in `manuscript_cohort_v1` with **Bethesda category III, IV, or V** on the resolved cytology layer (`fna_bethesda_final` ∈ {3, 4, 5}), a non-missing **first surgery date** (`surg_first_date`), and a non-empty **final surgical histology string** (`histology_final`). This yields a **patient-level** analytic file (`indeterminate_molecular_cohort_v1`, *N* = 641 in the March 2026 MotherDuck snapshot). *Primary utilization cohort:* **Bethesda III or IV** in `manuscript_cohort_v1` **without** requiring surgery/histology; refresh *N* in SQL after view split (see editorial doc).

**Index nodule size.** Size strata used `COALESCE(path_tumor_size_cm, imaging_nodule_size_cm)` from the manuscript layer; missing sizes labeled **Unknown**.

**Molecular testing.** A patient was **preoperatively molecular-tested** if there existed at least one row in `molecular_test_episode_v2` with `platform` ∈ {ThyroSeq, Afirma}, a non-null resolved/native test date, and test date **on or before** `surg_first_date`. When multiple tests qualified, the analysis retained the **latest** preoperative test for classification (utilization remains binary at the patient level).

**Result classification.** `overall_result_class` values were mapped to **Benign / Suspicious / Malignant / Inconclusive** using the explicit `mol_result_class_map_v1` view (`negative` → Benign, `positive` → Malignant, `suspicious` → Suspicious, `other` and technical outcomes → Inconclusive). Trailing whitespace was trimmed before mapping.

**Linkage architecture.** Episode-level FNA keys require **`research_id` together with `fna_episode_id`** (episode numbers repeat across patients). Production v3 linkage tables (`preop_surgery_linkage_v3`, `surgery_pathology_linkage_v3`) are complete at the graph level, but **`fna_molecular_linkage_v2` is empty** and **`fna_molecular_linkage_v3` did not intersect** the operated Bethesda III–V FNA–surgery chain in the audited snapshot; therefore **patient-level temporal joining** was used for ThyroSeq/Afirma uptake rather than FNA–molecular graph edges alone.

**Malignancy (ROM).** Surgical **rate of malignancy** used a **keyword flag** on `histology_final`: `FALSE` if “NIFTP” appeared; `TRUE` for standalone PTC tokens, substrings carcinoma/metastatic/lymphoma/sarcoma/malignant (see SQL). This is **descriptive** and should be sensitivity-checked against structured `tumor_pathology.histology_1_type` where both exist.

**Surgery extent (exploratory).** First operative episode `procedure_normalized` contrasted hemithyroidectomy vs total thyroidectomy; completion thyroidectomy after initial lobectomy inferred from paired `tumor_episode_master_v2` procedure text patterns (see SQL).

**Era.** **pre_2021** vs **2021+** split applied to surgery year (primary timeline). Molecular test-year analyses are secondary where noted in tables.

## Results (skeleton)

*Replace bracketed numbers after refreshing MotherDuck.*

- The operated Bethesda III–V cohort with documented final histology comprised **641** patients; **69 (10.8%)** had at least one **preoperative** ThyroSeq or Afirma result (**Table 1**).
- Utilization by **surgery year** and Bethesda category is summarized in **Table 1a**; **size** and **era** stratifications appear in **Table 1b–c** (exports under `outputs/`).
- Among tested patients, **platform × Bethesda × molecular class** distributions are shown in **Table 2** (column percents sum to 100 within each Bethesda stratum among the *tested* subset).
- **ROM** by platform and molecular class, including a **never-tested** comparator, is shown in **Table 3**. Never-tested rows inherit the dominant surgical referral pattern for indeterminate cytology (high ROM is **expected** and must be interpreted descriptively).
- **Figure:** `outputs/plots/utilization_trend_by_year.png` — percent preoperative molecular testing by surgery year.
- **Figure:** `outputs/plots/stacked_result_by_platform.png` — collapsed result mix by platform among tested patients.
- **Sankey data:** `outputs/sankey_edges.csv` encodes Bethesda → molecular class → first procedure → completion flag aggregates.

## Limitations (brief)

Single-institution structured + NLP harmonization; **molecular day-level dates** are incomplete for a minority of episodes; keyword ROM is **not** central review; FNA–molecular graph linkage is **not** relied upon for the primary utilization numerator in this subcohort.
