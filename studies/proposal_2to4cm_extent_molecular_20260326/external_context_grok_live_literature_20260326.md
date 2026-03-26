# External context — Grok memo normalized (live literature pass)

**Source file:** `/Users/ros/Downloads/GROK_2_4_cm_manuscript_feedback.txt`  
**Normalized:** 2026-03-26  
**Authority rule:** This study folder (`proposal_2to4cm_extent_molecular_20260326/`) is the **only** quantitative source of truth for **this** paper. Grok content must **not** override `analysis_manifest.json`, `CLAIM_SOURCE_LEDGER.md`, or CSV-backed numbers in this folder.

---

## A. Safe to use for this paper (after author verification of citations)

Use as **secondary literature and framing**, not as numeric results for this manuscript.

1. **Reporting framework** — STROBE-oriented transparency for retrospective cohorts; align with `strobe_checklist_v1.md` and formal STROBE citation once authors add a bibliography.

2. **Topic-level literature themes** (verify primary sources before citing):
   - Evolution of ATA / management guidance on extent (lobectomy vs total) for differentiated thyroid cancer, including contemporary updates framed in guideline text.
   - Mixed observational evidence on whether extent differs by oncologic outcomes after multivariable adjustment, with **size-stratified** nuance in some cohorts.
   - Role of **molecular and cytologic risk tools** as adjuncts to anatomy/size for decision-making (as **background**, not as this study’s findings).

3. **Writing discipline (qualitative)** — Grok’s advice to avoid causal language, to qualify “equivalence/superiority” claims, and to acknowledge incomplete outcome ascertainment **matches** the caution in `MANUSCRIPT_STATE_AUDIT.md` / `manuscript_submission_v1.md` for **this** design (cross-sectional association with surgical extent choice).

4. **Journal-fit brainstorming** — Shortlist themes (e.g., specialty endocrine/surgery journals vs broader surgical oncology) are useful **only** as a starting point; word limits and policies must be confirmed from each journal’s current author instructions.

5. **Reviewer-facing themes** — Generic categories (confounding, missing data, single-database limits, multiplicity) are useful in `reviewer_attack_sheet_v1.md` **if** responses are tied to **this** folder’s methods and Ns.

---

## B. Background only (do not present as this study’s results)

1. **Listed references and narrative reviews** — Ringel et al. 2025 ATA update, large NCDB-style analyses, meta-analyses (e.g., pooled recurrence/OS/DSS summaries), and size-subgroup papers cited in the memo are **context for Introduction/Discussion** once verified; they are **not** outputs of the 2–4 cm **preoperative imaging cohort** analysis in this folder.

2. **Practice context** — Discussion of lobectomy adoption, completion thyroidectomy rates reported in **other** studies/reviews, and debates about 2–4 cm thresholds belong in **Discussion** as literature comparison only.

3. **Molecular panel performance literature** — Papers on ThyroSeq/BRAF/RAS prognostication belong in **background** or comparison to the **exploratory** n=20 concordance tables here; they do **not** substitute for this cohort’s sparse testing.

4. **Database-hardening / lakehouse narrative** — Any language about “7/7 readiness gates,” broad recurrence-date resolution fractions, NSQIP linkage at **population** scale, or cure-model (PTCM/MCM) infrastructure refers to **other** repo documentation and **must not** be pasted as if it were the methods/results of **this** extent-choice manuscript without rewriting to match `study_pipeline.py` / frozen CSVs.

---

## C. Do NOT use as quantitative source of truth

**Flag: Grok infers “the paper” from repo-wide materials.** The following numbers and endpoints **conflict** with this folder’s actual study (see `MANUSCRIPT_STATE_AUDIT.md`, `analysis_manifest.json`, `manuscript_submission_v1.md`).

| Grok / repo-wide framing | This folder’s truth (do not replace) |
|--------------------------|-------------------------------------|
| Total surgical patients **N = 10,871**; cancer subcohort **N = 4,136** | **Primary analytic N = 558**; broad nodal sensitivity **N = 635** |
| Recurrence **18.3%** (1,986/10,871); structural recurrence counts | **Primary outcome** for regression is **`initial_total`** vs lobectomy cohort; recurrence is **not** the primary endpoint in the submission v1 backbone |
| Complications **2.6%**; RLN injury **0.54%** (repo/NSQIP narrative) | Not established as v1 manuscript primary results from this folder’s cited tables |
| Molecular tested **10,025**; BRAF+ counts | **Preoperative molecular testing: 20 / 558 (3.6%)** in primary cohort |
| Cure modeling (PTCM/MCM), “high-cure DTC” emphasis | **Not** the v1 manuscript’s stated primary analytic goal |
| “88.8% recurrence dates unresolved,” RAI dose recovery caps, etc. | May appear in **other** audit docs; **do not** silently merge into this paper’s limitations without checking **this** folder’s `missingness_summary.csv` / discussion text |

**Additional rule:** Any Grok sentence that says “our cohort,” “we found,” or implies **oncologic outcome rates** from the **10,871 / 4,136** frame must be **deleted or rewritten** to match the **558 / 635** imaging-defined extent-choice analysis.

---

## D. Conflicts to resolve explicitly in prose (Grok vs this manuscript)

1. **Completion thyroidectomy** — Grok suggests discussing ~19–45% completion in the literature; **this** frozen run reports **0 / 238** by pipeline completion flags (`table7_completion_thyroidectomy.csv`). Discussion must **not** blur population review statistics with **this** cohort’s operational definition without clear wording.

2. **“Large audited cancer-eligible N=4,136”** — **Incorrect** as the claim for **this** paper; the submission package centers on **N = 558** (strict) / **635** (broad).

3. **Molecular integration as a main quantitative pillar** — Literature supports molecular prognostication in general; **here**, molecular testing is **rare** and models are **exploratory / unstable** per `MANUSCRIPT_GAP_LIST.md`.

---

*End of normalized Grok memo. Authors should verify every external citation (DOI/PMID, year, journal) independently.*
