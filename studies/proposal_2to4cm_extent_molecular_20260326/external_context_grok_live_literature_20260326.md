# External context — Grok literature & insertion memos (live-web pass + folder truth)

**Sources:** (1) prior normalized memo: `/Users/ros/Downloads/GROK_2_4_cm_manuscript_feedback.txt`; (2) **latest Grok feedback** pasted in authoring task, 2026-03-26.  
**Updated:** 2026-03-27 (revision 3 — dual-definition completion truth table).

**Authority:** `studies/proposal_2to4cm_extent_molecular_20260326/` is the **only** quantitative source of truth for **this** paper. Grok text must **not** override `analysis_manifest.json`, `CLAIM_SOURCE_LEDGER.md`, or CSVs. **Do not** re-use older repo-wide Grok framing (e.g., N=10,871 / N=4,136, recurrence rates, lakehouse “readiness gate” KPIs) as if they described **this** manuscript.

---

## 0. This manuscript — verified figures (copy-paste truth)

Values below match **`CLAIM_SOURCE_LEDGER.md`**, **`analysis_manifest.json`**, and **`manuscript_submission_v1.md`** (frozen v1). Use these when reconciling any external memo.

| Topic | Value |
|-------|--------|
| **Primary analytic N** | **558** |
| **Broad nodal exclusion N** | **635** |
| **Initial lobectomy / initial total (primary)** | **238** / **320** |
| **Initial total as % of primary N** | **320/558 = 57.3%** |
| **Broad cohort: initial total count / %** | **375** / **375/635 = 59.1%** |
| **Pathology-defined 2–4 cm cohort (strict LN exclusion)** | **N = 0** (`path_sensitivity_n`) |
| **Preoperative molecular tested (primary)** | **20 / 558 (3.6%)** |
| **Completion after lobectomy — OED pipeline** | **0 / 238** (ever; 30/90/365 d all **0**) |
| **Completion after lobectomy — path-synoptic definite** | **25 / 238** ever (windowed counts in **`table7`**) |
| **Bethesda category missing (primary)** | **149 / 558 (26.7%)** — preserve in Methods/limitations |
| **Primary outcome (regression)** | Binary **`initial_total`** (not recurrence/DSS) |

**Parsimonious model (N = 558):** `age_at_surgery` **aOR 0.986** (95% **CI 0.975–0.998**, **p ≈ 0.026**); `sex_f` **aOR 0.97** (ns); `bethesda_ge4` **aOR 2.74** (95% **CI 1.81–4.15**, **p ≈ 1.74 × 10⁻⁶**); `has_mol` **aOR 0.61** (ns).

**Extended model (N = 558):** **`bilateral_nodule_indicator` aOR 2.01** (95% **CI 1.28–3.13**, **p ≈ 0.0023**); **`tirads_score`** not significant (per ledger).

**Broad parsimonious (N = 635):** e.g. **`bethesda_ge4` aOR 2.77** (95% **CI 1.88–4.07**); **`age_at_surgery` aOR 0.984**/year (95% **CI 0.973–0.995**).

**Univariable (primary):** age **p ≈ 0.007**; sex **p = 1.0**; **Bethesda ≥4 p ≈ 6.0 × 10⁻⁷**; preop molecular **p = 0.66**; bilateral nodule **p = 0.048**.

**Exploratory molecular-pathology table:** **`malignant_concordance_2x2`** tp/fn/fp/tn **9 / 11 / 0 / 0**, **n = 20** (descriptive only).

**Frozen run:** `analysis_manifest.json` — `run_utc` **2026-03-26T04:38:39+00:00**, `git_sha` **2e9a787b904cc2b8cab9f94789c07f2e8cf46772**, DuckDB **v1.4.4**.

---

## A. Fourteen references (Grok list — extent choice / practice variation / STROBE)

**Editorial note:** Grok’s heading requested “strictly **2025–2026**” sources, but the list below includes **2007, 2018, 2020, 2021, 2022, 2023, 2024** items (e.g., STROBE, Dhir, Wang). Treat the set as a **starter bibliography**; **verify** each citation (journal name, year, DOI/PMID, PMC ID) in PubMed/publisher sites before insertion. Some entries use “or equivalent” wording — **resolve** to a single verified reference.

1. **Ringel MD, Sosa JA, Baloch Z, et al.** 2025 American Thyroid Association Management Guidelines for Adult Patients with Differentiated Thyroid Cancer. *Thyroid*. 2025 Aug;35(8):841-985. DOI: 10.1177/10507256251363120. PMID: 40844370. Guideline — initial surgical extent (lobectomy appropriate/option for many unilateral **>2 and ≤4 cm** cT2N0M0 cases; contralateral nodules; patient preference).

2. **von Elm E, Altman DG, Egger M, et al.** The Strengthening the Reporting of Observational Studies in Epidemiology (STROBE) statement. *Ann Intern Med*. 2007 Oct 16;147(8):573-577. DOI: 10.7326/0003-4819-147-8-200710160-00010. PMID: 17938396. Reporting guideline.

3. **Montgomery KB, et al.** Evolving variation in extent of surgery for low-risk papillary thyroid cancer in the United States. *Surgery*. 2023;174(4):828-835. DOI: 10.1016/j.surg.2023.07.001. PMID: 37550165. National practice variation, 1–4 cm context.

4. **Wang X, et al.** Risk Factors That Influence Surgical Decision-Making for Low-Risk Differentiated Thyroid Cancer Patients with Tumor Diameter 1–4 cm: A Retrospective Study. *World J Surg Oncol* (or equivalent **PMC7719324**). 2020. Predictors including size (**≥2.15 cm**) and multifocality/bilateral disease — **parallel** to bilateral **aOR 2.01** and age effects in **this** cohort (**verify** final cite).

5. **Worrall BJ, et al.** Lobectomy and completion thyroidectomy rates increase after publication of the 2015 ATA guidelines. *Thyroid*. 2023 (**PMC10305631**). Lobectomy adoption + variation in upfront total.

6. **Dhir M, et al.** Correct extent of thyroidectomy is poorly predicted preoperatively by the ATA guidelines for low and intermediate risk thyroid cancers. *Surgery*. 2018. Limits of preoperative prediction in **1–4 cm** context.

7. **Hao Q, Segel JE, Vanness DJ, Shen C, Hao J, Hollenbeak CS.** Hemithyroidectomy versus total thyroidectomy for patients with differentiated thyroid cancer: systematic review and meta-analysis. *Gland Surg*. 2025 Nov;14(11):2271-2287. PMID: 41377887. Pooled context for extent trade-offs.

8. **From Lobectomy to Completion Thyroidectomy: A Cohort Study and Systematic Review.** *Clin Endocrinol (Oxf)*. **2025/2026** (Grok: recent SR; **N = 23,899** pooled; completion **~19.2%**, up to **~45%** in 1–4 cm–focused studies). **Verify** exact volume/issue/DOI — **not** in formal `references_working` list; if used, contrast **only** with **named** ascertainment (**OED 0/238** vs **path-synoptic definite 25/238**, **`table7`**).

9. **Barbaro D, et al.** Total thyroidectomy vs. lobectomy in differentiated thyroid cancer: narrative review. *J Clin Med* (or equivalent). 2021. **Verify** journal/volume.

10. **Kim MH, et al.** Management of 2–4 cm Papillary Thyroid Carcinoma. *J Endocr Surg*. 2020. Size-specific framing.

11. **Sutton W, Crepeau PK, Canner JK, Karzai S, Segev DL, Mathur A.** Impact of the 2015 American thyroid association guidelines on treatment in older adults with low-risk, differentiated thyroid cancer. *Am J Surg*. 2022 Jul;224(1 Pt B):412-417. PMID: 35123768. Age-stratified extent variation — **parallel** to age **aOR 0.986**/year.

12. **Kiss A, Szili B, Bakos B, Ármós R, Putz Z, Árvai K, et al.** Comparison of surgical strategies in the treatment of low-risk differentiated thyroid cancer. *BMC Endocr Disord*. 2023 Jan 26;23(1):23. PMID: 36703169. Size and age effects on extent.

13. **Loderer T, Bonati E, Donato V, Viani L, Cozzani F, Del Rio P.** Malignancy risk in Bethesda class IV thyroid nodules in an iodine deficient region. *Gland Surg*. 2023 Jul;12(7):884-893. PMID: 37727346. PMCID: PMC10506119. Bethesda influence on total vs lobectomy — **parallel** to **Bethesda ≥4 aOR 2.74**.

14. **Xu J, et al.** Lobectomy sufficiency for 1–4 cm differentiated thyroid cancer. *Sci Rep*. 2024. DOI: 10.1038/s41598-024-83893-4. Size-specific context (**Grok prior pass cited oncologic endpoints** — if cited here, use **only** as background; **this** paper does **not** report recurrence/DSS as primary).

**Scope filter (user request):** No **recurrence-outcome** or **molecular-diagnostic-performance** papers beyond incidental mention in **Xu** — authors should keep **Xu** and **Hao** citations tightly scoped to **extent choice / practice context** if they wish to honor that filter.

---

## B. Draft placeholder replacements (Grok — verify guideline text against primary source)

**Introduction — guideline:**  
“According to the 2025 American Thyroid Association guidelines, for patients with unilateral intrathyroidal differentiated thyroid cancer measuring **>2 and ≤4 cm** without extrathyroidal extension or clinical nodal disease, either lobectomy or total thyroidectomy may be performed depending on disease features, presence of contralateral nodules, and patient preference (**Ringel MD et al., Thyroid 2025**).”

**Introduction — practice variation:**  
“Despite guideline support for de-escalation, significant practice variation persists in the choice of initial surgical extent for **imaging-defined 2–4 cm** nodules, with upfront total thyroidectomy rates often **exceeding 50%** in real-world cohorts (**Montgomery KB et al., Surgery 2023**; **Worrall BJ et al., 2023**).”  
*Truth check:* **this** cohort reports **57.3%** initial total (**320/558**) and **59.1%** broad (**375/635**) — compatible with “often exceeding 50%” but **do not** attribute Montgomery/Worrall denominators to **this** database.

**Discussion — suggested sentences (adapt; keep associational language):**

- “These preoperative predictors align with recent analyses showing **Bethesda category** and **bilateral/multifocal disease** as key drivers of total thyroidectomy selection (**Wang X et al., 2020**; **Kiss A et al., 2023**).”

- “**Age** remains an independent factor, consistent with national trends demonstrating lower odds of total thyroidectomy with increasing patient age (**Sutton W et al., 2022**).” *Pair with **aOR 0.986**/year **(CI 0.975–0.998)** from **this** cohort.*

- “The **low rate of molecular testing (3.6%)** and **ascertainment-dependent completion counts** reflect **selective** contemporary practice and imaging-based selection (**Loderer T et al., 2023**; external completion reviews if verified).”  
  *Truth + caveat:* **20/558** tested; **OED pipeline 0/238** vs **path-synoptic definite 25/238** per **`table7_completion_thyroidectomy.csv`**; **contrast** external pooled completion rates **without** implying **this** database captured all completions (**dual definitions** — see manuscript limitations).

- “Limitations of preoperative prediction are well documented (**Dhir M et al., 2018**), underscoring the value of this **imaging-defined** cohort approach.”

- “Findings are placed in context of the **2025 ATA update**, which continues to endorse lobectomy as an appropriate option for many low-risk **2–4 cm** cases (**Ringel MD et al., Thyroid 2025**).”

- “Persistent variation in initial extent choice highlights the ongoing need for individualized decision-making tools (**Hao Q et al., 2025** meta-analysis).”

---

## C. Journal-fit memo — three targets only (Grok: publisher sites ~March 2026)

**Verify** all limits on each journal’s **current** author instructions before submission.

### Thyroid (Liebert / ATA)

- **Fit:** Strong — ATA-aligned retrospective surgical **decision** cohorts.  
- **Word limit:** ~**3,000** (excl. abstract, refs, tables/figures/legends).  
- **Abstract:** Structured, **≤350** words.  
- **Figures/tables:** up to **10** combined.  
- **Notes:** IRB/ethics in Methods; line numbering may be requested; Vancouver style; hybrid OA.

### Head & Neck (Wiley)

- **Fit:** Strong — surgical decision / observational cohorts.  
- **Word limit:** No strict ceiling (concise; often **<4,000**).  
- **Abstract:** Structured **~150–250** words — may require **condensing** the current structured abstract.  
- **Figures/tables:** flexible (**~8** sufficient per Grok).  
- **Notes:** High-res figures; hybrid OA.

### Annals of Surgical Oncology (Springer)

- **Fit:** Strong — surgical oncology cohorts, extent-of-surgery decisions.  
- **Word limit:** **~3,000–5,000** (excl. abstract) per Grok.  
- **Abstract:** Structured **~250** words.  
- **Figures/tables:** **6–8** main (+ supplements).  
- **Notes:** Clear clinical implications; supplement for detailed methods/audit; hybrid OA.

---

## D. “Do not overclaim” memo (Grok, tailored to **this** folder)

1. Frame strictly as **preoperative predictors of initial extent choice** in an **imaging-defined 2.0–4.0 cm** index nodule cohort (**N = 558** primary); **do not** imply primary **postoperative oncologic** or **recurrence** findings from this analysis.  
2. Report **associations** only (e.g., “**Bethesda ≥4** associated with higher odds of initial total thyroidectomy [**aOR 2.74**, **95% CI 1.81–4.15**]”); avoid causal “necessity/superiority” language.  
3. **Molecular testing (3.6%)** and **completion (OED 0/238; path-synoptic definite 25/238)** are **descriptive / exploratory**; **do not** claim diagnostic performance, causal guidance value, or broad generalizability from **n = 20** concordance.  
4. State **pathology-defined sensitivity N = 0** clearly; **do not** extrapolate imaging–pathology discordance to populations beyond this run’s linkage.  
5. Emphasize **observational** design and **unmeasured confounding** (surgeon preference, counseling, indications not in tables); cite **STROBE** and preoperative-prediction limits (**Dhir 2018**).  
6. Conclusions: **practice patterns** and **alignment with 2025 ATA options** — not prescriptive “should” policy.

---

## E. Legacy pitfall table (older Grok / repo-wide memo — still forbidden)

| Wrong frame (do **not** import) | **This** paper’s truth |
|----------------------------------|-------------------------|
| N = 10,871 / 4,136 “cancer cohort” | **N = 558** (primary) / **635** (broad) |
| Recurrence %, cure models (PTCM/MCM), complication/RLN KPIs as **this** results | Outcome ** **`initial_total`** **; primary claims from logistic + Table 1 |
| Molecular tested **10,025** etc. | **20 / 558 (3.6%)** preop molecular |
| Implied oncologic superiority/equivalence | **Association** + guideline **context** only |

---

*Authors must verify every bibliographic field and quoted guideline clause against primary sources. Grok output is not a substitute for reading Ringel et al. 2025.*
