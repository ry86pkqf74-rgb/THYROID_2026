# Final static QC sweep — ETE / AJCC 8 submission package (2026-03-26)

Read-only repo sweep. No analyses rerun; CSV row checks and greps only. Scope: ETE manuscript support under `manuscripts/ete_ajcc8_202603/`, canonical analytics under `studies/proposal2_ete_staging/`, CT timing exports under `outputs/manuscript_forensics_20260318/`, forensics docs/crosswalk, and the paired **wrong-cohort** draft in `manuscripts/pool_malignancy_202603/`.

---

## 1. Files checked (evidence map)

| Path | Role | Notes |
|------|------|--------|
| `manuscripts/ete_ajcc8_202603/MANUSCRIPT_REVISION_PACKET_20260326.md` | Primary submission-facing narrative + QC tables | CT counts, PSM, interactions, N1%, AUCs aligned with frozen audit below. |
| `manuscripts/ete_ajcc8_202603/revision_rerun_20260326/README.md` | Sensitivity context | States **712** pairs, OR **1.30**, p **0.13** vs frozen **711** / **1.43** / **0.03**. |
| `manuscripts/ete_ajcc8_202603/revision_rerun_20260326/table6_propensity_matching_effect_rerun.csv` | Sensitivity numerics | `712` pairs; `OR_structural_recurrence=1.3044`; `Fisher_p=0.132`. |
| `manuscripts/ete_ajcc8_202603/revision_rerun_20260326/psm_reproduction_summary.txt` | Sensitivity | `frozen_audit_pairs_expected: 711`. |
| `studies/proposal2_ete_staging/analysis_metadata.yaml` | Canonical cohort + AUC + PSM + endpoint counts | `cohort.N_total=3278`; `724/1736/818`; `auc` block; `psm_summary` OR `1.4339`, Fisher `0.030`; interactions; `endpoint_extension.counts.structural_events=504`. |
| `studies/proposal2_ete_staging/audit_tables/table6_propensity_matching_effect.csv` | Frozen PSM effect | `711` pairs; `OR_structural_recurrence=1.4339`; `Fisher_p=0.030`. |
| `studies/proposal2_ete_staging/audit_tables/table8_interaction_tests.csv` | Frozen interactions | `mETE x age_at_surgery` … `p=0.258`; `mETE x n_positive_flag` … `p=0.006`. |
| `studies/proposal2_ete_staging/audit_tables/table6_propensity_matching_balance.csv` | Post-match SMD | `n_positive_flag` SMD after match **-0.5757** (packet rounds **−0.58**). |
| `studies/proposal2_ete_staging/audit_report.md` | Expanded cohort Table 1 + sensitivity table | N1 (any): **412 (56.9%)** / **1166 (67.2%)** / **611 (74.7%)** (line 40). Sensitivity mETE ORs: age ≥55 **0.87**, age \<55 **0.44**, etc. (lines 104–111). |
| `studies/proposal2_ete_staging/analysis_report.md` | **Classic N=596** report (distinct pipeline) | N1 (any): **82 (44.3%)** / **170 (68.3%)** / **105 (64.8%)** (lines 64–65). Ordinal `ete_micro` OR **0.42** Table 4 — **not** the expanded‑cohort OR **~0.60**. |
| `studies/proposal2_ete_staging/cox_regression_report.md` | Cox supplement | **N = 5,794** (different analytic frame than 3,278 expanded PTC file). |
| `outputs/manuscript_forensics_20260318/ct_imaging_surgery_timing.csv` | Institutional CT timing | **7702** lines = **7701** data rows + header. Provenance only; this total should **not** be quoted in the manuscript text. |
| `outputs/manuscript_forensics_20260318/ptc_ct_imaging_events.csv` | PTC CT extract | **3019** lines = **3018** rows + header; **650** patients; pathologic rows **1245**; patients with ≥1 pathologic row **331**; among pathologic rows, **508** preoperative + **73** perioperative (0–29 d) = **581** (**46.67%** of 1245); **664** ≥30 d — **matches revision packet text**. |
| `outputs/manuscript_forensics_20260318/final_manuscript_dataset_provenance.json` | Forensics linkage manifest | **Conflicts** with frozen audit on several cohort sizes (see §3). CT timing block matches 3018/650. |
| `docs/manuscript_forensics_20260318/final_metric_crosswalk.csv` | Metric ledger | MET08 records **503** pairs “reproduced” vs manuscript **711** — flagged in packet. MET06 primary ordinal OR **0.42** vs expanded **0.60** — cohort label risk. |
| `manuscripts/pool_malignancy_202603/manuscript_v1.md` | Other paper | Abstract: **6,630** patients, **1,497** pairs, Cox HR **~1.84** — **inconsistent** with ETE package. |
| `studies/proposal2_ete_staging/analysis_metadata.yaml` `outputs.figures` | Declared figure list | Lists **`fig1_ete_distribution.png`** … **`fig9_forest_expanded.png`** — **no matching `.png` files** found under `studies/proposal2_ete_staging/figures/` (only **`.html`** bundles: `fig1_ajcc_stage_distribution.html`, etc.). |
| `studies/proposal2_ete_staging/figures/*.html` | Bundled interactive figures | Large minified HTML (Plotly); not journal-ready exports without rasterization. |
| **DOCX / PDF** | — | **`*.docx`**: none in `THYROID_2026/` workspace glob. **`*.pdf`**: none under `THYROID_2026/`. **DOCX authenticity (OOXML)** could not be tested — no submission binary present in repo. |

---

## 2. Search terms — hits and interpretation

| Term | Verdict | Evidence |
|------|---------|----------|
| **701** vs **7,701** | **Risk if either appears in manuscript text** | Row count on `ct_imaging_surgery_timing.csv` supports **7,701** data rows as an internal provenance check. Literal **701** appears only as **`days_from_surgery`** in patient rows (e.g. research_id 9645), **not** as exam N. Submission text should avoid citing the total institutional CT exam count altogether. |
| **650**, **1,245**, **331**, **581**, **508**, **73** | **Consistent** with `ptc_ct_imaging_events.csv` | Validated by pandas summary on current export (§1). |
| **596** vs **589** | **Both “correct” for different statements** | **596** rows `tables/analytic_cohort.csv` / metadata `original_classic_N`; **589** = deduped classic in forensics `primary_classic_ptc` — packet: footnote **7** `research_id` collisions if citing 589. |
| **3,269** | **Canonical complete-case expanded ordinal N** | `analysis_metadata.yaml` `expanded_cohorts[0].cc_n: 3269`; packet cites **3,269** for CC ordinal. |
| **523** | **Stale / wrong context if cited as expanded ordinal N** | `final_manuscript_dataset_provenance.json` `complete_case_ordinal: 523` — **conflicts** with **3,269**; packet says do not cite 523 for expanded ordinal without definition. |
| **711** vs **712** vs **503** | **711 = frozen publishable**; **712 = rerun**; **503 = erroneous repro log** | `table6_propensity_matching_effect.csv`; `effect_rerun.csv`; `final_metric_crosswalk.csv` MET08. |
| **504** | **Structural events (expanded file)** | `analysis_metadata.yaml` `endpoint_extension.counts.structural_events: 504`. |
| **ln_ratio**, **n_positive_flag** | **Defined in packet + staging** | Ordinal CC uses **`ln_ratio`**; PSM / structural logistic uses **`n_positive_flag`** (N1 from `n_stage_ajcc8`). **Note:** `docs/.../statistical_methods_execution_report.md` text defines `n_positive_flag` as `(ln_positive > 0)` in one place — **differs** from `proposal2_endpoint_psm_strata.py` (AJCC N1). Prefer code + revision packet wording for submission. |
| **AICC** | **No matches** | No erroneous “AICC” for “AJCC” in swept paths. |
| **AJCC** | Widespread | Consistent terminology in ETE packet. |

---

## 3. Anchor checklist (requested)

| Anchor | Expected (canonical) | Found |
|--------|----------------------|-------|
| **3,278** | Expanded PTC N | `analysis_metadata.yaml` `cohort.N_total`; `analytic_cohort_expanded.csv`; packet. |
| **724 / 1,736 / 818** | ETE distribution | Same YAML `cohort`; `audit_report.md` Table 1 N row. |
| **504** | Structural endpoint events | `analysis_metadata.yaml` `endpoint_extension.counts.structural_events`. |
| **711** pairs | Frozen PSM | `table6_propensity_matching_effect.csv` line 2. |
| **OR 1.4339, p = 0.030** | Frozen PSM | Same file; packet rounds to 1.43 / 0.03. |
| **Age interaction p = 0.258** | Structural interaction | `table8_interaction_tests.csv` row `mETE x age_at_surgery`. |
| **Nodal interaction p = 0.006** | Structural interaction | `table8_interaction_tests.csv` row `mETE x n_positive_flag`. |
| **Baseline N1(any) 67.2% vs 56.9%** | Expanded cohort | `audit_report.md` Table 1 (micro vs no ETE); gross **74.7%** also present. |
| **S3 AUC 0.851 / 0.876 / 0.025** | CV AUC + Δ | `analysis_metadata.yaml` `AUC_Base_CV_mean: 0.851`, `AUC_Full_CV_mean: 0.8762`, `delta_AUC_CV: 0.0252`; packet rounds. |
| **S6 frozen cohort values** | No file `tableS6_sensitivity.csv` on disk | **`studies/manuscript_tables/tableS6_sensitivity.csv`** referenced in `docs/statistical_analysis_plan_thyroid_manuscript.md` — **path not present** in repo glob. **Functional equivalent:** `audit_report.md` **§ Sensitivity Analyses** table (Primary CC expanded 0.6, MI 0.6, age≥55 **0.87** p **0.352**, age\<55 **0.44**, etc.) matches the sensitivity narrative in the revision packet. |

---

## 4. Stale or conflicting values (submission risk)

1. **`final_manuscript_dataset_provenance.json` vs frozen audit**  
   - `complete_case_ordinal: 523` vs **`3269`** / **`3,269`** manuscript convention.  
   - `psm_matched: 1006` vs frozen **`711`** pairs.  
   - `psm_pool: 2451` vs code narrative **2460** pool (packet / metadata — pool definition drift).  
   - `primary_classic_ptc: 589` vs manuscript **596** classic rows.  
   **Risk:** any cover letter / supplement that copies this JSON without the packet’s footnotes.

2. **`final_metric_crosswalk.csv` MET08** — documents **503** pairs “reproduced” vs **711** published. Stale unless updated; already acknowledged in revision packet.

3. **`analysis_report.md` (classic) vs `audit_report.md` (expanded)**  
   - **mETE ordinal OR 0.42** (classic CC, Table 4) vs **~0.60** expanded (`audit_report.md` / metadata).  
   - Stage migration percentages differ (classic narrative **69.4%** T‑stage among mETE vs expanded **71.5%** `1241/1736`).  
   **Risk:** pasting “one Results paragraph” from `analysis_report.md` into an expanded-cohort discussion without relabeling cohort.

4. **`manuscripts/pool_malignancy_202603/manuscript_v1.md`** — entire Abstract/Results numerology (**6,630**, **1,497** pairs, HR **1.84**) conflicts with ETE packet; packet already flags as **non-source**.

5. **`analysis_metadata.yaml` `outputs.figures` PNG list** — files **not present**; repo holds **HTML** under different stem names. Metadata is **stale relative to disk**.

6. **PSM numerical instability (documented)** — frozen **711** / OR **1.43** / p **0.03** vs rerun **712** / **1.30** / **0.13**. Not a “typo” but a **transparency / interpretation** issue for submission text.

---

## 5. Suspicious or easy-to-misread wording

- **“701 CT exams”** or **“7,701 CT exams”** — do **not** use either phrasing in the manuscript. `701` is almost certainly a mistranscribed `days_from_surgery` value, while `7,701` is an internal export row count that is not manuscript-relevant.  
- **“Within 30 days of surgery”** for pathologic CTs — must keep **preop + peri (0–29 d)** semantics (packet wording).  
- **`n_positive_flag` definition** — ensure Methods match **AJCC N1** (not raw LN count positivity) to avoid reviewer contradiction with forensics prose.

---

## 6. DOCX / PDF / figure authenticity

| Asset class | Finding |
|-------------|---------|
| **DOCX** | **None in repo** — cannot verify PKZIP + `word/document.xml`, embedded media, or placeholder garbage bytes. |
| **PDF** | **None in repo** under `THYROID_2026/`. |
| **Figures** | Declared **PNG** outputs **missing**; **HTML**-only bundles present — for journals requiring TIFF/EPS/PDF figures, current tree is **incomplete** unless figures are exported elsewhere. |

---

## 7. Duplicate / parallel “truth” sources

| Pair | Issue |
|------|--------|
| `analysis_report.md` vs `audit_report.md` | Same study family, **different cohorts** (596 classic vs 3278 expanded). Both legitimate; **mis-mixing** is the hazard. |
| Frozen `audit_tables/*.csv` vs `revision_rerun_20260326/*_rerun.csv` | **Primary vs sensitivity** — must label clearly in manuscript. |
| Forensics JSON / crosswalk vs `audit_tables` | Forensics layer shows **stale** PSM and CC counts — **do not** treat as publication canon without reconciliation. |

---

## 8. Recommended patch list (ranked)

### Blocking (before journal submit)

1. **Ensure the compiled manuscript (Word/PDF) is not derived from `pool_malignancy_202603/manuscript_v1.md`** Abstract/Results — replace with ETE-specific text from the revision packet or regenerate from canonical tables only.  
2. **Figure pipeline:** produce journal-compliant figure files that match frozen runs; **do not** reference missing `fig*.png` paths in `analysis_metadata.yaml` without updating artifacts or the manifest.  
3. **Methods:** single authoritative definition of **`n_positive_flag`** (AJCC N1 per staging code); align any forensics-method boilerplate that says `ln_positive > 0`.  
4. **PSM sensitivity:** if main text reports frozen **711 / 1.43 / 0.03**, add explicit note that blind re-execution on current exports yielded **712 / 1.30 / 0.13** (already in packet / rerun folder).

### Should-fix (credibility / reviewer friction)

5. Update or **`# NOTE:`**-quarantine **`final_manuscript_dataset_provenance.json`** cohort_size fields (**523**, **1006**, **589**) if that file ships to collaborators — or regenerate from `audit_tables` + `analytic_cohort_expanded.csv` only.  
6. Refresh **`final_metric_crosswalk.csv` MET08** narrative or mark **503** as superseded to avoid internal contradiction in `docs/`.  
7. Reconcile **`analysis_metadata.yaml` `outputs.figures`** list with actual **`figures/*.html`** or exported PNGs.

### Cosmetic

8. Round consistently (**1.4339** vs **1.434** vs **1.43**; **−0.5757** vs **−0.58**) — pick one journal style.  
9. Optional footnote when citing **596** vs **589** classic N (dedup story).

---

## 9. Final recommendation

**Scope split**

| Scope | Verdict |
|--------|---------|
| **Whole `THYROID_2026` repo** (forensics JSON, crosswalk, pool draft, staging `figures/*.html`) | Still has **collateral staleness** risks documented in §4–§7 — do not cite those artifacts in the journal file without reconciliation. |
| **Canonical journal bundle** — DOCX + figures under GitHub `manuscripts/ete_ajcc8_202603/ETE_submission_package_UPDATED_3_26/` (and matching local export) | **Pass** on numeric/stale-term QC for main + supplement + front matter (**§10**). **Should-fix:** several figure files use a **`.png` extension but are JPEG-encoded** (§10.3); rename or re-export if the portal is strict. |

**Safe to submit the GitHub/Downloads bundle as-is for *content*?** **Yes**, with the **figure extension/format** caveat above.  

**Safe to treat the entire monorepo as “submission-clean”?** **No** — unrelated paths (`pool_malignancy_202603`, `final_manuscript_dataset_provenance.json`, etc.) remain **out of scope** for the journal package unless explicitly updated.

---

## 10. Addendum — GitHub + local final package verification (2026-03-27)

### 10.1 GitHub location

Public repo: [https://github.com/ry86pkqf74-rgb/THYROID_2026](https://github.com/ry86pkqf74-rgb/THYROID_2026)  

Submitted journal assets live on **`main`** under:

**`manuscripts/ete_ajcc8_202603/ETE_submission_package_UPDATED_3_26/`**

(Local export folder **`/Users/ros/Downloads/V3_CLAUDE_ETE_FINAL_3_27/`** uses a different directory name but holds the **same file names and byte sizes** as that GitHub tree.)

GitHub directory listing (via API, `main`) — **16 files**:

| File | Size (bytes) on GitHub |
|------|-------------------------|
| `00_QC_Report_FINAL.docx` | 10486 |
| `01_Title_Page.docx` | 9437 |
| `02_Manuscript_Main_Blinded.docx` | 24196 |
| `03_Cover_Letter.docx` | 10411 |
| `04_Supplementary_Materials.docx` | 11636 |
| `05_STROBE_Checklist.docx` | 11514 |
| `Figure_1_Cohort_Flow.png` … `Figure_S6_Forest_Expanded.png` | 82394 … 192107 (per-file sizes match local `ls` from prior session) |

**Byte identity check:** SHA-256 of GitHub `raw.githubusercontent.com` **`02_Manuscript_Main_Blinded.docx`** **matches** local Downloads copy (`980eb7d7b6f6baa152e11fba51a94c1ed6b2d1cc204696b51a11726d47ef1aa3`). SHA-256 of **`Figure_S3_ROC_Curves.png`** also **matches** locally (`3b99d40adbef1a62b474675fab26360b5282209ac170bf8bb4e52f6c64015acf`).

### 10.2 DOCX authenticity (OOXML)

All six `.docx` files unzip with normal OPC structure (`[Content_Types].xml`, `word/document.xml`, `_rels`, styles, etc.). **No evidence** of non-Office placeholders or HTML renamed to `.docx`.

### 10.3 Figure files — format vs extension

Decoded with Pillow: **8/10** paths ending in `.png` are **JPEG (JFIF)** images; **`Figure_S3_ROC_Curves.png`** and **`Figure_S6_Forest_Expanded.png`** are **true PNG**. **Recommendation:** rename to `.jpg` for JPEGs or re-save as PNG so extensions match encoding and journal checks pass.

### 10.4 Full stale-term sweep — final submission DOCX (local = GitHub)

Extracted plain text from all six DOCX (same contents as GitHub).

| Check | Result |
|--------|--------|
| **Wrong cohort / pool paper** (`6630`, `1497`, Cox **1.84**, etc.) in **`02_Manuscript_Main_Blinded.docx`**, **`04_Supplementary_Materials.docx`**, **`01`/`03`/`05`** | **Not found** as claims (cover/title/STROBE contain no numeric “wrong paper” anchors in spot check). |
| **`00_QC_Report_FINAL.docx`** hits on `6630`, `1497`, `AICC` | **False positive:** text is the QC **“Searched all documents for: … 6630, 1497, … AICC … All absent.”** ledger — not Results numerology. |
| **Suspicious “701 CT exams”** phrasing | **Not found.** |
| **Main manuscript** anchors | **Present:** `3,278`, `711`, `504`, OR **1.43**, **p = 0.030**, **p = 0.258**, **p = 0.006**, **56.9%** / **67.2%** / **74.7%**, **1,736** mETE, **596** classic subgroup, etc. (extracted-body spot check). |
| **OR 0.42** (classic-only ordinal) in main | **0** occurrences; **0.60** appears (**8×**) — consistent with **expanded** primary story, not accidental paste from `analysis_report.md`. |
| **Supplement** | Contains **712** vs **711** sensitivity wording (**expected** transparency). |

**Conclusion (§10):** The **final submission documents** in GitHub/local bundle are **aligned with the frozen ETE story** on automated text checks; the only systematic **non-content** issue flagged is **mislabeled JPEG-as-PNG** figure files.

---

*Sweep completed: static read + CSV line/schema checks only; addendum adds DOCX text extraction, GitHub API listing, and SHA-256 spot checks. No statistical models re-fit.*
