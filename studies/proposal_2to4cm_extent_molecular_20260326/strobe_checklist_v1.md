# STROBE checklist — observational cohort (submission v1)

STROBE = Strengthening the Reporting of Observational Studies in Epidemiology. Items adapted to this study; **NA** where not applicable or not documented in-folder.

| Item | Reported? | Where / notes |
|------|-----------|---------------|
| **Title and abstract** | Yes | `abstract_structured_v1.md`; indicates observational design and key sizing. |
| **Background / rationale** | Yes | `manuscript_submission_v1.md` Introduction. |
| **Objectives** | Yes | Associations between preoperative factors and initial extent. |
| **Study design** | Yes | Retrospective cohort; database-backed. |
| **Setting** | Partial | Integrated database; **institution name not** in study folder — **author to add**. |
| **Participants** | Yes | Eligibility in Methods; N=558 primary, 635 broad. |
| **Variables** | Yes | Outcome `initial_total`; predictors listed; definitions in `supplement_exclusions_and_definitions.csv`. |
| **Data sources / measurement** | Partial | Table/query list in `supplement_methods_v1.md`; **full data dictionary** is repo-wide, not duplicated here. |
| **Bias** | Partial | Discussion addresses confounding / selection; **no formal quantitative bias analysis** in outputs. |
| **Study size** | Yes | Manifest + flow narrative; **pathology arm N=0** stated. |
| **Quantitative variables** | Yes | Table 1, regression; continuous age, TIRADS in extended model. |
| **Statistical methods** | Yes | Methods + `supplement_methods_v1.md`. |
| **Participants / descriptive** | Yes | Results + Table 1. |
| **Main results** | Yes | aORs with CIs; univariable p-values. |
| **Other analyses** | Yes | Broad sensitivity; molecular descriptive; completion zero. |
| **Key results summary** | Yes | Abstract + Discussion (supported vs not proven). |
| **Limitations** | Yes | Discussion + `MANUSCRIPT_GAP_LIST.md`. |
| **Interpretation** | Yes | Cautious, associational. |
| **Generalizability** | Partial | Single database; not externally validated — authors may add context. |
| **Funding** | **NA in folder** | **Author to supply.** |
| **Conflicts of interest** | **NA in folder** | **Author to supply.** |
| **Ethical approval** | **NA in folder** | **Author to supply** (typically retrospective DB studies still need IRB determination). |

## Flow diagram

**Yes.** **Figure 1** submit asset (`fig1_cohort_flow_publication.png`/`.pdf`) is publication-ready (`figure_legends_v2.md`). Legacy horizontal-bar export (`fig_cohort_flow.png`) is internal only. `cohort_flow.csv` remains the numeric cross-check.

## Data sharing

Tabular exports reside in this study directory; broader repo policy per `README.md` / Zenodo (repo-level, not restated as journal-specific requirement).
