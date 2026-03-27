# Proposal: 2–4 cm extent + molecular

Run: `.venv/bin/python studies/proposal_2to4cm_extent_molecular_20260326/study_pipeline.py`

Requires `LOCAL_DB_PATH`.

**Main manuscript (IMRAD):** `manuscript_submission_v1.md` + `abstract_structured_v1.md` + `cover_letter_v1.md`.

**Figures (canonical for submission):** `fig1_cohort_flow_publication.png` / `.pdf`, `fig2_forest_primary_publication.png` / `.pdf` — legends in **`figure_legends_v2.md`**. Legacy raster exports from the pipeline (`fig_cohort_flow.png`, `fig_forest_total_vs_lobectomy.png`, 150 DPI) are internal/reproducibility only.

**Completion thyroidectomy (ascertainment must be named):** Among **238** initial lobectomy patients in the primary analytic cohort, **0** had later completion captured by the **OED-only** operative-episode pipeline (`operative_episode_detail_v2`), **25/238 (10.5%)** had **path-synoptic definite** later completion, **26** had **any** later thyroid-related event (OED or path), and **1** remained **ambiguous**. Do **not** report “0 completion” without the **OED-only** qualifier.

- **Tabular:** `table7_completion_thyroidectomy.csv`
- **Pipeline export:** also writes `fig_completion_rates.png`, `initial_ultimate_extent_transition_counts.csv`, and `completion_cases.csv` (see note below).
- **Independent audit:** `.venv/bin/python studies/proposal_2to4cm_extent_molecular_20260326/run_completion_audit_local DuckDB.py` → `completion_audit_outputs/` (`final_verdict.md`, `candidate_completion_cases.csv`, …).

**`completion_cases.csv`:** OED completion flags for **all** patients whose **first** qualifying procedure was **hemithyroidectomy** in the surgical spine (`cohort_logic.completion_after_lobectomy` over `first_clean`), **not** restricted to primary **N = 558**. The manuscript lobectomy denominator is **238**; use `patient_level_dataset.csv`, `table7_completion_thyroidectomy.csv`, and `completion_audit_outputs/candidate_completion_cases.csv` for case-level review of that arm.
