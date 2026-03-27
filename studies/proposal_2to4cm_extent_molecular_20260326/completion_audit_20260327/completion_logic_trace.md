# completion_logic_trace — independent audit

**Audit UTC tag:** 20260327
**Token mode:** secrets.toml:LOCAL_DB_PATH
**Patient cohort:** `studies/proposal_2to4cm_extent_molecular_20260326/patient_level_dataset.csv` (primary preop 2–4 cm, strict LN exclusion)

## 1. Repository logic (verified in code)

- **Initial / first surgery:** `cohort_logic.first_qualifying_surgeries()` on `operative_episode_detail_v2`, earliest row among `hemithyroidectomy` ∪ `total_thyroidectomy`.
- **Completion after lobectomy:** `completion_after_lobectomy()` — ONLY flags when a **later** `operative_episode_detail_v2` row has `procedure_normalized == 'total_thyroidectomy'` (strict `>` on `resolved_surgery_date`). Does **not** include `completion_thyroidectomy` normalized label.
- **Ultimate total:** `initial_total OR completion_total_flag` from above.
- **Pipeline call:** `study_pipeline.py` builds `comp_df` from full OED pull + `first_clean`, merges into `patient_level_dataset.csv`.

## 2. Three-way cross-check results

- **A. Study replay vs CSV:** mismatched rows = **0** (expect 0).
- **B. OED timeline:** lobectomy patients with **any** OED row strictly after index date = **0**.
- **C. Path synoptics:** lobectomy patients with a **later** `path_synoptics` row where `completion` ∈ {yes,y} = **25**.

## 3. Root cause when path shows completion but study shows 0

`operative_episode_detail_v2` is anchored on `operative_details` (one primary row per patient in affected cases). Second surgeries appear in `path_synoptics` but often **do not** generate a second OED row, so `completion_after_lobectomy` never sees them.

## 4. Manuscript claim assessment

- Phrasing such as **“0 patients underwent completion thyroidectomy”** without qualification is **overstated** for **clinical** completion if path_synoptic `completion=yes` on a dated second procedure is accepted as evidence.
- **Under strict OED + study rule:** **0 / 238** — supported.
- **Under path_synoptic later + completion=yes:** **≥ 25 / 238** patients merit review as staged/completion procedures not visible in OED (plus possible additional ambiguous rows without `completion` filled).

## 5. Safest wording pending chart review

"Zero **pipeline-detected** completion thyroidectomies in `operative_episode_detail_v2` after index lobectomy (`table7`); separate **path_synoptic** rows suggest **additional** second-stage thyroid procedures in a subset — see independent audit bundle."

## 6. SQL

All statements logged in `studies/proposal_2to4cm_extent_molecular_20260326/completion_audit_20260327/audit_sql_executed.sql`.
