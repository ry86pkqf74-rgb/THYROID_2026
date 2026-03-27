# local DuckDB schema profile — lobectomy vs total (2026-03-25)

Live queries against `thyroid_master.duckdb` (prod).

## Key tables

| Table | Rows | Notes |
|-------|------:|-------|
| `patient_analysis_resolved_v1` | 10,871 | Spine: `surg_first_date`, `first_surgery_date` disagree for ~2.3k rows — use `COALESCE(surg_first_date, first_surgery_date)` |
| `operative_episode_detail_v2` | 9,371 | ~1 row/patient; only **3** patients have 2 rows — completion **cannot** be derived from OED for most |
| `imaging_nodule_master_v1` | 19,891 | Size = `max_dimension_cm` (not `tumor_size_cm`); 2,394 rows with 2–4 cm |
| `molecular_test_episode_v2` | 10,126 | `overall_result_class`: mostly `other`; actionable: `suspicious`, `negative`, `positive`, … |
| `molecular_test_episode_v2` (ThyroSeq+Afirma) | 859 | Preop tests (`test_date < surg_first_date`): **189** |

## `overall_result_class` (molecular)

`other` (9,807), `suspicious` (253), `negative` (47), `non_diagnostic` (15), `positive` (3), `cancelled` (1).

**Genetics “positive/high-risk” proxy:** `IN ('suspicious','positive')` **OR** `high_risk_marker_flag` (after safe bool cast).

## `procedure_normalized` (first OED row)

`total_thyroidectomy` (4,561), `hemithyroidectomy` (3,810), `unknown` (644), `other` (356).

Matches `surg_total_thyroidectomy` / `surg_hemithyroidectomy` flags exactly for TT/hemi rows.

## Completion thyroidectomy signal

- `operative_episode_detail_v2`: essentially no multi-episode coverage.
- `tumor_episode_master_v2` text patterns (first lobectomy-like **not** containing `total`, second containing `completion` or `total thyroid`): **7** patient matches — **report as severe under-ascertainment**.

## Imaging preop N0 rule

- `ct_imaging.pathologic_lymph_nodes`: BOOLEAN with ~1.9k `TRUE` rows.
- Preop filter: `TRY_CAST(date_of_exam AS DATE) <= surgery_anchor` AND `pathologic_lymph_nodes IS TRUE` → exclude.
- `mri_imaging`: same pattern (715 rows total).
- Patients **without** preop CT/MRI remain eligible (lack of evidence ≠ negative) — limitation.

## Distant disease exclusions

`path_m_stage_raw` in `{M1, m1, 1}` and `histology_final` ILIKE `%metastatic%` (case variants) used as flags.
