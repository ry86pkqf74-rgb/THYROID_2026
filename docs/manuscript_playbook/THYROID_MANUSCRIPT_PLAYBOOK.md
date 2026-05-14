# THYROID_2026 — Manuscript-Writing Playbook

> Reusable operating manual for every manuscript in the thyroid surgery registry
> program (M001–M104+). Built from the M011 "Beyond Bethesda?" build, 2026-05-14.
>
> **This file should be promoted into a Cowork skill** (suggested name
> `thyroid-manuscripts`) and committed to the `THYROID_2026` repo under
> `docs/manuscript_playbook/`. Until then it lives here as durable memory.
> A future session writing any thyroid manuscript should read this first.

---

## 0. Fast start for a new manuscript

1. Read `PROJECT_CONTEXT.md` (project ID, datasets, repo, infra) and this file.
2. Confirm the BigQuery project: **`thyroid-canonical-pub-2026`** (number 915373663815).
3. Locate the manuscript record in Airtable (base has a Manuscript table; M011 = `recY4el1867Zbopiu`). Read its aim, cohort n, variables.
4. Build in `pub_workspace` with a per-manuscript prefix (`m0XX_`). Never write to `pub_canonical`.
5. Follow the standard pipeline in §4. Reuse the M011 SQL in `m011/sql/` as templates.
6. Deliverables go to a manuscript folder; final SQL/DDL is committed to `THYROID_2026`.

---

## 1. BigQuery capability map (`thyroid-canonical-pub-2026`)

**Connector:** BigQuery MCP. `execute_sql_readonly` for SELECT, `execute_sql` for DDL/`CREATE MODEL`. Every call needs `projectId: thyroid-canonical-pub-2026`. There is **no list-projects capability** — the project ID must be known.

### Datasets
| Dataset | Role |
|---|---|
| `pub_canonical` | **Governed canonical layer — source of truth.** ~180 tables/views. |
| `pub_views_readable` | Human-readable views over the canonical layer (PascalCase names). |
| `pub_eval` | Evaluation views (QC layer). |
| `pub_workspace` | **Per-manuscript workspace** — build `m0XX_*` tables here. |
| `pub_archive` | Snapshots / pre-migration backups. |
| `pub_raw`, `pub_staging`, `pub_semantic`, `pub_signoff`, `pub_legacy_source_20260416` | Upstream / governance / legacy. Many `pub_canonical` `*_v2` objects are **views into `pub_legacy_source_20260416`**. |

### Core canonical tables (the ones manuscripts actually use)
| Domain | Table | Grain | Key columns |
|---|---|---|---|
| Patient master | `manuscript_cohort_v1` | patient | `research_id` (**INT64**), demographics, `surgery_date`/`first_surgery_date`/`surg_first_date`, `histology_final` (free text), `path_*` outcome fields, `mol_*`, `fna_bethesda_final`, `imaging_tirads_best/worst` |
| Patient master (full) | `canonical_patient_master` (2,314 cols), `canonical_patient_master_v1_9` (+ workup census, 2,375 cols) | patient | `research_id` |
| FNA cytology | `canonical_fna_events_v1` | FNA event (8,050 rows) | `research_id` (STRING), `fna_event_id` (STRING), `bethesda_final_num`, `fna_date_resolved`, `days_to_surgery`, `is_first_fna`/`is_last_fna` |
| FNA episode hub | `fna_episode_master_v2` (view) | FNA episode | `research_id`, `fna_episode_id` (**INT64**), `bethesda_category`, `linked_imaging_nodule_id`, `linked_molecular_episode_id`, `linked_surgery_episode_id` |
| FNA→Bethesda rollup | `extracted_fna_bethesda_v1` (view) | patient | `bethesda_final`, `worst_bethesda_num`, `best_bethesda_num` |
| US nodule features | `canonical_us_nodule_v2` | nodule-exam (37,579) | `research_id`, `us_exam_id` (hash), `exam_date`, `nodule_index_within_exam`, `nodule_id` (hash), composition/echogenicity/shape/margins/calcifications/`ete_on_us_presence_simple`, `size_cm_max` |
| US nodule TI-RADS (all systems) | `canonical_us_nodule_tirads_multisystem_v1` | nodule-exam (37,579) | same keys + `acr2017_category_imputed`/`_strict`, `acr2017_total_pts_*`, `eutirads_category`, `ata_pattern`, `ktirads_category`, `ctirads_category`, `park_x1..x12` (**clean booleans** for individual features), Kwak/BTA/AACE/Horvath/SRU |
| US↔FNA linkage | `imaging_fna_linkage_v3` | nodule↔FNA pair (9,911) | `research_id`, `nodule_id` (legacy `RID-US-exam-idx` format!), `fna_episode_id` (INT64), `linkage_confidence_tier` (exact_match/high_confidence/plausible/weak/unlinked), `analysis_eligible_link_flag`, `img_date`, `fna_date`, `day_gap` |
| Legacy nodule long | `imaging_nodule_long_v2` (view) | nodule-exam (19,891) | `nodule_id` (legacy format — **bridges to linkage**), `research_id`, `resolved_exam_date`, `nodule_index_within_exam`, own `linked_fna_episode_id` |
| Molecular | `molecular_test_episode_v2` (view) | molecular episode (10,126; only ~859 real Afirma/ThyroSeq) | `research_id`, `platform` (Afirma/ThyroSeq/**Other**=not-tested placeholder), `overall_result_class`, `braf_flag`/`ras_flag`/`tert_flag`/`ret_*`/`ntrk_flag`/`high_risk_marker_flag`/`inadequate_flag`, `test_date_native`, `linked_fna_episode_id` (**within-patient ordinal, NOT a global key**) |
| Molecular rollup | `canonical_molecular_tested_v1` (view) | patient | `molecular_tested_confirmed`, `has_afirma`/`has_thyroseq`, `braf/ras/tert_positive_canonical`, `molecular_risk_tier` |
| Operative | `canonical_operative_events_v1` | surgery episode (11,773) | `research_id`, `surgery_episode_id`, `resolved_surgery_date`, `procedure_normalized`, neck-dissection flags |
| Malignant pathology | `canonical_path_malignant_events_v2` | tumor event (6,469) | `research_id`, `primary_histology`, `size_greatest_dimension_cm`, ETE/LVI/VI/margin/nodal fields, AJCC staging, `linkage_confidence_tier` |
| Tumor characteristics | `canonical_tumor_characteristics_v1` | tumor (11,103) | `research_id` (INT64), per-tumor size/stage/invasion/multifocality |
| Diagnosis unified | `canonical_diagnosis_unified_v1` (view) | patient | `diagnosis_primary`, `is_malignant` |
| Path outcome class | `path_outcome_classification_v1` (view) | patient | `current_fna_path_outcome`, `bethesda_final`, `tumor_1_histologic_type` |
| Histology lookup | `canonical_histology_lookup_v1` | 38 rows | `histology_final_raw` → `histology_group` / `histology_group_semantic` |
| Data dictionary | `data_dictionary_v279` / `pub_views_readable.Data_Dictionary` | — | field-level documentation |

### BigQuery ML
- `CREATE MODEL ... OPTIONS(model_type='LOGISTIC_REG', input_label_cols=['label'], data_split_method='NO_SPLIT')` works well for the standard manuscript logistic models. Categorical predictors as STRING are auto one-hot encoded.
- `ML.PREDICT` returns `predicted_label_probs` (array of `{label, prob}`) — extract with `(SELECT p.prob FROM UNNEST(predicted_label_probs) p WHERE p.label=1)`.
- **Do not trust `ML.EVALUATE` `roc_auc`** for single-categorical models — it gave nonsensical values (<0.5) in M011. Compute AUC yourself from predictions with the Mann-Whitney rank statistic (average-rank tie handling); see `m011_models.sql` step 6e.
- BQML can also do `CREATE MODEL` BOOSTED_TREE / DNN, `AI.FORECAST`, and `ML.GENERATE_TEXT` — not needed for standard manuscripts (avoid ML-heavy designs unless the manuscript is explicitly about it).
- What BQML/SQL **cannot** do: bootstrap CIs, DeLong paired tests, logistic calibration slope/intercept, decision-curve bootstrap bands → do these in R (`pROC`, `rms`, `dcurves`) or Python (`statsmodels`, `sklearn`). Templates in `m011/scripts/`.

---

## 2. Join keys, bridges, and ID conventions

- **`research_id` is INT64 in `manuscript_cohort_v1`, `canonical_tumor_characteristics_v1`** but **STRING almost everywhere else**. Always `CAST(research_id AS STRING)` when joining the patient master to nodule/FNA/US tables. Values match exactly after cast.
- **`nodule_id` has two incompatible forms:** `imaging_fna_linkage_v3` and `imaging_nodule_long_v2` use a legacy string `"{research_id}-US-{exam}-{idx}"`; `canonical_us_nodule_v2` and `canonical_us_nodule_tirads_multisystem_v1` use an MD5-style hash. **Bridge** legacy→hash via `imaging_nodule_long_v2` joined to the canonical US tables on **(`research_id`, `resolved_exam_date`=`exam_date`, `nodule_index_within_exam`)** — this matched 100% in M011.
- **`molecular_test_episode_v2.linked_fna_episode_id` is a within-patient ordinal (values 1–6), NOT a global FNA episode key.** Do **not** join it globally to `fna_episode_id`. Attach molecular at **patient level** (or, if true nodule-level molecular linkage is required, reconstruct via `research_id` + per-patient FNA ordering — fragile, document it).
- `molecular_test_episode_v2` / `canonical_molecular_tested_v1`: `platform='Other'` and `overall_result_class='other'` are **not-tested placeholders**. Real molecular testing = `platform IN ('Afirma','ThyroSeq')` (~859 episodes program-wide).
- Linkage confidence tiers (`imaging_fna_linkage_v3`): `exact_match` > `high_confidence` > `plausible` > `weak` > `unlinked`. "High-confidence nodule-linked frame" = exact/high/plausible.

---

## 3. Known data caveats (carry into every Limitations section)
- `surg_first_date` and `surgery_date` are identical duplicates; `first_surgery_date` is most complete but diverges in 171 patients (some corrupt dates, max gap 21,550 days). Use `COALESCE(surgery_date, first_surgery_date, surg_first_date)`.
- Lymph-node fields: 51 raw-vs-final disagreements + 38 impossible rows (Linear THY-87, THY-89 pending).
- `manuscript_cohort_v1.histology_final` is **free text** with whitespace/case variants — normalise with `LOWER(TRIM(REGEXP_REPLACE(...)))` and a CASE classifier; have a pathologist review.
- `acr_strict` (complete-case ACR TI-RADS) is missing ~68% of the time — use ACR-imputed as primary, ACR-strict as a complete-case sensitivity analysis.
- Daily QC pipeline `cowork_qc_daily_check` runs 06:00 CDT (checks SURG01, LN01-03, pub_eval integrity).
- This is a **surgery-enriched cohort** — never export malignancy rates to unselected outpatient nodule populations.

---

## 4. Standard manuscript pipeline (the M011 template)

1. **Locate tables** — list datasets, get schemas for the domain tables (§1).
2. **Patient base + outcomes** — start from `manuscript_cohort_v1`; classify `final_path_class` / `histology_group`; build `any_malignancy`, `clin_sig_malignancy`, NIFTP 3-way columns, incidental-PTMC flag.
3. **Predictor rollups** — patient-level preoperative rollups (Bethesda first/last/highest; max TI-RADS per system; molecular; individual US features). Always filter to **preoperative** (date ≤ surgery_date).
4. **Two analytic frames** — Frame A (high-confidence nodule-linked, via `imaging_fna_linkage_v3` + the nodule_id bridge) and Frame B (patient-level fallback). State which is primary.
5. **Cohort audit** — STARD denominators, missingness by variable, linkage-confidence tiers → one tidy `m0XX_cohort_audit` table.
6. **Modeling dataset** — complete-case flags; categorical predictors as STRING.
7. **BQML models** — sequential nested models; `NO_SPLIT`; reference models for clean ΔAUC chains.
8. **Predictions + metrics** — `ML.PREDICT` → long predictions table → rank-based AUC + Hanley-McNeil CI + Brier + calibration deciles + threshold metrics (sens/spec/PPV/NPV + DCA net benefit).
9. **Advanced stats in R/Python** — bootstrap CIs, DeLong, calibration slope/intercept, LR tests, adjusted ORs.
10. **Tables** — Table 1 characteristics, Table 2 pathology by Bethesda, Table 3 risk heat table, Table 4 model performance, Table 5 subgroup, Tables 6/7 risk groups & selection bias.
11. **Figures** — STARD flow, heat map, ROC, calibration, DCA, forest plot, combined-risk bars (SVG is fine; render from the metric tables).
12. **Export** — SQL + scripts + tables + figures + README to the manuscript folder; commit SQL/DDL to `THYROID_2026`.
13. **Sensitivity analyses** — NIFTP 3-way, clin-sig outcome, Frame A vs Frame B, first vs highest FNA, molecular test type, era split.
14. **Write to the data, not the hoped-for thesis.** Prepare two framings (positive / limits) so the paper is publishable either way.

**Reporting standards:** STARD 2015 (diagnostic accuracy), TRIPOD (prediction models), RECORD/STROBE (retrospective observational).

---

## 4b. Iterative-build safeguards — run EVERY iteration

The risk in iterative manuscript builds is not a wrong number once — it is a number that **quietly drifts between v1 and v3**. Wire these six checks into every rebuild. Reference implementation: `manuscripts/m011_beyond_bethesda_202605/sql/m011_safeguards.sql` + `m011_iteration_diff.sql`.

**Order each iteration:** snapshot → cohort-scoped QC → provenance manifest → column audit → (rebuild) → iteration diff → route findings to Linear.

1. **Snapshot before overwrite.** Before re-running the build, freeze the current locked-number tables to `pub_archive` as `m0XX_<table>_<version>_baseline_<YYYYMMDD>`. `pub_archive` convention is `<table>_pre_<reason>_<date>`. This is the safety net *and* the thing the next iteration diffs against.

2. **Iteration diff (highest payoff).** After rebuilding, diff the fresh `pub_workspace.m0XX_*` against the most recent `pub_archive` baseline and report: (a) **patients added / dropped** from the cohort, (b) **locked metric values that moved** (model AUCs, cohort-audit counts) with magnitude, (c) **which feeder table's `last_modified` changed** since the provenance manifest. (a)+(b) catch "a number drifted"; (c) catches "builder didn't retrigger" and "legacy carry-over broke" *before* a co-author sees it. Explain every ADDED/DROPPED/CHANGED row.

3. **Cohort-scoped QC.** Run the manuscript-relevant subset of `pub_signoff.qc_assertions_v1` (bethesda_enum, surgery_date_in_range, research_id uniqueness/integrity, master-join orphans) **filtered to the manuscript cohort**, plus temporal checks (FNA/US/molecular date ≤ surgery date) and domain-specific checks (LN non-negative/plausible). Materialize as `m0XX_cohort_qc`; every error-severity row must be 0 before numbers are locked. The project-wide pipeline `cowork_qc_nonblocking_pipeline_v1` runs daily at 06:00 CDT — but it is project-wide; scope it to the cohort so each build starts from a known-clean slice.

4. **Provenance manifest.** Record, per iteration, every canonical table/view that fed the numbers with its `last_modified_time` and `row_count` at build time (from each dataset's `__TABLES__`). Materialize as `m0XX_provenance_manifest`. Three months later "where did N come from" is answerable, not archaeology.

5. **Competing-source column flag.** For every column the manuscript uses, check `pub_signoff.canonical_column_verification_registry_v1` (`verification_status`) and `pub_signoff.deprecation_registry_v1`. Flag any column that is deprecated, or that touches a known open source-of-truth conflict — **surgery date (THY-87)**, **LN-positive (THY-89)**, **free-text histology**. If the manuscript leans on `first_surgery_date` or `ln_positive_final` while those issues are open, the report must say so, so a soon-to-be-deprecated column isn't baked in. Materialize as `m0XX_column_source_audit`.

6. **Route findings to Linear; verify columns with the GCloud AI agent.** When a safeguard finds a violation or a competing-source dependency, file or update a **Linear issue** (workspace `rostemp`, `THY-` prefix) — don't leave it in a comment. As an independent second pass on column choice, point the BigQuery console **custom Agent** / `AI.GENERATE` over `pub_canonical.data_dictionary_v279` at the column audit (requires a BigQuery→Vertex AI connection; otherwise the human-ratified `canonical_column_verification_registry_v1` is the authoritative check).

---

## 5. GitHub — version control

- **Repo:** `THYROID_2026` — `github.com/ry86pkqf74-rgb/THYROID_2026` (owner Logan Glosser / `ry86pkqf74-rgb`). This is the **analysis/manuscript repo** — distinct from `ROS_FLOW_2_1` (the ResearchFlow ML training fleet, unrelated).
- BigQuery DDL for the canonical layer + console artifacts lives under `docs/bigquery_studio_integration/sql/` (three `pub_eval` views, the canonical census table, `v1_9` master view, QC pipeline `.sqlx`, governance inserts).
- **Recommended layout for each manuscript** in the repo: `manuscripts/m0XX/{sql,scripts,tables,figures,README.md}` — mirror the M011 folder.
- The `gh` CLI is authenticated on the user's Mac (token in macOS keychain: `security find-generic-password -s 'gh:github.com' -w`, base64-decode the `go-keyring-base64:` value). From the sandbox, `git push` of the large repo is blocked — use the **GitHub Git Data API** (blob → tree → commit → ref) or push from the Mac directly.
- This session could not fetch the live repo (Linux sandbox down). When the sandbox is available, clone/pull `THYROID_2026`, reconcile this playbook against the actual `docs/` contents, and commit it.

---

## 6. Environment notes
- The Linux sandbox (`mcp__workspace__bash`) was **down 2026-05-14** ("no space left on device") — Python/R/`git`/`gh` unavailable. Workaround used for M011: all cohort building, modeling and metrics done in BigQuery + BigQuery ML; R/Python scripts written for the user to run when the sandbox returns. Retry the sandbox at the start of each session.
- `session_info` transcripts expose only assistant narration, **not** tool-call arguments — don't rely on them to recover IDs/paths; use this file instead.
- File tools (Read/Write/Edit) are scoped to the connected workspace folder; request the manuscript folder with `request_cowork_directory` at the start of a session.
