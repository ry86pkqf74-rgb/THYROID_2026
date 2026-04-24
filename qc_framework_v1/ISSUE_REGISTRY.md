# THYROID_2026 V1_0 Issue Registry

Single source of truth for every data-integrity issue confirmed against
`md:thyroid_canonical_publication_v1_0` (and its local backup
`backups/thyroid_2026_full_backup_20260422_174849.duckdb`).

Each entry has:
- **ID / Table / Column(s)** — what and where
- **Category** — normalization | linkage | rollup | dedup | derivation | temporal | rule-violation | parsing
- **Severity** — critical (drops from cohort_v2) | warning | info
- **Scope** — patient | event | schema (table-wide)
- **Detection SQL** — the query that surfaces the issue
- **Observed** — live count from the 2026-04-22 run
- **Fix** — exact, concrete steps to apply
- **Status** — pending | applied | wont-fix

---

## Summary table

| ID | Table | Severity | Scope | Observed | Short description |
|---|---|---|---|---:|---|
| LN01 | manuscript_cohort_v1 | critical | patient | 2 pts (rollup) / 0 row-level (RESOLVED 2026-04-23) | `ln_positive_final > path_ln_examined_raw` |
| LN02 | manuscript_cohort_v1 | critical | patient | 19 pts (rollup) / 28 rows (RESOLVED 2026-04-23) | `ln_positive_final > 0` with `path_ln_examined_raw = 0` |
| LN03 | manuscript_cohort_v1 | warning | patient | resolved architecturally (RESOLVED 2026-04-23) | `path_ln_positive_raw ≠ ln_positive_final` |
| LN04 | manuscript_cohort_v1 | info | patient | resolved via availability flags (RESOLVED 2026-04-23) | both LN columns null |
| REC01 | recurrence_event_clean_v1 | critical | patient | 28 pts (18 PTC) | recurrence date < first_surgery_date (per operative SoT) |
| REC02 | manuscript_cohort_v1 | warning | patient | 1,764 pts | flag=TRUE, date NULL |
| REC03 | manuscript_cohort_v1 | warning | patient | TBD | date present, flag not TRUE |
| SURG01 | manuscript_cohort_v1 | critical | patient | 171 pts | three surgery-date columns disagree |
| SURG02 | manuscript_cohort_v1 | info | patient | 8,559 pts | three surgery-date columns identical (collapse) |
| HIST01 | manuscript_cohort_v1 | warning | patient | 77 pts → 0 via view (RESOLVED 2026-04-23) | whitespace on `histology_final` |
| HIST02 | manuscript_cohort_v1 | warning | patient | 59 distinct → 19 buckets (RESOLVED 2026-04-23) | unnormalized PTC variant |
| HIST03 | manuscript_cohort_v1 | warning | patient | 181 rows flagged via `histology_metastatic_prefix_flag` (RESOLVED 2026-04-23) | `metastatic ` prefix |
| FNA01 | canonical_fna_events_v1 | critical | patient | 349 ev / 286 pts (154 PTC) | FNA after first surgery (per operative SoT, 2026-04-22 run) |
| TIR01 | canonical_us_nodule_v2 | warning | event | 0 | points↔category band mismatch (ACR 2017) |
| TIR02 | canonical_us_nodule_v2 | warning | event | 0 | concordance flag inconsistent with cat columns |
| TIR03 | canonical_us_nodule_v2 | critical | patient | 56 pts / 60 ex | multi-nodule report under-exploded |
| ETE01 | canonical_path_malignant_events_v1 | warning | event | 35 raw → 6 buckets (RESOLVED 2026-04-23) | ETE string not in controlled vocab |
| ETE02 | canonical_path_malignant_events_v1 | critical | event | 568 rows / 338 pts queued (RESOLVED 2026-04-23) | `gross_ete=1` paired with "minimal/microscopic/present_unspecified" |
| AJCC01 | canonical_path_malignant_events_v1 | warning | event | 53 pts / 55 ev | AJCC8 calc flag TRUE, N NULL |
| AJCC02 | manuscript_cohort_v1 | warning | patient | 269 pts | AJCC8 calc flag TRUE, N NULL (cohort) |
| AJCC03 | canonical_path_malignant_events_v1 | warning | event | 220 rows | AJCC7 calc flag TRUE, component NULL |
| US01 | canonical_us_nodule_v2 | warning | event | 3,657 rows | all size fields null (non-aggregate rows) |
| US02 | canonical_us_nodule_v2 | warning | event | 5,039 rows | laterality + location both null |
| US03 | canonical_us_nodule_v2 | info | event | 141 rows | aggregate rows mixed into nodule table |
| US04 | canonical_us_nodule_v2 | warning | event | ~8,000 rows | `resolution_rule='inm_v1_only'` (or NULL) AND no size/location |
| PATH01 | canonical_path_malignant_events_v1 | critical | **schema** | 3 distinct IDs / 6,689 rows | `surgery_episode_id` / `path_surgery_id` are patient-local ordinals; operative table now has 11,773 globally-unique IDs. Path must be re-keyed to global op namespace (see OP05). |
| PATH02 | canonical_path_malignant_events_v1 | warning | event | TBD | `primary_histology` non-normalized (case/whitespace/typos/prefixes) |
| PATH03 | canonical_path_malignant_events_v1 | warning | event | 62 rows / 56 pts (14 PTC) | borderline/benign histology (adenoma/UMP/FTUMP) in malignant table |
| PATH04 | canonical_path_malignant_events_v1 | warning | event | TBD | `histology_variant` non-normalized |
| PATH05 | canonical_path_malignant_events_v1 | warning | event | TBD | `margin_status` unparseable placeholders |
| PATH06 | canonical_path_malignant_events_v1 | warning | event | TBD | `lymphatic_invasion` non-normalized |
| PATH07 | canonical_path_malignant_events_v1 | warning | event | TBD | `vascular_invasion` non-normalized |
| PATH08 | canonical_path_malignant_events_v1 | warning | event | TBD | `perineural_invasion` non-normalized |
| PATH09 | canonical_path_malignant_events_v1 | warning | event | TBD | `capsular_invasion` mixes state + extent |
| PATH10 | canonical_path_malignant_events_v1 | warning | event | TBD | `extranodal_extension` state + nodal-location prose |
| PATH11 | canonical_path_malignant_events_v1 | critical | event | 47 rows | nodal positive > 0, denominator 0 or NULL |
| PATH12 | canonical_path_malignant_events_v1 | warning | event | 106 rows | `size_greatest_dimension_cm > tumor_size_cm_per_surgery` |
| PATH13 | canonical_path_malignant_events_v1 | warning | event | 15 grp / 30 rows | duplicate tumor-event rows |
| PATH14 | canonical_path_malignant_events_v1 | critical | **schema** | 1,666 pts vs 0 | `number_of_tumors` and `multifocality_flag` uniformly broken (RESOLVED 2026-04-22 — rebuilt at episode grain with focality + episode_laterality split) |
| PATH15 | canonical_path_malignant_events_v1 | warning | event | 3,026 rows | `specimen_focus_id` / linkage_confidence / linkage_score NULL (DEMOTED 2026-04-22 — downstream has no hard dependency) |
| PATH16 | canonical_path_malignant_events_v1 | warning | event | 1,434 rows | `resolution_rule='STL_only'` marks the weak-linkage pathway |
| PATH17 | canonical_path_malignant_events_v1 | critical | event | 663 rows / 556 pts (461 PTC) | direct laterality ↔ site contradictions (updated: broader detection includes bilateral-without-bilateral-token cases) |
| PATH18 | canonical_path_malignant_events_v1 | warning | event | 3,153 rows | `bilateral` laterality with single-site label (grain mismatch) |
| PATH19 | canonical_path_malignant_events_v1 | warning | event | 300 rows | `metastatic`/`recurrent` prefix in `primary_histology` (rolled into PATH02) |
| PATH20 | canonical_path_malignant_events_v1 | critical | **schema** | 3,152 → 207 (RESOLVED 2026-04-22) | `discordance_t_stage_flag` rebuilt AJCC8-correct in `manuscript_workspace.path_event_discordance_v1` |
| PATH21 | canonical_path_malignant_events_v1 | warning | **schema** | 0 → 219 (RESOLVED 2026-04-22) | `discordance_laterality_flag` rebuilt via laterality↔site contradiction rule |
| USGLAND01 | canonical_us_thyroid_gland_v2 | warning | event | 6,785 / 13,578 | shell rows with no gland measurements |
| USGLAND02 | canonical_us_thyroid_gland_v2 | critical | **schema** | 13,578 / 13,578 | all parenchymal-phenotype fields uniformly NULL |
| USLN01 | canonical_us_lymph_node_v2 | critical | **schema** | 6,801 / 6,801 | table is entirely shell rows |
| CT01 | ct_imaging | warning | event | 975 rows | LN mentioned, no location/details |
| CT02 | ct_imaging | warning | event | 170 rows | `thyroid_not_visualized=TRUE` with other thyroid flags |
| CT03 | ct_imaging | warning | event | 23 rows | `thyroid_normal=TRUE` with abnormality flags |
| CT04 | ct_imaging | warning | event | 5,233 rows | `tracheal_deviation` present, direction NULL |
| MRI01 | mri_imaging | critical | event | 45 rows | explicit API/parse errors |
| MRI02 | mri_imaging | warning | event | 71 rows | LN mentioned, no location/details |
| MRI03 | mri_imaging | warning | event | 5 rows | `thyroid_normal=1` with abnormality flags |
| IEM01 | imaging_exam_master_v1 | critical | event | 2,050 rows | `exam_date` NULL |
| IEM02 | imaging_exam_master_v1 | warning | event | 7,319 rows | `largest_nodule_cm` NULL despite canonical sizes |
| IEM03 | imaging_exam_master_v1 | warning | event | 19 exams | `n_nodules` disagrees with canonical |
| IEM04 | imaging_exam_master_v1 | warning | event | 7 exams | `largest_nodule_cm` disagrees with canonical > 0.1 cm |
| IEM05 | imaging_exam_master_v1 | warning | event | 2,506 src rows | `raw_us_tirads_scored_v1` source overcounts nodules |
| IPS01 | imaging_patient_summary_v1 | info | schema | 0 | **CLEAN** — passes all integrity checks |
| NM01 | nuclear_med | critical | event | 1,364 rows | `scandate` unparseable |
| NM02 | nuclear_med | critical | **schema** | 2,220 / 2,220 | `scan_present` 100% non-standard — inspect first |
| NM03 | nuclear_med | warning | event | 522 rows | no findings_text and no impression_text |
| NM04 | nuclear_med | warning | event | 64 / 110 rows | `scantype` / `radiotracer` NULL |
| GEN01 | canonical_molecular_genetics_v2 | critical | **schema** | 3 IDs → 1,383 UIDs / 1,384 rows (RESOLVED 2026-04-23) | `molecular_episode_id` collapsed to ordinals |
| GEN02 | canonical_molecular_genetics_v2 | warning | event | ThyroSeq 264 / Afirma 60 (324 rows, stricter regex) | `platform_version` NULL while raw signal carries version (broader 422-row count used broader regex) |
| GEN03 | canonical_molecular_genetics_v2 | warning | event | TBD | `parse_status` sparse / inconsistent |
| GEN04 | canonical_molecular_genetics_v2 | warning | event | TBD | `overall_result_class` dominated by NULL/other |
| GEN05 | canonical_molecular_genetics_v2 | warning | **schema** | 1,384 / 1,384 | `molecular_confidence` 100% NULL |
| GEN06 | canonical_molecular_genetics_v2 | warning | event | ~65% NULL | `resolved_test_date` missing |
| GEN07 | canonical_molecular_genetics_v2 | warning | event | TBD | `risk_of_malignancy_pct` out of 0–100 range |
| GEN08 | canonical_molecular_genetics_v2 | critical | event | 496 rows / 465 pts (172 PTC) | `fusion_flag=TRUE` but `gene_fusions_list` empty/NULL (direct row-level check, not unnest-view) |
| GEN09 | specimen_genomic_assay_v1 | critical | **schema** | 98% broken → research_id-only binding (RESOLVED 2026-04-23) | linkage to specimen + molecular episode missing |
| GEN10 | canonical_molecular_genetics_from_notes_v2 | warning | **schema** | table-wide (RESOLVED 2026-04-23) | rename to `molecular_mentions_*` to disambiguate from structured layer |
| GEN11 | canonical_molecular_genetics_v2 | warning | event | mostly NULL | `specimen_adequacy` not populated |
| GEN12 | canonical_molecular_genetics_v2 | warning | event | TBD | `mutation_status` / `fusion_status` / `cna_status` / `gep_status` non-normalized |
| FNA02 | canonical_fna_events_v1 | warning | event | 1,516 rows / 1,141 pts (464 PTC) | `fna_date_raw` present but `fna_date_resolved` NULL |
| FNA03 | canonical_fna_events_v1 | warning | event | 280 rows / 222 pts (115 PTC) | `days_to_surgery < 0` |
| FNA04 | canonical_fna_events_v1 | info | event | 2 excess rows / 2 pts | strict duplicate-signature rows (dedup) |
| FNA05 | canonical_fna_patient_rollup_v1 | warning | patient | 6 pts (2 PTC) | rollup `bethesda_final` NULL despite non-null preop event-level Bethesda |
| IFNA01 | imaging_fna_linkage_v3 | critical | **schema** | 3,339 / 3,339 eligible | `size_score = 0.5` on 100% of eligible links (size neutralized) |
| IFNA02 | imaging_fna_linkage_v3 | critical | event | 445 links / 255 pts (97 PTC) | explicit laterality conflict with both sides populated (eligible) |
| IFNA03 | imaging_fna_linkage_v3 | critical | event | 510 links / 226 pts (109 PTC) | negative computed day-gap (img_date > fna_date) among "eligible" |
| IFNA04 | imaging_fna_linkage_v3 | warning | event | 873 FNA eps / 798 pts (299 PTC) | one FNA episode → multiple eligible nodules |
| IFNA05 | imaging_fna_linkage_v3 | warning | event | 151 nodules / 76 pts (32 PTC) | one nodule → multiple eligible FNA episodes |
| IFNA06 | imaging_fna_linkage_v3 | critical | event | 814 FNA eps / 787 pts (295 PTC) | rank-1 ties: multiple rank-1 nodules per FNA episode |
| OP01 | canonical_operative_events_v1 | warning | event | 33 rows / 33 pts (23 PTC) | `procedure_normalized='total_thyroidectomy'` with unilateral laterality |
| OP02 | canonical_operative_events_v1 | warning | event | 3 rows / 3 pts (1 PTC) | `procedure_normalized='hemithyroidectomy'` with `laterality='bilateral'` |
| OP03 | canonical_operative_procedure_codes_v1 | warning | event | 904 rows / 212 pts (81 PTC) | ambiguous multi-episode procedure-code rows |
| OP04 | canonical_operative_procedure_codes_v1 | warning | event | 11,134 rows / 3,611 pts (1,289 PTC) | procedure-code rows with NULL `linked_surgery_episode_id` |
| OP05 | canonical_path_malignant_events_v1 | critical | **schema** | 5,254 rows / 3,220 pts (2,624 PTC) | path rows with non-null `surgery_episode_id` that do NOT match any operative episode — path still carries patient-local ordinals, operative table carries global IDs |
| GEN13 | specimen_genomic_assay_v1 | critical | event | 9,267 rows / 9,250 pts (2,737 PTC) | assay rows with non-null `molecular_episode_id` fail to match any canonical_molecular_genetics_v2 row on `(research_id, molecular_episode_id)` |
| GEN14 | specimen_genomic_assay_v1 | warning | event | 311 rows / 223 pts (105 PTC) | assay `surgery_episode_id` non-null but no operative match |
| GEN15 | canonical_molecular_genetics_v2 | warning | event | 360 rows / 347 pts (122 PTC) | `linked_fna_episode_id` present but no FNA index match |
| GEN16 | canonical_molecular_genetics_v2 | critical | event | 180 rows / 175 pts (115 PTC) | `braf_flag=TRUE` but `braf_variant` NULL |
| REC04 | recurrence_event_clean_v1 | warning | event | 2 rows / 2 pts (both PTC) | `recurrence_date > last_known_alive_date` |
| REC05 | recurrence_event_clean_v1 | critical | event | 1,818 rows / 1,818 pts (1,420 PTC) | `structural_recurrence_flag=TRUE` but `recurrence_site` NULL |
| SPEC01 | specimen_master_v1 / specimen_tumor_focus_v1 / specimen_source_xref_v1 | info | schema | 0 | **CLEAN** — specimen scaffold has 10,139 / 11,103 unique IDs, 100% referential integrity to path_malignant |

---

## Detail entries

### LN01 — `ln_positive_final > path_ln_examined_raw`
- **Table/col**: `main.manuscript_cohort_v1` (`ln_positive_final`, `path_ln_examined_raw`) — deprecated; superseded by `manuscript_workspace.ln_per_patient_multisource_v1`.
- **Category**: rollup / rule-violation
- **Severity / scope**: critical / patient
- **Observed**:
  - Row-level on `canonical_path_malignant_events_v1`: **0 rows** (i.e. no single path event has `ln_involved > ln_examined`). The cohort-level violations came purely from collapsed cross-event aggregation.
  - Patient-rollup on the multisource view: **2 pts** have `SUM(ln_involved) > SUM(ln_examined)` across events.
- **Fix applied (2026-04-23, migration 11)**:
  1. Built `manuscript_workspace.ln_per_patient_multisource_v1` (grain = research_id) with per-source LN columns: `ln_path_positive` / `ln_path_examined` (SUM over path events), `ln_us_suspicious_count` (NULL — USLN01 shell), `ln_ct_{suspicious,pathologic}_count`, `ln_mri_suspicious_count`, `ln_clinical_positive_flag`, plus availability booleans per source. Multi-source, never collapsed.
  2. Queued 2 rollup-level LN01 patients into `qc_manual_review_queue_v1`.
  3. Cohort v2 reads from this view; the three deprecated cohort_v1 LN columns are `COMMENT ON COLUMN`-marked and logged in `canonical_deprecation_log_v1` under `prompt_10`.
- **Status**: RESOLVED 2026-04-23 (view `manuscript_workspace.ln_per_patient_multisource_v1`; 2 rows queued under `qc_manual_review_queue_v1`; deprecation log entry `prompt_10`).

### LN02 — `ln_positive_final > 0` with denominator 0 or NULL
- **Table/col**: `main.manuscript_cohort_v1` — deprecated; superseded by `manuscript_workspace.ln_per_patient_multisource_v1`.
- **Category**: rollup
- **Severity / scope**: critical / patient
- **Observed**:
  - Row-level on path events: **28 rows / 27 pts** have `ln_involved > 0 AND (ln_examined IS NULL OR = 0)`.
  - Patient-rollup on the multisource view: **19 pts** have `SUM(ln_involved) > 0 AND SUM(ln_examined) = 0 or NULL`.
- **Fix applied (2026-04-23, migration 11)**: Queued 19 rollup-level LN02 patients into `qc_manual_review_queue_v1` with `(ln_path_positive, ln_path_examined)` context. Staging (N1a/N1b) remains uncomputable for these patients until chart review either adds a denominator or downgrades the positive count.
- **Status**: RESOLVED 2026-04-23 (19 rows queued under `qc_manual_review_queue_v1`; deprecation log entry `prompt_10`).

### LN03 — raw vs final LN disagreement
- **Table/col**: `main.manuscript_cohort_v1` (`path_ln_positive_raw`, `ln_positive_final`) — deprecated columns.
- **Category**: rollup
- **Severity / scope**: warning / patient
- **Observed**: 51 patients (pre-resolution).
- **Fix applied (2026-04-23, migration 11)**: Architectural resolution — both columns are deprecated in favor of a single path-stream source on the multisource view (`ln_path_positive`, `ln_path_examined`). The raw/final fork no longer exists; no queue emission needed. Deprecation log entry `prompt_10` covers all three cohort_v1 LN columns.
- **Status**: RESOLVED 2026-04-23 (resolved architecturally — columns deprecated, no queue emission).

### LN04 — LN data missing for both columns
- **Severity / scope**: info / patient
- **Observed**: 2,998 patients (pre-resolution — both `ln_positive_final` and `path_ln_examined_raw` NULL on `manuscript_cohort_v1`).
- **Fix applied (2026-04-23, migration 11)**: The multisource view exposes per-source availability booleans (`ln_data_available_{path,us,ct,mri,clinical}`). Post-resolution coverage on the 7,012-patient union: path 3,999 / us 4,077 (shell only) / ct 3,086 / mri 462 / clinical 1,643. Zero patients have no LN data in any source. Downstream cohort_v2 + Methods will report per-source N with non-random-missingness language.
- **Status**: RESOLVED 2026-04-23 (resolved via per-source availability flags on `ln_per_patient_multisource_v1`).

### REC01 — recurrence before first surgery
- **Table/col**: `main.recurrence_event_clean_v1` joined to `main.manuscript_cohort_v1.first_surgery_date`
- **Category**: temporal
- **Severity / scope**: critical / patient
- **Observed**: 31 patients
- **Fix**:
  1. For each of the 31 patients, emit a row into `qc_manual_review_queue_v1` with fields `(research_id, recurrence_date, first_surgery_date, source_note_ref)`.
  2. Chart review resolves each as one of: (a) recurrence date mis-entered (fix date), (b) surgery date mis-entered (fix date), (c) this is actually a pre-op persistence/residual, not a recurrence (change event type).
  3. Block from cohort v2 until resolved.
- **Fix applied (2026-04-23, migration 21)**: Built `manuscript_workspace.recurrence_event_clean_v1_first_surg_flag` joining `main.recurrence_event_clean_v1` → MIN(`canonical_path_malignant_events_v1.surgery_date`) per patient. Two flags: `recurrence_before_first_surgery_flag` (20 events / 19 patients) and `recurrence_before_first_surgery_7d_buf_flag` (19 events, tolerates date-mismatch noise). Note: canonical_recurrence_v1 is a shell table (10,871 rows but all date fields NULL); the dated recurrence signal lives in recurrence_event_clean_v1 (1,946 rows, 182 with dates). Previous registry count "31 patients" was from a now-stale build; current figure is 20 events / 20 distinct patients.
- **Status**: RESOLVED 2026-04-23 (20 rows queued under `REC01`; deprecation log entry `prompt_20`).

### REC02 — recurrence flag without date
- **Severity / scope**: warning / patient
- **Observed**: 1,764 patients
- **Fix**: Chart review for imputation of a date where possible; where no date is recoverable, downgrade `any_recurrence_flag` to NULL and add `any_recurrence_unknown_date = TRUE`. Time-to-event analyses drop these, cumulative-incidence analyses can keep via interval-censoring.
- **Fix applied (2026-04-23, migration 22)**: View `manuscript_workspace.manuscript_cohort_v1_recurrence_clean` adds `any_recurrence_final` (TRUE/FALSE/NULL triple-state) + `recurrence_unknown_date_flag` (REC02) + `recurrence_orphan_date_flag` (REC03 guard). Time-to-event analytics filter `recurrence_unknown_date_flag=FALSE`.
- **Status**: RESOLVED 2026-04-23 (1,764 rows queued under `REC02`; deprecation log entry `prompt_21`).

### REC03 — date without flag
- **Severity / scope**: warning / patient
- **Observed**: 0 patients (guard against future regression).
- **Fix**: If `recurrence_date` is present, force `any_recurrence_flag = TRUE`. Standardize the derivation rule.
- **Fix applied (2026-04-23, migration 22)**: `recurrence_orphan_date_flag` in `manuscript_cohort_v1_recurrence_clean` fires if date populated without flag; 0 rows currently trip it.
- **Status**: RESOLVED 2026-04-23 (regression guard only — no queue emission).

### SURG01 — three surgery-date columns disagree
- **Table/col**: `main.manuscript_cohort_v1` (`first_surgery_date`, `surg_first_date`, `surgery_date`)
- **Category**: linkage / rollup
- **Severity / scope**: critical / patient
- **Observed**: 171 patients (all are sd=sfd with first_surgery_date dissenting — no true 3-way disagreement).
- **Fix**:
  1. Declare `surgery_date` column (source: `canonical_path_malignant_events_v1` / `specimen_master_v1`) the authoritative first surgery date.
  2. Overwrite `first_surgery_date := MIN(surgery_date)` per patient from the path events.
  3. Drop `surg_first_date` from cohort_v2 (deprecated).
  4. For 171 disagreement patients, recompute from path events; if still disagreement, add to `qc_manual_review_queue_v1`.
- **Fix applied (2026-04-23, migration 23)**: View `manuscript_cohort_v1_surgery_reconciled` adds `surgery_date_canonical` (consensus-of-2 rule → first_surgery_date fallback) and `surgery_date_source_rank` ∈ {all_three_agree, consensus_2of3, all_three_disagree_first_surgery_fallback, two_agree, two_disagree_first_surgery_fallback, single_only, all_null}. `surg_first_date` scheduled for DROP at prompt 46.
- **Status**: RESOLVED 2026-04-23 (171 rows queued under `SURG01`; deprecation log entry `prompt_22`).

### SURG02 — all three surgery-date columns identical
- **Severity / scope**: info / patient
- **Observed**: 8,559 patients (schema smell)
- **Fix**: Once SURG01 is fixed, drop `surg_first_date` and `surgery_date` from cohort_v1, keep only `first_surgery_date`. Zero data loss.
- **Fix applied (2026-04-23, migration 23)**: No queue emission — captured in `surgery_date_source_rank='all_three_agree'`. `surg_first_date` DROP scheduled for prompt 46.
- **Status**: RESOLVED 2026-04-23 (architecturally — covered by migration 23's surgery_date_canonical).

### HIST01 — whitespace in `histology_final`
- **Observed**: 77 patients with leading/trailing/internal whitespace variants ("PTC ", " metastatic PTC follicular", embedded newlines in "**THYROID bIOPSY\nAnaplastic carcinoma" and "MTC\nPTC mixed composit").
- **Fix**: View `manuscript_workspace.manuscript_cohort_v1_histology_clean` normalizes via `LOWER(TRIM(REGEXP_REPLACE(histology_final,'\s+',' ','g')))` before controlled-vocab mapping. Original `histology_final` preserved for audit.
- **Status**: RESOLVED 2026-04-23 (migration 08) — no queue emission (whitespace is resolved by the view, not by row-level review).

### HIST02 — unnormalized PTC variant
- **Observed**: 59 distinct `histology_final` values spanning case-inconsistency (PTC/pTC, Follicular/follicular, Anaplastic/anaplastic), abbreviations (PTC/MTC/pTC), variant free-text ("tall cell variant", "classical", "diffuse sclerosing", "follicular variant"), typos ("metastatitic", "pooly differentied", "paillary"), and mixed forms ("MTC\nPTC mixed composit", "metastatic PTC/anaplastic carcinoma", "High-grade PTC with thymic like features").
- **Fix**: View `manuscript_workspace.manuscript_cohort_v1_histology_clean` adds two columns:
  - `histology_final_clean` — controlled-vocab CASE mapping. 59 distinct → 19 buckets (papillary thyroid carcinoma 3,246; follicular thyroid carcinoma 491; medullary thyroid carcinoma 161; NIFTP 117; poorly differentiated thyroid carcinoma 38; FTUMP 34; anaplastic thyroid carcinoma 23; differentiated high grade thyroid carcinoma 10; plus 11 low-count buckets for follicular adenoma, thymic-like, NUT carcinoma, angiosarcoma, adenoid cystic, oncocytic, etc.; 6,734 NULL passthrough).
  - `histology_variant_extracted` — semicolon-joined tokens from {tall cell, columnar, diffuse sclerosing, follicular variant, solid variant, classical, oncocytic, hurthle} for the 20 rows carrying variant descriptors.
- **Status**: RESOLVED 2026-04-23 (migration 08) — no queue emission (controlled-vocab mapping IS the resolution; audit done by comparing original vs clean column).

### HIST03 — `metastatic ` prefix
- **Observed**: 179 patients with leading `metastatic ` / `Metastatic ` / `  metastatic...` prefixes, collapsing the site/context signal ("this presentation is a metastasis") into the histology string.
- **Fix**: `histology_metastatic_prefix_flag` boolean column on `manuscript_cohort_v1_histology_clean` — TRUE when normalized string starts with `metastatic `. Observed: 181 rows flagged (matches 179+2: `metastatic/recurrent PTC`×1 and leading-whitespace " metastatic PTC follicular"×1 that the prompt's literal count missed). `histology_final_clean` classifies after prefix stripping, so "metastatic PTC" and "PTC" both map to `papillary thyroid carcinoma` while the flag preserves the distinction.
- **Status**: RESOLVED 2026-04-23 (migration 08) — no queue emission (the flag is the resolution; downstream cohort rules decide whether to include metastatic-presentation rows).

### FNA01 — FNA after first surgery
- **Observed**: 349 events / 286 patients (154 PTC) — refined from earlier 211/262 estimate using operative SoT (canonical_operative_events_v1).
- **Fix**: Add `fna_context` column ∈ {`preop`, `intraop`, `postop`, `unknown`} on `canonical_fna_events_v1`. Rule: `CASE WHEN fna_date_resolved <= first_surgery_date THEN 'preop' WHEN fna_date_resolved > first_surgery_date THEN 'postop' ELSE 'unknown' END`. Preop-covariate features must filter `fna_context = 'preop'`. Do NOT drop postop FNAs from the table — they are legitimate surveillance events.
- **Fix applied (2026-04-23, migration 24)**: Built `manuscript_workspace.first_surgery_date_v1` (MIN TRY_CAST resolved_surgery_date AS DATE, per research_id) + `canonical_fna_events_v1_temporal` with `first_surgery_date`, `post_surgery_fna_flag`, `fna_pre_surgery_flag`, and `fna_temporal_status` ∈ {pre_op, post_op, no_surgery_date, no_fna_date}. Index-FNA selection downstream filters `fna_pre_surgery_flag=TRUE`.
- **Status**: RESOLVED 2026-04-23 (349 rows queued under `FNA01`; deprecation log entry `prompt_23`).

### TIR01 — points vs category band mismatch
- **Observed**: 0 rows. **No fix needed.** (Confirms the ACR 2017 bands 0=TR1, 2=TR2, 3=TR3, 4–6=TR4, 7+=TR5 are honored in the source.)
- **Fix applied (2026-04-23, migration 25)**: Regression guard view `canonical_us_nodule_v2_tirads_guard` with `tirads_band_expected` + `tirads_band_mismatch_flag`. 0 violations on 37,579 rows — guard fires if future data loads introduce band drift.
- **Status**: RESOLVED 2026-04-23 (regression guard only — no queue emission).

### TIR02 — concordance flag wrong
- **Observed**: 0 rows. **No fix needed.**
- **Fix applied (2026-04-23, migration 25)**: Same guard view carries `tirads_concordance_mismatch_flag` cross-checking `acr2017_vs_updated_concordant` against categorical equality. 0 violations on 37,579 rows.
- **Status**: RESOLVED 2026-04-23 (regression guard only — no queue emission).

### TIR03 — multi-nodule report under-exploded (parsing bug)
- **Table/col**: `canonical_us_nodule_v2`
- **Category**: parsing
- **Severity / scope**: critical / patient
- **Detection** (per exam):
  ```sql
  SELECT us_exam_id
  FROM canonical_us_nodule_v2
  WHERE NOT is_aggregate_row
  GROUP BY us_exam_id
  HAVING COUNT(*) >= 5
     AND COUNT(DISTINCT tirads_reported_in_text) >= 3
     AND COUNT(DISTINCT acr2017_tirads_category) <= 2;
  ```
- **Observed**: registry-stale 56 pts / 60 exams vs current detection **448 exams / 319 patients** on 2026-04-23 snapshot. Growth reflects canonical_us_nodule_v2 batch-3 expansion and possibly noisier inm_v1_only rows. Logan to review.
- **Fix**:
  1. Re-run the LLM-assisted nodule extraction (the `inm_v1+llm` branch, which is clean) over the 448 affected exams.
  2. Replace those exams' rows in `canonical_us_nodule_v2`.
  3. Blocked until rebuilt; patients drop from cohort_v2.
- **Candidate list built (2026-04-23, migration 26)**: `manuscript_workspace.qc_tir03_llm_candidates_v1` (TABLE, static snapshot) with 448 exams / 319 patients. LLM re-parse skeleton at `qc_framework_v1/tir03_llm_reparse.py` — NOT executed; awaits Logan authorization after candidate review.
- **Status**: candidates-built 2026-04-23 (LLM run pending authorization; deprecation log entry `prompt_25`).

### ETE01 — `extrathyroidal_extension` not in controlled vocab
- **Table/col**: `canonical_path_malignant_events_v1.extrathyroidal_extension`
- **Observed**: 35 distinct raw values across 6,689 rows / 4,138 patients. Top raws: `x` 5,069, NULL 445, `present` 365, `minimal` 292, `false` 201, `microscopic` 120, plus 29 lower-count variants including typos (`extesive`, `microscopiic`), case/whitespace drift (`Yes;`, `Yes;minimal;`), narrative strings (`present (microscopic perithyroidal soft tissue only…)`, `x\n(single microscopic focus of extension)`), and explicit-unknowns (`c/a`, `n/a`, `indeterminate`, `* (see margin comment)`).
- **Fix applied (2026-04-23, migration 10)**:
  1. Built `manuscript_workspace.canonical_path_malignant_events_v1_ete_clean` (view over raw table) with three derived columns:
     - `ete_grade` — controlled vocab `{none, minimal, microscopic, gross, extensive, present_unspecified, NULL}`.
     - `ete_grade_grouped` — collapses `{minimal, microscopic} → 'minimal_microscopic'`; other buckets pass through.
     - `ete_discordance_flag` — see ETE02.
  2. Normalize pipeline: `LOWER(TRIM(REGEXP_REPLACE(ete, '\s+', ' ', 'g')))` — collapses embedded newlines and case/whitespace drift.
  3. CASE-ordering (most-specific wins): extensive → gross → microscopic → minimal → focal(→minimal) → none → explicit-unknowns(→NULL) → yes/present/true prefix(→present_unspecified) → NULL fallback. `x (single microscopic focus of extension)` correctly reclassifies to `microscopic` via the contains-microscopic match.
  4. Raw column retained for audit; `COMMENT ON COLUMN` marks it deprecated and points to the clean view.
- **Observed post-normalization**: bucket distribution on 6,689 rows — NULL 5,583 / present_unspecified 415 / minimal 327 / none 201 / microscopic 127 / extensive 36 (and 0 gross-only, since every 'gross' string also matched an overriding branch; all gross signal rides on `gross_ete=1` and surfaces via ETE02).
- **Status**: RESOLVED 2026-04-23 (view `manuscript_workspace.canonical_path_malignant_events_v1_ete_clean`; deprecation log entry `prompt_09`).

### ETE02 — `gross_ete=1` paired with "minimal/microscopic/present_unspecified"
- **Observed**: 568 event rows / 338 patients where `gross_ete=1` AND `ete_grade IN ('minimal','microscopic','present_unspecified')` — the structured gross flag disagrees with the narrative grade.
- **Fix applied (2026-04-23, migration 10)**:
  1. Flag-only — no overwrite. The clean view exposes `ete_discordance_flag BOOLEAN` that captures the contradiction without silently collapsing one source onto the other.
  2. Queued 568 rows into `manuscript_workspace.qc_manual_review_queue_v1` under `issue_id='ETE02'` with `context_json` containing raw `extrathyroidal_extension`, derived `ete_grade`, `ete_grade_grouped`, and `gross_ete`. Composite `source_pk` = `research_id | surgery_episode_id | path_surgery_id | specimen_id | specimen_focus_id` (table has no single-column PK; NULL-safe via COALESCE). Idempotent via NOT EXISTS guard.
  3. Manual review will decide per-row: upgrade grade to `gross`/`extensive`, or downgrade the `gross_ete` flag, based on path report context.
- **Status**: RESOLVED 2026-04-23 (568 rows queued under `qc_manual_review_queue_v1`; deprecation log entry `prompt_09`).

### AJCC01 — AJCC8 calc flag TRUE with N NULL (event grain)
- **Observed**: 53 patients / 55 events (matches your manual count)
- **Fix**: Recompute `ajcc8_stage_calculable_flag := (t_stage_ajcc8 IS NOT NULL AND n_stage_ajcc8 IS NOT NULL AND m_stage_ajcc8 IS NOT NULL)`. Overwrite.
- **Fix applied (2026-04-23, migration 20)**: Built `manuscript_workspace.canonical_path_malignant_events_v1_ajcc_flag` with 4 flag columns. `ajcc8_calc_flag_inconsistent` fires on 55 rows (calc_flag=true but any of T/N/M NULL). **Issue-ID mapping in migration 20**: migration uses AJCC01=AJCC7-event, AJCC02=AJCC8-event, AJCC03=overall-missing-despite-components — i.e., registry-AJCC01 ↔ migration-AJCC02 (55 rows queued), registry-AJCC03 ↔ migration-AJCC01 (220 rows queued). Queue reflects this; numbers match registry observations.
- **Status**: RESOLVED 2026-04-23 (55 rows queued under `AJCC02` per migration 20's labeling; deprecation log entry `prompt_19`).

### AJCC02 — same at cohort grain (manuscript_cohort_v1)
- **Observed**: 269 patients
- **Fix**: Same as AJCC01, applied on `manuscript_cohort_v1.ajcc8_calculable_flag` after rebuild.
- **Status**: pending — **cohort-grain rebuild deferred**. Migration 20 handled event-grain only. Cohort-grain AJCC8 integrity will be addressed when `manuscript_cohort_v1` is rebuilt (post prompt 46 DROP/RENAME pass).

### AJCC03 — AJCC7 calc flag TRUE with component NULL (event grain)
- **Observed**: 220 rows
- **Fix**: Same pattern as AJCC01 for AJCC7 columns.
- **Fix applied (2026-04-23, migration 20)**: `ajcc7_calc_flag_inconsistent` fires on 220 rows. Per migration 20's labeling convention, these are queued under issue_id='AJCC01' (220 rows). See AJCC01 entry note above for label↔registry mapping. Additionally, migration 20 introduced **AJCC03 = overall_stage missing despite T/N/M all present**: 384 ajcc7 + 592 ajcc8 = 959 rows queued under issue_id='AJCC03' (new sub-issue beyond original registry definition).
- **Status**: RESOLVED 2026-04-23 (220 rows queued under `AJCC01`; new 959 rows queued under `AJCC03` for overall-stage-missing sub-issue; deprecation log entry `prompt_19`).

### US01 — no size on non-aggregate row
- **Table/col**: `canonical_us_nodule_v2`
- **Observed**: 3,647 rows (3,067 shell + 580 sizeless-only). Registry estimate 3,657 — within churn.
- **Fix**: Two-step.
  1. Re-run LLM extraction on source imaging notes for these rows; merge any size values recovered.
  2. Rows that remain all-size-null after re-extraction: mark `row_quality = 'incomplete'`. Analytic views filter `row_quality = 'complete'`.
- **Fix applied (2026-04-23, migration 27)**: View `canonical_us_nodule_v2_filtered` adds `us_row_type` ∈ {nodule_with_measures (31,819), nodule_sizeless (580), nodule_locationless (1,972), aggregate_row (141), shell (3,067)}. Downstream per-nodule analytics filter `us_row_type='nodule_with_measures'`. LLM re-extraction remains future work (same LLM infrastructure as TIR03).
- **Status**: RESOLVED 2026-04-23 (3,647 rows queued under `US01`; deprecation log entry `prompt_26`).

### US02 — no laterality + no location
- **Observed**: 5,039 rows (3,067 shell + 1,972 locationless-only). Exact match to registry.
- **Fix**: Same two-step pattern as US01 — LLM re-extraction, then mark incomplete.
- **Fix applied (2026-04-23, migration 27)**: Same `us_row_type` classifier handles locationless rows. Queue emission tagged `US02`.
- **Status**: RESOLVED 2026-04-23 (5,039 rows queued under `US02`; deprecation log entry `prompt_26`).

### US03 — aggregate rows mixed into nodule-grain table
- **Observed**: 141 rows (exact match to registry). `is_aggregate_row=TRUE` is the definitive signal; `nodule_index_within_exam` is never NULL in practice (placeholder logic from prompt would catch 0 rows).
- **Fix**: Split into its own table `canonical_us_exam_aggregate_v1`. Remove from `canonical_us_nodule_v2`. Any analysis needing "overall thyroid findings" joins the aggregate table; nodule-grain analyses use the cleaned nodule table.
- **Fix applied (2026-04-23, migration 27)**: View-based solution — `us_row_type='aggregate_row'` filters them out of per-nodule analytics without deleting. Split into a separate table deferred to the prompt 46 DROP/RENAME pass.
- **Status**: RESOLVED 2026-04-23 (141 rows queued under `US03`; deprecation log entry `prompt_26`; table-level split deferred).

### US04 — `inm_v1_only` rows with incompleteness
- **Observed**: `resolution_rule` distribution: inm_v1_only=32,145 + inm_v1+llm=4,812 + NULL=622 → 32,767 weak / 4,812 strong.
- **Fix**: Re-route these exams through the `inm_v1+llm` resolution (the clean branch). Rebuild `canonical_us_nodule_v2` with `resolution_rule = 'inm_v1_only'` as a fallback only when LLM returns empty.
- **Fix applied (2026-04-23, migration 28)**: Added `us_resolution_strength` ∈ {weak (inm_v1_only | NULL), strong (any _llm), other} to `canonical_us_nodule_v2_filtered`. Downstream cohort filters `us_resolution_strength='strong'` when high-confidence phenotype required. Upstream rebuild remains pending but no longer blocks the analytic layer.
- **Status**: RESOLVED 2026-04-23 (parser-provenance band; no queue emission; deprecation log entry `prompt_27`). Upstream rebuild tracked as separate future work.

### PATH01 — surgery linkage collapsed (SCHEMA-LEVEL)
- **Table/col**: `canonical_path_malignant_events_v1.surgery_episode_id`, `.path_surgery_id`
- **Category**: linkage
- **Severity / scope**: critical / **schema**
- **Detection**:
  ```sql
  SELECT COUNT(DISTINCT surgery_episode_id) FROM canonical_path_malignant_events_v1;
  -- returns 3 across 6,689 rows / 4,137 patients
  ```
- **Observed**: distribution `(1,1)`=5,100, `(NULL,NULL)`=1,434, `(2,2)`=145, `(3,3)`=7, `(1,2)`=3. These are ordinals, not IDs.
- **Fix** (this is a rebuild, not a flag):
  1. Derive canonical surgery key = `md5(research_id || '|' || CAST(surgery_date AS VARCHAR))` → new column `surgery_episode_uid` on `canonical_path_malignant_events_v1`.
  2. Cross-check: if `specimen_master_v1` carries a better linkage, prefer it and use the md5 derivation as fallback.
  3. For rows where `surgery_date` is NULL: inherit from sibling rows with the same patient where a date exists; if none, emit into manual review.
  4. Rename existing `surgery_episode_id` → `surgery_episode_ordinal_deprecated`. Document clearly.
  5. Re-derive `path_surgery_id := surgery_episode_uid` or drop entirely.
- **Status**: pending (upstream rebuild required)

### PATH02 — `primary_histology` non-normalized
- **Table/col**: `canonical_path_malignant_events_v1.primary_histology`
- **Category**: normalization
- **Observed**: case/whitespace/typo set: `PTC` 5,068, `metastatic PTC` 190, `PTC ` 88, `Metastatic PTC` 12, `pTC` 3, `metastatitic PTC` 1, `PTC classical` 4, `PTC follicular variant` 2, `PTC microcarcinoma` 6, `recurrent/metastatic PTC` 5
- **Fix**:
  1. Build `primary_histology_map(raw → canonical, is_metastatic, is_recurrent)`. Example mappings: `{PTC, Metastatic PTC, pTC, metastatitic PTC, PTC , recurrent/metastatic PTC} → canonical='PTC'`; `metastatic` flag = TRUE where applicable.
  2. Create two new columns: `primary_histology_canonical`, `is_metastatic_presentation`.
  3. Preserve raw in `primary_histology_raw`.
  4. For the multi-histology-per-patient signal (455 patients): count DISTINCT `primary_histology_canonical` per patient, not raw. Only patients with canonical count > 1 are flagged — those are real biology and not fixes.
- **Status**: pending

### PATH03 — benign / borderline / uncertain histologies in malignant table
- **Observed**: 62 rows total. Includes `follicular adenoma`, `Atypical hurthle cell neoplasm`, `Oncocytic tumor of uncertain malignant potential`, FTUMP/NIFTP-like language, and other "uncertain malignant potential" strings.
- **Detection**:
  ```sql
  SELECT COUNT(*) FROM canonical_path_malignant_events_v1
  WHERE LOWER(COALESCE(primary_histology,'')) LIKE '%adenoma%'
     OR LOWER(COALESCE(primary_histology,'')) LIKE '%uncertain malignant potential%'
     OR LOWER(COALESCE(primary_histology,'')) LIKE '%ftump%'
     OR LOWER(COALESCE(primary_histology,'')) LIKE '%niftp%';
  ```
- **Fix**: Move all 62 rows to a new table `canonical_path_nonmalignant_or_uncertain_events_v1`. Emit a per-row entry to `qc_manual_review_queue_v1` for reviewer confirmation (some may legitimately belong in the malignant table — e.g., adenoma coexisting with a missed malignant diagnosis). Do not silently delete.
- **Status**: pending

### PATH04 — `histology_variant` non-normalized
- **Observed**: typos (`microcarcioma` 5, `micorcarcinoma` 2, `tal cell variant` 3, `tall cell varaint` 4), case variants, multiline combos (`classical\ntall cell features` 39)
- **Fix**:
  1. Canonical variant vocabulary (same as HIST02).
  2. Split multiline values on `\n` → primary variant + `secondary_features` array.
  3. Typo correction via fuzzy match + manual review.
- **Fix applied (2026-04-23, migration 13)**: Built `manuscript_workspace.dim_histology_variant_v1` (15-bucket vocab) + `canonical_path_malignant_events_v1_variant_clean` view. Precedence-ordered CASE (most-specific first): diffuse_sclerosing → cribriform_morular → hobnail → columnar_cell → tall_cell → warthin_like → solid_variant → follicular_variant_infiltrative → follicular_variant_encapsulated → oncocytic → insular → microcarcinoma → classical → other → NULL. Typo branches caught hurthel/oxyphilic/folliucalr/follicualr/collumnar/classsical/microcaricnoma/microcarcinooma/cribiform. Final: microcarcinoma 2,517 / classical 1,473 / NULL 1,167 / follicular_variant_encapsulated 894 / oncocytic 291 / tall_cell 215 / insular 33 / hobnail 20 / other 17 / solid_variant 17 / diffuse_sclerosing 17 / follicular_variant_infiltrative 11 / cribriform_morular 8 / warthin_like 6 / columnar_cell 3 = 6,689.
- **Status**: RESOLVED 2026-04-23 (view-only; no queue — controlled-vocab mapping IS the resolution; 17 residual 'other' rows are genuinely unmappable anaplastic-like/poorly-differentiated/mixed-metastatic descriptors).

### PATH05 — `margin_status` unparseable
- **Observed**: `x` 5,266, `involved` 831, NULL 502, `c/a` 43, `indeterminate` 12, `X` 10, `present` 7, `0.1` 3, `negative` 3, `involvd` 1, `<1` 1, `1` 1, `n/s` 1
- **Fix applied (2026-04-23, migration 14)**: `margin_status_clean` column on `canonical_path_malignant_events_v1_invasion_clean` view. Canonical vocab {negative, positive, indeterminate, close, NULL}. `x/X/c/a/n/s` → NULL (not-assessable placeholders). Distribution: 5,279 negative / 847 positive / 502 NULL / 48 close / 13 indeterminate. Numeric strings retained in raw; per-mm split deferred.
- **Status**: RESOLVED 2026-04-23 (clean column; raw preserved; deprecation log entry `prompt_13`).

### PATH06 — `lymphatic_invasion` non-normalized
- **Observed**: `x` 3,997, `present` 1,118, `indeterminate` 93, `extensive` 92, `focal` 15; typos `indeeterminate`, `preesent`, `extensivre`
- **Fix applied (2026-04-23, migration 14)**: `lymphatic_invasion_clean` column. Collapses extent+status into single controlled label {negative, present, focal, extensive, indeterminate, NULL}; `x` → negative per Emory convention (structured 'x' sentinel = documented-absent). Distribution: 4,000 negative / 1,337 NULL / 1,123 present / 118 indeterminate / 95 extensive / 16 focal.
- **Status**: RESOLVED 2026-04-23.

### PATH07 — `vascular_invasion` non-normalized
- **Observed**: `x` 4,711, `present` 410, `focal` 325, `extensive` 242, `indeterminate` 73; typos `presnt`, `extrensive`, `foacl`, `preent`, `estensive`
- **Fix applied (2026-04-23, migration 14)**: Same pattern — `vascular_invasion_clean` column. Distribution: 4,719 negative / 883 NULL / 414 present / 333 focal / 249 extensive / 91 indeterminate.
- **Status**: RESOLVED 2026-04-23.

### PATH08 — `perineural_invasion` non-normalized
- **Observed**: NULL 4,471, `x` 2,056, `present` 153, `focal` 5
- **Fix applied (2026-04-23, migration 14)**: `perineural_invasion_clean` column. Distribution: 4,471 NULL / 2,057 negative / 153 present / 5 focal / 3 indeterminate.
- **Status**: RESOLVED 2026-04-23.

### PATH09 — `capsular_invasion` mixes state and extent
- **Observed**: `minimally invasive` 373, `present` 346, `minimal` 157, `widely invasive` 82, `no` 79, `yes` 57, `focal` 57, plus prose (`yes (minimal)`, `present, minimal`, `into but not through`, `minimally invasvie`)
- **Fix applied (2026-04-23, migration 14)**: `capsular_invasion_clean` column. Single-column controlled vocab {negative, minimally_invasive, present_nos, widely_invasive, indeterminate, other, NULL}; prose phrases routed to widely_invasive / minimally_invasive / present_nos per precedence rules. Distribution: 4,779 NULL / 662 negative / 491 widely_invasive / 430 present_nos / 259 minimally_invasive / 67 indeterminate / 1 other. Three-column split (status/extent/through) deferred — single column covers needed downstream analyses.
- **Status**: RESOLVED 2026-04-23.

### PATH10 — `extranodal_extension` state + location prose
- **Observed**: NULL 4,983, `x` 1,114, `present` 490, plus embedded nodal locations (`present\ncentral compartment`, `present\nJugular chain LNs`, `focal\nleft level 4`)
- **Fix applied (2026-04-23, migration 14)**: `extranodal_extension_clean` column. Location-prose stripped at newline; state in {negative, present, indeterminate, other, NULL}. Distribution: 4,983 NULL / 1,114 negative / 570 present / 19 indeterminate / 3 other. Location parsing to nodal-compartment vocab deferred to LN2/LN3 domain work.
- **Status**: RESOLVED 2026-04-23.

### PATH11 — path-event nodal positive without denominator
- **Detection**:
  ```sql
  SELECT COUNT(*) FROM canonical_path_malignant_events_v1
  WHERE (ln_involved > 0 AND COALESCE(ln_examined,0)=0)
     OR (nodal_disease_positive_count > 0 AND COALESCE(nodal_disease_total_count,0)=0);
  ```
- **Observed**: 28 + 19 = 47 rows
- **Fix**: Recompute denominator from `canonical_cervical_ln_clinical_events_v1` joined per `(research_id, surgery_date)`. If still zero with positive numerator, set numerator to NULL and flag `ln_qc_review = TRUE`. Critical because it invalidates nodal-count derivations.
- **Status**: pending

### PATH12 — focus size exceeds surgery size
- **Observed**: 106 rows
- **Fix applied (2026-04-23, migration 16)**: Built `manuscript_workspace.canonical_path_malignant_events_v1_size_disagreement_flag` at surgery grain. Insight: most row-level disagreements are legitimate multi-tumor (surgery size = max focus; row shows non-max focus). Real violation is at surgery grain where MAX(size_greatest_dimension_cm) disagrees with the surgery-level `tumor_size_cm_per_surgery`. Four flags: `size_single_tumor_mismatch_flag`, `size_surgery_understates_max_focus_flag`, `size_surgery_overstates_max_focus_flag`, `size_any_disagreement_flag`. 94 queue rows total (78 understates / 15 overstates / 1 single-tumor). Unit-mismatch heuristic (mm-as-cm > 20) checked but no rows affected.
- **Status**: RESOLVED 2026-04-23 (94 rows queued under `PATH12`; deprecation log entry `prompt_15`).

### PATH13 — duplicate tumor-event rows
- **Observed**: 15 duplicate groups / 30 rows
- **Fix applied (2026-04-23, migration 17)**: Built `manuscript_workspace.canonical_path_malignant_events_v1_dedup_flag` with ROW_NUMBER/COUNT window functions partitioned by `(research_id, surgery_date, tumor_ordinal, specimen_id, laterality, site)`. Dedup key tighter than prior estimate; only 3 dup groups (6 rows) survived. `dup_suppress_flag` = (size>1 AND rank>1) — downstream can filter `dup_suppress_flag = FALSE`. Tiebreaker: lowest `synoptic_row_ix`. 3 group-level queue rows under PATH13 for chart review.
- **Status**: RESOLVED 2026-04-23 (3 dup groups queued; deprecation log entry `prompt_16`; revised group count 15→3 reflects the tighter composite key).

### PATH14 — `number_of_tumors` / `multifocality_flag` uniformly broken (NEW)
- **Table/col**: `canonical_path_malignant_events_v1` (both columns)
- **Category**: derivation
- **Severity / scope**: critical / schema (column-wide build bug)
- **Detection**:
  ```sql
  WITH per_pt AS (
    SELECT research_id, COUNT(*) AS n FROM canonical_path_malignant_events_v1 GROUP BY 1
  )
  SELECT
    (SELECT COUNT(*) FROM per_pt WHERE n > 1)                                              AS pts_with_multi_rows,
    (SELECT COUNT(DISTINCT research_id) FROM canonical_path_malignant_events_v1 WHERE number_of_tumors > 1) AS pts_with_not_col_gt1,
    (SELECT COUNT(DISTINCT research_id) FROM canonical_path_malignant_events_v1 WHERE multifocality_flag = TRUE) AS pts_with_flag_true;
  ```
- **Observed**: 1,666 / 0 / 0. Both derived columns are entirely unpopulated or wrong.
- **Resolution (2026-04-22)**: Built `manuscript_workspace.path_episode_multifocality_v1`
  at (research_id, surgery_episode_uid) grain. Contract:
    `number_of_tumors`    INTEGER  (COUNT(*) per episode)
    `focality`            VARCHAR  ('unifocal' | 'multifocal')
    `episode_laterality`  VARCHAR  ('left'|'right'|'bilateral'|'isthmus'|'other'|'unknown')
  The old boolean columns (multifocality_flag, bilateral_flag) were dropped
  from the view contract — focality and episode_laterality are orthogonal
  axes that supersede them cleanly.
  Results:
    4,203 episodes total
    1,633 multifocal (1,630 distinct patients)   |   2,570 unifocal
    2,275 bilateral  (incl. 1,212 unifocal-bilateral crossing-midline cases
                      that the old bilateral_flag would have missed entirely)
    932 right | 816 left | 33 isthmus | 25 other | 122 unknown
  Reconciliation with the registry's 1,666 figure:
    1,666 patients-with->1-row = 1,630 with a multifocal episode
                               +   36 with multi-episode unifocal (staged
                                   completion surgery — clinically unifocal)
- **Status**: resolved (view in manuscript_workspace; main.* untouched)

---

## Resolved architectural decisions

1. **ETE canonical vocabulary**: `{none, minimal, microscopic, gross, extensive, NULL}`. `minimal` and `microscopic` are semantically equivalent but both are kept as distinct canonical labels (preserve whichever term was reported). Any analyses that stratify by "microscopic ETE" must group `{minimal, microscopic}` together.
2. **LN source architecture — multi-source, never collapsed.** Replace the single-column `ln_positive_final` / `path_ln_examined_raw` with per-source columns on `manuscript_cohort_v1`:
   - `ln_path_positive` / `ln_path_examined` ← `canonical_path_malignant_events_v1` (surgical path)
   - `ln_us_suspicious_count` / `ln_us_examined_count` ← `canonical_us_lymph_node_v2` (once rebuilt — currently shell; see USLN01)
   - `ln_ct_suspicious_count` / `ln_ct_pathologic_count` ← `ct_imaging`
   - `ln_mri_suspicious_count` ← `mri_imaging`
   - `ln_clinical_positive_count` ← `canonical_cervical_ln_clinical_events_v1`
   Within each source, numerator ≤ denominator must hold. Analysts choose which source to use per analysis.
3. **Surgery date source of truth**: **TBD — reconciliation required**. Plan: enumerate every surgery-date-bearing source (`canonical_path_malignant_events_v1.surgery_date`, `specimen_master_v1.surgery_date`, `manuscript_cohort_v1.{first_surgery_date, surg_first_date, surgery_date}`), cross-tabulate per patient, identify patients where they all agree (most patients per SURG02 finding), then see where disagreements localize. Declare winner based on coverage + disagreement concentration. Tracked as task SURG-SOT.
4. **TIR03 rebuild scope**: Re-run LLM extraction on the **60 flagged exams only**, not the full ~32k `inm_v1_only` rows. The TIR03 flag is narrow by design — COUNT(*)≥5 ∧ ≥3 distinct reported TIRADS ∧ ≤2 ACR categories — and only fires on exams where the parser demonstrably lost information. After that, spot-check a random 200-row sample of the remaining `inm_v1_only` rows for US01/US02 issues before deciding if broader rebuild is needed.
5. **Manual-review queue**: Create `manuscript_workspace.qc_manual_review_queue_v1` now with columns `(queue_id, issue_id, research_id, source_table, source_pk, context_json, reason, status, reviewer_notes, resolved_at)`. Rows from REC01 (31), PATH12 (106), PATH13 edge cases (2), SURG01 unresolved after rebuild, and any hand-review cases from PATH17 land here.

---

## Batch #3 — pathology linkage, US gland/LN, CT, MRI

### PATH15 — specimen-focus-level linkage missing for ~45% of rows
- **Table/col**: `canonical_path_malignant_events_v1` (`specimen_focus_id`, `linkage_confidence_tier`, `linkage_score`)
- **Category**: linkage
- **Severity / scope**: **warning** / event  *(DEMOTED 2026-04-22 from critical — see Resolution)*
- **Detection**:
  ```sql
  SELECT
    SUM(CASE WHEN specimen_focus_id IS NULL THEN 1 ELSE 0 END) AS null_focus_id,
    SUM(CASE WHEN linkage_confidence_tier IS NULL THEN 1 ELSE 0 END) AS null_confidence,
    SUM(CASE WHEN linkage_score IS NULL THEN 1 ELSE 0 END) AS null_score
  FROM canonical_path_malignant_events_v1;
  ```
- **Observed**: 3,026 rows missing all three. Correlates with PATH16 (`resolution_rule='STL_only'`).
- **Resolution (2026-04-22)**: Built `manuscript_workspace.path_focus_link_v1`
  as a single-tier (specimen_id, tumor_ordinal=tumor_index) exact join.
  Coverage: 5,097/6,689 (76.2%) `exact`; 1,592 (23.8%) `none` (tumor-ordinal
  misalignment between path and focus extractors). Did **not** queue the
  unlinked 1,592 and did **not** exclude them from cohort_v2 because
  `specimen_focus_id` provides no unique clinical signal for this manuscript:
    - every per-focus clinical field (size, invasion, margins, histology,
      laterality) is already native to `canonical_path_malignant_events_v1`
    - multifocality is derived from `COUNT(*) OVER (PARTITION BY
      research_id, surgery_episode_uid)` (prompt 03)
    - `specimen_genomic_assay_v1` has specimen_focus_id populated on
      263/10,370 rows (2.5%) — molecular data is structurally specimen-level,
      not focus-level
    - PATH13 dedup (prompt 363) uses non-NULL focus_id only as a tie-break
      preference; 5,097 populated rows is more than enough
  Methods-section note: "`specimen_focus_id` populated for 5,097/6,689
  (76.2%) of path malignant rows via unique `(specimen_id, tumor_ordinal)`
  match; remaining 1,592 retained with NULL focus_id — no downstream
  manuscript analysis depends on focus-level resolution for these rows."
- **Status**: resolved (single-tier view in manuscript_workspace; severity demoted)

### PATH16 — `resolution_rule='STL_only'` build pathway is weak
- **Table/col**: `canonical_path_malignant_events_v1.resolution_rule`
- **Category**: parsing / build
- **Severity / scope**: warning / event (source-level marker for PATH15)
- **Detection**:
  ```sql
  SELECT resolution_rule, COUNT(*) FROM canonical_path_malignant_events_v1
  GROUP BY 1 ORDER BY 2 DESC;
  -- STL+TEM: 5,255, STL_only: 1,434
  ```
- **Observed**: 1,434 `STL_only` rows drive most of the PATH01/PATH15 null-linkage rows.
- **Fix**: This isn't fixed directly — it's a **tag** pointing to the rows that PATH15's re-linkage pass must prioritize. Keep the column; downstream analyses can filter by `resolution_rule = 'STL+TEM'` when strict linkage is required.
- **Fix applied (2026-04-23, migration 18)**: Pivoted from resolution_rule to the more authoritative `linkage_confidence_tier` column. Built `manuscript_workspace.canonical_path_malignant_events_v1_weak_linkage_flag` with `path_weak_linkage_flag = (linkage_confidence_tier='weak')`. Only 9 rows at weak tier (scores 0.069-0.337); the earlier 1,434 `STL_only` count conflated build-path with linkage-confidence. exact_match(67) > high_confidence(3,460) > plausible(127) > weak(9); 3,026 NULL (linkage not assigned). Downstream can filter `linkage_confidence_tier NOT IN ('weak')` to exclude.
- **Status**: RESOLVED 2026-04-23 (9 rows queued under `PATH16`; deprecation log entry `prompt_17`).

### PATH17 — laterality vs site direct contradictions
- **Table/col**: `canonical_path_malignant_events_v1` (`laterality`, `site`)
- **Category**: normalization / rollup
- **Severity / scope**: critical / event
- **Detection**:
  ```sql
  SELECT COUNT(*) FROM canonical_path_malignant_events_v1
  WHERE (LOWER(laterality) LIKE 'left%'  AND LOWER(site) LIKE '%right lobe%')
     OR (LOWER(laterality) LIKE 'right%' AND LOWER(site) LIKE '%left lobe%')
     OR (LOWER(laterality) = 'isthmus' AND LOWER(site) LIKE '%left lobe%')
     OR (LOWER(laterality) = 'isthmus' AND LOWER(site) LIKE '%right lobe%');
  ```
- **Observed**: 115 + 101 = 216 direct conflicts (+40 isthmus-vs-lobe conflicts)
- **Fix**:
  1. For the 216 direct conflicts, declare `site` authoritative (more specific anatomy). Recompute `laterality` from `site` with the rule `{right lobe → right, left lobe → left, isthmus → isthmus, bilateral terms → bilateral}`.
  2. The 40 isthmus-vs-lobe rows go to `qc_manual_review_queue_v1` — these may be tumors crossing the isthmus boundary which is clinically meaningful.
  3. Once fixed, populate `discordance_laterality_flag = TRUE` on every corrected row so the lineage is preserved (fixes PATH22).
- **Fix applied (2026-04-23, migration 19)**: Built `manuscript_workspace.canonical_path_malignant_events_v1_laterality_clean` with independent parsers for laterality and site into `{left, right, bilateral, isthmus, extra_thyroidal, nonlobular, NULL}`. Precedence-ordered with typo branches (rigth / let lobe / isthmsu / isthus / leeft / lt lobe / LL/RL abbrevs). `derived_laterality_final` coalesces best signal. **Critical tightening**: `site_laterality_contradict_flag` fires ONLY on pure left↔right direct contradictions (not bilateral-vs-right — laterality may reflect patient-level bilateral disease while site refers to this specific tumor). 219 rows flagged (vs. initial 3,538 over-flag before tightening). Note: bilateral-vs-single-site PATH18 grain-mismatch repurposed here as "laterality field is site-prose" (`laterality_has_site_prose_flag`).
- **Status**: RESOLVED 2026-04-23 (219 rows queued under `PATH17`; deprecation log entry `prompt_18`). PATH21 (never-populated `discordance_laterality_flag`) was also resolved separately via migration 04 in prior batch with 219 TRUE — consistent count.

### PATH18 — `bilateral` laterality paired with single-site label (grain mismatch)
- **Category**: normalization / grain
- **Severity / scope**: warning / event
- **Observed**: 3,153 rows (`bilateral + right lobe` 1,446; `bilateral + left lobe` 1,368; `bilateral + isthmus` 237; plus small counts)
- **Fix**: This reflects a grain mismatch — `laterality` is patient/surgery-level ("patient has bilateral disease"), `site` is focus-level ("this particular tumor is in the right lobe"). That's legitimate biologically but creates confusion in naïve aggregations. Fix:
  1. Rename `laterality` → `surgery_laterality` (patient/surgery grain — reflects bilateral disease status).
  2. Rename `site` → `focus_site` (focus grain — this row's anatomy).
  3. Document the grain difference in column comments + cohort_v2 spec.
  4. Any analysis needing focus laterality uses `focus_site`; any analysis needing "did the patient have bilateral disease" uses `surgery_laterality`.
- **Fix applied (2026-04-23, migration 19)**: Repurposed PATH18 semantically — original "bilateral + single-site" is NOT a violation (legit grain split; bilateral = patient-level disease, site = this tumor's anatomy). The actual PATH18 violation is rows where the laterality field has been HIJACKED by site-prose (laterality_token ∈ {extra_thyroidal, nonlobular} AND site has a real side token). `laterality_has_site_prose_flag` on the clean view. 40 rows flagged. Rename to surgery_laterality/focus_site deferred to prompt 46 DROP/RENAME pass. Grain semantics now documented in column COMMENTs.
- **Status**: RESOLVED 2026-04-23 (40 rows queued under `PATH18`; deprecation log entry `prompt_18`). Semantic reframe from "bilateral+single-site grain mismatch" to "laterality field polluted by site-prose".

### PATH19 — `metastatic`/`recurrent` mixed into `primary_histology`
- **Category**: classification / normalization
- **Severity / scope**: warning / event
- **Detection**:
  ```sql
  SELECT COUNT(*) FROM canonical_path_malignant_events_v1
  WHERE LOWER(primary_histology) LIKE 'metastatic%'
     OR LOWER(primary_histology) LIKE '%recurrent%';
  ```
- **Observed**: 300 rows
- **Fix**: Covered by PATH02 — the mapping table splits out `is_metastatic_presentation` and `is_recurrent` as separate boolean columns, leaves the normalized tumor type in `primary_histology_canonical`. Count this as validation that PATH02's map must include 300 rows' worth of prefixed strings.
- **Status**: pending (rolled into PATH02)

### PATH20 — `discordance_t_stage_flag` extremely common (47%)
- **Table/col**: `canonical_path_malignant_events_v1.discordance_t_stage_flag`
- **Category**: rollup / rule-violation
- **Severity / scope**: critical / schema
- **Detection**:
  ```sql
  SELECT SUM(CASE WHEN discordance_t_stage_flag THEN 1 ELSE 0 END)::FLOAT
       / COUNT(*) AS pct
  FROM canonical_path_malignant_events_v1;
  -- 47.1%
  ```
- **Observed**: 3,152 of 6,689 rows (47.1%)
- **Fix**:
  1. Decompose `discordance_t_stage_flag` into its three constituent checks: size discordance, ETE discordance, multifocality discordance. Store as three separate flags: `discordance_t_size`, `discordance_t_ete`, `discordance_t_multifocal`.
  2. For each of the 3,152 rows, compute which of the three triggered — this tells us which upstream source (canonical_tumor_characteristics_v1, tumor_episode_master_v2, specimen_tumor_focus_v1) to trust on that dimension.
  3. Declare per-dimension source of truth based on which path is most complete across the 3,152 rows. Likely: size → specimen_tumor_focus_v1 (most granular); ETE → canonical_tumor_characteristics_v1 (closest to report text); multifocality → recount from rows after PATH01/PATH14 rebuild.
  4. Overwrite the T-stage components using the declared SoT. Recompute `t_stage_ajcc8` and `t_stage_ajcc7` from the reconciled components.
- **Status**: pending (blocked on PATH01, PATH14)

#### Resolution (2026-04-22) — migration 04
- View `manuscript_workspace.path_event_discordance_v1` rebuilds the flag AJCC8-correct against `canonical_path_malignant_events_v1_keyed`.
- AJCC8 thyroid T-stage rule unified across DTC/MTC/ATC (size + gross ETE); ETE free-text (35 variants) normalized to `{present, absent, unknown, other}` aligned with Logan's Script 390 / 392 conventions.
- Histology bucketed into `{DTC, MTC, ATC, non_staged (NIFTP/FTUMP/benign), other, unknown_histology}`; non-staged histology → `derived='not_applicable'`, flag → NULL.
- T3b vs T4a vs T4b collapsed to `indeterminate_t3b_t4a_t4b_requires_llm` when `gross_ete=TRUE` (no structured invasion-target column — see `qc_framework_v1/LLM_TODO.md` item #1).
- Discordance flag TRUE only when both sides decisive and disagree; NULL when derived is indeterminate/NA or ETE-unknown.
- Post-rebuild: **207 TRUE / 3,560 FALSE / 2,922 NULL** (from 3,152 TRUE / 511 FALSE / 3,026 NULL). 17× reduction. Top pattern: reported T3b vs derived T1a-T3a (128 rows) — path report saw ETE that the structured `extrathyroidal_extension` column missed (extraction gap, not path-report error).
- 207 rows queued to `qc_manual_review_queue_v1` under `issue_id='PATH20'`.

### PATH21 — `discordance_laterality_flag` never populated
- **Table/col**: `canonical_path_malignant_events_v1.discordance_laterality_flag`
- **Category**: derivation
- **Severity / scope**: warning / schema
- **Observed**: 0 rows TRUE despite 216 PATH17 conflicts
- **Fix**: Populate during PATH17 fix. The flag becomes TRUE on every row where the original `laterality` didn't match the `site`-derived laterality.
- **Status**: pending (rolled into PATH17)

#### Resolution (2026-04-22) — migration 04
- Rebuilt in `manuscript_workspace.path_event_discordance_v1` alongside PATH20.
- Normalization matches migration 03 pattern: lowercase, then `{left, right, bilateral, isthmus, other}` via substring matching (79 distinct `laterality` values upstream).
- `site` gets the same normalization.
- Flag TRUE only when `lat_norm ∈ {left, right}` AND `site_norm` names the opposite side (or bilateral). `isthmus` is not a side and short-circuits to NULL; bilateral laterality short-circuits to NULL (no contradiction possible with a single-side site).
- Post-rebuild: **219 TRUE / 2,432 FALSE / 4,038 NULL** — matches the PATH17 expected count (~216). Breakdown: 116 `left↔right`, 102 `right↔left`, 1 `left↔bilateral`.
- 219 rows queued to `qc_manual_review_queue_v1` under `issue_id='PATH21'`.

---

### USGLAND01 — half shell rows with no gland measurements
- **Table/col**: `main.canonical_us_thyroid_gland_v2` (all measurement fields)
- **Category**: build
- **Severity / scope**: warning / event
- **Detection**:
  ```sql
  SELECT COUNT(*) FROM canonical_us_thyroid_gland_v2
  WHERE rl_length_cm IS NULL AND rl_width_cm IS NULL AND rl_depth_cm IS NULL
    AND ll_length_cm IS NULL AND ll_width_cm IS NULL AND ll_depth_cm IS NULL
    AND isthmus_thickness_mm IS NULL;
  -- 6,785 of 13,578 (50%)
  ```
- **Observed**: 6,785 of 13,578 rows. Maps 1:1 to `source_us_nodules_tirads = TRUE` rows (these were placeholder rows created when the TIRADS-source path didn't carry measurements).
- **Fix**:
  1. Re-parse the 6,785 shell rows' source imaging notes with the gland-measurement extractor. If measurements exist in the narrative, populate.
  2. Rows where measurements genuinely don't exist in the report: keep the row (exam-linkage is useful) but add `row_quality = 'no_measurements'` for analytic filtering.
  3. Analytic views filter `row_quality = 'complete'` when gland-size covariates are needed.
- **Fix applied (2026-04-23, migration 29)**: View `canonical_us_thyroid_gland_v2_shape` adds `gland_row_type` ∈ {measured, shell} + `shell_row_flag`. Queue emission at per-patient grain: 6,785 patients have only-shell exams (each with exactly 1 gland exam).
- **Status**: RESOLVED 2026-04-23 (6,785 rows queued under `USGLAND01`; deprecation log entry `prompt_28`). LLM re-parse deferred to USGLAND02 workstream.

### USGLAND02 — all parenchymal-phenotype fields uniformly NULL
- **Table/col**: `canonical_us_thyroid_gland_v2` (`background_echogenicity`, `heterogeneity`, `hashimoto_pattern`, `vascularity_overall`, `calcifications_parenchymal`)
- **Category**: derivation (planned-but-not-executed backfill)
- **Severity / scope**: critical / schema
- **Observed**: 13,578 / 13,578 rows have all 8 phenotype fields NULL (including `goiter_flag`, `pyramidal_present_flag`, `substernal_extension_flag`). All rows have `nlp_backfill_pending = TRUE`.
- **Fix**: Execute the planned NLP backfill. The infrastructure exists (columns + flag), just the backfill hasn't been run. Write a backfill script that runs an LLM extractor over the source imaging narratives and populates these columns. Set `nlp_backfill_pending = FALSE` on success.
- **Tracked (2026-04-23, migration 29)**: `parenchymal_all_null_flag` regression guard on `canonical_us_thyroid_gland_v2_shape`. TODO + execution gate in `qc_framework_v1/NOTES/usgland_parenchymal_rebuild.md` (patch target: `canonical_us_thyroid_gland_v2_parench_patch_v1`). No queue emission — this is a schema-wide rebuild, not a row-level defect.
- **Status**: tracked 2026-04-23 — LLM rebuild script pending cost/sample sign-off.

### USLN01 — US lymph node table is entirely shell
- **Table/col**: `main.canonical_us_lymph_node_v2` (everything except `us_exam_id`)
- **Category**: build
- **Severity / scope**: critical / schema
- **Observed**: 6,801 / 6,801 rows — 6,793 all-NULL structured fields (99.9% shell). Non-shell count = 8.
- **Fix**: Full rebuild from source imaging reports.
  1. Write a new LLM extraction pipeline targeting cervical LN findings in US reports.
  2. For each exam, extract per-LN rows with fields: laterality, neck_level (I–VI), region, short_axis_mm, long_axis_mm, suspicion features (microcalcifications, cystic change, hyperechoic foci, round shape, loss of hilum), suspicion_level (ACR TI-RADS LN criteria), biopsy_recommended.
  3. Replace the shell rows in `canonical_us_lymph_node_v2`.
  4. Until rebuilt, the table is unusable and LN-US-source columns on cohort_v2 (`ln_us_suspicious_count`, etc. per Resolved Decision #2) remain NULL.
- **Candidate list built (2026-04-23, migration 30)**: `manuscript_workspace.qc_usln01_llm_candidates_v1` (TABLE, static snapshot) = 855 exams where parent-gland impression text mentions LN concerns (lymph node / lymphadenopath / abnormal node / cervical node / LAD). LLM skeleton at `qc_framework_v1/usln01_llm_extract.py` — NOT executed.
- **Status**: candidates-built 2026-04-23 (LLM run pending authorization; deprecation log entry `prompt_29`).

---

### CT01 — LN mentioned in CT report but no location/details extracted
- **Table/col**: `main.ct_imaging`
- **Category**: parsing (extraction sparsity)
- **Severity / scope**: warning / event
- **Detection**:
  ```sql
  SELECT COUNT(*) FROM ct_imaging
  WHERE lymph_nodes_mentioned IS TRUE
    AND lymph_node_locations IS NULL
    AND lymph_node_details IS NULL;
  ```
- **Observed**: registry 975 rows; current snapshot 4,080 rows with `lymph_nodes_mentioned=TRUE` and `lymph_node_locations IS NULL`. Delta flagged — parser may have been tightened since registry draft; Logan to reconcile.
- **Fix**: Re-run LN-detail extraction on the source reports with an upgraded prompt targeting location (neck level) and size. Merge results. For reports where no detail genuinely exists in text, leave NULL + set `ln_detail_extraction_attempted = TRUE` so we don't re-try forever.
- **Fix applied (2026-04-23, migration 31)**: Advisory flag `ct_ln_underspecified_flag` on `manuscript_workspace.ct_imaging_clean`. No queue emission — number overshoots registry pending reconciliation.
- **Status**: flagged 2026-04-23 (advisory flag on clean view; queue deferred until registry count reconciled; deprecation log entry `prompt_30`).

### CT02 — `thyroid_not_visualized=TRUE` paired with other thyroid flags
- **Table/col**: `ct_imaging` (flag set)
- **Category**: rule-violation (boolean semantic conflict)
- **Severity / scope**: warning / event
- **Observed**: 140 rows (registry estimate 170).
- **Fix**: Declare `thyroid_not_visualized` the dominant flag. For the 140 rows, null out every other thyroid_* flag (nodule, enlarged, heterogeneous, postsurgical, other_abnormality) because they can't simultaneously be TRUE with "not visualized." Log original values to `ct_imaging_flag_overrides_v1` for audit.
- **Fix applied (2026-04-23, migration 31)**: `ct02_notvisualized_contradiction` flag on `ct_imaging_clean`. 140 rows queued under `CT02`. Flag-only — no silent overwrite of source values.
- **Status**: RESOLVED 2026-04-23 (140 rows queued under `CT02`; deprecation log entry `prompt_30`).

### CT03 — `thyroid_normal=TRUE` paired with other thyroid abnormality flags
- **Table/col**: `ct_imaging`
- **Category**: rule-violation
- **Severity / scope**: warning / event
- **Observed**: 23 rows (exact match to registry).
- **Fix**: Declare the **abnormality flags** dominant (if any abnormality is coded, the thyroid isn't "normal"). For 23 rows, set `thyroid_normal = FALSE`, preserve the abnormality flags. Log to the override table.
- **Fix applied (2026-04-23, migration 31)**: `ct03_normal_contradiction` flag on `ct_imaging_clean`. 23 rows queued under `CT03`.
- **Status**: RESOLVED 2026-04-23 (23 rows queued under `CT03`; deprecation log entry `prompt_30`).

### CT04 — `tracheal_deviation` present but `tracheal_deviation_direction` NULL
- **Table/col**: `ct_imaging` (`tracheal_deviation`, `tracheal_deviation_direction`)
- **Category**: parsing (incomplete structuring)
- **Severity / scope**: warning / event
- **Observed**: registry 5,233; current snapshot 3,413 rows with `tracheal_deviation NOT IN ('none')` and direction unresolvable (NULL / not_mentioned / unknown / none / present_unspecified / null-text). Schema: `tracheal_deviation` enum = {present, none, not_mentioned, NULL}; when `='present'` alone, only 49 rows have unresolvable direction.
- **Fix**:
  1. Re-extract direction from the source report text with a targeted prompt: "If tracheal deviation is mentioned, extract direction ∈ {left, right, anterior, posterior, NULL}."
  2. Rows where direction genuinely isn't stated in the report → leave NULL + `tracheal_direction_unstated = TRUE`.
  3. The manuscript doesn't currently use this covariate, so low analytic priority — but it's a symptom of incomplete structuring worth fixing.
- **Fix applied (2026-04-23, migration 31)**: Advisory flag `ct_tracheal_direction_missing_flag` on `ct_imaging_clean` (3,413 rows). No queue emission — low analytic priority and count differs from registry.
- **Status**: flagged 2026-04-23 (advisory flag on clean view; low analytic priority; deprecation log entry `prompt_30`).

---

### MRI01 — explicit API/parse errors (45 rows)
- **Table/col**: `main.mri_imaging.error`
- **Category**: extraction-failure
- **Severity / scope**: critical / event
- **Detection**: `WHERE error IS NOT NULL`
- **Observed**: 45 rows, error text: `API/parse error after retries: Expecting value: line 1 column 1 (char 0)`
- **Fix**:
  1. Re-run the MRI extractor on the 45 source reports with a hardened JSON parser (strip markdown fences, retry with structured output mode).
  2. Rows that still fail: move to `qc_manual_review_queue_v1` with `issue_id='MRI01'` for hand-annotation.
  3. Clear `error` column on successful re-extraction.
- **Fix applied (2026-04-23, migration 32)**: `mri_parse_error_flag` on `manuscript_workspace.mri_imaging_clean`; 45 rows queued for hardened re-extraction.
- **Status**: flagged 2026-04-23 (view + queue; deprecation log `prompt_31`).

### MRI02 — LN mentioned but no location/details
- **Table/col**: `mri_imaging` (`lymph_nodes_mentioned`, `lymph_node_locations`, `lymph_node_details`, `lymph_node_locations_with_size`)
- **Category**: parsing
- **Severity / scope**: warning / event
- **Observed**: 71 rows
- **Fix**: Same pattern as CT01. Targeted re-extraction; if still missing, mark attempted + leave NULL.
- **Fix applied (2026-04-23, migration 32)**: `mri_ln_underspecified_flag` on `mri_imaging_clean` surfaces 129 rows (registry 71 — count overshoot, parser tightened since registry draft). Advisory-only — no queue emission pending Logan's count reconciliation.
- **Status**: flagged 2026-04-23 advisory (count overshoot; deprecation log `prompt_31`).

### MRI03 — `thyroid_normal=1` paired with other abnormal flags
- **Table/col**: `mri_imaging`
- **Category**: rule-violation
- **Severity / scope**: warning / event
- **Observed**: 5 rows
- **Fix**: Same as CT03 pattern. Abnormality flags dominant; set `thyroid_normal = 0`.
- **Fix applied (2026-04-23, migration 32)**: `mri_normal_contradiction_flag` on `mri_imaging_clean`; 5 rows queued for human review.
- **Status**: flagged 2026-04-23 (view + queue; deprecation log `prompt_31`).

---

## Batch #4 — exam-master, patient summary, nuclear med

### IEM01 — `imaging_exam_master_v1.exam_date` NULL
- **Table/col**: `main.imaging_exam_master_v1.exam_date`
- **Category**: derivation / source-merge
- **Severity / scope**: critical / event
- **Detection**:
  ```sql
  SELECT COUNT(*) FROM imaging_exam_master_v1 WHERE exam_date IS NULL;
  ```
- **Observed**: 2,050 of 13,347 rows (15.4%)
- **Fix**:
  1. Join back to each source table (`raw_us_tirads_excel_v1`, `raw_imaging_12_slots_v1`, `raw_us_tirads_scored_v1`) on `exam_id` and re-pull the date column. Source-specific date columns likely exist under different names.
  2. If multiple sources carry the date and they agree, use any. If they disagree, use the earliest (conservative for time-anchored analyses).
  3. Rows where no source carries a date: set `exam_date_unresolved = TRUE`, leave `exam_date = NULL`. Downstream views must filter these out of time-anchored analyses.
- **Fix applied (2026-04-23, migration 33)**: `manuscript_workspace.imaging_exam_master_v1_datecheck` surfaces `exam_date_unresolved_flag`. All 2,050 NULL-date rows come from source `raw_imaging_12_slots_v1`, which has been archived to `"Thyroid 2026 UPdated".archive_legacy.main__raw_imaging_12_slots_v1_20260417T073708Z` (column `exam_date_norm`). Per "no cross-DB canonical sourcing" rule, recovery path documented but not executed. 2,050 rows queued under IEM01.
- **Status**: flagged 2026-04-23 (view + queue; deprecation log `prompt_32`; recovery blocked on cross-DB policy).

### IEM02 — `imaging_exam_master_v1.largest_nodule_cm` NULL despite canonical having sizes
- **Table/col**: `imaging_exam_master_v1.largest_nodule_cm`
- **Category**: rollup / synchronization
- **Severity / scope**: warning / event
- **Detection**:
  ```sql
  SELECT COUNT(*) FROM imaging_exam_master_v1 WHERE largest_nodule_cm IS NULL;
  ```
- **Observed**: 7,319 rows (54.8%)
- **Fix**: Recompute table-wide from canonical:
  ```sql
  WITH c AS (
    SELECT us_exam_id AS exam_id, MAX(size_cm_max) AS largest_cm
    FROM canonical_us_nodule_v2
    WHERE NOT is_aggregate_row
    GROUP BY us_exam_id
  )
  UPDATE imaging_exam_master_v1 i
  SET largest_nodule_cm = c.largest_cm
  FROM c WHERE i.exam_id = c.exam_id;
  ```
  Rows where canonical also has no size (rolls up from US01) remain NULL. Blocked on TIR03 fix + US01 rebuild so the canonical source is trustworthy first.
- **Fix applied (2026-04-23, migration 34)**: `manuscript_workspace.imaging_exam_master_v1_size_clean` exposes `largest_nodule_cm_final` (COALESCE(IEM, canonical)), `largest_nodule_cm_source`, `iem02_size_recovered_flag`. Of 7,319 NULL rows: 4,289 backfilled from canonical (silent), 3,030 unresolvable — 204 US-source unresolvables queued (12_slots excluded as canonical has no coverage there).
- **Status**: flagged 2026-04-23 (view + queue; deprecation log `prompt_33`).

### IEM03 — exam-level nodule count mismatch vs canonical
- **Table/col**: `imaging_exam_master_v1.n_nodules` vs `canonical_us_nodule_v2` nodule count per exam
- **Category**: rollup / synchronization
- **Severity / scope**: warning / event
- **Detection**:
  ```sql
  WITH c AS (
    SELECT us_exam_id AS exam_id,
           COUNT(DISTINCT nodule_index_within_exam) AS canonical_n
    FROM canonical_us_nodule_v2 WHERE NOT is_aggregate_row
    GROUP BY us_exam_id
  )
  SELECT COUNT(*) FROM imaging_exam_master_v1 i
  JOIN c ON i.exam_id = c.exam_id
  WHERE i.n_nodules IS DISTINCT FROM c.canonical_n;
  ```
- **Observed**: 19 exams disagree. Examples: `4052d82b...` has IEM n=14 vs canonical=7; `514a3690...` has IEM=17 vs canonical=10.
- **Fix**: Declare `canonical_us_nodule_v2` the authoritative source for `n_nodules`. Overwrite IEM's `n_nodules` from the CTE above. Run **after** TIR03 fix so canonical is trustworthy. Log pre/post values to `imaging_exam_master_n_nodules_audit_v1` for traceability.
- **Fix applied (2026-04-23, migration 35)**: `manuscript_workspace.imaging_exam_master_v1_count_clean` exposes `n_nodules_canonical`, `n_nodules_final` (canonical wins), `n_nodules_source`, `iem03_n_nodules_mismatch_flag`. Current mismatch count: **116** (registry 19 — count risen since snapshot). All 116 concentrate in source `raw_us_tirads_scored_v1`. 116 rows queued as audit trail (canonical authoritative; no human review needed).
- **Status**: flagged 2026-04-23 (view + audit queue; deprecation log `prompt_34`).

### IEM04 — exam-level largest-nodule size mismatch
- **Category**: rollup / synchronization
- **Severity / scope**: warning / event
- **Detection**: `ABS(i.largest_nodule_cm - c.largest_nodule_cm) > 0.1` between IEM and canonical
- **Observed**: 7 exams
- **Fix**: Same rebuild as IEM02 — overwrite from canonical. Folded into IEM02's fix.
- **Fix applied (2026-04-23, migration 34)**: `iem04_size_mismatch_flag` on `imaging_exam_master_v1_size_clean`; 7 rows queued. Resolution rule: prefer IEM value (legacy analytic history), but flag for reviewer — canonical correctness triggers row-level update via queue resolution.
- **Status**: flagged 2026-04-23 (view + queue; deprecation log `prompt_33`).

### IEM05 — `raw_us_tirads_scored_v1` source pathway overcounts
- **Table/col**: `imaging_exam_master_v1.source` = `'raw_us_tirads_scored_v1'`
- **Category**: parsing (legacy source)
- **Severity / scope**: warning / event (source-level marker)
- **Observed**: Most IEM03 mismatches concentrate in `raw_us_tirads_scored_v1`-sourced rows (2,506 rows from this source total; `n_nodules` appears roughly 2× canonical in affected exams).
- **Fix**: This isn't fixed directly — IEM02/IEM03's canonical overwrite corrects the symptoms. Keep the source tag for audit. Downstream views that need raw-parsed exam counts (none currently in manuscript) would need to further filter this source.
- **Fix applied (2026-04-23, migration 35)**: `iem05_legacy_source_flag` on `imaging_exam_master_v1_count_clean` (= `source='raw_us_tirads_scored_v1'`, 2,506 rows). Advisory only — no queue. All IEM05 rows ride on the IEM03 canonical rebuild.
- **Status**: flagged 2026-04-23 advisory (view flag only; deprecation log `prompt_34`).

### IPS01 — `imaging_patient_summary_v1` passed all integrity checks (CLEAN)
- **Table**: `main.imaging_patient_summary_v1`
- **Category**: none
- **Severity / scope**: info / schema
- **Detection** (all returned 0):
  - `first_exam_date > last_exam_date`
  - negative `n_exams` / `n_total_nodules`
  - `n_exams = 0` with other data present
  - negative `dominant_nodule_size_cm`
  - `max_tirads_ever` out of [1,5]
  - `worst_tirads_category` not in {TR1..TR5}
  - `n_exams = 1` with `longitudinal_assessment_available = TRUE`
  - `n_total_nodules = 0` with `dominant_nodule_size_cm` present
- **Observed**: All checks returned 0 violations.
- **Fix**: **None required.** Use `imaging_patient_summary_v1` as a trusted patient-grain rollup. Document in cohort_v2 spec that this table is validated.
- **Status**: closed (clean)

---

### NM01 — `nuclear_med.scandate` unparseable
- **Table/col**: `main.nuclear_med.scandate`
- **Category**: normalization
- **Severity / scope**: critical / event
- **Detection**:
  ```sql
  SELECT COUNT(*) FROM nuclear_med
  WHERE TRY_CAST(scandate AS DATE) IS NULL AND scandate IS NOT NULL;
  ```
- **Observed**: 1,364 of 2,220 rows (61.4%)
- **Fix**:
  1. Tiered parser: try `YYYY-MM-DD`, `MM/DD/YYYY`, `DD-Mon-YYYY`, `Mon DD YYYY`, Excel serial-number date (numeric 5-digit range), two-digit-year variants.
  2. Create new column `scandate_parsed DATE` populated from whichever parse succeeds.
  3. For remaining unparseable rows (~edge cases), pass the raw string through an LLM date-extractor with retry; populate `scandate_parsed` if the LLM returns a confident date.
  4. Rows still unresolved: `scandate_parsed = NULL`, preserve raw in `scandate_raw`. Log distinct unresolved formats to `nuclear_med_scandate_unresolved_formats_v1` for one-shot review.
  5. Rename original `scandate` → `scandate_raw`, promote `scandate_parsed` → `scandate`.
- **Fix applied (2026-04-23, migration 36)**: `manuscript_workspace.nuclear_med_clean` surfaces `scandate_parsed`, `scandate_parse_source`, `nm01_scandate_unresolved_flag`. Distribution: 855 ISO + 1,363 MM/DD/YY + 1 NULL + 1 unresolved (`"2/18/216"` typo). Two-format tiered parser resolves 100% of well-formed rows — no LLM pass needed. 1 row queued (the typo).
- **Status**: flagged 2026-04-23 (view + queue; deprecation log `prompt_35`; 2,218/2,220 non-null dates fully parsed).

### NM02 — `nuclear_med.scan_present` uniformly non-standard
- **Table/col**: `nuclear_med.scan_present`
- **Category**: normalization
- **Severity / scope**: critical / schema
- **Detection**:
  ```sql
  SELECT COUNT(*) FROM nuclear_med
  WHERE scan_present NOT IN ('yes','no','YES','NO','Yes','No')
    AND scan_present IS NOT NULL;
  ```
- **Observed**: 2,220 / 2,220 rows (100%)
- **Fix** (two-step — inspect, then map):
  1. **Inspect first**: `SELECT scan_present, COUNT(*) FROM nuclear_med GROUP BY 1 ORDER BY 2 DESC LIMIT 50;` to see what the column actually holds. The 100% non-standard result suggests the column isn't storing yes/no at all — likely scan-type codes, boolean-as-int, or some freetext indicator.
  2. Build `scan_present_map(raw_value, scan_present_bool BOOLEAN)` based on what the inspection reveals. Apply the map, replace column. Preserve raw in `scan_present_raw`.
  3. If the column turns out to encode something different than "did the scan happen" (e.g., modality codes), rename the column to match its actual semantic and add a real boolean `scan_present_bool` derived from `scandate_parsed IS NOT NULL`.
- **Fix applied (2026-04-23, migration 37)**: Inspection confirmed `scan_present` is the full **radiology report narrative** (2,220/2,220 rows populated; avg ~1,911 / max 11,985 chars). Column is mislabeled. `manuscript_workspace.nuclear_med_clean` adds `scan_report_text_length` (INT) + `scan_present_bool` (BOOLEAN, derived from `scandate_parsed IS NOT NULL`) — 2,218 TRUE + 1 FALSE + 1 NULL. No queue emission (pure semantic fix).
- **Status**: RESOLVED 2026-04-23 (semantic rename at view layer; deprecation log `prompt_36`).

### NM03 — `nuclear_med` rows with no findings and no impression
- **Table/col**: `nuclear_med` (`findings_text`, `impression_text`)
- **Category**: parsing
- **Severity / scope**: warning / event
- **Detection**: `findings_text IS NULL AND impression_text IS NULL`
- **Observed**: 522 rows
- **Fix**:
  1. Cross-reference with `scan_present` (after NM02 fix). If `scan_present_bool = FALSE`, empty text is expected — accept and move on.
  2. If `scan_present_bool = TRUE` or `NULL` with empty text, re-run text extractor on source report. Populate whichever field the report yields.
  3. Rows still empty after re-extraction: `text_extraction_attempted = TRUE` flag set; leave NULL.
- **Fix applied (2026-04-23, migration 38)**: `nm03_narrative_empty_flag` on `nuclear_med_clean` surfaces 522 rows with both `findings_text` and `impression_text` empty. All 522 queued as Tier-2 LLM-re-parse candidates (source: `scan_present` narrative text — see LLM_TODO.md).
- **Status**: flagged 2026-04-23 (view + queue; deprecation log `prompt_37`; LLM pass deferred).

### NM04 — `nuclear_med.scantype` / `radiotracer` NULL
- **Table/col**: `nuclear_med` (`scantype`, `radiotracer`)
- **Category**: parsing
- **Severity / scope**: warning / event
- **Observed**: 64 rows null `scantype`, 110 rows null `radiotracer`
- **Fix**: Re-extract from `findings_text` or `impression_text` using an LLM with a fixed vocabulary:
  - `scantype ∈ {I-123 uptake/scan, I-131 uptake/scan, Tc-99m pertechnetate, PET-CT, whole-body I-131, other}`
  - `radiotracer ∈ {I-123, I-131, Tc-99m pertechnetate, F-18 FDG, F-18 DOPA, other}`
  For the overlap with NM03 (no text at all), unrecoverable — leave NULL.
- **Fix applied (2026-04-23, migration 38)**: `nm04_scantype_null_flag` (64 rows, registry-numbered) + `nm04_radiotracer_null_flag` (110 rows, advisory) on `nuclear_med_clean`. 64 scantype-null rows queued as LLM-re-parse candidates from `scan_present` narrative; radiotracer-null flagged but not queued (advisory).
- **Status**: flagged 2026-04-23 (view + queue; deprecation log `prompt_37`; LLM pass deferred).

---

## Run log

- 2026-04-22: Initial population from `run_qc_live.py` against `md:thyroid_canonical_publication_v1_0`. Cohort v1=10,871 → critical-excluded=667 → v2=10,204 → v2_ptc=2,710. All 20 rules executed cleanly. PATH14 added after multi-tumor cross-check.
- 2026-04-22: Batch #3 appended — PATH15–21 (linkage, laterality/site, metastatic-in-histology, discordance flags), USGLAND01/02, USLN01, CT01–04, MRI01–03. Resolved-decisions block replaces open questions: ETE vocab locked, LN multi-source architecture, TIR03 scope=60 exams, manual-review queue to be created.
- 2026-04-22: Batch #4 appended — IEM01–05 (exam-master sync to canonical), IPS01 clean validation, NM01–04 (nuclear_med normalization). NM02 flagged as "inspect first, then map" since 100% non-standard suggests a semantic mismatch, not a dirty vocabulary.
- 2026-04-22: Batch #5 appended — GEN01 (molecular_episode_id collapse, like PATH01), GEN02 (platform_version missing only when raw-text signal exists: 267 ThyroSeq + 119 Afirma + 36 NGS = 422 rows, **not** all 1,384), GEN03-12. Logan's correction incorporated: null platform_version is NOT inherently a defect; only the narrow "raw signal present, structured column NULL" case is in scope.
- 2026-04-23: **Prompts 31–40 applied** (migrations 32–41). 10 manuscript_workspace views + 10 deprecation log entries (`prompt_31..prompt_40`) + 4,549 queue rows across 12 issue IDs. Coverage: MRI01/02/03 (mig 32); IEM01 (33) + IEM02/04 (34) + IEM03/05 (35); NM01 (36) + NM02 (37) + NM03/04 (38); GEN02 (39) + GEN06/07 (40) + GEN12/05 (41). RESOLVED without queue: NM02 (semantic rename), GEN12 (0 residual post-normalization). Registry count drift noted on several issues (IEM03 19→116; MRI02 71→129; GEN07 6→2) — counts reconciled in migration headers.

---

## Batch #5 — genetics

### GEN01 — `molecular_episode_id` collapsed
- **Table/col**: `main.canonical_molecular_genetics_v2.molecular_episode_id`
- **Category**: linkage
- **Severity / scope**: critical / schema
- **Detection**:
  ```sql
  SELECT COUNT(DISTINCT molecular_episode_id) FROM canonical_molecular_genetics_v2;
  -- returns 3 across 1,384 rows / 1,151 patients
  ```
- **Observed**: 3 distinct IDs across the whole table. Same ordinal-pattern as PATH01.
- **Fix** (identical pattern to PATH01):
  1. Derive `molecular_episode_uid = md5(research_id || '|' || CAST(resolved_test_date AS VARCHAR) || '|' || COALESCE(platform,'UNKNOWN') || '|' || COALESCE(platform_version,''))`.
  2. Where `resolved_test_date` is NULL (903 rows), fall back to `report_date` or FNA-event date match; if still null, use a row hash + manual-review flag.
  3. Rename existing `molecular_episode_id` → `molecular_episode_ordinal_deprecated`.
  4. Downstream specimen_genomic_assay_v1 linkage (GEN09) will rebind on the new uid.
- **Status**: RESOLVED 2026-04-23 (migration 05)
- **Resolution (2026-04-23)**:
  - View: `manuscript_workspace.molecular_episode_uid_v1` over `main.canonical_molecular_genetics_v2`.
  - Final hash: `md5(research_id | COALESCE(resolved_test_date,'') | COALESCE(platform,'') | COALESCE(report_text_ref,''))`.
  - Deviations from original spec (documented in migration header):
    1. Dropped `platform_version` from the hash (100% NULL across all 1,384 rows; zero discriminative value).
    2. Added `report_text_ref` as tie-breaker (100% populated, 4 distinct values — reduces collisions from 36 → 1 on date-NULL rows).
    3. Emitted auxiliary `molecular_episode_uid_source` ∈ {`date_platform_report` (481 rows with resolved_test_date), `platform_report_no_date` (903 rows without)}.
  - Final counts: 1,384 rows → **1,383 distinct UIDs** across 1,151 patients. 1 residual collision is a byte-identical duplicate (research_id=10771, NGS_unspecified, null date, 2 identical rows collapsing to UID `16d2c48494aa51ac9b9998871e1881b4`).
  - Queue: 1 row in `qc_manual_review_queue_v1` with `issue_id='GEN01'` / `reason='byte-identical duplicate row — confirm intended collapse'` — human review confirms whether the collapse is intended before downstream rebinds.
  - Downstream unblocked: GEN09 (specimen_genomic_assay_v1), GEN13 (assay molecular_episode_id mismatch), GEN15 (molecular linked_fna_episode_id).

### GEN02 — `platform_version` NULL when raw text contains a version signal
- **Table/col**: `canonical_molecular_genetics_v2` (`platform`, `platform_version`, `platform_raw`)
- **Category**: normalization (narrow scope per Logan's correction)
- **Severity / scope**: warning / event
- **Detection**:
  ```sql
  SELECT platform, COUNT(*) FROM canonical_molecular_genetics_v2
  WHERE platform_version IS NULL
    AND platform_raw IS NOT NULL
    AND REGEXP_MATCHES(LOWER(platform_raw), 'v ?[0-9]|gsc|gec|thyroseq 2|thyroseq 3|v2|v3')
  GROUP BY platform;
  ```
- **Observed**: 422 rows total (ThyroSeq 267, Afirma 119, NGS_unspecified 36). **NOT** the full set of null-version rows — most genuinely-null rows are expected.
- **Fix**:
  1. Regex-extract version from `platform_raw`. Canonical mapping:
     - `ThyroSeq`: match `v ?2|thyroseq 2` → `v2`; match `v ?3|thyroseq 3` → `v3`
     - `Afirma`: match `gec` → `GEC`; match `gsc` → `GSC`
     - `NGS_unspecified`: preserve raw version string
  2. Populate `platform_version` on the 422 rows.
  3. **Do not** touch rows where `platform_raw` has no version signal — those legitimately stay NULL (Logan's explicit decision).
  4. Add column comment: "NULL indicates no version stated in source; not a defect unless `platform_raw` contains a version signal."
- **Fix applied (2026-04-23, migration 39)**: `manuscript_workspace.canonical_molecular_genetics_v2_platform_clean` adds `platform_subversion` (VARCHAR) + `platform_version_derived` (INTEGER; 3=ThyroSeq v3/Afirma GSC, 2=ThyroSeq v2/Afirma GEC). 376 of 1,384 rows get a derived version via regex chain (238 ThyroSeq + 138 Afirma; registry numbers were snapshot-stale). 1,008 rows queued under GEN02 as LLM-pass candidates (raw text lacks version token).
- **Status**: flagged 2026-04-23 (view + queue; deprecation log `prompt_38`).

### GEN03 — `parse_status` sparse / shallow
- **Table/col**: `canonical_molecular_genetics_v2.parse_status`
- **Category**: parsing
- **Severity / scope**: critical / schema
- **Observed**: distribution: `no_detailed_block` 591, `ok` 371, `minimal` 186, `partial` 173, `empty_block` 63. Only 371 rows (27%) have full-structured parse.
- **Fix**:
  1. Re-run the structured molecular-report parser on the 591 `no_detailed_block` + 63 `empty_block` + 186 `minimal` = 840 rows with a hardened prompt that extracts variants, fusions, CNAs, GEP results, and ROM independently rather than requiring a "detailed block" section.
  2. After re-parse, expected distribution target: `ok` > 70% (~970 rows), `partial` < 10%, `no_detailed_block`/`empty_block` < 5% combined.
  3. Rows still unparseable after the hardened pass: `parse_status = 'genuinely_sparse_source'` + `parse_attempted_hardened = TRUE`, accept and move on.
- **Status**: pending

### GEN04 — `overall_result_class` dominated by NULL and 'other'
- **Table/col**: `canonical_molecular_genetics_v2.overall_result_class`
- **Category**: normalization / classification
- **Severity / scope**: critical / schema
- **Observed**: distribution: `other` 553, NULL 525, `suspicious` 248, `negative` 41, `non_diagnostic` 13, `positive` 3, `cancelled` 1. Together NULL + `other` = 78% of the table.
- **Fix**:
  1. Canonical vocabulary: `{positive, suspicious, negative, non_diagnostic, cancelled, indeterminate, NULL}`.
  2. For the 553 `other` rows, re-classify from `test_result_summary` + underlying parsed variants/fusions using platform-specific rules:
     - ThyroSeq: `positive` if any known-driver variant/fusion, `negative` if none found and adequate, `non_diagnostic` if adequacy fails, `suspicious` for intermediate.
     - Afirma GEC: `benign`/`suspicious` mapped to `negative`/`suspicious`.
     - Afirma GSC: `benign`/`suspicious` same.
  3. For the 525 NULL rows, fire GEN03's re-parse first, then re-classify.
  4. After both fixes, `other` should become a tiny residual (<10 rows) flagged for manual review.
- **Status**: pending (blocked on GEN03)

### GEN05 — `molecular_confidence` 100% NULL
- **Table/col**: `canonical_molecular_genetics_v2.molecular_confidence`
- **Category**: derivation
- **Severity / scope**: warning / schema
- **Observed**: 1,384 / 1,384 rows NULL
- **Fix**: Derive from `parse_status` + `specimen_adequacy_norm`:
  - `parse_status='ok' AND specimen_adequacy_norm='ADEQUATE'` → `high`
  - `parse_status IN ('partial','minimal') OR specimen_adequacy_norm='LIMITED'` → `medium`
  - `parse_status IN ('no_detailed_block','empty_block') OR specimen_adequacy_norm IN ('INADEQUATE','LOW_THYROID_CELL_CONTENT')` → `low`
  - Cancelled/inadequate flags → `unknown`
  Run after GEN03 so `parse_status` is trustworthy first.
- **Fix applied (2026-04-23, migration 41)**: `gen05_molecular_confidence_stub_flag` on `canonical_molecular_genetics_v2_status_clean` — always TRUE (100% NULL on canonical). Advisory-only flag; no queue emission (1,384 identical rows = noise). Full derivation deferred pending GEN03 re-parse.
- **Status**: flagged 2026-04-23 advisory stub (no queue; deprecation log `prompt_40`; full derivation still blocked on GEN03).

### GEN06 — `resolved_test_date` NULL (65%)
- **Table/col**: `canonical_molecular_genetics_v2.resolved_test_date`
- **Category**: derivation
- **Severity / scope**: critical / event
- **Observed**: 903 of 1,384 rows NULL
- **Fix**: Tiered fallback:
  1. If `report_date` is present → use it (rename resolved column derivation to include this path).
  2. If not, match to `canonical_fna_events_v1` on `(research_id, assay_type)` within a ±90 day window → use FNA date.
  3. If still NULL, the test is temporally unanchored; set `resolved_test_date_unresolved = TRUE`. Exclude from time-anchored analyses.
- **Fix applied (2026-04-23, migration 40)**: `manuscript_workspace.canonical_molecular_genetics_v2_date_clean` exposes `resolved_test_date_final` + `resolved_test_date_source` ∈ {resolved, native, sga, unresolved}. Tier chain: `resolved_test_date` → `test_date_native` → `specimen_genomic_assay_v1_relinked.test_date_native` (MIN per molecular_episode_id) → unresolved. Of the 903 NULL rows, 378 recovered via SGA fallback, 525 unresolvable — 525 queued.
- **Status**: flagged 2026-04-23 (view + queue; deprecation log `prompt_39`).

### GEN07 — ROM numeric out-of-range
- **Table/col**: `canonical_molecular_genetics_v2` (`rom_percent_point`, `rom_percent_low`, `rom_percent_high`)
- **Category**: parsing
- **Severity / scope**: warning / event
- **Observed**: 2 + 2 + 2 = 6 values out of [0,100]
- **Fix**: For 6 affected rows, re-extract from `rom_percent_raw` + source text. Negative values are likely parser sign-flip on leading `-` or `(-`; fix by taking absolute value when the raw text starts with `(~` or `(-`. Values > 100 → manual review (edge case).
- **Fix applied (2026-04-23, migration 40)**: `gen07_rom_out_of_range_flag` on `canonical_molecular_genetics_v2_date_clean` catches any of `rom_percent_point/low/high` outside [0,100]. Current count: **2** (registry 6 — count dropped since snapshot; both are raw `"395%"` / `"599%"` strings — classic OCR typos). 2 rows queued for hand correction.
- **Status**: flagged 2026-04-23 (view + queue; deprecation log `prompt_39`).

### GEN08 — fusion flag TRUE but no structured fusion rows (patient-level)
- **Table/col**: `canonical_molecular_genetics_v2.fusion_flag` vs `molecular_fusions_unnested_VIEW_v2`
- **Category**: derivation / rule-violation
- **Severity / scope**: critical / patient
- **Detection**:
  ```sql
  WITH f AS (SELECT research_id, COUNT(*) n FROM molecular_fusions_unnested_VIEW_v2 GROUP BY 1)
  SELECT COUNT(DISTINCT m.research_id)
  FROM canonical_molecular_genetics_v2 m
  LEFT JOIN f ON m.research_id = f.research_id
  WHERE m.fusion_flag = TRUE AND COALESCE(f.n,0) = 0;
  ```
- **Observed**: 486 patients have `fusion_flag=TRUE` with no fusion rows in the unnested view.
- **Fix**:
  1. Run GEN03 first (re-parse) — many will resolve.
  2. After re-parse, for remaining `fusion_flag=TRUE` rows without structured fusions: force `fusion_flag := FALSE` and log to `gen08_fusion_flag_demotions_v1` with original reason. Structured fusion detail is the source of truth.
  3. Document: `fusion_flag` is a derived boolean `EXISTS (fusion row in unnested view)` — can't be set independently going forward.
- **Status**: pending (blocked on GEN03)

### GEN09 — `specimen_genomic_assay_v1` linkage mostly broken
- **Table/col**: `main.specimen_genomic_assay_v1` (linkage fields)
- **Category**: linkage
- **Severity / scope**: critical / schema
- **Observed**:
  - 10,370 rows, 10,107 (97.5%) have null `specimen_id`/`specimen_focus_id`
  - only 3 distinct `molecular_episode_id`, 7 distinct `fna_episode_id`, 2 distinct `surgery_episode_id`
  - 10,155 rows have `review_flag = TRUE`
  - dominant bucket: `Other / unresolved_review / D_unlinked / molecular_test_episode_v2` = 9,170 rows
- **Fix** (major rebuild, depends on multiple upstream fixes):
  1. **Blocked on**: GEN01 (molecular_episode_uid), PATH01 (surgery_episode_uid), and a clean FNA episode key (currently only 7 distinct values — there's a parallel FNA01-adjacent linkage bug to investigate).
  2. Once canonical uids exist upstream, rebuild `specimen_genomic_assay_v1` by joining:
     - assay row → `canonical_molecular_genetics_v2.molecular_episode_uid`
     - molecular episode → FNA event (via `(research_id, date window, tissue_type)`)
     - FNA event → `canonical_fna_events_v1.specimen_id_from_fna` if present
     - Surgery specimen → `canonical_path_malignant_events_v1.specimen_id` via `(research_id, surgery_episode_uid)`
  3. Populate linkage tiers: `A_direct_match`, `B_same_patient_date_range`, `C_same_patient_only`, `D_unlinked`.
  4. Set `review_flag := (tier NOT IN ('A_direct_match','B_same_patient_date_range'))`.
  5. Target: reduce `D_unlinked` from 9,170 to under 2,000; target `A/B` tier to over 6,000 rows combined.
- **Status**: RESOLVED 2026-04-23 (migration 06)
- **Resolution (2026-04-23)**:
  - Per Logan's direction ("don't over-engineer, they should all simply go to a research ID"), the elaborate tier-based linkage scheme was collapsed to research_id-only binding.
  - View: `manuscript_workspace.specimen_genomic_assay_v1_relinked` — for every assay row, reports whether its `research_id` exists in `canonical_molecular_genetics_v2` and/or `specimen_master_v1`. Downstream joins bind on `research_id`.
  - Final distribution (10,370 assay rows):
    - rid in both molecular + specimen: 1,175
    - rid in specimen_master only:      6,702
    - rid in canonical_molecular only:    313 rows / 243 patients
    - rid in neither:                   2,180 rows / 2,177 patients
  - **No queue emitted** (gut-check outcome 2026-04-23):
    - The 2,177 "neither" patients: 2,176/2,177 had surgery AND are in `canonical_path_benign_events_v1`, 0/2,177 in `path_malignant`. All have `platform='Other'` with no date / no payload — empty placeholder rows for benign-cohort patients who never had a molecular test. Absence from canonical_molecular is correct; absence from specimen_master is a separate benign-cohort coverage gap, not a GEN09 defect.
    - The 313 "molecular-only" rows (243 patients): also all benign-cohort (0/243 malignant, 243/243 benign, all with surgery). 43/243 have rows in `canonical_molecular_genetics_from_notes_v2`, so the molecular source may be op-note-derived rather than FNA. specimen_master's missing rows are the same benign-cohort coverage gap.
    - Queuing either group under GEN09 would be a category error — neither represents a linkage failure that a row-level human review can fix. The view itself IS the GEN09 resolution: downstream joins on `research_id`, with the two boolean flags exposing the coverage gap directly for cohort construction.
  - Follow-up: specimen_master_v1 benign-cohort coverage gap is its own potential issue (2,177 benign-cohort pts not indexed, plus 243 molecular-only pts); out of scope for GEN09.
  - Deferred / not built (per "don't over-engineer"): surgery_episode_id imputation, date-window probing, specimen_id reconstruction from collection date, molecular_episode_uid propagation.

### GEN10 — `canonical_molecular_genetics_from_notes_v2` unlinked/unverified
- **Table/col**: `main.canonical_molecular_genetics_from_notes_v2` (`linked_test_episode_id`, `source_episode_id`, `verification_status`, `confidence_score`)
- **Category**: design / linkage
- **Severity / scope**: warning / schema
- **Observed**: 1,738 rows, 100% have NULL `linked_test_episode_id`, NULL `source_episode_id`, NULL `confidence_score`. All entities come through as "unverified."
- **Fix** (design decision rather than defect):
  1. **Rename** table → `molecular_genetics_notes_mentions_v1` to make purpose explicit: this is an NLP mention-layer, not a verified assay table.
  2. Document: "Use for signal corroboration, not primary assay attribution. Analysts must not treat a mention here as an assay result."
  3. **Optional cross-link pass**: attempt soft-linkage to `canonical_molecular_genetics_v2.molecular_episode_uid` by `(research_id, note_date within ±90 days of resolved_test_date, entity_value_norm matches a known variant/fusion)`. Populate `linked_test_episode_id` where match confidence > 0.8. Leave unverified where confidence is lower.
  4. `verification_status` vocabulary for soft-linked rows: `{matches_canonical, contradicts_canonical, unlinked, mention_only}`.
- **Status**: RESOLVED 2026-04-23 (migration 07) — rename portion only; soft-linkage deferred
- **Resolution (2026-04-23)**:
  - View: `manuscript_workspace.molecular_mentions_from_notes_v2` — 1:1 pass-through over `main.canonical_molecular_genetics_from_notes_v2` (1,738 rows / 605 patients / 28 columns).
  - Deprecation note added to `qc_framework_v1/README.md` stating the source table must not be joined as a peer of `canonical_molecular_genetics_v2`; it is an NLP mentions layer for corroboration only.
  - Deferred: the optional soft-linkage pass (joining on `(research_id, note_date ± 90d, variant match)` to populate `linked_test_episode_id` with confidence scoring). Can be built later if a downstream analysis needs notes↔canonical attribution; out of scope for the rename.

### GEN11 — `specimen_adequacy_norm` mostly NULL
- **Table/col**: `canonical_molecular_genetics_v2.specimen_adequacy_norm`
- **Category**: parsing
- **Severity / scope**: warning / event
- **Observed**: 1,121 of 1,384 rows NULL; only 224 `ADEQUATE`, plus small counts for `LIMITED`, `LOW_THYROID_CELL_CONTENT`, `INADEQUATE`
- **Fix**: Re-extract adequacy from source report text using fixed vocabulary `{ADEQUATE, LIMITED, LOW_THYROID_CELL_CONTENT, INADEQUATE, NULL}`. Many ThyroSeq/Afirma reports state adequacy explicitly ("specimen adequate for analysis", "limited thyroid cell content"). Run as part of the GEN03 hardened re-parse.
- **Status**: pending (rolled into GEN03 re-parse)

### GEN12 — mutation/fusion/CNA/GEP status fields non-normalized
- **Table/col**: `canonical_molecular_genetics_v2` (`gene_mutations_status`, `gene_fusions_status`, `cna_status`, `gep_status`)
- **Category**: normalization
- **Severity / scope**: warning / schema
- **Observed**: mixed values including empty strings `''`, `Failed`, `Positive_high`, `Negative`, NULL, `Positive`
- **Fix**:
  1. Canonical vocab per column: `{Positive, Negative, Inadequate, Not_performed, NULL}`.
  2. Variants where present: `Positive_high`/`Positive_low` → collapse to `Positive` + move high/low signal to new column `gep_signal_strength` (GEP only; mutations/fusions/CNAs don't have a strength axis).
  3. Empty strings → NULL.
  4. `Failed` → `Inadequate`.
  5. Apply via mapping table.
- **Fix applied (2026-04-23, migration 41)**: `manuscript_workspace.canonical_molecular_genetics_v2_status_clean` exposes 6 `*_status_norm` columns mapped to canonical vocab `{Positive, Negative, Failed, Inadequate, NotApplicable}` (CNA adds `{Positive_high, Positive_low}`). Empty strings → NULL; `"no result"/"no result due to inadequate rn"` → `Inadequate`; `"N/A"` → `NotApplicable`. Post-normalization, `gen12_status_nonstandard_flag` = **0 rows** — all observed values mapped cleanly; no queue emission.
- **Status**: RESOLVED 2026-04-23 (0 residual after normalization; deprecation log `prompt_40`).

---

## Batch #6 — FNA / imaging-FNA linkage / operative re-keying / genomic binding / recurrence site (2026-04-22 PM run against backup)

### Key reframing from this batch
1. **OP05 supersedes PATH01's fix path.** Operative table is now clean: 11,773 globally-unique `surgery_episode_id` values, 1:1 with patient+episode combos, no dupes. Path malignant still carries patient-local ordinals (1,2,3,NULL). The fix is **not** `md5(research_id||surgery_date)` — it is a **left join back to `canonical_operative_events_v1` on `(research_id, surgery_date)`** to adopt the global ID. For path rows with no operative match (orphans), MD5 is the fallback. 5,254 path rows / 3,220 pts (2,624 PTC) are currently misaligned with the global namespace.
2. **Specimen scaffold is clean (SPEC01)**. `specimen_master_v1` (10,139 unique IDs), `specimen_tumor_focus_v1` (11,103 unique focus IDs), `specimen_source_xref_v1` all pass referential checks. No rebuild needed; the scaffold is ready to receive relinks from assay/molecular layers.
3. **`imaging_fna_linkage_v3` is a design defect, not a data defect.** The linker:
   - neutralized `size_score=0.5` on 100% of eligible links
   - permitted future imaging (img_date > fna_date) on 510 links
   - allowed explicit laterality conflicts through on 445 links
   - didn't enforce unique winners at rank 1 (814 tied FNA episodes, including pt 9096 with 13 rank-1 nodules)
   The fix is a rebuild of the linker itself, not an overlay.
4. **Molecular/genomic binding is an end-to-end episode-ID chain problem.** `specimen_genomic_assay_v1` holds only 3 distinct `molecular_episode_id` (patient-local ordinals again), of which 9,267 cannot match `canonical_molecular_genetics_v2`. Resolution must happen at the same time as GEN01 (canonical MD5 uid) is applied.

### FNA02 — `fna_date_resolved` NULL despite `fna_date_raw` present
- **Table/col**: `main.canonical_fna_events_v1` (`fna_date_raw`, `fna_date_resolved`)
- **Category**: parsing / temporal
- **Severity / scope**: warning / event
- **Observed**: 1,516 rows / 1,141 pts (464 PTC). Patient-level impact: 20% of FNA patients.
- **Fix**:
  1. Run a date-parse pass over `fna_date_raw` using the standard parsers (`TRY_STRPTIME` for ISO, US month/day, natural-language), plus a contextual fallback (note date ±14d if a tied note exists). Populate `fna_date_resolved_v2` and `fna_date_resolved_source` in {`existing`, `reparsed_iso`, `reparsed_us`, `contextual_note`, `unresolved`}.
  2. Rows that remain `unresolved` after all parsers → `qc_manual_review_queue_v1` with `issue_id='FNA02'`.
- **Status**: pending

### FNA03 — `days_to_surgery < 0` (redundant with FNA01 but derived column)
- **Table/col**: `main.canonical_fna_events_v1.days_to_surgery`
- **Category**: derivation / temporal
- **Severity / scope**: warning / event
- **Observed**: 280 rows / 222 pts (115 PTC). Overlaps partially with FNA01 but is its own derived column signal.
- **Fix**: Recompute `days_to_surgery` from `fna_date_resolved_v2` and the operative SoT date (see SURG reconciliation). Where still negative, apply the FNA01 post-surgery logic. Rows with `days_to_surgery IS NULL AND both dates populated` → manual review.
- **Status**: pending (follows FNA01, SURG reconciliation, FNA02)

### FNA04 — strict duplicate-signature FNA rows
- **Observed**: 4 duplicate-signature rows / 2 excess / 2 pts (1 PTC) — low volume, but fixable deterministically.
- **Fix**: In `manuscript_workspace.canonical_fna_events_v1_dedup`, keep the row with (a) non-NULL `fna_date_resolved`, (b) highest `bethesda_final_num` among ties, (c) lowest `fna_event_id`. Queue dropped rows with `issue_id='FNA04'`.
- **Status**: pending

### FNA05 — rollup `bethesda_final` NULL despite event-level Bethesda present
- **Table/col**: `main.canonical_fna_patient_rollup_v1.bethesda_final`
- **Observed**: 6 pts (2 PTC).
- **Fix**: Rebuild the rollup's `bethesda_final` as `MAX(bethesda_final_num) FILTER (WHERE fna_pre_surgery_flag=TRUE)` over the event-level clean table. The existing rollup code has a bug where pre-op filter drops valid rows for these 6 patients.
- **Status**: pending

### IFNA01-06 — `imaging_fna_linkage_v3` rebuild
- **Table**: `main.imaging_fna_linkage_v3`
- **Severity / scope**: critical / schema
- **Observed**: 3,339 eligible links total; 100% of them have `size_score=0.5` (IFNA01); 445 laterality conflicts admitted (IFNA02); 510 negative day-gap (IFNA03); 873 FNA→multi-nodule (IFNA04); 151 nodule→multi-FNA (IFNA05); 814 rank-1 ties (IFNA06).
- **Fix — full rebuild of the linker** as `manuscript_workspace.imaging_fna_linkage_v4`:
  1. **Eligibility gate**: require `(img_date <= fna_date)` always; drop any candidate where `img_date > fna_date` (fixes IFNA03). Tolerance: allow `img_date` up to 365 days before `fna_date`.
  2. **Laterality**: if both sides populated and differ → eligibility = FALSE (fixes IFNA02). If one side NULL, keep as `laterality_score=0.5`; if match, `1.0`.
  3. **Size scoring**: when both `img_size_cm` and `fna_size_cm` populated, compute `size_score = 1 - min(|Δ|/max(img,fna), 1)`. Only fall back to `size_score=0.5` if one or both missing (fixes IFNA01).
  4. **Composite score**: `linkage_score = 0.4*temporal + 0.3*laterality_score + 0.3*size_score`, break ties with nearest date, then lowest `imaging_exam_id`.
  5. **Unique winner enforcement**: assert one rank-1 nodule per FNA episode (and optionally one rank-1 FNA per nodule — configurable). Ties fall to a deterministic tie-break and are logged (fixes IFNA06).
  6. **Multiplicity flags**: expose `n_eligible_nodules_per_fna`, `n_eligible_fnas_per_nodule` as read-only diagnostics (IFNA04/05).
  7. Provide `manuscript_workspace.imaging_fna_linkage_v4_index` with exactly one row per FNA episode (the rank-1 winner).
- **Status**: pending (treated as a single rebuild prompt covering IFNA01-06)

### OP01 — `total_thyroidectomy` with unilateral laterality
- **Observed**: 33 rows / 33 pts (23 PTC).
- **Fix**: Emit to queue with `issue_id='OP01'`. Under normal AJCC/procedure semantics, a total thyroidectomy cannot be unilateral. Likely OR-note misclassification (really a completion or lobectomy mis-labeled). Chart review required. Set `procedure_normalized_trusted=NULL` for these rows until resolved.
- **Status**: pending (chart review)

### OP02 — `hemithyroidectomy` with `laterality='bilateral'`
- **Observed**: 3 rows / 3 pts (1 PTC).
- **Fix**: Same pattern — queue with `issue_id='OP02'`, chart review.
- **Status**: pending

### OP03 — ambiguous multi-episode procedure code rows
- **Observed**: 904 rows / 212 pts (81 PTC).
- **Fix**: For each ambiguous row, attempt re-attribution to a single surgery episode by (a) same `procedure_normalized` match, (b) date proximity ≤7 days. Rows still ambiguous after re-attribution → queue with `issue_id='OP03'`. Build `manuscript_workspace.canonical_operative_procedure_codes_v1_relinked`.
- **Status**: pending

### OP04 — procedure code rows with NULL `linked_surgery_episode_id`
- **Observed**: 11,134 rows / 3,611 pts (1,289 PTC).
- **Fix**: Patch into the relink view from OP03. For rows that can't be linked to any surgery episode (no candidate surgery in same patient), flag as `orphan_procedure_code_flag=TRUE`. Cohort queries should exclude orphans unless explicitly joined.
- **Status**: pending

### OP05 — path_malignant `surgery_episode_id` not matching operative namespace
- **Table/col**: `main.canonical_path_malignant_events_v1.surgery_episode_id`
- **Category**: linkage / schema
- **Severity / scope**: critical / schema
- **Observed**: 5,254 rows / 3,220 pts (2,624 PTC) where path non-null `surgery_episode_id` has no operative match — because path holds patient-local ordinals (3 distinct values), operative holds global IDs (11,773 distinct).
- **Fix** (supersedes PATH01's MD5 approach):
  1. Create view `manuscript_workspace.canonical_path_malignant_events_v1_keyed` that LEFT JOINs path to `main.canonical_operative_events_v1` on `(research_id, surgery_date = resolved_surgery_date)`.
  2. Add columns:
     - `surgery_episode_uid_global`: the operative table's global ID when matched.
     - `surgery_episode_uid_fallback`: `md5(research_id || '|' || CAST(surgery_date AS VARCHAR))` when no operative match.
     - `surgery_episode_uid`: `COALESCE(surgery_episode_uid_global, surgery_episode_uid_fallback)`.
     - `surgery_episode_uid_source` ∈ {`operative_match`, `md5_fallback`, `unknown_no_date`}.
  3. Emit rows with `surgery_episode_uid_source='md5_fallback'` to queue with `issue_id='OP05'`; these are pathology records for surgeries the operative table doesn't know about. Chart review required before including in staging analyses.
- **Status**: pending (critical — blocks nearly all path-surgery joins)

### GEN13 — assay → molecular canonical match fails
- **Observed**: 9,267 rows / 9,250 pts (2,737 PTC). Assay `molecular_episode_id` values are ordinals 1/2/3 and do not correspond to `canonical_molecular_genetics_v2.molecular_episode_id` values (also ordinals but not synchronized across the two tables).
- **Fix**: After GEN01 (assign `molecular_episode_uid` on canonical_molecular_genetics_v2) and a parallel pass on `specimen_genomic_assay_v1` using the same MD5 formula with the same inputs available in the assay table (`research_id`, `resolved_test_date`, `platform`, `platform_version`), LEFT JOIN on the shared UID. Gap rows → queue with `issue_id='GEN13'`.
- **Status**: pending (blocked on GEN01)

### GEN14 — assay `surgery_episode_id` non-null but no op match
- **Observed**: 311 rows / 223 pts (105 PTC).
- **Fix**: Same `(research_id, surgery_date ≈ resolved_test_date or tied surgery)` join used in OP05 to map assay's local surgery ordinal to the global op ID. Unmatched → queue with `issue_id='GEN14'`.
- **Status**: pending

### GEN15 — molecular `linked_fna_episode_id` non-null but no FNA match
- **Observed**: 360 rows / 347 pts (122 PTC).
- **Fix**: Rebuild `linked_fna_episode_uid` on the canonical molecular table by joining to `main.canonical_fna_events_v1` on `(research_id, fna_index = linked_fna_episode_id)`. Unmatched rows → queue with `issue_id='GEN15'`. Do NOT silently null — flag so analyst can see which molecular tests have broken FNA provenance.
- **Status**: pending

### GEN16 — `braf_flag=TRUE` but `braf_variant` NULL
- **Observed**: 180 rows / 175 pts (115 PTC) — the 115 PTC number is clinically significant because BRAF is the dominant PTC driver.
- **Fix**:
  1. Re-extract from raw reports where available: look for `V600E`, `K601E`, `p\.V600E`, `c\.1799T>A` patterns in `platform_raw`, `test_result_summary`, `report_text_ref`.
  2. Populate `braf_variant_reparsed` and `braf_variant_source` ∈ {`existing`, `reparsed`, `flag_without_variant`}.
  3. Rows that remain `flag_without_variant=TRUE` → queue with `issue_id='GEN16'`. In cohort_v2, require `braf_variant` non-null for any BRAF-stratified analysis.
- **Status**: pending

### REC04 — recurrence after last-known-alive
- **Observed**: 2 rows / 2 pts (both PTC).
- **Fix**: Queue with `issue_id='REC04'`. Low volume but date-integrity issue — either `last_known_alive_date` is stale or `recurrence_date` is mis-entered. Chart review.
- **Status**: pending (chart review)

### REC05 — structural recurrence flag TRUE but site NULL
- **Observed**: 1,818 rows / 1,818 pts (1,420 PTC). Source composition:
  - `extracted_recurrence_refined_v1` + `structural_date_unknown`: 1,764
  - `extracted_recurrence_refined_v1` + `structural_confirmed`: 54
- **Fix — this is a manuscript-critical completeness problem**:
  1. For the 54 `structural_confirmed` rows, the site extraction pass failed — re-run a targeted LLM site extraction over the source note text (prioritize these first; they are confirmed structural events with missing sites).
  2. For the 1,764 `structural_date_unknown` rows, the upstream flag is firing on weak evidence. Review the source rule — if the date is unknown AND the site is unknown, the `structural_recurrence_flag=TRUE` assertion is almost certainly too liberal. Consider downgrading these to `structural_recurrence_possible_flag` and leaving the strict flag TRUE only when either date or site is known.
  3. Queue all 1,818 rows with `issue_id='REC05'` and the source-rule tag in `context_json`.
- **Status**: pending (needs both LLM re-extraction + upstream rule revision)

### SPEC01 — specimen scaffold passes integrity checks (CLOSED)
- **Tables**: `main.specimen_master_v1`, `main.specimen_tumor_focus_v1`, `main.specimen_source_xref_v1`.
- **Evidence**: 10,139 rows / 10,139 distinct `specimen_id` in master; 11,103 rows / 11,103 distinct `specimen_focus_id` in tumor focus; 0 path malignant rows with non-null `specimen_id` missing from master; 0 with non-null `specimen_focus_id` missing from focus; 0 tumor-focus rows missing from master.
- **Status**: closed (no fix needed; treat as ready to receive relinks from assay/molecular layers)

---

## Run log

- **2026-04-22 17:48** — backup snapshot captured (`thyroid_2026_full_backup_20260422_174849.duckdb`, 323 MB).
- **2026-04-22 PM** — Logan fed batch #6: FNA02-05, IFNA01-06, OP01-05, GEN13-16, REC04-05, SPEC01 clean; revised counts on FNA01, PATH01, PATH03, PATH17, REC01, GEN02, GEN08. Key reframing: (a) PATH01 fix changes from MD5 to global op namespace mapping (OP05); (b) `imaging_fna_linkage_v3` is a linker design defect requiring full rebuild rather than overlay; (c) specimen scaffold is clean (SPEC01).
