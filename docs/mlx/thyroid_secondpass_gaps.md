# THYROID_2026 — Second-pass Gap & Verification Analysis

**Companion to** `docs/mlx/thyroid_mlx_extraction_gaps.md`. The first pass identified what to **extract**. This second pass identifies what to **verify, re-extract, and reconcile** — and surfaces several findings larger than anything in the first pass.

## TL;DR — three findings that matter more than the original ones

1. **Verification status is 100% NULL across all 76,641 LLM-extracted entities** in the `note_entities_*` tables. No adjudication pass has ever been run. The provenance schema is there; the data isn't.
2. **Date confidence is also 100% NULL** for those same 76,641 rows. We have no signal at all on date-attribution reliability — yet every survival, recurrence-interval, and complication-timing analysis depends on it.
3. **Several `note_entities_llm_*` tables are 80–97% empty or error-filled** — including dynamic risk response (97.7% empty), recurrence (92.3% empty), and synoptic pathology enrichment (81.5% empty). And the "empty" bucket isn't just LLM-correctly-returning-no-data — it includes JSON parse errors, context-overflow errors, and auth failures, which are pipeline bugs masquerading as null extractions.

These three findings together mean: the existing extracted data has no reliability signal, and a non-trivial fraction of "extracted" notes were never actually extracted because of silent pipeline failures.

---

## Tier 0 — cross-cutting verification debt (highest leverage)

### 0.1 Verification status: 100% NULL across all `note_entities_*` tables

| Table | Rows | Verified | NULL | % verified |
|---|---|---|---|---|
| `note_entities_procedures` | 21,942 | 0 | 21,942 | 0.0% |
| `note_entities_operative_detail` | 20,715 | 0 | 20,715 | 0.0% |
| `note_entities_problem_list` | 11,579 | 0 | 11,579 | 0.0% |
| `note_entities_complications` | 9,359 | 0 | 9,359 | 0.0% |
| `note_entities_medications` | 7,501 | 0 | 7,501 | 0.0% |
| `note_entities_staging` | 3,807 | 0 | 3,807 | 0.0% |
| `note_entities_genetics` | 1,738 | 0 | 1,738 | 0.0% |
| **Total** | **76,641** | **0** | **76,641** | **0.0%** |

The `verification_status` column was designed exactly for the two-model adjudication pattern your `extracted_ete_subgraded_v1` and `ete_adjudication_v1` tables already implement for ETE. It just hasn't been populated for these.

**Action:** run a `r1-distill-70b` adjudication pass over the existing extractions, populating `verification_status` ∈ {agreed, disagreed, primary_only, both_failed}. Two-model agreement on the same source text. Estimated cost: 76,641 calls × ~2s = 43 hours total on M5 Max. Run as nightly batch over 2 weeks.

**Priority subset (high-stakes entity types, run first):**
- `note_entities_operative_detail`: `gross_invasion` (16), `tracheal_involvement` (16), `esophageal_involvement` (2), `rln_signal_status` (50), `tracheostomy` (38), `intraop_complication` (27)
- `note_entities_staging` (3,807 — all of it)
- `note_entities_complications` (9,359 — affects every survival paper)
- `note_entities_genetics` (1,738 — molecular cohort definitions)

### 0.2 Date confidence: 100% NULL across same tables

Every row has `date_confidence IS NULL`. So when downstream rollups bin events into "≤6mo postop" vs ">12mo postop" (e.g., transient vs permanent hypoparathyroidism), they're using extracted dates with **no reliability signal**.

**Action:** as part of the adjudication pass above, have the second-pass model emit a `date_confidence` ∈ [0,1] per entity. Cases where date_confidence < 0.5 get routed to chart review or get their event flagged as "approximate timing only" in downstream rollups.

### 0.3 Silent pipeline failures inside `note_entities_llm_*` "empty" rows

Sample audit of `note_entities_llm_dynamic_risk_response.result_json` (11,037 rows, 251 with content, 10,786 "empty"):

| Result_json content | Count | Interpretation |
|---|---|---|
| `{"entities": []}` | 9,993 | LLM correctly returned no entities |
| `{"entities":[]}` | 415 | Same (whitespace variant) |
| `{"error": "Expecting value: line 1 column 1 (char 0)"}` | 103 | **JSON parse failure — model output malformed** |
| `{"error": "Input text is empty or not provided."}` | 19 | Source text was empty |
| `{"error": "Invalid syntax"}` | 18 | Pydantic validation failure |
| `{"error": "Invalid JSON format"}` | 11 | Same |
| `{"error": "JSON parse error: ..."}` | 8 | Same |
| `{"error": "The input text is too long to process. Please provide a shorter text."}` | 6 | **Context window overflow — chunking missing** |
| `{"error": "No JSON content found in the provided text."}` | 6 | Model didn't emit JSON |
| `{"error": "invalid_grant"}` | 5 | **Cloud LLM auth failure** |

187 of the 10,786 "empty" rows are actually pipeline errors, not legitimate "no extraction found". That's a 1.7% silent-failure rate on this specific table — almost certainly under-counted because the visible errors are only what makes it into result_json. Errors that crashed the wrapper before write may be missing entirely.

**Action:** re-run extractions for any row where `result_json` matches `{"error": ...}`. Use a more capable model (`llama33-70b` or `medgemma27b`) with proper chunking via `utils/chunk.py`. Audit similar error patterns in all `note_entities_llm_*` tables. Add an error-class column so failures are queryable.

### 0.4 LLM-table emptiness rate at corpus scale

| Table | Rows | Has content | % empty/error |
|---|---|---|---|
| `note_entities_llm_tirads_granular` | 10,084 | 10,084 | **0%** — working correctly |
| `note_entities_llm_pathology` | 10,084 | 4,670 | **53.7%** |
| `note_entities_llm_survival_followup` | 11,037 | 3,328 | **69.8%** |
| `note_entities_llm_functional_outcomes` | 11,037 | 2,132 | **80.7%** |
| `note_entities_llm_synoptic_pathology_enrichment` | 11,037 | 2,041 | **81.5%** |
| `note_entities_llm_us_nodule_dynamics` | 11,037 | 1,432 | **87.0%** |
| `note_entities_llm_recurrence` | 11,037 | 846 | **92.3%** |
| `note_entities_llm_dynamic_risk_response` | 11,037 | 251 | **97.7%** |

These are not all "true" misses. Some fraction is legitimate (the LLM correctly found nothing in a note that didn't contain the target info), but at 80–97% empty rates the more likely explanation is prompt mismatch + silent error rate + base-model capability gaps. With `medgemma27b` + tighter prompts these should drop to 40–60% empty.

**Action:** re-run the four worst tables (`dynamic_risk_response`, `recurrence`, `us_nodule_dynamics`, `synoptic_pathology_enrichment`) with the harness's `medgemma27b` or `llama33-70b` primary + improved prompts. Treat the old outputs as a baseline for delta-F1 reporting.

---

## Tier 1 — targeted verification opportunities

### 1.1 Recurrence event quality

`recurrence_event_clean_v1` profile:

| Field | Filled (of 1,946) | % |
|---|---|---|
| `recurrence_type` | 1,946 | 100% |
| `structural_recurrence_flag` | 1,818 pos | 93% |
| `biochemical_recurrence_flag` | 128 pos | 6.6% |
| `recurrence_site` | **0** | **0%** |
| `recurrence_date` | **182** | **9.4%** |
| `source_priority = 1` | 0 | 0% |

Three concrete issues:
- **`recurrence_site` is completely empty** (despite the column existing). Site (cervical local / cervical nodal / lung / bone / brain / other) is essential for site-stratified survival. Extract from the same notes that generated the event.
- **Only 9.4% have a recurrence_date.** Survival analysis is using imputed or join-derived dates for 91% of events. Time-to-recurrence is silently unreliable.
- **Biochemical recurrence at only 6.6%** of structural-positive events is implausible — biochemical recurrence usually precedes structural detection. Likely under-extraction from Tg/anti-Tg patterns in the lab table.

**Action:** dedicated LLM pass over the source notes that fed `recurrence_event_clean_v1`, schema requiring `site`, `date`, `date_confidence`. Then run the rule-based biochemical detector over `canonical_labs_thyroglobulin_v1` (Tg trajectory analysis, anti-Tg interference, post-treatment Tg threshold) to flag patients with biochemical events the LLM missed.

### 1.2 ETE event-resolved disagreement queue

`canonical_ete_event_resolved_v1`: 6,689 tumors / 4,137 patients

| Flag | Count | % |
|---|---|---|
| `pm_disagreement_flag` (patient-master vs event-level) | 356 | 5.3% |
| `t_stage_discordance_flag` (reported vs derived AJCC8) | 207 | 3.1% |
| `open_self_contradiction_flag` (within a single tumor) | 9 | 0.1% |
| `is_unresolved_ete` (ambiguous) | 0 | 0% |

**572 distinct adjudication candidates.** These already have multi-source provenance (`mig54_fresh_llm_ete_grade`, `inline_patient_grade`, `inline_event_grade`, `legacy_gross_ete_effective`); they just haven't had a final reconciliation pass. Audit query result showed that on the 356 `pm_disagreement` cases both visible grade columns read "microscopic" — meaning the disagreement is at a deeper level (probably the AJCC stage interpretation or evidence-quote diff), which warrants closer inspection.

**Action:** queue these 572 cases for a Llama 3.3 70B + R1 distill two-model pass. Output: `ete_final_adjudicated_v2` with a single resolved grade plus the reasoning.

### 1.3 Adopt the ETE event-resolved pattern for other path features

The ETE table is the gold standard for multi-source reconciliation. Replicate this for:

- **Capsular invasion** (1,243 / 11,688 = 11% filled in synoptic; far higher rate expected from notes)
- **Mitotic count per 2mm²** (713 / 11,688 = 6% filled; clinical guidelines need this for FT-NIFTP and high-grade categorization)
- **Ki-67 labeling index** (18 / 11,688 = 0.15% filled)
- **Perineural invasion** (1,508 / 11,688 = 13% filled)
- **Angioinvasion vessel quantification** (310 / 11,688 = 3%)
- **Extranodal extension** (1,374 / 11,688 = 12%)

Each should produce a `canonical_<feature>_event_resolved_v1` table with the same column pattern: `<feature>_grade`, `<feature>_source` (which source produced it), `inline_evidence`, `pm_disagreement_flag`, `self_contradiction_flag`, `unresolved_flag`. Then a `canonical_<feature>_patient_rollup_v1` aggregates per patient.

### 1.4 Numeric outlier and sentinel sweep

`path_synoptics.tumor_1_size_greatest_dimension_cm` (STRING column, 4,181 populated):

| Bucket | Count |
|---|---|
| Parseable as FLOAT64 | 4,132 (98.8%) |
| `<0.1` / `<.1` / `<0.2` (censored) | 28 |
| `n/s`, `n/a`, `c/a` | 15 |
| Typos: `0..2`, `0..9`, `3/.3` | 3 |
| Long text: "Cannot be determined…", "n/s (involved nearly all thyroid parenchyma)", ">15" | 3 |
| **Unparseable total** | **49** |

p50 size = 1.6 cm; p99 = 9.0 cm; max = 15.0 cm — distribution looks clean for parseable values, no overflow sentinels.

**Action:** normalization pass — small LLM (MedGemma-4B with regex prefilter) converts censored values to canonical form (`<0.1` → `value_numeric=0.05, is_censored=TRUE`), typos to corrected numbers, narrative cells to a `size_not_determinable` flag. Extend pattern to all numeric STRING columns in `path_synoptics` (`weight_total`, the `*_g` gland weights, dimensions for tumors 2–5, parathyroid weights/sizes).

### 1.5 `survival_cohort_enriched` row count anomaly

61,134 rows for 10,871 unique patients = **~5.6 rows per patient.** Either it's tumor-level (with multifocal expansion) or event-level. Some 7,000 rows (12%) lack a `recurrence_risk_band` while having full molecular fields (every row has BRAF/TERT/RAS/RET status filled). That row-count vs patient-count gap is worth a manuscript audit — if anything joins to this table assuming patient-level rows, the numbers will be wrong by ~5×.

**Action:** confirm grain (tumor / surgery-episode / event), document in the data dictionary, add a row count assertion (`assert n_rows = n_distinct_patients × expected_grain_factor`) to the QC suite. If grain is unintentionally inflated, dedupe.

---

## Tier 2 — extraction expansions to queue next

### 2.1 Lab-based response-to-therapy classification

`canonical_labs_thyroglobulin_v1`: 64,493 rows across the cohort. Schema includes `lab_datetime`, `value_numeric`, `is_censored`, `value_correction_note`. With this, classify every malignant patient into ATA 2015 dynamic risk response categories: **excellent / indeterminate / biochemical incomplete / structural incomplete** — by combining Tg trajectory + anti-Tg + structural recurrence flag. Currently `survival_cohort_enriched.tg_annual_log_slope` exists for 39,674 / 61,134 (65%) rows; the categorical classification doesn't.

This is mostly rule-based, not LLM — derive from labs + recurrence + imaging. But LLM-level extraction of "intermediate" findings in nuclear-med reports (focal uptake, retrosternal, mediastinal) feeds into the structural-incomplete decision.

### 2.2 Anti-Tg interference flag

Tg interpretation is invalid in the presence of anti-Tg antibodies. Need a per-Tg-result flag `anti_tg_interference_present` derived from concurrent anti-Tg measurements + qualitative lab comments. Affects every Tg-trajectory paper.

### 2.3 Specimen-level molecular ↔ tumor reconciliation

`specimen_genomic_assay_v1`, `molecular_test_episode_v2`, `specimen_master_v1`, and the tumor-level `tumor_episode_master_v2` exist but the join graph isn't documented and the multi-tumor patients (multifocal) need clear specimen → tumor → histology attribution. Important for any "BRAF-positive tumor associated with X outcome" claim — without specimen-level binding, we attribute molecular findings to the wrong tumor.

### 2.4 Verification of `histology_1_*` rollup logic

`tumor_pathology.histology_1_dominant`, `histology_1_largest_tumor_cm`, `histology_1_n_stage_ajcc8` are derived columns. Verify their derivation against `path_synoptics` tumor-level data — pick a 200-row subset, recompute by hand, compare. Manuscripts using `histology_1_*` columns inherit any bug.

### 2.5 Operative-detail extension targets

`note_entities_operative_detail` has 21 entity types but missing several with clinical impact:
- **Neuromuscular blockade / muscle relaxant timing** (relevant for RLN monitoring reliability)
- **Energy device watt settings, ligature distance from RLN** (for procedural risk modeling)
- **Trainee involvement** (Cleveland Clinic vs trainee surgeons)
- **Re-do vs initial** (already partly captured in `reoperative_field` but only 132 rows)
- **Substernal extraction technique** (relevant for goiter manuscripts)

### 2.6 PMH / risk factor extension

`note_entities_problem_list` is 11,579 rows but doesn't seem to surface key thyroid-cancer risk modifiers:
- Prior radiation exposure (childhood neck XRT, body XRT for other cancers)
- Family history with degree of relatedness
- Other endocrine/MEN syndromes
- Concurrent autoimmune disease (Hashimoto, Graves)
- Iodine deficiency / sufficient region of origin

The original gap doc covered some of this. Worth tagging which `problem_list` rows fall into these categories vs just "thyroid disease" generic.

---

## Tier 3 — quick-win deterministic checks

These don't need an LLM at all — they're SQL assertions worth adding to your QC framework:

1. **Stage discordance:** `WHERE reported_t_stage_ajcc8 != derived_t_stage_ajcc8` — 207 cases known. Each is a manuscript-risk landmine.
2. **N-stage vs LN-positive consistency:** `WHERE n_stage_ajcc8 = 'N0' AND ln_total_positive_from_locations > 0` — flag any patient with N0 + positive nodes documented elsewhere.
3. **M-stage vs distant-mets imaging:** any patient with `m_stage_ajcc8 = 'M0'` AND CT/MRI/NM with distant met findings → flag.
4. **Recurrence dates vs surgery dates:** `recurrence_date < surgery_date` → impossible; flag for review.
5. **Lab value sanity:** Tg > 100,000 ng/mL, Ca < 4 or > 20 mg/dL, PTH < 0 — sentinel checks.
6. **Tumor focality vs tumor count:** `tumor_focality = "unifocal"` AND `num_tumors_identified > 1` → contradiction; flag.
7. **Cohort drift:** counts of malignant cohort, FNA cohort, US cohort across views. Surface any unexpected month-over-month delta > 1%.
8. **Date monotonicity:** for any patient, `fna_date ≤ surgery_date ≤ first_imaging_followup_date`.

These should be added as `pub_eval.qc_assertions_v1` rows. The existing assertion harness (`pub_eval` dataset) already exists — extend it.

---

## Recommended order of operations

1. **Run adjudication pass over the existing 76,641 `note_entities_*` rows** with `r1-distill-70b`. Populates `verification_status` and `date_confidence`. Single highest-leverage fix in the database.
2. **Re-extract the worst empty `note_entities_llm_*` tables** (dynamic_risk_response, recurrence, us_nodule_dynamics) with `medgemma27b` + better prompts + chunking. Audit the error-class column on the first 1,000 to confirm pipeline failures dropped.
3. **Build the four highest-priority `canonical_<feature>_event_resolved_v1` tables** (capsular_invasion, perineural_invasion, angioinvasion, extranodal_extension) using the ETE template.
4. **Normalize the 49 unparseable tumor sizes and similar numeric STRING fields** with a MedGemma-4B + regex hybrid.
5. **Adjudicate the 572 ETE-disagreement cases** (pm_disagreement + t_stage_discordance).
6. **Extract recurrence_site and recurrence_date for the 1,946 recurrence events**, plus biochemical-recurrence rule application across the Tg table.
7. **Add the Tier 3 deterministic QC assertions** to `pub_eval.qc_assertions_v1`.
8. **Document the `survival_cohort_enriched` grain** and verify no manuscripts are silently inflating event counts.

Each is implementable in the existing `tools/thyroid_mlx_extract/` harness — they map to new TaskSpec entries in `config.py` plus the four `canonical_*_event_resolved_v1` tables that mirror the ETE pattern.
