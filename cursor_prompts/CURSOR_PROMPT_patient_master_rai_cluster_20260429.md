# Cursor Agent Task — `canonical_patient_master` RAI CLUSTER slice (Protocol v2)

**Generated:** 2026-04-29 (Cowork session, post-mig_136 PMH+PSH landing)
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** at handoff time — `git fetch && git pull` before starting
**Estimated effort:** 3-4 hours (~51 cols — biggest lane in this batch)
**Run order:** Lane 31 of new 4-prompt batch (mig_142)

---

## 1. Goal

Continue patient_master verification with the **radioactive iodine (RAI) cluster** (~51 unverified cols covering RAI eligibility, episodes, dose, dates, intent, source confidence, scan findings, post-RAI Tg, and stimulated TSH/Tg labs).

Probe scope:

```sql
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_catalog='thyroid_canonical_publication_v1_0' AND table_schema='main'
  AND table_name='canonical_patient_master'
  AND (column_name ILIKE '%rai%' OR column_name ILIKE '%radioactive%' OR column_name ILIKE 'i131%')
  AND column_name NOT IN (
    SELECT column_name FROM main.canonical_column_verification_registry_v1
    WHERE table_name='canonical_patient_master' AND verification_status <> 'not_started'
  )
ORDER BY column_name;
```

Confirm count is **exactly 51** before proceeding.

Sub-clusters:

- **RAI episode counts + eligibility** (~6 cols): `n_rai_episodes`, `confirmed_rai_episodes`, `rai_eligible_flag`, `rai_received_flag`, `rai_received_reconciled`, `rai_validation_tier`
- **Dates + days-from-surgery** (~9 cols): `rai_first_date`, `rai_first_days_from_surg`, `rai_first_episode_date`, `rai_first_episode_days_from_surg`, `rai_last_episode_date`, `rai_last_episode_days_from_surg`, `rai_episode_date_span_days`, `rai_date_confidence`, `rai_date_source`, `gm_rai_date_confidence`, `gm_rai_date_source`
- **Dose chain** (~10 cols): `rai_max_dose_mci`, `rai_min_dose_mci`, `rai_total_cumulative_dose_mci`, `rai_cumulative_dose_extreme`, `rai_dose_v9`, `rai_dose_confidence_worst`, `rai_dose_source`, `rai_max_dose_source`, `rai_dose_data_available`, `rai_dose_linkage`, `rai_n_episodes_with_dose`
- **Intent + adjudication** (~7 cols): `rai_intent_v9`, `rai_intent_list`, `rai_n_distinct_intents`, `rai_assertion_statuses`, `rai_has_adjudication`, `rai_has_completion_status`, `rai_flag_discordant`
- **Avidity + scan findings** (~5 cols): `rai_avid_flag`, `rai_avidity`, `rai_scan_findings_v9`, `nucmed_has_rai_scan`, `benign_rai_suspect_malignant`
- **Post-RAI labs + stimulated** (~7 cols): `post_rai_tg_count`, `post_rai_tg_last`, `post_rai_tg_nadir`, `rai_stimulated_tg`, `rai_stimulated_tsh`, `radtx_nlp_rai_ablation`, `radtx_nlp_rai_ablation_n_mentions`
- **NLP RAI detail** (~4 cols): `nlp_raidetail_has_data`, `nlp_raidetail_key_finding`, `nlp_raidetail_n_entities`, `nlp_raidetail_n_notes`

---

## 2. Methodology — derivation re-derivation against RAI canonical chain (verify upstream first)

### 2a. ⚠️ Upstream dependency check (FIRST STEP)

**Before any flips:** verify the RAI upstream canonical exists and is registered. Likely candidates:
- `canonical_rai_treatments_v1` or similar (check `information_schema.tables WHERE table_name LIKE '%rai%'`)
- `manuscript_workspace.rai_episodes_*` helper tables
- `note_entities_llm_rai_detail` (Tier 1 source)

If the SSOT canonical is **not yet verified**, STOP and report. Do not derivation-flip against an unverified upstream — open `CF-mig142-RAI-UPSTREAM-PENDING` and pause.

If the SSOT canonical IS verified, proceed with derivation re-derivation.

### 2b. Per-col derivation map (representative)

- `n_rai_episodes` → COUNT(DISTINCT episode_id) per pt from rai canonical
- `confirmed_rai_episodes` → COUNT WHERE finding_status='confirmed' per pt
- `rai_received_flag` → BOOL: any episode exists per pt
- `rai_received_reconciled` → BOOL: cross-source reconciled (chart + ICD + nucmed scan)
- `rai_first_date` / `rai_last_episode_date` → MIN/MAX date per pt; **MUST be DATE** per `feedback_clinical_dates_calendar_only.md`
- `rai_first_days_from_surg` → `(rai_first_date - first_surgery_date)` calendar days
- `rai_episode_date_span_days` → MAX - MIN per pt
- `rai_total_cumulative_dose_mci` → SUM(dose_mci) per pt
- `rai_max_dose_mci` / `rai_min_dose_mci` → MAX / MIN per pt
- `rai_cumulative_dose_extreme` → BOOL: total > extreme threshold (clinically: >600 mCi cumulative) — check existing PM build SSOT for threshold
- `rai_intent_v9` → version-pinned categorical (ablation / adjuvant / treatment / other); from RAI canonical's intent col
- `rai_intent_list` → STRING_AGG(DISTINCT intent) per pt; use list_sort for set-equal probes (per `project_medications_parathyroid_families_complete_2026-04-29.md`)
- `rai_avid_flag` → BOOL: any episode with avid scan
- `rai_avidity` → categorical (avid / non-avid / mixed)
- `rai_stimulated_tg` / `rai_stimulated_tsh` → from labs canonical (Tg / TSH) WHERE collected within RAI stimulation window; verify Lane 25 labs scope didn't already cover these
- `nlp_raidetail_*` → Tier 1 LLM provenance; passthrough verification

### 2c. ⚠️ Logan-ratified 2-digit year convention (RAI especially relevant)

Per `reference_2digit_year_convention.md`: 2-digit YY → 20YY (00=2000, 25=2025). RAI dates often parsed from VARCHAR; check that any 2-digit-year fields resolve to 20YY not 19YY.

### 2d. ⚠️ Cohort-uniformity sanity check (CRITICAL)

For every BOOLEAN col flipped: §2d sweep. Expected non-zero TRUE counts:
- `rai_received_flag` should be ~30-50% of cohort (typical thyroid cancer RAI rate)
- `rai_avid_flag` ~30-40% (subset of received)
- `nucmed_has_rai_scan` similar to received_flag
- `benign_rai_suspect_malignant` rare but should be > 0
- `rai_flag_discordant` should be > 0 (intent-vs-receipt mismatch)
- `rai_dose_data_available` should be ~70-90% of received_flag pts

If any TRUE-count is 0 across 10,871 → degenerate. Type-classify per §8.2 of handoff.

### 2e. ⚠️ NULL vs 0 dose semantics

`rai_total_cumulative_dose_mci` is NULL for non-recipients, not 0. COALESCE(0) is appropriate ONLY if the analyst is computing cohort-wide averages with non-recipients counted as zero — otherwise NULL is correct.

### 2f. Sign-off SQL

File: `qc_framework_v1/migrations/142_patient_master_rai_cluster_signoff_20260429.sql`

```
batch_id = 'mig_142_patient_master_rai_cluster_20260429'
verification_method options:
  - 'derivation_vs_canonical_rai_<canonical_name>_v1'
  - 'derivation_vs_canonical_labs_tg_v1' (post_rai_tg_*)
  - 'derivation_vs_canonical_labs_tsh_v1' (rai_stimulated_tsh)
  - 'patient_level_aggregate_rai_per_episode'
  - 'extraction_faithfulness_vs_note_entities_llm_rai_detail'
  - 'auto_provenance_skip' (gm_rai_*, source/script cols)
```

---

## 3. Acceptance gates

- ~51 RAI-cluster cols flipped (or report scope adjustment if upstream pending)
- 0 drift on derivation re-derivation
- Cohort-uniformity sweep run on every BOOLEAN
- All RAI date cols are DATE; CF rows for any TIMESTAMP/VARCHAR
- gate 4 = 0
- PM `n_verified` advances by the cluster count

---

## 4. Don't touch (active parallel lanes)

- MOLECULAR — Lane 27 (mig_137, scope ~3 cols)
- RECURRENCE-RESPONSE — Lane 28 (mig_138, scope ~4 cols)
- ETE — Sibling Lane 29 (mig_140)
- SURVIVAL — Sibling Lane 30 (mig_141)
- SMALL-CLUSTERS bundle — Sibling Lane 32 (mig_143)

---

## 5. Reference reading

Required:
- Auto-memory: `feedback_clinical_dates_calendar_only.md`
- Auto-memory: `feedback_motherduck_direct_check.md`
- Auto-memory: `feedback_recurrence_imaging_n_events_null.md`
- Auto-memory: `reference_2digit_year_convention.md`
- Auto-memory: `project_lab_consolidation_script_347_closeout.md` (Tg labs SSOT for post_rai_tg_*)
- Auto-memory: `project_lab_ingestion_refactor_script_348_closeout.md`
- Auto-memory: `project_medications_parathyroid_families_complete_2026-04-29.md` (STRING_AGG ordering pattern)
- Repo: `cursor_prompts/CURSOR_PROMPT_patient_master_lymph_node_cluster_20260429.md` (template structure)

---

## 6. File / commit conventions

Same as siblings.

---

## 7. If something unexpected surfaces

- **RAI upstream canonical not verified** → STOP and report; do NOT proceed with derivation flips. Open `CF-mig142-RAI-UPSTREAM-PENDING`
- `rai_dose_v9` / `rai_intent_v9` / `rai_scan_findings_v9` version-pinned — if v9 is stale and a v10 exists, document precedence
- `rai_cumulative_dose_extreme` threshold undocumented → STOP, ask Logan
- Dose data drift > 10% (some pts have wildly different dose totals between RAI canonical and PM) → STOP, ask Logan; could indicate stale build
- More than 5 cols can't be cleanly mapped → STOP, ask Logan

---

End of prompt. Lane 31 of 4-prompt batch (target: PM `n_verified` 830 → 881).
