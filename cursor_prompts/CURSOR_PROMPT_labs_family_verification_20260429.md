# Cursor Agent Task — Labs Family Verification (Protocol v2)

**Generated:** 2026-04-29 (Cowork session)
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** at handoff time — `git fetch && git pull` before starting
**Estimated effort:** 60-90 minutes (5 small tables, same shape)
**Run order:** Lane 8 (run first of new 3-prompt batch — smallest/easiest)

---

## 1. Goal

Verify the **5 lab canonicals** under Protocol v2 in one batch:

| Table | Rows | Patients | not_started cols |
|---|---:|---:|---:|
| canonical_labs_thyroglobulin_v1 | 53,006 | 3,124 | 11 |
| canonical_labs_calcium_v1 | 187 | 166 | 9 |
| canonical_labs_pth_v1 | 200 | 184 | 9 |
| canonical_labs_tsh_v1 | 556 | 449 | 9 |
| canonical_labs_vitamin_d_v1 | 86 | 82 | 9 |

All 5 share the same shape (research_id, lab_datetime, value_raw, value_numeric, is_censored, value_correction_note, unit_standardized, source, is_in_canonical_cancer_cohort, ingestion_date). Tg has 2 extra cols (analyte, assay_method).

Closes 5 Tier 2 canonicals in one push.

---

## 2. Methodology — structured-source compare + normalizer faithfulness

Pattern: **mig_85 surgery_date** style (mechanical_source_compare). Find the build script + upstream EHR source for each lab; compare canonical value to expected post-normalizer value.

### 2a. Find build SQL
Per `project_lab_consolidation_script_347.md` memory: 5 lab canonicals were built by Script 347 (close-out 2026-04-21 at 4d3c4fc). Normalizer at `qc_framework_v1/_lab_value_normalizer.py` (45 tests). Script 113 was FROZEN pending Script 348.

Find:
- `grep -rn "canonical_labs_thyroglobulin_v1" scripts qc_framework_v1 | head`
- Likely `scripts/347_*.py` or `scripts/348_*.py`

### 2b. Per-table verification — apply this template to each of 5 tables
- Re-derive value_numeric from value_raw via `_lab_value_normalizer.py` and compare per-row
- Verify is_censored flag matches detection of `<` / `>` prefixes in value_raw
- Verify unit_standardized is in expected vocab (e.g. ng/mL, mg/dL, mIU/L, pmol/L)
- Verify source enum is consistent (e.g. structured_ehr_tg, structured_ehr_calcium, etc.)
- Sanity check value_numeric ranges per analyte:
  - Tg: 0-10000 ng/mL (post-thyroidectomy <0.2 expected)
  - Ca: 6-15 mg/dL
  - PTH: 1-1000 pg/mL
  - TSH: 0.01-100 mIU/L
  - Vit D: 1-300 ng/mL

### 2c. Date type policy (lab_datetime)
**lab_datetime is TIMESTAMP — this is appropriate for labs** (real timestamps capture morning vs evening draws). Do NOT retype to DATE. Per Logan's `feedback_clinical_dates_calendar_only.md` memory: clinical event dates (surgery, fna, path, frozen_section) are DATE; lab measurement timestamps are TIMESTAMP. Confirm with Logan if any lab table has lab_datetime suspiciously truncated to 00:00:00.

### 2d. Sign-off SQL
Single migration `qc_framework_v1/migrations/<N>_labs_family_signoff.sql` with 5 sub-blocks (one per lab table), each:
- a: flip not_started cols via `verification_method='structured_source_compare_with_normalizer'`
- b: recompute counts + flip table_status

---

## 3. Acceptance gates

For each of 5 tables:
- All not_started cols flipped to verified
- table_status='verified'
- value_raw → value_numeric round-trip via normalizer matches 100% (or document drifts as CFs)
- is_censored flag matches `<` / `>` prefix detection
- unit vocab clean per analyte

---

## 4. Don't touch (active parallel lanes)

- `canonical_ete_subgrade_events_v1` / `canonical_ete_subgrade_patient_rollup_v1` — Cowork's lane
- Any table touched by sibling Cursor lanes 9 + 10 (molecular, US v2)

---

## 5. Reference reading (auto-memory)

Required:
- `project_lab_consolidation_script_347.md` — build context
- `project_lab_ingestion_refactor_script_348.md` — ingestion refactor close-out
- `feedback_clinical_dates_calendar_only.md` — lab_datetime exception
- `feedback_motherduck_direct_check.md`
- `feedback_surgical_git_add.md`

Repo:
- `qc_framework_v1/_lab_value_normalizer.py` — the normalizer + tests
- `qc_framework_v1/migrations/106_parathyroid_patient_rollup_signoff.sql` — multi-section template

---

## 6. File / commit conventions

- Author: `Logan Glosser <logan.glosser@gmail.com>`
- Surgical `git add` (memory: `feedback_surgical_git_add.md`)
- DuckDB CAST CURRENT_TIMESTAMP AS TIMESTAMP for any new ts values
- Single commit closing all 5 lab tables

---

## 7. If something unexpected surfaces

- Normalizer round-trip diff > 1% on any analyte → STOP, investigate. Could indicate post-build edits or normalizer drift.
- value_raw with format the normalizer doesn't handle → flag as CF; do not block sign-off if affects <0.5% rows.
- Cohort scope unexpected (e.g. labs_tg has 53k rows but only 3,124 patients — large per-patient series; expected for surveillance Tg) — confirm against memory + sample.

---

End of prompt. Lane 8 of new 3-prompt batch. Closes 5 Tier 2 lab canonicals. Update `MEMORY.md` with brief close-out entry.
