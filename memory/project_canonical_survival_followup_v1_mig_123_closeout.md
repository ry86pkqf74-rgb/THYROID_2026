# canonical_survival_followup_v1 — mig_123 close-out

**Date:** 2026-04-29 (UTC)  
**Migration:** `qc_framework_v1/migrations/123_canonical_survival_followup_v1_signoff.sql`  
**Builder SSOT:** `scripts/364B_survival_followup_consolidation.py`

## Verification summary

- **Grain:** One row per `canonical_patient_master` patient (10,871 / 10,871).
- **Re-derivation:** Full `_build_sql()` replay against MotherDuck with `note_entities_llm_survival_followup` staged from `scripts/output/parquet/main/note_entities_llm_survival_followup.parquet` (11,037 rows) because `main.note_entities_llm_survival_followup` is absent on the current publication catalog.
- **Drift:** 8 columns exact match across all patients; `last_followup_source` had **2** label-only mismatches (rids **7775**, **8222**) with **identical** `last_known_alive_date` — tied MAX lab dates across `canonical_labs_pth_v1` vs `canonical_labs_vitamin_d_v1`; Script 364B uses `ANY_VALUE(source)` in `latest_lab_date`.
- **Operative cross-check:** `first_surgery_date` matches `MIN(surgery_date_native)` from `canonical_operative_events_v1` — **0** mismatches.
- **Internal consistency:** `deceased` ⇒ `death_date` populated; `alive` ⇒ `last_known_alive_date` populated; vital enum clean; completeness flags align with Script 364B `YEAR_5_DAYS` / `YEAR_10_DAYS` thresholds.

## Registry

- **9** adjudicated columns → verified (+ **4** na: `research_id`, `build_ts`, `build_script`, `extraction_run_id`).
- **Table** `canonical_survival_followup_v1`: `verified`.

## Carry-forwards

| ID | Meaning |
|----|--------|
| CF-mig123-SURVIVAL-LLM-STAGING-ABSENT-ON-MD | Restore or attach `note_entities_llm_survival_followup` on publication DB before next `--commit` replay without local parquet staging. |
| CF-mig123-LAST-FOLLOWUP-SOURCE-ANY-VALUE | Optional: replace `ANY_VALUE(source)` with `ARG_MAX` for deterministic `last_followup_source` under tied lab dates. |

## Lane 13

`canonical_ete_event_resolved_v1` columns `last_known_alive_date` / `vital_status`: **mig_123** sets `verification_method` to `derivation_re_derivation_post_survival_followup_v1_verified` and appends close-out notes ( **`CF-mig121-ETE-EVENT-RESOLVED-SURVIVAL-PENDING`** narrative superseded). TIMESTAMP/DATE bridge on the ete layer remains per **CF-100** / mig_121 until any view DDL refresh.
