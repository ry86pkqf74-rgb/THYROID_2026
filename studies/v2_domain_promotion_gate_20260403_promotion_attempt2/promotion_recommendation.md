# V2 Domain Promotion Gate — FAIL

**Run label:** `20260403_promotion_attempt2`  
**Generated at:** `2026-04-03T09:51:08.408928+00:00`  
**Overall verdict:** `FAIL` (6 of 8 gates failed)

---

## Gate Scorecard

| Gate | Criterion | Status | Detail |
|------|-----------|--------|--------|
| G1 | Domain completeness (v2 only) | ❌ FAIL | Missing v2 canonical parquets: ['us_nodule_dynamics', 'frozen_section_detail'] |
| G2 | Schema compliance | ❌ FAIL | Schema failures: ['imaging', 'tirads_granular', 'labs', 'tg_kinetics', 'pathology', 'synoptic_pathology_enrichment', 'rai_detailed', 'rad_treatment', 'parathyroid_detail', 'recurrence', 'survival_followup', 'cervical_ln_detail', 'functional_outcomes', 'past_medical_hx', 'past_surgical_hx', 'presenting_symptoms', 'physical_exam', 'vascular_invasion', 'airway_invasion', 'dynamic_risk_response', 'patient_decision_adherence'] |
| G3 | Provenance columns | ❌ FAIL | NO domain has provenance columns (('preprocess_batch_id', 'preprocess_script_version', 'preprocessed_at_utc')). This is a structural gap in the extraction pipeline. |
| G4 | Duplicate rate | ❌ FAIL | Domains with >5% duplicates: [{'domain_name': 'labs', 'dup_rate': 0.122}, {'domain_name': 'tg_kinetics', 'dup_rate': 0.104}, {'domain_name': 'cervical_ln_detail', 'dup_rate': 0.0962}, {'domain_name': 'patient_decision_adherence', 'dup_rate': 0.0655}] |
| G5 | Date coverage (critical domains) | ✅ PASS | All critical domains have date coverage (entity_date or note_date) |
| G6 | Concordance floor (critical domains) | ✅ PASS | All critical domains meet 30% concordance floor |
| G7 | Unresolved discordance | ❌ FAIL | 2896 discordant rows in review queue — all require manual verification before promotion |
| G8 | MotherDuck v2_stage parity | ❌ FAIL | Parity failures: ['v2_stage.note_entities_llm_imaging', 'v2_stage.note_entities_llm_tirads_granular', 'v2_stage.note_entities_llm_labs', 'v2_stage.note_entities_llm_tg_kinetics', 'v2_stage.note_entities_llm_pathology', 'v2_stage.note_entities_llm_synoptic_pathology_enrichment', 'v2_stage.note_entities_llm_rai_detailed', 'v2_stage.note_entities_llm_rad_treatment', 'v2_stage.note_entities_llm_parathyroid_detail', 'v2_stage.note_entities_llm_recurrence', 'v2_stage.note_entities_llm_survival_followup', 'v2_stage.note_entities_llm_cervical_ln_detail', 'v2_stage.note_entities_llm_functional_outcomes', 'v2_stage.note_entities_llm_past_medical_hx', 'v2_stage.note_entities_llm_past_surgical_hx', 'v2_stage.note_entities_llm_presenting_symptoms', 'v2_stage.note_entities_llm_physical_exam', 'v2_stage.note_entities_llm_vascular_invasion', 'v2_stage.note_entities_llm_airway_invasion', 'v2_stage.note_entities_llm_dynamic_risk_response', 'v2_stage.note_entities_llm_patient_decision_adherence'] |

---

## Domain Inventory

| Domain | Tier | QA Tier | Parquet Exists | Canonical Output | Linkage Family |
|--------|------|---------|----------------|-----------------|----------------|
| staging | v1 | critical | ❌ | True | pathology |
| genetics | v1 | critical | ❌ | True | molecular |
| procedures | v1 | standard | ❌ | True | operative |
| operative_detail | v1 | standard | ❌ | True | operative |
| complications | v1 | critical | ❌ | True | operative |
| medications | v1 | standard | ❌ | True | followup |
| problem_list | v1 | informational | ❌ | True | demographics |
| llm | v1_debug | debug | ❌ | False | audit |
| imaging | v2 | standard | ✅ | True | imaging |
| tirads_granular | v2 | standard | ✅ | True | imaging |
| us_nodule_dynamics | v2 | standard | ❌ | True | imaging |
| labs | v2 | standard | ✅ | True | followup |
| tg_kinetics | v2 | standard | ✅ | True | followup |
| pathology | v2 | critical | ✅ | True | pathology |
| synoptic_pathology_enrichment | v2 | critical | ✅ | True | pathology |
| rai_detailed | v2 | critical | ✅ | True | rai |
| rad_treatment | v2 | standard | ✅ | True | rai |
| parathyroid_detail | v2 | standard | ✅ | True | operative |
| recurrence | v2 | critical | ✅ | True | followup |
| survival_followup | v2 | standard | ✅ | True | followup |
| cervical_ln_detail | v2 | standard | ✅ | True | pathology |
| functional_outcomes | v2 | informational | ✅ | True | followup |
| past_medical_hx | v2 | informational | ✅ | True | demographics |
| past_surgical_hx | v2 | informational | ✅ | True | demographics |
| presenting_symptoms | v2 | informational | ✅ | True | demographics |
| physical_exam | v2 | informational | ✅ | True | demographics |
| vascular_invasion | v2 | critical | ✅ | True | pathology |
| airway_invasion | v2 | standard | ✅ | True | operative |
| frozen_section_detail | v2 | standard | ❌ | True | operative |
| dynamic_risk_response | v2 | standard | ✅ | True | followup |
| patient_decision_adherence | v2 | informational | ✅ | True | followup |
| recurrence__sub | v2 | critical | ✅ | False | followup |
| complications__sub | v1 | critical | ✅ | False | operative |
| medications__sub | v1 | standard | ✅ | False | followup |
| operative_detail__sub | v1 | standard | ✅ | False | operative |
| operative_detail__sub | v1 | standard | ✅ | False | operative |
| parathyroid_detail__sub | v2 | standard | ✅ | False | operative |
| UNCLAIMED | unknown | unknown | ✅ | False | unknown |
| UNCLAIMED | unknown | unknown | ✅ | False | unknown |
| UNCLAIMED | unknown | unknown | ✅ | False | unknown |
| UNCLAIMED | unknown | unknown | ✅ | False | unknown |
| UNCLAIMED | unknown | unknown | ✅ | False | unknown |
| UNCLAIMED | unknown | unknown | ✅ | False | unknown |
| UNCLAIMED | unknown | unknown | ✅ | False | unknown |

---

## Per-Domain Validation Summary

| Domain | Rows | Patients | Schema OK | Dup Rate | Prov Cols | entity_date% |
|--------|------|----------|-----------|----------|-----------|--------------|
| imaging | 8,428 | 1,759 | ❌ | ✅ 3.19% | 0/3 | 71.7% |
| tirads_granular | 179 | 44 | ❌ | ✅ 2.23% | 0/3 | 60.3% |
| labs | 2,460 | 841 | ❌ | ❌ 12.20% | 0/3 | 71.1% |
| tg_kinetics | 173 | 61 | ❌ | ❌ 10.40% | 0/3 | 75.7% |
| pathology | 10,894 | 2,220 | ❌ | ✅ 4.31% | 0/3 | 70.0% |
| synoptic_pathology_enrichment | 38 | 8 | ❌ | ✅ 0.00% | 0/3 | 55.3% |
| rai_detailed | 3,747 | 650 | ❌ | ✅ 0.56% | 0/3 | 86.9% |
| rad_treatment | 580 | 213 | ❌ | ✅ 4.31% | 0/3 | 73.1% |
| parathyroid_detail | 255 | 118 | ❌ | ✅ 0.00% | 0/3 | 82.8% |
| recurrence | 303 | 143 | ❌ | ✅ 0.99% | 0/3 | 71.3% |
| survival_followup | 9,809 | 2,982 | ❌ | ✅ 0.01% | 0/3 | 82.6% |
| cervical_ln_detail | 104 | 48 | ❌ | ❌ 9.62% | 0/3 | 61.5% |
| functional_outcomes | 3,322 | 1,842 | ❌ | ✅ 0.27% | 0/3 | 43.9% |
| past_medical_hx | 865 | 295 | ❌ | ✅ 3.82% | 0/3 | 37.9% |
| past_surgical_hx | 3,919 | 1,878 | ❌ | ✅ 2.48% | 0/3 | 70.5% |
| presenting_symptoms | 280 | 120 | ❌ | ✅ 0.36% | 0/3 | 35.0% |
| physical_exam | 1,924 | 662 | ❌ | ✅ 1.56% | 0/3 | 71.7% |
| vascular_invasion | 4,241 | 998 | ❌ | ✅ 0.42% | 0/3 | 93.1% |
| airway_invasion | 3,116 | 1,477 | ❌ | ✅ 0.26% | 0/3 | 65.7% |
| dynamic_risk_response | 53 | 25 | ❌ | ✅ 3.77% | 0/3 | 75.5% |
| patient_decision_adherence | 641 | 398 | ❌ | ❌ 6.55% | 0/3 | 61.9% |

---

## Concordance Summary

| Domain | Algorithm Status | Rows | Patients | Structured Matches | Review Conflicts |
|--------|------------------|------|----------|--------------------|-----------------|
| complications | concordant_existing_extraction_only | 183 | 162 | 0 | 0 |
| complications | discordant_existing | 2 | 2 | 0 | 2 |
| complications | existing_missing_fill_candidate | 275 | 251 | 0 | 0 |
| genetics | concordant_existing | 207 | 147 | 207 | 0 |
| genetics | concordant_existing_extraction_only | 299 | 219 | 0 | 0 |
| genetics | discordant_existing | 46 | 40 | 0 | 46 |
| genetics | existing_missing_fill_candidate | 473 | 420 | 0 | 0 |
| medications | concordant_existing | 59 | 28 | 59 | 0 |
| medications | concordant_existing_extraction_only | 795 | 348 | 0 | 0 |
| medications | discordant_existing | 3 | 2 | 0 | 3 |
| medications | existing_missing_fill_candidate | 368 | 328 | 0 | 0 |
| medications | source_limited | 1 | 1 | 0 | 0 |
| operative_detail | concordant_existing | 29 | 15 | 29 | 0 |
| operative_detail | concordant_existing_extraction_only | 1 | 1 | 0 | 0 |
| operative_detail | discordant_existing | 747 | 397 | 0 | 747 |
| operative_detail | existing_missing_fill_candidate | 1,268 | 784 | 0 | 0 |
| problem_list | concordant_existing_extraction_only | 252 | 166 | 0 | 0 |
| problem_list | source_limited | 108 | 91 | 0 | 0 |
| procedures | concordant_existing | 1,249 | 939 | 1,249 | 0 |
| procedures | concordant_existing_extraction_only | 17 | 10 | 0 | 0 |
| procedures | discordant_existing | 1,085 | 811 | 0 | 1,085 |
| procedures | existing_missing_fill_candidate | 44 | 42 | 0 | 0 |
| staging | concordant_existing | 903 | 754 | 903 | 0 |
| staging | discordant_existing | 1,013 | 654 | 0 | 1,013 |
| unmapped | source_limited | 45,904 | 5,030 | 0 | 0 |

---

## Manual Review Queue

- **Total rows needing review:** 5,324
  - Discordant (conflict): 2,896
  - Fill candidates: 2,428

> **Strict policy:** No row may be auto-promoted. Every discordant row must have
> `verification_status` = `confirmed_correct` or `confirmed_incorrect` set by a reviewer
> in `manual_review_queue.csv` before re-running the gate.

---

## Promotion Command Sequence

Complete only after **all 8 gates PASS** and manual review is resolved.

```bash
# 1. Verify gate scorecard
cat studies/v2_domain_promotion_gate_20260403_promotion_attempt2/promotion_scorecard.csv

# 2. (MotherDuck) Promote v2_stage -> main
# Review and paste: studies/v2_domain_promotion_gate_20260403_promotion_attempt2/motherduck_promote.sql

# 3. Copy validated parquets to dated canonical export bundle
BUNDLE=exports/v2_llm_parquet_bundle_$(date +%Y%m%d_%H%M)
mkdir -p $BUNDLE
cp processed/output/v2_parquets/note_entities_llm_*.parquet $BUNDLE/

# 4. Generate manifest for the export bundle
.venv/bin/python scripts/112_v2_domain_promotion_gate.py \
  --v2-parquets-dir processed/output/v2_parquets \
  --db-path thyroid_master.duckdb \
  --run-label post_promotion_verify

# 5. Materialize v2 canonical facts to local DuckDB
.venv/bin/python scripts/103_fact_lineage_materialize.py --md
```
