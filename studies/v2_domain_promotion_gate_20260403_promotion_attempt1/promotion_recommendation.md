# V2 Domain Promotion Gate — FAIL

**Run label:** `20260403_promotion_attempt1`  
**Generated at:** `2026-04-03T09:27:27.246080+00:00`  
**Overall verdict:** `FAIL` (4 of 8 gates failed)

---

## Gate Scorecard

| Gate | Criterion | Status | Detail |
|------|-----------|--------|--------|
| G1 | Domain completeness | ❌ FAIL | Missing canonical parquets: ['staging', 'genetics', 'procedures', 'operative_detail', 'complications', 'medications', 'problem_list', 'us_nodule_dynamics', 'cervical_ln_detail', 'presenting_symptoms', 'frozen_section_detail'] |
| G2 | Schema compliance | ❌ FAIL | Schema failures: ['imaging', 'tirads_granular', 'labs', 'tg_kinetics', 'pathology', 'synoptic_pathology_enrichment', 'rai_detailed', 'rad_treatment', 'parathyroid_detail', 'recurrence', 'survival_followup', 'functional_outcomes', 'past_medical_hx', 'past_surgical_hx', 'physical_exam', 'vascular_invasion', 'airway_invasion', 'dynamic_risk_response', 'patient_decision_adherence', 'UNCLAIMED', 'UNCLAIMED', 'UNCLAIMED', 'UNCLAIMED', 'UNCLAIMED', 'UNCLAIMED', 'UNCLAIMED'] |
| G3 | Provenance columns | ✅ PASS | All domains have at least one provenance column |
| G4 | Duplicate rate | ✅ PASS | All domains below 5% duplicate threshold |
| G5 | Date coverage (critical domains) | ❌ FAIL | Critical domains with 0% entity_date fill: ['pathology', 'synoptic_pathology_enrichment', 'rai_detailed', 'recurrence', 'vascular_invasion'] |
| G6 | Concordance floor (critical domains) | ✅ PASS | All critical domains meet 30% concordance floor |
| G7 | Unresolved discordance | ❌ FAIL | 2869 discordant rows in review queue — all require manual verification before promotion |
| G8 | MotherDuck v2_stage parity | ✅ PASS | Skipped (--motherduck-check not set or MOTHERDUCK_TOKEN missing) |

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
| cervical_ln_detail | v2 | standard | ❌ | True | pathology |
| functional_outcomes | v2 | informational | ✅ | True | followup |
| past_medical_hx | v2 | informational | ✅ | True | demographics |
| past_surgical_hx | v2 | informational | ✅ | True | demographics |
| presenting_symptoms | v2 | informational | ❌ | True | demographics |
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
| imaging | 11,037 | 5,641 | ❌ | ✅ 0.00% | 1/3 | 0.0% |
| tirads_granular | 11,037 | 5,641 | ❌ | ✅ 0.00% | 3/3 | 0.0% |
| labs | 11,037 | 5,641 | ❌ | ✅ 0.00% | 1/3 | 0.0% |
| tg_kinetics | 11,037 | 5,641 | ❌ | ✅ 0.00% | 3/3 | 0.0% |
| pathology | 11,037 | 5,641 | ❌ | ✅ 0.00% | 1/3 | 0.0% |
| synoptic_pathology_enrichment | 11,037 | 5,641 | ❌ | ✅ 0.00% | 3/3 | 0.0% |
| rai_detailed | 11,037 | 5,641 | ❌ | ✅ 0.00% | 3/3 | 0.0% |
| rad_treatment | 11,037 | 5,641 | ❌ | ✅ 0.00% | 3/3 | 0.0% |
| parathyroid_detail | 11,037 | 5,641 | ❌ | ✅ 0.00% | 3/3 | 0.0% |
| recurrence | 11,037 | 5,641 | ❌ | ✅ 0.00% | 3/3 | 0.0% |
| survival_followup | 11,037 | 5,641 | ❌ | ✅ 0.00% | 3/3 | 0.0% |
| functional_outcomes | 11,037 | 5,641 | ❌ | ✅ 0.00% | 3/3 | 0.0% |
| past_medical_hx | 11,037 | 5,641 | ❌ | ✅ 0.00% | 3/3 | 0.0% |
| past_surgical_hx | 11,037 | 5,641 | ❌ | ✅ 0.00% | 3/3 | 0.0% |
| physical_exam | 11,037 | 5,641 | ❌ | ✅ 0.00% | 1/3 | 0.0% |
| vascular_invasion | 11,037 | 5,641 | ❌ | ✅ 0.00% | 3/3 | 0.0% |
| airway_invasion | 11,037 | 5,641 | ❌ | ✅ 0.00% | 3/3 | 0.0% |
| dynamic_risk_response | 11,037 | 5,641 | ❌ | ✅ 0.00% | 3/3 | 0.0% |
| patient_decision_adherence | 11,037 | 5,641 | ❌ | ✅ 0.00% | 3/3 | 0.0% |
| UNCLAIMED | 11,037 | 5,641 | ❌ | ✅ 0.00% | 1/3 | 0.0% |
| UNCLAIMED | 11,037 | 5,641 | ❌ | ✅ 0.00% | 1/3 | 0.0% |
| UNCLAIMED | 11,037 | 5,641 | ❌ | ✅ 0.00% | 1/3 | 0.0% |
| UNCLAIMED | 11,037 | 5,641 | ❌ | ✅ 0.00% | 1/3 | 0.0% |
| UNCLAIMED | 11,037 | 5,641 | ❌ | ✅ 0.00% | 3/3 | 0.0% |
| UNCLAIMED | 11,037 | 5,641 | ❌ | ✅ 0.00% | 1/3 | 0.0% |
| UNCLAIMED | 11,037 | 5,641 | ❌ | ✅ 0.00% | 1/3 | 0.0% |

---

## Concordance Summary

| Domain | Algorithm Status | Rows | Patients | Structured Matches | Review Conflicts |
|--------|------------------|------|----------|--------------------|-----------------|
| complications | concordant_existing_extraction_only | 183 | 162 | 0 | 0 |
| complications | discordant_existing | 2 | 2 | 0 | 2 |
| complications | existing_missing_fill_candidate | 275 | 251 | 0 | 0 |
| genetics | concordant_existing | 206 | 146 | 206 | 0 |
| genetics | concordant_existing_extraction_only | 299 | 219 | 0 | 0 |
| genetics | discordant_existing | 46 | 40 | 0 | 46 |
| genetics | existing_missing_fill_candidate | 473 | 420 | 0 | 0 |
| medications | concordant_existing | 59 | 28 | 59 | 0 |
| medications | concordant_existing_extraction_only | 791 | 347 | 0 | 0 |
| medications | discordant_existing | 3 | 2 | 0 | 3 |
| medications | existing_missing_fill_candidate | 368 | 328 | 0 | 0 |
| medications | source_limited | 1 | 1 | 0 | 0 |
| operative_detail | concordant_existing | 29 | 15 | 29 | 0 |
| operative_detail | concordant_existing_extraction_only | 1 | 1 | 0 | 0 |
| operative_detail | discordant_existing | 746 | 397 | 0 | 746 |
| operative_detail | existing_missing_fill_candidate | 1,264 | 784 | 0 | 0 |
| problem_list | concordant_existing_extraction_only | 251 | 165 | 0 | 0 |
| problem_list | source_limited | 108 | 91 | 0 | 0 |
| procedures | concordant_existing | 1,217 | 932 | 1,217 | 0 |
| procedures | concordant_existing_extraction_only | 17 | 10 | 0 | 0 |
| procedures | discordant_existing | 1,059 | 801 | 0 | 1,059 |
| procedures | existing_missing_fill_candidate | 43 | 41 | 0 | 0 |
| staging | concordant_existing | 903 | 754 | 903 | 0 |
| staging | discordant_existing | 1,013 | 654 | 0 | 1,013 |
| unmapped | source_limited | 45,590 | 5,029 | 0 | 0 |

---

## Manual Review Queue

- **Total rows needing review:** 5,292
  - Discordant (conflict): 2,869
  - Fill candidates: 2,423

> **Strict policy:** No row may be auto-promoted. Every discordant row must have
> `verification_status` = `confirmed_correct` or `confirmed_incorrect` set by a reviewer
> in `manual_review_queue.csv` before re-running the gate.

---

## Promotion Command Sequence

Complete only after **all 8 gates PASS** and manual review is resolved.

```bash
# 1. Verify gate scorecard
cat studies/v2_domain_promotion_gate_20260403_promotion_attempt1/promotion_scorecard.csv

# 2. (MotherDuck) Promote v2_stage -> main
# Review and paste: studies/v2_domain_promotion_gate_20260403_promotion_attempt1/motherduck_promote.sql

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
