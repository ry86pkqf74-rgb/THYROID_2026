# Prioritized manual-review work package (template)

Use when a **new** gate run repopulates `qa.manual_review_queue` with pending rows.

## 1. Discordant rows (zero tolerance)

- Filter `algorithm_status = 'discordant_existing'`.
- Adjudicate individually to `confirmed_correct` or `confirmed_incorrect`; second reviewer per playbook.
- Do **not** use `127` bulk script for these rows.

## 2. Bucket by QA tier (registry)

| Tier | Domains (v2 canonical_output) | Batch policy |
|------|--------------------------------|--------------|
| critical | pathology, synoptic_pathology_enrichment, rai_detailed, recurrence, vascular_invasion | Sample 10% (min 20); >90% pass → optional `--include-critical-after-sample` |
| standard | imaging, tirads_granular, us_nodule_dynamics, labs, tg_kinetics, rad_treatment, parathyroid_detail, survival_followup, cervical_ln_detail, airway_invasion, frozen_section_detail, dynamic_risk_response | `127` bulk `auto_accepted_standard` |
| informational | functional_outcomes, past_medical_hx, past_surgical_hx, presenting_symptoms, physical_exam, patient_decision_adherence | `127` bulk `auto_accepted_informational` |

## 3. Safe for batch adjudication

- `existing_missing_fill_candidate` + **standard** or **informational** tier + no discordant flag.
- Exclusions: nonsensical values, garbage `entity_type`, `source_limited` / `not_promotable` rubric rows (handle manually).

## 4. Must stay source_limited / not_promotable

- Rows failing clinical span checks per playbook; duplicates pending dedup; cross-domain escalation.

## 5. Hydration

- Gate CSV → `114_qa_schema_setup.py --md --md-sa --hydrate-from studies/v2_domain_promotion_gate_<label>/`
- Ensure `source_domain` maps to QA `domain` (fixed ordering in `114` col_map).
