# Queue summary artifact — `qa.manual_review_queue` (MotherDuck live)

**As-of:** 2026-04-07 (UTC) — generated from `md:Thyroid 2026` via fail-closed read.

**Domain column:** `qa.manual_review_queue.domain` is the **v2 source domain** (promotable stem) loaded from gate CSV `source_domain`. The v1 `comparison_domain` from the gate CSV is **not persisted** on this table; use the study `manual_review_queue.csv` for pairwise concordance keys when needed.

**PHI / cloud constraint:** No raw note text. Do not store `review_reason` / note snippets in cloud artifacts (see `scripts/120_review_queue_triage.py`).

## Headline

- **Total rows:** 16,866
- **Pending (`verification_status` IS NULL):** 0 — blocks `119 --release-mode` when non-zero.
- **Placeholder synthetic status:** 5,620 — non–manuscript-grade placeholder from `126 --synthetic-fill-mrq-verification` path.
- **`discordant_existing` rows:** 6 (current snapshot: all `confirmed_correct`, `promotion_approved=true`).

### By algorithm_status × promotability_class

| algorithm_status | promotability_class | n_rows |
| --- | --- | --- |
| existing_missing_fill_candidate | promotable_automated_governance | 11240 |
| existing_missing_fill_candidate | placeholder_non_manuscript | 5620 |
| discordant_existing | promotable_confirmed | 6 |

### By algorithm_status × domain × QA tier (full table)

| algorithm_status | domain | qa_tier | n_rows |
| --- | --- | --- | --- |
| discordant_existing | rad_treatment | standard | 4 |
| discordant_existing | medications | standard | 2 |
| existing_missing_fill_candidate | airway_invasion | standard | 4090 |
| existing_missing_fill_candidate | operative_detail | standard | 1980 |
| existing_missing_fill_candidate | staging | critical | 1499 |
| existing_missing_fill_candidate | pathology | critical | 1366 |
| existing_missing_fill_candidate | imaging | standard | 1202 |
| existing_missing_fill_candidate | rai_detailed | critical | 1128 |
| existing_missing_fill_candidate | procedures | standard | 1062 |
| existing_missing_fill_candidate | vascular_invasion | critical | 740 |
| existing_missing_fill_candidate | functional_outcomes | informational | 692 |
| existing_missing_fill_candidate | survival_followup | standard | 558 |
| existing_missing_fill_candidate | genetics | critical | 510 |
| existing_missing_fill_candidate | physical_exam | informational | 494 |
| existing_missing_fill_candidate | past_surgical_hx | informational | 462 |
| existing_missing_fill_candidate | medications | standard | 366 |
| existing_missing_fill_candidate | complications | critical | 203 |
| existing_missing_fill_candidate | labs | standard | 198 |
| existing_missing_fill_candidate | patient_decision_adherence | informational | 104 |
| existing_missing_fill_candidate | cervical_ln_detail | standard | 74 |
| existing_missing_fill_candidate | recurrence | critical | 44 |
| existing_missing_fill_candidate | rad_treatment | standard | 28 |
| existing_missing_fill_candidate | past_medical_hx | informational | 22 |
| existing_missing_fill_candidate | synoptic_pathology_enrichment | critical | 14 |
| existing_missing_fill_candidate | presenting_symptoms | informational | 12 |
| existing_missing_fill_candidate | frozen_section_detail | standard | 4 |
| existing_missing_fill_candidate | parathyroid_detail | standard | 4 |
| existing_missing_fill_candidate | dynamic_risk_response | standard | 2 |
| existing_missing_fill_candidate | tirads_granular | standard | 2 |

### By registry QA tier × likely reviewer channel

| qa_tier | reviewer_channel | n_rows |
| --- | --- | --- |
| standard | analyst_led_with_clinical_spot | 9570 |
| critical | clinician_led | 5504 |
| informational | analyst_led | 1786 |
| standard | clinician_primary_plus_second | 6 |

### By reason_code

| reason_code | n_rows |
| --- | --- |
| (none) | 16866 |

### By verification_status

| verification_status | n_rows |
| --- | --- |
| auto_accepted_standard | 6162 |
| SYNTHETIC_AUTOMATION_ONLY_NOT_MANUSCRIPT_SIGNOFF | 5620 |
| auto_accepted_critical_sample_ok | 3292 |
| auto_accepted_informational | 1786 |
| confirmed_correct | 6 |
