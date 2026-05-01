# mig_244 — `semantic_publication.vw_patient_domain_wide_safe_VIEW_v1`

**Applied:** 2026-05-01 (MotherDuck `thyroid_canonical_publication_v1_0`)  
**SQL:** `qc_framework_v1/migrations/244_vw_patient_domain_wide_safe_VIEW_v1_20260501.sql`  
**batch_id:** `mig_244_patient_domain_wide_safe`

## Purpose

Single curated patient-grain bridge (~46 columns, 10,871 rows) joining semantic safe views + minimal canonical rollups so analysts avoid multi-way joins for headline Table 1–4-style fields.

## Included domains

- Spine: `vw_cohort_membership_safe_VIEW_v1` + `vw_patient_master_safe_VIEW_v1`
- Path aggregates (dedup `publication_dedup_rank = 1`): max tumor cm, LV invasion heuristic, macroscopic ENE text cue
- Operative: `canonical_operative_patient_rollup_v1` (TT / lobectomy / LN dissection / n_surgeries)
- LN totals: `vw_ln_patient_safe_VIEW_v1`
- Recurrence (safe filter): `vw_recurrence_safe_VIEW_v1`
- Molecular episode rollup: `vw_molecular_safe_VIEW_v1`
- Survival SSOT: `canonical_survival_followup_v1`
- Limitation flags: borderline staging OR across `canonical_path_malignant_events_v1`; recurrence date quarantine OR across `canonical_recurrence_resolved_v1`; US nodule NLP pending OR across `canonical_us_nodule_v2`

## Explicitly omitted

- **Frozen section:** use `vw_frozen_section_safe_VIEW_v1` (mig_242) — separate compact surface.
- **Ethnicity-only column:** cohort spine exposes combined race bucket as `race_self_reported`.
- **`ln_size_max_mm`:** no patient-safe LN mm aggregate in semantic v1.

## Post-apply checks

```sql
SELECT COUNT(*) FROM semantic_publication.vw_patient_domain_wide_safe_VIEW_v1;  -- 10871
SELECT * FROM semantic_publication.vw_publication_qc_status_VIEW_v1;
```
