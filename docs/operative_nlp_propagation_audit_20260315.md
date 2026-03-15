# Operative NLP Propagation Audit — 20260315

Generated: 2026-03-15T01:22:15.731934
Source: MotherDuck `thyroid_research_2026` (prod)

## Summary

- Fully propagated (OK): **6** fields
- Pipeline gap (upstream present, downstream absent/partial): **4** fields
- Source-limited (0% upstream): **5** fields

## Episode-Level Fields (operative_episode_detail_v2 → analytic tables)

| Field | Upstream | % | Status | Downstream Tables |
|-------|----------|---|--------|-------------------|
| `rln_monitoring_flag` | 1702 | 18.2% | PIPELINE_GAP | episode_analysis_resolved=0; episode_analysis_resolved=0; patient_analysis_resolved=COLUMN_MISSING; manuscript_cohort=COLUMN_MISSING |
| `rln_finding_raw` | 371 | 4.0% | PIPELINE_GAP | episode_analysis_resolved=0; episode_analysis_resolved=0; patient_analysis_resolved=COLUMN_MISSING; manuscript_cohort=COLUMN_MISSING |
| `parathyroid_autograft_flag` | 40 | 0.4% | OK | episode_analysis_resolved=40; episode_analysis_resolved=40; patient_analysis_resolved=COLUMN_MISSING; manuscript_cohort=COLUMN_MISSING |
| `gross_ete_flag` | 22 | 0.2% | PIPELINE_GAP | episode_analysis_resolved=COLUMN_MISSING; episode_analysis_resolved=COLUMN_MISSING; patient_analysis_resolved=COLUMN_MISSING; manuscript_cohort=COLUMN_MISSING |
| `local_invasion_flag` | 25 | 0.3% | OK | episode_analysis_resolved=25; episode_analysis_resolved=25; patient_analysis_resolved=COLUMN_MISSING; manuscript_cohort=COLUMN_MISSING |
| `tracheal_involvement_flag` | 9 | 0.1% | OK | episode_analysis_resolved=9; episode_analysis_resolved=9; patient_analysis_resolved=COLUMN_MISSING; manuscript_cohort=COLUMN_MISSING |
| `esophageal_involvement_flag` | 0 | 0.0% | SOURCE_LIMITED | episode_analysis_resolved=0; episode_analysis_resolved=0; patient_analysis_resolved=COLUMN_MISSING; manuscript_cohort=COLUMN_MISSING |
| `strap_muscle_involvement_flag` | 186 | 2.0% | OK | episode_analysis_resolved=186; episode_analysis_resolved=186; patient_analysis_resolved=COLUMN_MISSING; manuscript_cohort=COLUMN_MISSING |
| `reoperative_field_flag` | 46 | 0.5% | OK | episode_analysis_resolved=49; episode_analysis_resolved=46; patient_analysis_resolved=COLUMN_MISSING; manuscript_cohort=COLUMN_MISSING |
| `drain_flag` | 169 | 1.8% | PIPELINE_GAP | episode_analysis_resolved=0; episode_analysis_resolved=0; patient_analysis_resolved=COLUMN_MISSING; manuscript_cohort=COLUMN_MISSING |
| `operative_findings_raw` | 588 | 6.3% | OK | episode_analysis_resolved=594; episode_analysis_resolved=587; patient_analysis_resolved=COLUMN_MISSING; manuscript_cohort=COLUMN_MISSING |
| `parathyroid_identified_count` | 0 | 0.0% | SOURCE_LIMITED | episode_analysis_resolved=COLUMN_MISSING; episode_analysis_resolved=COLUMN_MISSING; patient_analysis_resolved=COLUMN_MISSING; manuscript_cohort=COLUMN_MISSING |
| `frozen_section_flag` | 0 | 0.0% | SOURCE_LIMITED | episode_analysis_resolved=COLUMN_MISSING; episode_analysis_resolved=COLUMN_MISSING; patient_analysis_resolved=COLUMN_MISSING; manuscript_cohort=COLUMN_MISSING |
| `berry_ligament_flag` | 0 | 0.0% | SOURCE_LIMITED | episode_analysis_resolved=COLUMN_MISSING; episode_analysis_resolved=COLUMN_MISSING; patient_analysis_resolved=COLUMN_MISSING; manuscript_cohort=COLUMN_MISSING |
| `ebl_ml_nlp` | 0 | 0.0% | SOURCE_LIMITED | episode_analysis_resolved=COLUMN_MISSING; episode_analysis_resolved=COLUMN_MISSING; patient_analysis_resolved=COLUMN_MISSING; manuscript_cohort=COLUMN_MISSING |

## Patient-Level Aggregates (patient_analysis_resolved_v1)

| Field | Count | % |
|-------|-------|---|
| `op_rln_monitoring_any` | 1701 | 15.6% |
| `op_drain_placed_any` | 169 | 1.6% |
| `op_strap_muscle_any` | 186 | 1.7% |
| `op_reoperative_any` | 46 | 0.4% |
| `op_parathyroid_autograft_any` | 40 | 0.4% |
| `op_local_invasion_any` | 25 | 0.2% |
| `op_tracheal_inv_any` | 9 | 0.1% |
| `op_esophageal_inv_any` | 0 | 0.0% |
| `op_intraop_gross_ete_any` | 22 | 0.2% |
| `op_n_surgeries_with_findings` | 8733 | 80.3% |
| `op_findings_summary` | 587 | 5.4% |

## Pipeline Gap Detail

- **`rln_monitoring_flag`**: 1702 rows in `operative_episode_detail_v2` but absent from downstream analytic tables. Script 86 sync skipped this field.
- **`rln_finding_raw`**: 371 rows in `operative_episode_detail_v2` but absent from downstream analytic tables. Script 86 sync skipped this field.
- **`gross_ete_flag`**: 22 rows in `operative_episode_detail_v2` but absent from downstream analytic tables. Script 86 sync skipped this field.
- **`drain_flag`**: 169 rows in `operative_episode_detail_v2` but absent from downstream analytic tables. Script 86 sync skipped this field.

## Source-Limited Fields

- **`esophageal_involvement_flag`**: 0% in upstream extraction — NLP entity type not in vocab or not extracted
- **`parathyroid_identified_count`**: 0% in upstream extraction — NLP entity type not in vocab or not extracted
- **`frozen_section_flag`**: 0% in upstream extraction — NLP entity type not in vocab or not extracted
- **`berry_ligament_flag`**: 0% in upstream extraction — NLP entity type not in vocab or not extracted
- **`ebl_ml_nlp`**: 0% in upstream extraction — NLP entity type not in vocab or not extracted

---
*Audit generated by `scripts/99_comprehensive_final_verification.py` on 2026-03-15T01:22:15.731934*