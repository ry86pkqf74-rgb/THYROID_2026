## Cohort Flow

| Step | N | Pct_of_total | Population | Source |
| --- | --- | --- | --- | --- |
| Total surgical patients | 10,871 | --- | Full surgical cohort | `path_synoptics` |
| Analysis-eligible (cancer) | 4,136 | 38.0% of 10,871 | Cancer with complete staging | `patient_analysis_resolved_v1` |
| With histology (cancer) | 4,137 | 38.1% of 10,871 | Cancer histology confirmed | `patient_analysis_resolved_v1` |
| Molecular-tested | 10,025 | 92.2% of 10,871 | Any molecular panel result | `extracted_molecular_panel_v1` |
| BRAF positive | 376 | 3.8% of 10,025 mol-tested | BRAF confirmed positive | `extracted_braf_recovery_v1` |
| RAS positive | 292 | 2.9% of 10,025 mol-tested | RAS confirmed positive | `extracted_ras_patient_summary_v1` |
| RAI received (dose-verified) | 35 | 0.3% of 10,871 | `rai_validation_tier = 'confirmed_with_dose'` | `extracted_rai_validated_v1` |
| Tg available | 2,559 | 23.5% of 10,871 | Thyroglobulin lab data | `thyroglobulin_labs` |
| TIRADS available | 3,474 | 32.0% of 10,871 | Validated TIRADS score | `extracted_tirads_validated_v1` |
| Any confirmed complication | 287 | 2.6% of 10,871 | Post-op complication confirmed | `patient_refined_complication_flags_v2` |
| RLN injury confirmed | 59 | 0.54% of 10,871 | 3-tier refined RLN injury | `extracted_rln_injury_refined_v2` |

*All percentages reference explicit denominators. Molecular prevalences (BRAF, RAS) use the molecular-tested denominator (N = 10,025). All other rates use the full surgical cohort (N = 10,871). Metric definitions per `manuscript_metrics_v2`.*
