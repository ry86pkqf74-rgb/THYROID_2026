# MIG-001+002 summary

**Date:** 2026-05-06  
**Migration:** `mig_080_h2_preop_rln_vc_columns`  
**DFL:** DFL-20260506-102  
**Linear:** THY-13, THY-14  

## Result

Completed with a source-limited implementation.

BigQuery `pub_canonical.canonical_patient_master` now has four new columns:

- `comp_rln_injury_preop`
- `comp_rln_injury_preop_source`
- `comp_vc_paralysis_preop`
- `comp_vc_paralysis_preop_source`

The update affected 119 patients, matching the Snowflake patient rollup from the preoperative-laryngoscopy field.

## Key Counts

| Metric | Count |
|---|---:|
| Patients with source preop-laryngoscopy text | 119 |
| `comp_rln_injury_preop` populated | 119 |
| `comp_rln_injury_preop = TRUE` | 8 |
| `comp_vc_paralysis_preop` populated | 119 |
| `comp_vc_paralysis_preop = TRUE` | 3 |

## Source And Method

Snowflake source:

- `THYROID_VALIDATION.PUBLIC.CANONICAL_PATIENT_MASTER_FLAT.OPS_PREOP_LARYNGOSCOPY`

Snowflake working tables:

- `NLP_PREOP_LARYNGOSCOPY_NOTES_v1`
- `NLP_PREOP_RLN_VC_RESULTS_v1`
- `NLP_PREOP_RLN_VC_PATIENT_v1`

BigQuery workspace table:

- `thyroid-canonical-pub-2026.pub_workspace.nlp_preop_rln_vc_patient_v1`

BigQuery canonical table:

- `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master`

`AI_CLASSIFY` labels:

- RLN: `preop_rln_injury_present`, `preop_rln_injury_absent`, `preop_rln_status_unstated`
- VC paralysis: `preop_vc_paralysis_present`, `preop_vc_paralysis_absent`, `preop_vc_status_unstated`

## Dry Run

The BigQuery `UPDATE` was dry-run before execution.

- Estimated bytes processed: 46,333,768

## Limitations

The original handoff expected a dated preop H&P / laryngoscopy note slice. In Snowflake, the available note-search table (`CLINICAL_NOTES_SEARCH_V1`) contains only `research_id`, `note_type`, `note_index`, and `note_text`; it does not carry `note_date`. A whole-H&P pilot produced implausible positive classifications consistent with consent / operative-risk boilerplate. To avoid false positives, the live migration used the targeted `OPS_PREOP_LARYNGOSCOPY` source instead.

Interpretation: these columns are valid as sparse, laryngoscopy-documented preoperative flags. They should not be described as complete preoperative voice-clinic or H&P status.

## Governance

- DFL row appended before migration: DFL-20260506-102.
- BigQuery migration log success row appended: `mig_080_h2_preop_rln_vc_columns`.
- Earlier blocked-attempt row remains for auditability: `mig_079_mig001_002_preop_rln_vc_blocked`.
- No PHI or note text was written to BigQuery, Airtable, Linear, Git, or chat.
