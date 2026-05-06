# MIG-001+002 initially blocked summary

**Date:** 2026-05-06  
**Task:** H2 follow-up MIG-001 + MIG-002, preoperative RLN injury and vocal-cord paralysis extraction  
**DFL:** DFL-20260506-102  
**BQ migration log:** `mig_079_mig001_002_preop_rln_vc_blocked`

## Outcome

Initially blocked before any canonical BigQuery mutation. This blocker was later superseded in the same Cursor session after the user provided a PAT file and the repo Snowflake connector workaround succeeded. Final successful migration summary: `_scripts/mig001_002_preop_rln_vc_summary.md`.

No `pub_canonical.canonical_patient_master` schema or data changes were applied. The four target columns remain absent:

- `comp_rln_injury_preop`
- `comp_rln_injury_preop_source`
- `comp_vc_paralysis_preop`
- `comp_vc_paralysis_preop_source`

## What was verified

- Airtable MCP connector is reachable.
- Linear MCP connector is reachable.
- H2 manuscript record exists in Airtable with status `Analysis`.
- Linear issues THY-13 and THY-14 exist.
- BigQuery service-account access works.
- `pub_signoff.bq_migration_log_v1` schema was confirmed; it uses `description` and `notes`, not a `status` column.
- `pub_canonical.canonical_patient_master` has 10,871 rows and does not yet contain the four target preop columns.
- Cortex object search can see `THYROID_VALIDATION.PUBLIC.CLINICAL_NOTES_SEARCH_V1` with 11,050 rows and prior AI_CLASSIFY result tables.

## Blocker

The user requested a Cortex-only path because the available Snowflake PAT is Cortex-scoped. The local Cortex CLI can perform object discovery, but could not execute the required warehouse SQL / DDL / AI_CLASSIFY workflow:

- `snow sql -c thyroid_2026 ...` failed with PAT invalid for generic warehouse SQL.
- `snow cortex complete ... --backend rest -c thyroid_2026` also failed with the same PAT-invalid connection error.
- `cortex --print` is unavailable for this subscription/trial account.
- `cortex analyst query --view THYROID_VALIDATION.PUBLIC.CLINICAL_NOTES_SEARCH_V1 ...` returned a Cortex Analyst API error.

The migration requires executable Snowflake SQL for:

1. Creating `NLP_PREOP_LARYNGOSCOPY_NOTES_v1`.
2. Running `AI_CLASSIFY` over the note slice.
3. Creating `NLP_PREOP_RLN_VC_PATIENT_v1`.
4. Exporting the patient-level rollup for BigQuery promotion.

## Governance

- DFL row was appended before the attempt: DFL-20260506-102.
- BigQuery migration log row was appended after the blocked attempt.
- No PHI was queried, exported, pasted, or committed.
- Note bodies stayed in Snowflake and were not retrieved into local files or chat.

## Required unblock

Provide one of:

1. A generic-scope Snowflake PAT for `thyroid_2026` that can run warehouse SQL and Cortex functions, or
2. A non-interactive Cortex execution path that supports SQL/DDL and AI_CLASSIFY calls under the current auth.

After unblock, rerun MIG-001+002 from the DFL/logged state and dry-run the BigQuery promotion before any `pub_canonical` update.
