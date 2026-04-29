# Complications patient rollup mig_108 close-out

Date: 2026-04-29

Migration: `qc_framework_v1/migrations/108_complications_patient_rollup_signoff.sql`

Table: `main.canonical_complications_patient_rollup_v1`

## Result

- `canonical_complications_patient_rollup_v1` signed off under Protocol v2.
- Events source was already verified in mig_99 (`canonical_complications_events_v1`, commit `cbccd4a`).
- Rollup was verify-only; no rebuild performed.

## Verification evidence

- Cohort parity: rollup `10,871` rows / `10,871` distinct `research_id` = CPM `10,871` / `10,871`.
- Source events: `5,050` rows / `2,481` patients.
- Rollup `build_ts`: `2026-04-28 19:36:23.768812` for all rows.
- Re-derived `49` derivable columns from verified events + first-surgery temporal logic:
  - `10,871` patients compared
  - `0` patients with any drift
  - `0` total cell drifts
- No-present-evidence cohort: `10,416` patients; `0` count/date contamination.

## Registry final state

- Column registry: `49` verified + `2` na.
- Table signoff: `table_status='verified'`, `n_columns_total=51`, `n_not_started=0`, `n_failed=0`, `n_na=2`.

## Reusable pattern

For patient-rollup signoffs with fresh `build_ts`, use the mig_105 medications pattern:
1. Re-derive all patient-level aggregate columns from the verified events table.
2. Compare every derivable column with `IS DISTINCT FROM`.
3. Confirm cohort parity to `canonical_patient_master`.
4. Flip only `verification_status='not_started'` columns, then recompute table signoff counts.