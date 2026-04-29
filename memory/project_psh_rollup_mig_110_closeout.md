# PSH patient rollup mig_110 close-out

Date: 2026-04-29

Migration: `qc_framework_v1/migrations/110_psh_patient_rollup_signoff.sql`

Table: `main.canonical_psh_patient_rollup_v1`

## Result

- `canonical_psh_patient_rollup_v1` signed off under Protocol v2.
- Events source was already verified in mig_104 (`canonical_psh_events_v1`, commit `d971cdc`).
- Rollup was verify-only; no rebuild performed.

## Verification evidence

- Cohort parity: rollup `10,871` rows / `10,871` distinct `research_id` = CPM `10,871` / `10,871`.
- Source events: `3,919` rows / `1,878` patients.
- Rollup `build_ts`: `2026-04-22 00:00:00` for all rows.
- Re-derived `26` derivable columns from verified PSH events using Script 365 rollup logic:
  - `10,871` patients compared.
  - `0` drift rows across all `26` derivable columns.
  - `0` live-not-fresh and `0` fresh-not-live anti-join rows.
- No STRING_AGG columns exist in the PSH rollup, so no ordering-artifact exception was needed.

## Registry final state

- Column registry: `26` verified + `2` na.
- Table signoff: `table_status='verified'`, `n_columns_total=28`, `n_not_started=0`, `n_failed=0`, `n_na=2`.

## Reusable pattern

For Script 365 full-cohort rollups built before events verification but with unchanged events data:
1. Re-derive the rollup from verified events with the exact Script 365 phenotype match logic.
2. Confirm rollup/CPM parity and event-patient parity to `n_findings_any > 0`.
3. Compare every derivable column with `IS DISTINCT FROM`.
4. If drift is zero, apply a verify-only registry flip and recompute table signoff counts.
