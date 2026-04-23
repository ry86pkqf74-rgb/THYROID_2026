# Script 398 — Close-out

- **Git commit / SHA:** `(pending; run after Phase 4)`
- **Tag:** `v1_0-cpm-tnm-disagreement-audit-20260423_032802`
- **UTC timestamp:** 2026-04-23T03:28:05.356569+00:00
- **Probe SHA256 (consumed):** `e18249f97003de97cf2d5973a151ac0635baee550faaa11f8da6103ed2ea333b`
- **Target table FQN:** `thyroid_canonical_publication_v1_0.manuscript_workspace.cpm_tnm_cross_source_disagreements_v1`

## Halt-gate verdicts (Phase 0) — H1–H8

| gate | result |
|---|---|
| H1 | Target T/N/M counts 363/2055/1838, total 4256 |
| H2 | `manuscript_workspace` exists |
| H3 | Target table absent, or 4256 rows + idempotent `__readme` |
| H4 | CPM = 10,871 |
| H5 | No `UPDATE` to `main.canonical_patient_master` in apply SQL |
| H6 | Sidecar column list matches `TARGET_COLS` |
| H7 | No duplicate (research_id, axis) in union |
| H8 | `dominant_tumor_ajcc8_{t,n,m}_stage` present on CPM |

## Materialized write

- **Rows:** 4256 (T=363 + N=2055 + M=1838)
- **CPM primary columns updated:** 0 (read-only to CPM)

- **DTC (PTC,FTC,HCC,DTC_NOS) per-axis (diagnosis breakdown):** T=317 N=1900 M=1689
- **Non-DTC per axis:** T=46 N=155 M=149

## Top-3 patterns (current snapshot)

- T: T3a↔T3b (135), T3b↔T4a (64), T1a↔T3a (31)
- N: N1a↔Nx (1423), N1a↔N1b (541), N0↔Nx (63)
- M: M0↔M1 (1838)

## Zero CPM write confirmation (H5 + P1 + P6)

- P1: CPM row count unchanged; P6: T-disag pre/post query still 363.

## CF-398 follow-ups

- **CF-398-1:** Per-pattern triage (v2- vs primary-wins).
- **CF-398-2:** Re-derive `ajcc8_stage_group` under v2 for audit rows; flag restage.
- **CF-398-3:** v2 column provenance (phase 4.6, builder design).
- **CF-398-4:** Script 399 — surgical `UPDATE` after adjudication.
