> **HISTORICAL / SUPERSEDED:** This document is a point-in-time snapshot from its generation date. For current canonical state, see [`docs/final_source_of_truth_contract.md`](../../docs/final_source_of_truth_contract.md) and [`studies/CURRENT_MOTHERDUCK_REPO_STATE.md`](../CURRENT_MOTHERDUCK_REPO_STATE.md). Row counts cited here may no longer match live MotherDuck.

# Live MotherDuck publication sign-off reaudit — report

**UTC window:** 2026-04-07 (queries and `119` report ~19:01Z)  
**Catalog:** `current_database()` = **`Thyroid 2026`**  
**Git HEAD at generator run:** `5f12da7eb7d22d13fe84327506a30190d41ab99d` (`studies/CURRENT_MOTHERDUCK_REPO_STATE.md`)

## Executive summary

Live MotherDuck was queried fail-closed with service-account preference (`--md-sa`), attribution `MOTHERDUCK_CUSTOM_USER_AGENT=THYROID_2026_live_publication_signoff_reaudit/1.0`, and session hints per `commands_run.log`. Scripts **`144`**, **`120`**, and **`119 --release-mode`** were re-run; exports live under this folder.

**Formalization automation:** **`119 --release-mode` → PASS WITH WARNINGS** (`26 PASS / 1 WARN / 0 FAIL`) — see `119_release_validation/validation_report.md`. The single WARN is **specimen-adjacent genomic review burden** (~**9,966** open genomic link reviews + **1** open merge review). Specimen/FHIR **diagnostic** checks are **PASS** (clean) in this run.

**Manuscript sign-off:** **Not ready.** The manual review queue is **empty of NULL verification statuses**, but **11,244** rows are overwhelmingly **`auto_accepted_*`**, with only **four** `confirmed_correct`. Longitudinal canonical labs remain **Tg-family only** (`thyroid_tumor_markers`). `qa.promotion_review_decisions` remains tiny (**3** rows). These are **governance and institutional lab** gaps, not a failing `119` gate.

## Live measurements (high-signal)

| Metric | Value | File |
|--------|------:|------|
| Attached DBs (`119`) | 11 | `validation_report.md` |
| `qa.release_manifest` latest `release_tag` | **20260410** | `live_sql_exports/05_*.csv` |
| `release_*` schemas (`119` list) | 8 named | `validation_report.md` |
| MRQ total / pending NULL | 11,244 / **0** | `06_*.csv`, `120` triage |
| MRQ `confirmed_correct` | **4** | `07_*.csv` |
| Genomic link review open | **9,966** | `13_*.csv` |
| Merge queue `open` | **1** | `14_*.csv` |
| `longitudinal_lab_canonical_v1` rows / groups | 76,971 / 1 analyte group | `10_*.csv` |
| `longitudinal_lab_deduped_v` rows | 55,210 | `11_*.csv` |
| `main.molecular_results` | **0** | `12_*.csv` |
| `qa.promotion_review_decisions` | **3** rows | `08_*.csv`, `09_*.csv` |

**MotherDuck database row (`Thyroid 2026`):** type **DUCKLAKE**, not transient, **7-day** historical snapshot retention (`live_sql_exports/01_md_information_schema_databases.csv`).

## Code / tooling fixes applied this session

- **`scripts/144_md_repo_current_state_summary.py`** — honors `MOTHERDUCK_CUSTOM_USER_AGENT` and `MOTHERDUCK_SESSION_HINT` when set (aligned with operator attribution requirements).
- **`studies/20260407_live_publication_signoff_reaudit/collect_live_evidence.py`** — corrected `MD_INFORMATION_SCHEMA.QUERY_HISTORY` columns (`query_text`, `end_time`/`execution_time`).

## Artifact index

| Path | Purpose |
|------|---------|
| `live_sql_exports/` | CSV snapshots from live SQL |
| `119_release_validation/validation_report.md` | Timestamped `119 --release-mode` |
| `review_queue_triage_parent/review_queue_triage_20260407_190119/` | `120 --md` bundle |
| `blocker_matrix.md` | Blocker taxonomy |
| `repo_truth_reconciliation.md` | Claim-by-claim stale/current assessment |
| `commands_run.log` | Exact command sequence |

## Exactly one manuscript verdict

**TECHNICALLY PASSING BUT BLOCKED BY HUMAN REVIEW + FINAL LAB WAVE**
