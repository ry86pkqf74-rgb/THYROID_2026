# THYROID_2026 Dive Audit — Executive Report

**Audit date**: 2026-04-17
**Owner**: `logan_glosser_eras`
**Canonical database**: `thyroid_canonical_publication_v1_0`
**Scope**: all 31 Dives belonging to the THYROID_2026 publication project (19 M-series + 12 T-series)
**Method**: per-Dive JSX extraction → fully-qualified SQL parsing → column-level cross-check against live `information_schema.columns`

---

## Headline

**The fleet is clean.** Every Dive binds to the canonical DB. No Dive references a dropped table, a destructive Phase 266c-Phase-4 rename, an archive schema, or an alternate database. 154 of 157 column references resolve unchanged against the current schema. The remaining 3 references resolve — but sit on top of the Phase 4.6 AJCC T/stage-group semantic shift and need your one-line confirmation on intent before I touch them.

| Classification | Count | Action |
|---|---|---|
| HEALTHY (no issues) | 20 | None |
| DOC_DEBT (SQL healthy, description misleading) | 8 | Optional, two paths available |
| SEMANTIC_SHIFT_WATCH (column exists, meaning shifted) | 3 | **Awaiting your decision** |
| BROKEN (refs missing/renamed) | 0 | n/a |

---

## Decision required (3 Dives)

Phase 4.6 promoted the `_corrected` AJCC derivations to bare names and renamed the prior bare columns with `_with_microete_t3b_DEPRECATED`. Three Dives project columns whose meaning shifted under that change. They will render without error — but historical screenshots used the pre-4.6 derivation.

- **M043 — LN Metastasis Predictors**: projects `ajcc8_t_stage`
- **M044 — AJCC Staging ETE Impact**: projects `ajcc8_stage_group`
- **T1 — Whole-Cohort Pathology Descriptives**: projects `ajcc8_stage_group`

**Question for you**: for each, keep the new corrected semantic (default, no SQL edit; I'd add a one-line Phase 4.6 caption note) or restore the deprecated with-microETE-T3b semantic (SQL edit)?

---

## Optional cleanup (8 Dives)

T4, T5, T6, T7, T8, T9, T10, T12 each have a JSX description claiming a dedicated `cohort_m0NN_*` view, but the actual `useSQLQuery` queries `cohort_descriptive_full_cohort_v1` with inline filters. Output is unaffected; this is cosmetic. Two paths: rewrite the description (small) or build the dedicated views (bigger lift).

---

## What's in this folder

All artifacts in `/THYROID_2026/dive_audit_20260418/`:

- **`dive_inventory.md`** — Every Dive with ID, title, bound cohort view, access mode, timestamp. Documents the M-series (direct DB) vs. T-series (share) split.
- **`dive_sql_references.csv`** — Every (Dive, db, schema, table, column, ref_type) tuple I extracted from the JSX. ~140 rows.
- **`dive_validation_report.csv`** — Every reference classified against `information_schema`. 154 × `EXISTS`, 3 × `EXISTS_SEMANTIC_SHIFT`, 0 × `MISSING` / `RENAMED` / `STALE_DB`.
- **`dive_triage_report.md`** — Per-Dive triage grouped by bucket with decision sequence. Read this before approving any edit.
- **`DIVE_AUDIT_REPORT_20260418.md`** — this file.

---

## What I did NOT do

- No `edit_dive_content` calls were issued.
- No `query_rw` calls were issued.
- No schema modifications, no Dive modifications, no share modifications.

Per the original brief, every potential edit is held pending your per-Dive approval.

---

## Suggested next step

Give me a thumbs-up/thumbs-down on each of M043, M044, T1 — keep new semantic (with caption note) or restore deprecated semantic. That's the only blocker to closing the audit. Doc-debt cleanup on T4–T10+T12 can follow whenever you want, separately.
