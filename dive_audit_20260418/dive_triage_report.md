# THYROID_2026 Dive Triage — 2026-04-17

**Source artifacts**: `dive_inventory.md`, `dive_sql_references.csv`, `dive_validation_report.csv`
**Schema authority**: live `information_schema.columns` on `thyroid_canonical_publication_v1_0` (queried 2026-04-17, all 21 referenced cohort views; 718 column rows confirmed)
**Dives audited**: 31 of 31

---

## Triage summary

| Bucket | Count | Dives | SQL action required? |
|---|---|---|---|
| **BROKEN** (refs missing tables/columns) | **0** | — | n/a |
| **SEMANTIC_SHIFT_WATCH** (column exists, but Phase 4.6 changed its meaning) | **3** | M043, M044, T1 | **Logan decision required** — see §1 |
| **DOC_DEBT** (SQL healthy, description misleading) | **8** | T4, T5, T6, T7, T8, T9, T10, T12 | Optional — see §2 |
| **HEALTHY** (no issues) | **20** | All M-series except M043/M044; T2, T3, T11 | None |

No Dive references a dropped table, a renamed `*_deprecated_un_versioned_20260417` column, a `Thyroid 2026 UPdated` archive schema, or any database other than `thyroid_canonical_publication_v1_0`. All 31 Dives bind to the canonical DB (19 directly, 12 via the `thyroid_publication_v1_0_readonly` share — both aliased to `thyroid_canonical_publication_v1_0` so fully-qualified SQL works unchanged).

---

## §1 — SEMANTIC_SHIFT_WATCH (3 Dives)

These Dives reference columns that **still exist on the bound cohort view**, but Phase 4.6 promoted the `_corrected` derivation to the bare name and renamed the prior bare column with a `_with_microete_t3b_DEPRECATED` suffix. Historical Dive output may differ from current output even though the SQL still parses and returns rows. **No edit should be made without your decision on which derivation to project.**

### M043 — LN Metastasis Predictors (`cohort_m043_ln_predictors_v1`)

- **Reference**: `ajcc8_t_stage` (1 SELECT clause, 1 GROUP BY)
- **Status**: column present on view; semantic shifted under Phase 4.6
- **Decision needed**: keep new corrected semantic (default), or restore prior with-microETE-T3b semantic via `ajcc8_t_stage_with_microete_t3b_DEPRECATED`?
- **If keeping new semantic**: no edit required; cohort view already projects the corrected column. Recommend a one-line caption note in the Dive header (`/* Phase 4.6: ajcc8_t_stage = corrected derivation */`) so future readers don't misread historical screenshots.
- **If restoring old semantic**: SQL change required. I will not make this change without your explicit go-ahead.

### M044 — AJCC Staging ETE Impact (`cohort_m044_ajcc_ete_v1`)

- **Reference**: `ajcc8_stage_group` (1 SELECT clause, 1 GROUP BY, 1 ORDER BY)
- **Status**: column present on view; downstream-derived from `ajcc8_t_stage` whose semantic shifted
- **Decision needed**: confirm which derivation `cohort_m044_ajcc_ete_v1` projects. CPM still exposes `ajcc8_stage_group_corrected` as a separate un-renamed fallback if you want explicit naming.
- **If keeping new semantic**: no edit required; same caption-note recommendation as M043.
- **If restoring old semantic**: SQL change required (project `ajcc8_stage_group_corrected` or rebuild from `ajcc8_t_stage_with_microete_t3b_DEPRECATED`). I will not make this change without your go-ahead.

### T1 — Whole-Cohort Pathology Descriptives (`cohort_descriptive_full_cohort_v1`)

- **Reference**: `ajcc8_stage_group` (1 SELECT clause inside a CASE, 1 GROUP BY)
- **Status**: column present on view; same Phase 4.6 derivation shift as M044
- **Decision needed**: same as M044 — new corrected semantic vs. deprecated with-microETE-T3b semantic.
- **If keeping new semantic**: no edit required.
- **If restoring old semantic**: SQL change required.

---

## §2 — DOC_DEBT (8 Dives, optional cleanup)

These Dives are **functionally healthy** — every SQL reference resolves and returns data. The issue is cosmetic: the Dive description (the comment block at the top of the JSX) advertises a dedicated `cohort_m0NN_*` view, but the actual `useSQLQuery` hits `cohort_descriptive_full_cohort_v1`. This is a documentation lie, not a SQL bug, so it has zero effect on outputs.

| Dive | Description claims | SQL actually queries |
|---|---|---|
| T4 — Molecular Testing Applications | (dedicated molecular view) | `cohort_descriptive_full_cohort_v1` |
| T5 — Post-op Surveillance & Tg Kinetics | (dedicated surveillance view) | `cohort_descriptive_full_cohort_v1` |
| T6 — RAI Treatment Outcomes | (dedicated RAI view) | `cohort_descriptive_full_cohort_v1` |
| T7 — Parathyroid Intraop & Pathology | (dedicated parathyroid view) | `cohort_descriptive_full_cohort_v1` |
| T8 — TIRADS Decision Support | (dedicated TIRADS view) | `cohort_descriptive_full_cohort_v1` |
| T9 — Risk Stratification & Reclassification | (dedicated risk view) | `cohort_descriptive_full_cohort_v1` |
| T10 — Age & Epidemiology | (dedicated age view) | `cohort_descriptive_full_cohort_v1` |
| T12 — Hereditary & Immunologic | (dedicated hereditary view) | `cohort_descriptive_full_cohort_v1` |

**Two paths forward — your pick, per Dive**:
- **Path A (description-only edit)**: rewrite the JSX header comment to honestly say "queries `cohort_descriptive_full_cohort_v1` with inline filters". Zero query-output change. Eight one-line edits.
- **Path B (build the dedicated views)**: create the dedicated `cohort_m0NN_*` views these descriptions promise, then point the SQL at them. Bigger lift; only worth it if you'd benefit from the named filter-encapsulation downstream.

I will not make either edit without per-Dive go-ahead.

---

## §3 — HEALTHY (20 Dives, no action)

**M-series (17)**: M025, M028, M029, M030, M031, M032, M033, M035, M036, M037, M038, M039, M040, M042, M045, M046, M047
**T-series (3)**: T2 (Frozen Section), T3 (Graves/Hashimoto), T11 (Indeterminate Nodule Outcomes)

For each: every column referenced exists on the bound view; the bound view name in the description matches the bound view name in the SQL; no Phase 4.6 semantic-shift columns are touched; no destructive-rename casualties.

---

## §4 — What I checked, explicitly

For each of the 31 Dives I extracted (and recorded in `dive_sql_references.csv`):
- the database string in `REQUIRED_DATABASES`
- every fully-qualified table reference in every `useSQLQuery` template literal
- every column name (including columns inside `CASE WHEN`, `COUNT(CASE WHEN ...)`, `GROUP BY`, `ORDER BY`, and `WHERE`)
- every join key

Then for each (table, column) pair I queried `information_schema.columns` on `thyroid_canonical_publication_v1_0` and recorded the result in `dive_validation_report.csv`. 154 of 157 column references classified `EXISTS`; 3 classified `EXISTS_SEMANTIC_SHIFT` (the §1 cases). Zero `MISSING`, zero `RENAMED`, zero `STALE_DB`.

Specifically verified absent from any Dive's SQL:
- bare `t_stage`, `n_stage`, `m_stage`, `overall_stage` on CTC/TEM (Phase 266c Phase 4 destructive renames) — **0 hits**
- `Thyroid 2026 UPdated`, `Thyroid 2026`, archive DBs — **0 hits**
- any `_deprecated_un_versioned_20260417` column — **0 hits**
- references to dropped tables from Phase 4.6, Phase 5.3, 266b merge, 266c Phases 2/3 — **0 hits**

---

## §5 — Recommended decision sequence

1. **Confirm M043/M044/T1 semantic intent** (§1). This is the only blocker before declaring the fleet "audit-clean."
2. **Optionally** decide doc-debt path A vs. B for T4–T10 + T12 (§2). Can wait.
3. **Optionally** add Phase 4.6 caption notes to M043/M044/T1 even if keeping the new corrected semantic, to prevent future readers misinterpreting old screenshots.

No `edit_dive_content` calls have been made. No DB writes have occurred. Everything above is observational.
