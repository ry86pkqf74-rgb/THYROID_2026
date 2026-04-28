# Verification Progress Dashboard

**Last refreshed:** 2026-04-28 (post-mig_83 — AIRWAY INVASION SIGNED OFF)
**Master plan:** [`MASTER_VERIFICATION_PLAN.md`](MASTER_VERIFICATION_PLAN.md)
**Active protocol:** v2 (full-row mechanical compare — see plan §6 and §6a)
**Source registries:** `main.canonical_column_verification_registry_v1`, `main.canonical_table_signoff_registry_v1`

This file is regenerated each session after registry writes. Updates land in
the same Cowork session that runs the `query_rw` updates, then commit + push.

---

## Headline numbers

| Metric | Count |
|---|---|
| Tables in scope | **184** base tables (`main` + `manuscript_workspace`) |
| Tables registered | 175 |
| Tables verified under Protocol v2 | **2 / 184** (1.1 %) — FNA pilot + airway invasion |
| Tables `verified` in registry (pre-v2 legacy + v2) | 12 (10 are pre-v2 placeholders with NULL signed_off_ts) |
| Columns in scope | 5,494 |
| Columns Logan-verified (v2) | **61 / 5,494** (38 FNA + 23 airway invasion) |
| Columns at `not_started` (in v2 queue) | **4,683** |
| Columns at `na` (legacy v1 auto-skip, pending re-tier) | **750** |
| Columns at `failed` (deferred carry-forward) | 1 (`canonical_fna_events_v1.days_to_surgery`) |

**Note:** Under Protocol v2 the `na` status is deprecated. As each table reaches
its slot in the priority queue, its remaining `na` columns will be re-tiered
under v2 (Step A) and either reset to `not_started` (if they have a source
counterpart) or kept at `not_started` and flipped to `verified` only at table
sign-off (Step D) for `auto_no_source_counterpart` columns.

## Verified table snapshots

### `main.canonical_fna_events_v1` (PILOT)

38 columns / 8,050 rows / 14-migration arc (mig_65 → mig_78):

| Method | Cols | Source / Rule |
|---|---|---|
| `auto_no_source_counterpart` | 14 | Provenance + pipeline trace; verified at Step D |
| `mechanical_source_compare` | 7 | `FNAs 12_5_2025.xlsx > FNA Bethesda` |
| `mechanical_derivation_compare` | 14 | Re-run derivation rule against stored value |
| `manual_source_review` | 3 | Per-row review (`laterality`, `bethesda_calculated_num`, `fna_site`) |

### `main.canonical_airway_invasion_events_v1`

23 columns / 3,155 rows / 4-migration arc (mig_80 → mig_83):

| Method | Cols | Source / Rule |
|---|---|---|
| `auto_no_source_counterpart` | 15 | Provenance + LLM metadata; verified at Step D |
| `mechanical_derivation_compare` | 1 | `t4a_implication` (derived per Logan's findings-vs-staging rule) |
| `manual_source_review` | 7 | 7 clinical findings (per-row Logan review across mig_80-82) |

## Recently verified

- **`main.canonical_airway_invasion_events_v1`** — signed off 2026-04-28 via mig_83 (4-migration arc mig_80 → mig_83). Final state: 3,155 rows / 2,622 patients / 196 positive (138 pT4a + 58 not_pT4a). Established **findings-vs-staging separation rule** (memory: `feedback_findings_vs_staging.md`).
- **`main.canonical_fna_events_v1`** — signed off 2026-04-28 via mig_78 (PILOT, 14-migration arc). Final state: 8,050 rows / 38 cols verified + 1 deferred carry-forward (`days_to_surgery`).

## Next up

Logan's "final cleaning" plan continues with cohort-priority Tier 2 tables. Suggested queue based on staging/clinical importance:

1. **`canonical_path_malignant_events_v1`** (~60 cols) — primary path findings, drives most cohort definitions
2. **`canonical_operative_events_v1`** (~54 cols) — surgery dates/procedures, unblocks FNA `days_to_surgery` carry-forward
3. **`canonical_extrathyroidal_extension_events_v1`** — ETE is the other major staging concern (pT3b vs pT4a); same pattern as airway likely applies
4. **`canonical_lymph_node_events_v1`** family — pN staging
5. Other Tier 2 events tables alphabetically

## Verified tables

See [`VERIFIED_TABLES.md`](VERIFIED_TABLES.md) — 2 entries.

## Failed / blocked

- **`canonical_fna_events_v1.days_to_surgery`** — DEFERRED carry-forward. Cross-table derivation (fna_date_resolved + canonical_operative_events_v1.surgery_date). Will be revisited when operative events table is verified.

---

*Refresh command (run after each batch via Cowork `query_rw`):*
```sql
UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total = subq.n_total,
    n_verified      = subq.n_verified,
    n_not_started   = subq.n_not_started,
    n_failed        = COALESCE(subq.n_failed,0),
    n_na            = subq.n_na,
    table_status    = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed,0) = 0 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress'
      ELSE 'not_started'
    END
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;
```
