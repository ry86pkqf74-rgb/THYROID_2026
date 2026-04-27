# Verification Progress Dashboard

**Last refreshed:** 2026-04-27 (post-mig_64 — pilot Step A re-tier complete)
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
| Tables verified | **0 / 184** (0 %) |
| Columns in scope | 5,496 |
| Columns Logan-verified | **0 / 5,496** |
| Columns at `not_started` (in v2 queue) | **4,734** |
| Columns at `na` (legacy v1 auto-skip, pending re-tier) | **762** |

**Note:** Under Protocol v2 the `na` status is deprecated. As each table reaches
its slot in the priority queue, its remaining `na` columns will be re-tiered
under v2 (Step A) and either reset to `not_started` (if they have a source
counterpart) or kept at `not_started` and flipped to `verified` only at table
sign-off (Step D) for `auto_no_source_counterpart` columns.

## By tier

| Tier | Tables | Cols | Verified | Not started | NA (v1 legacy) |
|---|---|---|---|---|---|
| pilot | 1 | 40 | 0 | **40** | 0 |
| tier1_anchor | 1 | 1,592 | 0 | 1,588 | 4 |
| tier1_events | 18 | 466 | 0 | 320 | 146 |
| tier1_source | 12 | 909 | 0 | 860 | 49 |
| tier2_canonical | 16 | 333 | 0 | 282 | 51 |
| tier2_rollups | 19 | 616 | 0 | 569 | 47 |
| tier3_extraction | 17 | 372 | 0 | 63 | 309 |
| tier3_helper | 91 | 1,168 | 0 | 1,012 | 156 |
| **total** | **175** | **5,496** | **0** | **4,734** | **762** |

## Pilot table snapshot — `main.canonical_fna_events_v1`

After mig_64 Step A re-tier, all 40 columns have a verification method assigned:

| Method | Cols | Source / Rule |
|---|---|---|
| `auto_no_source_counterpart` | 14 | Pure provenance + pipeline trace; verified at Step D table sign-off |
| `mechanical_source_compare` | 7 | `FNAs 12_5_2025.xlsx` sheet `FNA Bethesda` (wide), unpivoted by FNA index 1..12 |
| `mechanical_derivation_compare` | 16 | Re-run derivation rule against stored value |
| `manual_source_review` | 3 | Logan reviews each row alongside upstream raw text |

**Adjudicated columns requiring full per-row review:** `laterality`, `bethesda_calculated_num`, `subtype` (3 of 40).

## Next up

1. **`main.canonical_fna_events_v1.fna_date_raw`** — first canary verification CSV. Pure source compare against the `Date` cell of `FNAs 12_5_2025.xlsx > FNA Bethesda`. Will validate the CSV format before batch-generating the remaining 22 substantive columns.
2. After pilot signs off, queue is `tier1_events` alphabetical: `canonical_path_malignant_events_v1` (60 cols), `canonical_operative_events_v1` (54), `canonical_path_benign_events_v1` (55), …

## Verified tables

(none yet — see [`VERIFIED_TABLES.md`](VERIFIED_TABLES.md))

## Failed / blocked

(none)

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
