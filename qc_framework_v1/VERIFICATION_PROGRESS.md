# Verification Progress Dashboard

**Last refreshed:** 2026-04-27
**Master plan:** [`MASTER_VERIFICATION_PLAN.md`](MASTER_VERIFICATION_PLAN.md)
**Source registries:** `main.canonical_column_verification_registry_v1`, `main.canonical_table_signoff_registry_v1`

This file is auto-regenerated each session. To refresh manually, run
`qc_framework_v1/scripts/refresh_progress_dashboard.py` (TBD; for now, regenerated
inline by Claude/Cowork at the end of each verification session).

---

## Headline numbers

| Metric | Count |
|---|---|
| Tables in scope | **184** base tables (`main` + `manuscript_workspace`) |
| Tables registered | 175 |
| Tables verified | **0 / 184** (0 %) |
| Columns in scope | 5,496 |
| Columns verified | **767 / 5,496** (14 % — all `na_provenance` auto-verified) |
| Columns to actually verify | **4,729** |

## By tier

| Tier | Tables | Cols | Verified | Not started | NA |
|---|---|---|---|---|---|
| pilot | 1 | 40 | 0 | 35 | 5 |
| tier1_anchor (`canonical_patient_master`) | 1 | 1,592 | 0 | 1,588 | 4 |
| tier1_events | 18 | 466 | 0 | 320 | 146 |
| tier1_source | 12 | 909 | 0 | 860 | 49 |
| tier2_canonical | 16 | 333 | 0 | 282 | 51 |
| tier2_rollups | 19 | 616 | 0 | 569 | 47 |
| tier3_extraction | 17 | 372 | 206 (10 tables fully auto-verified) | 63 | 103 |
| tier3_helper | 91 | 1,168 | 0 | 1,012 | 156 |
| **total** | **175** | **5,496** | **206** | **4,729** | **561** |

## By verification category

| Category | Cols | Status |
|---|---|---|
| `na_provenance` | 767 | auto-verified |
| `derived` | 1,150 | needs derivation rule certification |
| `source` | 745 | needs sample-based verification |
| `adjudicated` | 2,834 | needs Logan CSV review |

## Next up

The pilot table starts the workflow. After pilot, tables are processed in
priority-tier order: tier1_events → tier1_source → tier1_anchor (split into batches)
→ tier2_canonical → tier2_rollups → tier3_extraction → tier3_helper.

| # | Table | Tier | Cols | To verify |
|---|---|---|---|---|
| 1 | `main.canonical_fna_events_v1` | pilot | 40 | 35 |
| 2 | `main.canonical_path_malignant_events_v1` | tier1_events | 60 | TBD after re-tier |
| 3 | `main.canonical_operative_events_v1` | tier1_events | 54 | TBD |
| 4 | `main.canonical_path_benign_events_v1` | tier1_events | 55 | TBD |
| 5 | `main.canonical_us_lymph_node_v2` | tier1_source | 29 | TBD |

## Verified tables

(none yet — see [`VERIFIED_TABLES.md`](VERIFIED_TABLES.md) once tables sign off)

## Failed / blocked

(none)

---

*Refresh command (run after each batch):*
```sql
-- Update table-level counts
UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total = subq.n_total,
    n_verified      = subq.n_verified,
    n_not_started   = subq.n_not_started,
    n_failed        = subq.n_failed,
    n_na            = subq.n_na,
    table_status    = CASE
      WHEN subq.n_not_started + subq.n_failed = 0 THEN 'verified'
      WHEN subq.n_verified > 0 OR subq.n_failed > 0 THEN 'in_progress'
      ELSE 'not_started'
    END
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified' THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed' THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na' THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;
```
