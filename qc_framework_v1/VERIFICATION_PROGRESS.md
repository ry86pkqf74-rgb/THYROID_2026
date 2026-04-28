# Verification Progress Dashboard

**Last refreshed:** 2026-04-28 (post-mig_89 — PATH MALIGNANT SIGNED OFF)
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
| Tables verified under Protocol v2 | **3 / 184** (1.6 %) — FNA pilot + airway invasion + path malignant |
| Tables `verified` in registry (pre-v2 legacy + v2) | 13 (10 are pre-v2 placeholders with NULL signed_off_ts) |
| Columns in scope | 5,490 (started 5,494; dropped 4 in mig_84) |
| Columns Logan-verified (v2) | **117 / 5,490** (38 FNA + 23 airway invasion + 56 path malignant) |
| Columns at `not_started` (in v2 queue) | **4,627** |
| Columns at `na` (legacy v1 auto-skip, pending re-tier) | **741** |
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

### `main.canonical_path_malignant_events_v1`

56 columns / 6,689 rows / 4,137 patients / 6-migration arc (mig_84 → mig_89):

| Method | Cols | Source / Rule |
|---|---|---|
| `auto_no_source_counterpart` | 12 | Provenance + pipeline trace; Step D batch flip |
| `mechanical_source_compare` | 1 | `surgery_date` against `path_synoptics.surg_date` (mig_85) |
| `mechanical_derivation_compare` | 43 | `tumor_ordinal` two-path rule (mig_86) + 36 cols via CTC pre361 mass-equivalence (mig_87) + 6 cols via Script 361 UPDATE rule re-run (mig_88) |

**Architectural innovations established (carry forward to subsequent tables):**
1. **CTC-equivalence verification pattern** — for canonicals built via SELECT * + filter + UPDATE chains, archived pre-script snapshot is the value-source-of-truth; one mass-equivalence query verifies dozens of inherited cols at once.
2. **Script-rule re-run verification** — for post-build UPDATE-derived cols, re-execute original UPDATE logic as SELECT and compare.

## Recently verified

- **`main.canonical_path_malignant_events_v1`** — signed off 2026-04-28 via mig_89 (6-migration arc mig_84 → mig_89). Final state: 6,689 rows / 4,137 patients / 56 cols. Established **CTC-equivalence verification pattern** + **Script-rule re-run verification** (carry forward to subsequent tables built by Script-361-style copy-and-update chains).
- **`main.canonical_airway_invasion_events_v1`** — signed off 2026-04-28 via mig_83 (4-migration arc mig_80 → mig_83). Final state: 3,155 rows / 2,622 patients / 196 positive (138 pT4a + 58 not_pT4a). Established **findings-vs-staging separation rule** (memory: `feedback_findings_vs_staging.md`).
- **`main.canonical_fna_events_v1`** — signed off 2026-04-28 via mig_78 (PILOT, 14-migration arc). Final state: 8,050 rows / 38 cols verified + 1 deferred carry-forward (`days_to_surgery`).

## Next up

With path malignant closed and the CTC-equivalence pattern proven, the queue can move quickly through other Script-361-built canonicals:

1. **`canonical_operative_events_v1`** (~54 cols) — built by `scripts/362_operative_consolidation.py`; same SELECT*+UPDATE pattern as path malignant. Unblocks FNA `days_to_surgery` carry-forward.
2. **`canonical_extrathyroidal_extension_events_v1`** — ETE is the other major staging concern (pT3b vs pT4a); same airway-style findings-vs-staging pattern likely applies.
3. **`canonical_lymph_node_events_v1`** family — pN staging.
4. Other Tier 2 events tables alphabetically.

## Verified tables

See [`VERIFIED_TABLES.md`](VERIFIED_TABLES.md) — 3 entries.

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
