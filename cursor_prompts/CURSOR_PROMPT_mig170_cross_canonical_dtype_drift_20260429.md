# Cursor Prompt — mig_170 Cross-Canonical Data-Type Drift Audit

**Lane:** 59 / mig_170
**Batch_id:** `mig_170_cross_canonical_dtype_drift_20260429`
**Generated:** 2026-04-29 (late evening)
**Type:** Read-only audit. Output is a Markdown report + commented probe SQL stub. No data writes.

---

## §0 Why this lane exists

mig_126 (meta-registries close-out) found that `canonical_column_verification_registry_v1` had **9 data_type drifts** between the registry's stored `data_type` field and `information_schema.columns.data_type` for the same `(table_name, column_name)`. That cleanup happened at the registry level, but the underlying question — *do same-name columns across canonical tables actually share the same data_type?* — has never been audited globally.

This lane probes for **cross-canonical data-type drift**: when the same logical concept (e.g., `recurrence_date`, `histology_final`, `t_stage`) appears in multiple canonicals (PM + a Tier-2 events table + a rollup), are all three storing it in the same dtype? If not, that's a JOIN trap waiting to happen for the manuscript pipeline.

## §1 Governance posture

- Read-only against MotherDuck. No `query_rw`.
- Output: `qc_framework_v1/reports/mig_170_cross_canonical_dtype_drift_20260429.md` + `qc_framework_v1/migrations/170_cross_canonical_dtype_drift_probes_20260429.sql` (commented probes).
- Findings open `CF-mig170-DTYPE-DRIFT-<col>-<table_a>-VS-<table_b>` for follow-up retype lanes (mig_170b cluster).

## §2 Required scope

For every column in PM that has a same-named col on at least one verified Tier-2 events / rollup canonical, list the dtype on each side:

```sql
WITH verified_tables AS (
  SELECT table_name FROM main.canonical_table_signoff_registry_v1
  WHERE table_status='verified' AND table_name LIKE 'canonical_%'
),
cols_per_table AS (
  SELECT c.table_name, c.column_name, c.data_type
  FROM information_schema.columns c
  JOIN verified_tables v ON c.table_name = v.table_name
  WHERE c.table_catalog='thyroid_canonical_publication_v1_0' AND c.table_schema='main'
),
shared AS (
  SELECT column_name, COUNT(DISTINCT table_name) AS n_tables_with_col,
         COUNT(DISTINCT data_type) AS n_distinct_dtypes,
         STRING_AGG(DISTINCT (table_name || ':' || data_type), ' | ' ORDER BY table_name) AS table_dtype_pairs
  FROM cols_per_table
  GROUP BY 1
  HAVING COUNT(DISTINCT table_name) >= 2 AND COUNT(DISTINCT data_type) >= 2
)
SELECT * FROM shared ORDER BY n_distinct_dtypes DESC, n_tables_with_col DESC, column_name;
```

Expected: dozens of cross-canonical shared columns, of which several may have dtype drift.

## §3 Per-finding analysis

For each col with `n_distinct_dtypes >= 2`, classify:

- **BENIGN** — dtypes are compatible (e.g., one is BIGINT and the other INTEGER; auto-cast safe). Document, no CF.
- **CASTABLE** — dtypes are different but lossless (e.g., DATE vs TIMESTAMP — DATE is a subset). Open `CF-mig170-DTYPE-DRIFT-CASTABLE-<col>`; recommend retype to the more restrictive type.
- **DATA-LOSS-RISK** — JOIN/COALESCE between these would silently fail or coerce (e.g., VARCHAR vs DOUBLE; TIMESTAMP vs VARCHAR). Open `CF-mig170-DTYPE-DRIFT-DANGEROUS-<col>` with high priority.
- **SEMANTIC-DRIFT** — dtypes are same but the col means something different in each context (e.g., `recurrence_date` is event-grain on Tier-2 but rollup-MIN on PM). Document; probably not actionable but worth noting.

## §4 Cross-canonical JOIN trap probe

For any DATA-LOSS-RISK pair, run a probe to quantify how much data would mismatch on a naive JOIN:

```sql
SELECT
  COUNT(*) FILTER (WHERE pm.<col> IS NOT NULL OR tier2.<col> IS NOT NULL) AS n_either_nonnull,
  COUNT(*) FILTER (WHERE pm.<col> IS NOT NULL AND tier2.<col> IS NOT NULL AND CAST(pm.<col> AS VARCHAR) <> CAST(tier2.<col> AS VARCHAR)) AS n_both_nonnull_unequal,
  COUNT(*) FILTER (WHERE pm.<col> IS NOT NULL AND tier2.<col> IS NULL) AS n_pm_only,
  COUNT(*) FILTER (WHERE pm.<col> IS NULL AND tier2.<col> IS NOT NULL) AS n_tier2_only
FROM main.canonical_patient_master pm
LEFT JOIN main.<tier2_table> tier2 USING (research_id);
```

(Adjust JOIN key per table — some Tier-2s are event-grain, in which case use a DISTINCT subquery per research_id.)

## §5 Findings table (in report)

| col | n_tables | n_distinct_dtypes | drift_classification | dtypes_per_table | recommended_action |
|---|---|---|---|---|---|
| recurrence_date | 3 | 2 | CASTABLE | PM:DATE \| recurrence_v1:DATE \| recurrence_resolved_v1:TIMESTAMP | retype recurrence_resolved_v1.recurrence_date to DATE in mig_170b |
| histology_final | 4 | 2 | CASTABLE | PM:VARCHAR \| path_malignant_events:VARCHAR \| path_malignant_patient_rollup:VARCHAR \| path_gland_events:**ENUM** | review enum compatibility |
| ... | | | | | |

Sort by drift_classification (DATA-LOSS-RISK first), then col.

## §6 SQL stub structure

`170_cross_canonical_dtype_drift_probes_20260429.sql` — fully commented probes:
- §2 shared-cols query
- One §4 JOIN-trap probe per DATA-LOSS-RISK col

No registry writes; no apply.

## §7 Git workflow

- Files: 170 SQL stub + 170 report
- Commit: `qc: mig_170 cross-canonical data-type drift audit (read-only)`
- Push.

## §8 Out of scope

- DO NOT execute any retypes — mig_170b cluster after Logan ratifies findings.
- DO NOT touch unverified canonicals (only those with table_status='verified').
- DO NOT touch VIEW-layer cols (covered by mig_164's pass-through verification).
- DO NOT modify the registry's stored data_type field — that's a separate mig_126 follow-up.
