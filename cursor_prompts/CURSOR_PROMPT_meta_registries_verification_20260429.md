# Cursor Agent Task — Meta-Registry Pair Verification (Protocol v2)

**Generated:** 2026-04-29 (Cowork session, post-Cursor-14 mig_122)
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** at handoff time — `git fetch && git pull` before starting (current tip `8810385`)
**Estimated effort:** 45-60 minutes (2 small self-referential tables; verification is meta)
**Run order:** Lane 18 of next 3-prompt batch (run middle — special methodology)

---

## 1. Goal

Verify the **two meta-registries** under Protocol v2 (paired sign-off):

| Table | Rows | Cols total | not_started | na |
|---|---:|---:|---:|---:|
| canonical_table_signoff_registry_v1 | 175 | 13 | 12 | 1 |
| canonical_column_verification_registry_v1 | 5,503 | 14 | 12 | 2 |

These tables **track verification of every other canonical** in the lakehouse. Verifying them is meta — they verify themselves. Their values are the source-of-truth for the project's verification state.

---

## 2. Schema overview

### canonical_table_signoff_registry_v1 (13 cols)
```
schema_name, table_name, n_columns_total, n_verified, n_not_started,
n_failed, n_na, table_status, signed_off_ts, signoff_migration,
priority_tier, notes, registered_ts
```

### canonical_column_verification_registry_v1 (14 cols)
```
schema_name, table_name, column_name, data_type, ordinal_position,
category, upstream_source, verification_status, verified_by, verified_ts,
verification_method, batch_id, notes, registered_ts
```

`registered_ts` and `verified_ts` are TIMESTAMP — provenance/audit (allowlist OK).

---

## 3. Methodology — meta-consistency verification

Since these tables track every other canonical, the verification method is **internal-consistency-against-information_schema** and **referential-integrity-on-self**.

### 3a. Internal consistency probes (signoff_registry_v1)

```sql
-- Probe 1: every (schema_name, table_name) row corresponds to a real table or view
SELECT t.schema_name, t.table_name
FROM main.canonical_table_signoff_registry_v1 t
LEFT JOIN information_schema.tables i 
  ON i.table_catalog='thyroid_canonical_publication_v1_0' 
 AND i.table_schema=t.schema_name AND i.table_name=t.table_name
WHERE i.table_name IS NULL
  AND NOT (t.table_name LIKE '%legacy%' OR t.table_name LIKE '%archived%');
-- expect: 0 (every registry row corresponds to a live table)

-- Probe 2: n_columns_total matches information_schema column count
SELECT t.table_name, t.n_columns_total, COUNT(c.column_name) AS actual_n_cols
FROM main.canonical_table_signoff_registry_v1 t
LEFT JOIN information_schema.columns c 
  ON c.table_catalog='thyroid_canonical_publication_v1_0' 
 AND c.table_schema=t.schema_name AND c.table_name=t.table_name
WHERE t.table_name LIKE 'canonical_%'
GROUP BY t.table_name, t.n_columns_total
HAVING t.n_columns_total != COUNT(c.column_name);
-- expect: 0 rows (or document any mismatches)

-- Probe 3: math integrity (n_verified + n_na + n_not_started + n_failed = n_columns_total)
SELECT table_name, n_columns_total, (n_verified + n_na + n_not_started + n_failed) AS sum_
FROM main.canonical_table_signoff_registry_v1
WHERE n_columns_total != (n_verified + n_na + n_not_started + n_failed);
-- expect: 0

-- Probe 4: table_status encoding consistency
SELECT table_name, table_status, n_not_started, n_failed, n_verified, n_columns_total, n_na
FROM main.canonical_table_signoff_registry_v1
WHERE (table_status='verified' AND (n_not_started > 0 OR n_failed > 0))
   OR (table_status='not_started' AND n_verified > 0);
-- expect: 0 rows
```

### 3b. Internal consistency probes (column_verification_registry_v1)

```sql
-- Probe 1: every (schema_name, table_name, column_name) row corresponds to a real column
SELECT r.schema_name, r.table_name, r.column_name
FROM main.canonical_column_verification_registry_v1 r
LEFT JOIN information_schema.columns c 
  ON c.table_catalog='thyroid_canonical_publication_v1_0' 
 AND c.table_schema=r.schema_name 
 AND c.table_name=r.table_name 
 AND c.column_name=r.column_name
WHERE c.column_name IS NULL
LIMIT 30;
-- expect: 0 rows OR document orphan registry rows

-- Probe 2: data_type matches information_schema 
SELECT r.table_name, r.column_name, r.data_type AS reg_type, c.data_type AS info_type
FROM main.canonical_column_verification_registry_v1 r
JOIN information_schema.columns c 
  ON c.table_catalog='thyroid_canonical_publication_v1_0' 
 AND c.table_schema=r.schema_name 
 AND c.table_name=r.table_name 
 AND c.column_name=r.column_name
WHERE r.data_type != c.data_type LIMIT 30;
-- expect: 0 rows or document drift (likely indicates type evolution; CF candidate)

-- Probe 3: every 'verified' col has verified_by + batch_id + verification_method
SELECT COUNT(*) AS gate4_check
FROM main.canonical_column_verification_registry_v1
WHERE verification_status='verified'
  AND (verified_by IS NULL OR batch_id IS NULL OR verification_method IS NULL);
-- expect: 0 (this is gate 4 of the standing audit; should already pass)

-- Probe 4: ordinal_position matches information_schema (columns weren't reshuffled)
SELECT r.table_name, r.column_name, r.ordinal_position AS reg_pos, c.ordinal_position AS info_pos
FROM main.canonical_column_verification_registry_v1 r
JOIN information_schema.columns c 
  ON c.table_catalog='thyroid_canonical_publication_v1_0' 
 AND c.table_schema=r.schema_name 
 AND c.table_name=r.table_name 
 AND c.column_name=r.column_name
WHERE r.ordinal_position != c.ordinal_position LIMIT 30;
```

### 3c. Cross-table consistency (signoff vs column registry)

```sql
-- Probe: n_verified in signoff matches per-table verified count in column registry
WITH col_agg AS (
  SELECT schema_name, table_name,
    COUNT(*) FILTER (WHERE verification_status='verified') AS n_verified,
    COUNT(*) FILTER (WHERE verification_status='na') AS n_na,
    COUNT(*) FILTER (WHERE verification_status='not_started') AS n_not_started,
    COUNT(*) FILTER (WHERE verification_status='failed') AS n_failed
  FROM main.canonical_column_verification_registry_v1
  GROUP BY 1,2
)
SELECT t.table_name,
  t.n_verified - col_agg.n_verified AS d_verified,
  t.n_na - col_agg.n_na AS d_na,
  t.n_not_started - col_agg.n_not_started AS d_not_started,
  t.n_failed - col_agg.n_failed AS d_failed
FROM main.canonical_table_signoff_registry_v1 t
JOIN col_agg USING (schema_name, table_name)
WHERE (t.n_verified, t.n_na, t.n_not_started, t.n_failed) IS DISTINCT FROM 
      (col_agg.n_verified, col_agg.n_na, col_agg.n_not_started, col_agg.n_failed);
-- expect: 0 rows (consistent across both registries)
```

### 3d. Per-col verification map

For each not_started col in the meta-registries, the verification method is:
- IDs/keys (schema_name, table_name, column_name): internal_consistency (referential to information_schema)
- Counts (n_columns_total, n_verified, n_na, n_not_started, n_failed): math integrity (sum = total per row)
- Statuses (table_status, verification_status): enum check + cross-registry consistency
- Provenance (signed_off_ts, registered_ts, verified_ts): existence + monotonicity (verified_ts >= registered_ts)
- Free-text (notes, signoff_migration, batch_id, verification_method): non-null on verified rows
- Metadata (priority_tier, category, upstream_source, data_type): vocab/enum integrity

### 3e. Sign-off SQL
File: `qc_framework_v1/migrations/<next-N>_meta_registries_signoff.sql`
- ~24 col flips total across 2 tables (12 + 12 not_started)
- 3 already-na cols carry over
- 2 table_status updates
- Single migration covering both registries (paired)

---

## 4. Acceptance gates

- All 24 not_started cols flipped (or fewer with documented na)
- 0 errors on probes 3a × 4 + 3b × 4 + 3c
- Both tables: table_status='verified', signoff_migration populated
- After this lane: registries themselves are first-class verified canonicals

---

## 5. Don't touch (active parallel lanes)

- `canonical_survival_followup_v1` — Lane 15 (still in flight)
- `canonical_molecular_genetics_from_notes_v2` — Lane 16 (still in flight)
- `canonical_recurrence_resolved_v1` — Sibling Lane 17
- `canonical_recurrence_v1` Script 203 rebuild — Sibling Lane 19

---

## 6. Reference reading

Required:
- Auto-memory: `project_cleanliness_audit_2026-04-29.md` (mig_109 5-gate audit pattern)
- Auto-memory: `feedback_audit_regex_word_boundary.md` (mig_117 audit refinement)
- Auto-memory: `feedback_motherduck_direct_check.md`
- Auto-memory: `feedback_surgical_git_add.md`
- Repo: `qc_framework_v1/migrations/109_verified_tables_cleanliness_audit_20260429.sql` (audit template)
- Repo: `qc_framework_v1/migrations/117_audit_drift_reconciliation_20260429.sql` (allowlist + regex refinement)

---

## 7. File / commit conventions

- Author: `Logan Glosser <logan.glosser@gmail.com>`
- Single commit closing both meta-registries
- DuckDB CAST CURRENT_TIMESTAMP AS TIMESTAMP
- Surgical git add

---

## 8. If something unexpected surfaces

- Probe 3a probe 1 returns >0 rows → orphan registry rows (table named in registry but not present in catalog); CF candidate
- Probe 3b probe 1 returns >0 rows → orphan column-registry rows; CF candidate
- Probe 3c returns >0 rows → cross-registry math diverged; STOP and reconcile (likely an UPDATE that touched col registry without touching signoff registry)
- data_type or ordinal_position drift on >5 cols → indicates schema evolution since registry seed; document as CF
- Verifying the row that says "this row is verified" recursively — accept the inherent self-reference as a fixed point; document in close-out

---

End of prompt. Lane 18 of new 3-prompt batch. Closes both meta-registries (special meta verification). The lakehouse is then audit-clean at the meta level — registries themselves are verified.
