# Cursor Composer Dispatch — mig_283: Cohort view full audit (TIMESTAMP/DATE drift sweep)

**Generated:** 2026-05-03 by Cowork at HEAD `1284973`.
**Lane:** mig_283 — mig_280 fixed `cohort_m037_ln_metastasis_v1` + `cohort_m025_tirads_performance_v1` BinderException from TIMESTAMP/DATE drift cascade. CF-mig280-OTHER-VIEW-AUDIT was opened. Sweep all `manuscript_workspace.cohort_*` and `main.cohort_*` views for similar drift; CREATE OR REPLACE any that fail to resolve.
**Recommended agent:** **Cursor Composer** — mechanical sweep; per-view fix follows the mig_280 pattern.
**Estimated runtime:** 30-60 min.
**Closes:** CF-mig280-OTHER-VIEW-AUDIT.

---

## §0 — First message to paste into Cursor Composer

> mig_283 dispatch. Sweep all cohort_* views in MD for TIMESTAMP-vs-DATE drift like mig_280. For each that throws BinderException on resolve, apply the same CREATE OR REPLACE VIEW pattern. MotherDuck DB is `thyroid_canonical_publication_v1_0`.

---

## §1 — Inventory all cohort_* views

```sql
SELECT table_schema, table_name, table_type
FROM information_schema.tables
WHERE (table_schema='manuscript_workspace' OR table_schema='main')
  AND (table_name ILIKE 'cohort_%' OR table_name ILIKE '%_cohort_%')
ORDER BY table_schema, table_name;
```

Expected list (incomplete; probe for ground truth):
- `manuscript_workspace.cohort_m025_tirads_performance_v1` — already fixed (mig_280)
- `manuscript_workspace.cohort_m032_descriptive_25yr_v1`
- `manuscript_workspace.cohort_m037_ln_metastasis_v1` — already fixed (mig_280)
- `manuscript_workspace.cohort_m044_ajcc_ete_v1`
- `manuscript_workspace.cohort_m040_reoperative_v1` (if exists)
- `manuscript_workspace.cohort_m051_*` (if exists)
- `main.cohort_m038_massive_goiter_v1`

---

## §2 — Per-view resolve probe

```sql
-- For each view, force resolve. BinderException = needs fix.
SELECT * FROM <schema>.<view_name> LIMIT 1;
```

Capture the exact error message + offending column for each failing view.

---

## §3 — Apply per failing view (mig_280 pattern)

For each view that fails:

### §3a — Pre-snapshot view definition

```sql
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.view_def_<view_name>_pre_mig283_20260503 AS
SELECT view_name, view_definition, CURRENT_TIMESTAMP AS snapshot_at
FROM information_schema.views
WHERE table_schema='<schema>' AND table_name='<view_name>';
```

### §3b — CREATE OR REPLACE with corrected casts

Common fix patterns (per mig_280):
- `WHERE date_col >= TIMESTAMP '<lit>'` → `WHERE date_col >= DATE '<lit>'`
- `CAST(date_col AS TIMESTAMP)` → drop cast (already DATE)
- `DATE_DIFF('day', a, b::TIMESTAMP)` → `DATE_DIFF('day', a, b)`
- `WHERE date_col IS NOT NULL AND date_col != '0000-00-00'` → drop string compare

### §3c — Verify resolve

```sql
SELECT COUNT(*) FROM <schema>.<view_name>;  -- should return non-NULL count
```

---

## §4 — Disposition table (Cursor surfaces to Logan)

| view | resolved? | error | fix applied | new count |
|---|---|---|---|---|
| cohort_m044_ajcc_ete_v1 | ? | ? | ? | ? |
| cohort_m032_descriptive_25yr_v1 | ? | ? | ? | ? |
| ... | | | | |

---

## §5 — Registry signoff

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_283', CURRENT_TIMESTAMP, 'cursor_composer_mig283',
 'mig_283: Cohort view full audit. Probed N views; M failing on BinderException; applied mig_280 pattern (TIMESTAMP→DATE casts) to N-M-K. Disposition table in scripts/output/mig_283_disposition.md. Closes CF-mig280-OTHER-VIEW-AUDIT.');
```

---

## §6 — Surgical git add

```
qc_framework_v1/migrations/283_cohort_view_full_audit_20260503.sql
scripts/output/mig_283_disposition.md
scripts/output/mig_283_apply_log.txt
cursor_prompts/CURSOR_PROMPT_MIG_283_COHORT_VIEW_FULL_AUDIT_20260503.md
```

Commit message:
```
fix(md): mig_283 cohort_* view full audit (mig_280 pattern sweep)

- Probed all cohort_* views in main + manuscript_workspace
- Fixed N views with TIMESTAMP→DATE drift via CREATE OR REPLACE
- Disposition table in scripts/output/mig_283_disposition.md
- Closes CF-mig280-OTHER-VIEW-AUDIT
```

---

**End of mig_283 dispatch.**
