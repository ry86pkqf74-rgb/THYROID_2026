# Cursor Composer Dispatch — mig_280: cohort_m037 + cohort_m025 BinderException repair (TIMESTAMP vs DATE drift)

**Generated:** 2026-05-03 by Cowork at HEAD `2cfa535`.
**Lane:** mig_280 — During mig_277 NIFTP full reclassify, the dependent cohort views threw `BinderException` on TIMESTAMP-vs-DATE column drift:
- `manuscript_workspace.cohort_m037_ln_metastasis_v1` — ERROR
- `manuscript_workspace.cohort_m025_tirads_performance_v1` — ERROR

These views were created against pre-mig_160b column types (TIMESTAMP) but mig_160b retyped CPM date cols to DATE. The view bodies still reference the old type, so the resolve fails on mig_277 cascade. mig_280 = `CREATE OR REPLACE VIEW` for both with corrected DATE casts.

**Recommended agent:** **Cursor Composer** — mechanical type-cast fix.
**Estimated runtime:** 20 min.
**Closes:** CF-mig277-COHORT-VIEW-BINDER + CF-mig160b-COHORT-VIEW-CASCADE.

---

## §0 — First message to paste into Cursor Composer

> mig_280 dispatch. Read `cursor_prompts/CURSOR_PROMPT_MIG_280_COHORT_VIEW_DATE_RETYPE_20260503.md`. MotherDuck DB is `thyroid_canonical_publication_v1_0`. Reproduce the BinderException, identify the offending CAST, swap to DATE, CREATE OR REPLACE both views, verify resolve.

---

## §1 — Reproduce the error

```sql
-- Force resolve to surface the exact binder error message + line
SELECT * FROM manuscript_workspace.cohort_m037_ln_metastasis_v1 LIMIT 1;
SELECT * FROM manuscript_workspace.cohort_m025_tirads_performance_v1 LIMIT 1;
```

Expected: BinderException citing a TIMESTAMP-vs-DATE comparison or arithmetic. Note the offending column.

---

## §2 — Inspect view definitions

```sql
SELECT view_definition FROM information_schema.views
WHERE table_schema='manuscript_workspace'
  AND table_name IN ('cohort_m037_ln_metastasis_v1','cohort_m025_tirads_performance_v1');
```

Identify all `CPM.<date_col>` references and flag any that:
- Use `TIMESTAMP` literals (e.g. `WHERE first_surgery_date >= TIMESTAMP '2010-01-01'`)
- Compare CPM date cols (now DATE per mig_160b) to TIMESTAMP-typed expressions
- DATE_DIFF / DATE_ADD with mixed-type arguments

Cross-reference against the mig_160b list of retyped cols (per `project_mig_160b_closeout_2026-04-30.md`):
- 4 ISO VARCHAR + 7 MM/DD + 8 TIMESTAMP → DATE
- 2 TZ → TS

---

## §3 — Apply

For each view:

### §3a — Pre-snapshot the view definition

```sql
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.view_def_<view_name>_pre_mig280_20260503 AS
SELECT view_name, view_definition, CURRENT_TIMESTAMP AS snapshot_at
FROM information_schema.views
WHERE table_schema='manuscript_workspace' AND table_name='<view_name>';
```

### §3b — CREATE OR REPLACE with corrected casts

Pattern:
- `WHERE first_surgery_date >= TIMESTAMP '2010-01-01'` → `WHERE first_surgery_date >= DATE '2010-01-01'`
- `CAST(some_date_col AS TIMESTAMP)` → drop the cast (already DATE) OR `CAST(some_date_col AS DATE)`
- `DATE_DIFF('day', surg_date, follow_date::TIMESTAMP)` → `DATE_DIFF('day', surg_date, follow_date)`

### §3c — Verify resolve

```sql
SELECT COUNT(*) AS n FROM manuscript_workspace.cohort_m037_ln_metastasis_v1;
SELECT COUNT(*) AS n FROM manuscript_workspace.cohort_m025_tirads_performance_v1;
```

Both should return non-NULL counts.

### §3d — Cohort sanity check

```sql
-- M037: malignant + LN-staging-known
SELECT COUNT(*) FROM manuscript_workspace.cohort_m037_ln_metastasis_v1;
-- Expected: ~1,500-2,500 range (varies on inclusion crit)

-- M025: TIRADS-categorized
SELECT COUNT(*) FROM manuscript_workspace.cohort_m025_tirads_performance_v1;
-- Expected: ~1,500-3,500 range
```

If counts look way off vs prior, surface to Logan before signoff.

---

## §4 — Registry signoff

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_280', CURRENT_TIMESTAMP, 'cursor_composer_mig280',
 'mig_280: CREATE OR REPLACE VIEW for manuscript_workspace.cohort_m037_ln_metastasis_v1 + cohort_m025_tirads_performance_v1 to fix BinderException from mig_160b TIMESTAMP→DATE drift cascade surfaced during mig_277 apply. Pre-state: BinderException on resolve. Post-state: views resolve cleanly. Cohort counts: M037 n=<X>, M025 n=<Y>. Closes CF-mig277-COHORT-VIEW-BINDER + CF-mig160b-COHORT-VIEW-CASCADE.');
```

---

## §5 — Carry-forwards

| ID | Status | Notes |
|---|---|---|
| CF-mig277-COHORT-VIEW-BINDER | **CLOSED on apply** | Both views resolve |
| CF-mig160b-COHORT-VIEW-CASCADE | **CLOSED on apply** | First post-mig_160b view repair |
| CF-mig280-OTHER-VIEW-AUDIT | **OPEN** | Sweep all `manuscript_workspace.cohort_*` + `main.cohort_*` views for similar TIMESTAMP/DATE drift; expected hits: 0-3 more |

---

## §6 — Surgical git add

```
qc_framework_v1/migrations/280_cohort_view_date_retype_20260503.sql
scripts/output/mig_280_apply_log.txt
cursor_prompts/CURSOR_PROMPT_MIG_280_COHORT_VIEW_DATE_RETYPE_20260503.md
```

Commit message:
```
fix(md): mig_280 repair cohort_m037 + cohort_m025 BinderException (DATE retype cascade)

- CREATE OR REPLACE both views with DATE casts (was TIMESTAMP, broke on mig_160b)
- Surfaced during mig_277 apply (NIFTP reclassify cascade)
- Both views resolve cleanly post-fix
- Closes CF-mig277-COHORT-VIEW-BINDER + CF-mig160b-COHORT-VIEW-CASCADE
- Opens CF-mig280-OTHER-VIEW-AUDIT for full cohort_* sweep
```

---

**End of mig_280 dispatch.**
