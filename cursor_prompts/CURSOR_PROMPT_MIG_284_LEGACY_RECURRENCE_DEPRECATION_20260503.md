# Cursor Composer Dispatch — mig_284: Legacy recurrence table deprecation (mig_269b)

**Generated:** 2026-05-03 by Cowork at HEAD `1284973`.
**Lane:** mig_284 — mig_269 built `canonical_recurrence_events_v1` + `canonical_recurrence_patient_rollup_v1` as the new SSOT and repointed CPM cols. Legacy `canonical_recurrence_v1`, `canonical_recurrence_resolved_v1`, and `recurrence_event_clean_v1` were left in place pending consumer audit. mig_284 = consumer audit + drop.
**Recommended agent:** **Cursor Chat (Sonnet 4 / GPT-5) → Composer** — needs walking through the consumer dependency graph; drops are mechanical after.
**Estimated runtime:** 60 min.
**Triggered by:** mig_269 close-out CF-mig269b-LEGACY-DEPRECATE.
**Severity:** LOW. New SSOT is live; drops just remove dead code.
**Closes:** CF-mig269b-LEGACY-DEPRECATE.

---

## §0 — First message to paste into Cursor Chat

> mig_284 dispatch. Read `cursor_prompts/CURSOR_PROMPT_MIG_284_LEGACY_RECURRENCE_DEPRECATION_20260503.md`. Identify all downstream consumers of `canonical_recurrence_v1` / `canonical_recurrence_resolved_v1` / `recurrence_event_clean_v1` (cohort views, scripts, manuscript queries). Repoint each to `canonical_recurrence_events_v1` / `_patient_rollup_v1`. Surface a per-consumer disposition table to me before applying drops. MotherDuck DB is `thyroid_canonical_publication_v1_0`.

---

## §1 — Consumer audit

### §1a — MD view dependency probe

```sql
SELECT table_schema, table_name, view_definition
FROM information_schema.views
WHERE view_definition ILIKE '%canonical_recurrence_v1%'
   OR view_definition ILIKE '%canonical_recurrence_resolved_v1%'
   OR view_definition ILIKE '%recurrence_event_clean_v1%'
ORDER BY table_schema, table_name;
```

### §1b — Repo grep (Cursor: use built-in grep)

```bash
cd "/Users/ros/THyroid 2026"
grep -rn "canonical_recurrence_v1\|canonical_recurrence_resolved_v1\|recurrence_event_clean_v1" \
  scripts/ M044_submission_package_v1_0/ M038_submission_package_v1_0/ \
  manuscript_outputs/ snowflake_trial/ qc_framework_v1/ \
  --include="*.sql" --include="*.py" --include="*.md" 2>/dev/null
```

### §1c — Manuscript SQL package audit

For M044 (`08_analysis_code/M044_ETE_analysis.sql`) the header notes:
> -- Recurrence column-of-record: main.canonical_recurrence_resolved_v1

This MUST be repointed to `canonical_recurrence_patient_rollup_v1` as part of mig_284, OR M044 v1.1 numbers will become non-reproducible after the legacy table is dropped.

---

## §2 — Disposition table (Cursor surfaces to Logan)

| Consumer | Type | Currently uses | Needs repoint? |
|---|---|---|---|
| `manuscript_workspace.cohort_m044_ajcc_ete_v1` | view | canonical_recurrence_resolved_v1? | ? |
| `M044_submission_package_v1_0/08_analysis_code/M044_ETE_analysis.sql` | script | canonical_recurrence_resolved_v1 | YES |
| `scripts/m044_*.py` | scripts | ? | ? |
| `manuscript_workspace.cohort_m037_ln_metastasis_v1` | view | ? | ? |
| ... | | | |

---

## §3 — Apply (after Logan disposition sign-off)

### §3a — Repoint consumers

For each downstream view: `CREATE OR REPLACE VIEW … FROM main.canonical_recurrence_patient_rollup_v1`
For each downstream SQL/Python: `git add` the edited script + commit per repoint

### §3b — Pre-snapshot legacy tables

```sql
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_recurrence_v1_pre_mig284_20260503 AS SELECT * FROM main.canonical_recurrence_v1;
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_recurrence_resolved_v1_pre_mig284_20260503 AS SELECT * FROM main.canonical_recurrence_resolved_v1;
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.recurrence_event_clean_v1_pre_mig284_20260503 AS SELECT * FROM main.recurrence_event_clean_v1;
```

### §3c — Drop legacy tables

```sql
DROP TABLE main.canonical_recurrence_v1;
DROP TABLE main.canonical_recurrence_resolved_v1;
DROP TABLE main.recurrence_event_clean_v1;
```

### §3d — Verify

```sql
-- Should return 0 rows (tables gone)
SELECT table_name FROM information_schema.tables
WHERE table_schema='main' AND table_name IN ('canonical_recurrence_v1','canonical_recurrence_resolved_v1','recurrence_event_clean_v1');

-- Verify SSOT still resolves
SELECT COUNT(*) FROM main.canonical_recurrence_events_v1;  -- should be 1,946
SELECT COUNT(*) FROM main.canonical_recurrence_patient_rollup_v1;  -- should be ~1,400
```

### §3e — Registry signoff

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_284', CURRENT_TIMESTAMP, 'cursor_composer_mig284',
 'mig_284: Legacy recurrence table deprecation. Repointed N consumers from canonical_recurrence_v1/_resolved_v1/recurrence_event_clean_v1 to canonical_recurrence_patient_rollup_v1 + canonical_recurrence_events_v1. Pre-snapshot to archive_pub_v1_0. Dropped 3 legacy tables. Closes CF-mig269b-LEGACY-DEPRECATE.');
```

---

## §4 — Carry-forwards

| ID | Status | Notes |
|---|---|---|
| CF-mig269b-LEGACY-DEPRECATE | **CLOSED on apply** | Legacy tables dropped + consumers repointed |
| CF-mig284-MANUSCRIPT-RE-VERIFY | **OPEN** | Cowork should re-run M044 validation runner post-mig_284 to confirm rebuilt cohort views still match v1.1 numbers |

---

## §5 — Surgical git add

```
qc_framework_v1/migrations/284_legacy_recurrence_deprecation_20260503.sql
scripts/output/mig_284_disposition.md
scripts/output/mig_284_apply_log.txt
M044_submission_package_v1_0/08_analysis_code/M044_ETE_analysis.sql  (if repointed)
<other touched script paths>
cursor_prompts/CURSOR_PROMPT_MIG_284_LEGACY_RECURRENCE_DEPRECATION_20260503.md
```

Commit message:
```
chore(md): mig_284 legacy recurrence table deprecation (mig_269b)

- Repointed N consumers to canonical_recurrence_patient_rollup_v1 + canonical_recurrence_events_v1
- Pre-snapshot canonical_recurrence_v1 / _resolved_v1 / recurrence_event_clean_v1 to archive
- Dropped 3 legacy tables
- Closes CF-mig269b-LEGACY-DEPRECATE
```

---

**End of mig_284 dispatch.**
