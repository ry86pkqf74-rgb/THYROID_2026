# Cursor Composer Dispatch — mig_294: Drop legacy `nlp_tirads_max_category` from CPM

**Generated:** 2026-05-04 by Cowork at HEAD `7279f23`.
**Lane:** mig_294 — mig_288 added clean `tirads_resolved` enum on CPM and marked `nlp_tirads_max_category` as DEPRECATED. After consumer audit, drop the legacy col to prevent future contamination of analyses that pick the dirty col by mistake.
**Recommended agent:** **Cursor Chat (Sonnet 4 / GPT-5) → Composer** — needs consumer audit before drop.
**Estimated runtime:** 30 min.
**Severity:** LOW.
**Closes:** CF-mig282-LEGACY-NLP-COL.

---

## §0 — First message to paste into Cursor Chat

> mig_294 dispatch. Audit downstream consumers of `canonical_patient_master.nlp_tirads_max_category` (the dirty 345-distinct-value VARCHAR). If clean, drop the col. MotherDuck DB is `thyroid_canonical_publication_v1_0`.

---

## §1 — Consumer audit

### §1a — MD view dependencies
```sql
SELECT table_schema, table_name
FROM information_schema.views
WHERE view_definition ILIKE '%nlp_tirads_max_category%'
ORDER BY table_schema, table_name;
```

### §1b — Repo grep
```bash
cd "/Users/ros/THyroid 2026"
grep -rn "nlp_tirads_max_category" \
  scripts/ M*_submission_package_v1_0/ snowflake_trial/ qc_framework_v1/ \
  --include="*.sql" --include="*.py" --include="*.md" 2>/dev/null
```

For each consumer found: repoint to `tirads_resolved` (mig_288) before drop.

## §2 — Apply

### §2a — Pre-snapshot
```sql
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.cpm_nlp_tirads_max_category_pre_mig294_20260504 AS
SELECT research_id, nlp_tirads_max_category FROM main.canonical_patient_master;
```

### §2b — Drop col
```sql
ALTER TABLE main.canonical_patient_master DROP COLUMN nlp_tirads_max_category;
```

### §2c — Verify
```sql
SELECT column_name FROM information_schema.columns
WHERE table_schema='main' AND table_name='canonical_patient_master'
  AND column_name = 'nlp_tirads_max_category';
-- Expected: 0 rows
```

### §2d — Registry signoff
```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_294', CURRENT_TIMESTAMP, 'cursor_composer_mig294',
 'mig_294: Dropped canonical_patient_master.nlp_tirads_max_category (was dirty 345-distinct-value VARCHAR; superseded by tirads_resolved enum from mig_288). Pre-snapshot to archive. N consumers repointed before drop. Closes CF-mig282-LEGACY-NLP-COL.');
```

---

## §3 — Surgical git add

```
qc_framework_v1/migrations/294_drop_legacy_tirads_col_20260504.sql
scripts/output/mig_294_apply_log.txt
scripts/output/mig_294_consumer_audit.md
cursor_prompts/CURSOR_PROMPT_MIG_294_DROP_LEGACY_TIRADS_COL_20260504.md
```

---

**End of mig_294 dispatch.**
