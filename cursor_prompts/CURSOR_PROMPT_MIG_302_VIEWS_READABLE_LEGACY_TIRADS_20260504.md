# Cursor Composer Dispatch — mig_302: Drop legacy tirads col from `views_readable.Patient_Master_Canonical`

**Generated:** 2026-05-04 by Cowork at HEAD `17baa2b`.
**Lane:** mig_302 — Cowork audit 2026-05-04 confirmed `nlp_tirads_max_category` was successfully dropped from `main.canonical_patient_master` (mig_294b) but it still appears in:
- `views_readable.Patient_Master_Canonical` (1 hit) — Logan-facing read-only view
- `readonly_share.main.canonical_patient_master` (1 hit) — external share copy (separate DB)

mig_302 = repoint `views_readable.Patient_Master_Canonical` to drop the legacy col + add `tirads_resolved`. Skip `readonly_share` (separate DB; Logan handles).
**Recommended agent:** **Cursor Composer**.
**Estimated runtime:** 15 min.
**Severity:** LOW.

---

## §0 — First message

> mig_302 dispatch. Drop `nlp_tirads_max_category` from `views_readable.Patient_Master_Canonical` + replace with `tirads_resolved`. MotherDuck DB is `thyroid_canonical_publication_v1_0`.

## §1 — Apply

```sql
-- Get current view definition
SELECT view_definition FROM information_schema.views
WHERE table_schema='views_readable' AND table_name='Patient_Master_Canonical';

-- Pre-snapshot
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.view_def_patient_master_canonical_pre_mig302_20260504 AS
SELECT view_name, view_definition, CURRENT_TIMESTAMP AS snapshot_at
FROM information_schema.views
WHERE table_schema='views_readable' AND table_name='Patient_Master_Canonical';

-- CREATE OR REPLACE with corrected projection: drop nlp_tirads_max_category, add tirads_resolved
CREATE OR REPLACE VIEW views_readable.Patient_Master_Canonical AS
SELECT * REPLACE (NULL AS nlp_tirads_max_category)
FROM main.canonical_patient_master;
-- (or rebuild explicitly without the col)
```

## §2 — Verify

```sql
SELECT column_name FROM information_schema.columns
WHERE table_schema='views_readable' AND table_name='Patient_Master_Canonical'
  AND column_name IN ('nlp_tirads_max_category', 'tirads_resolved');
-- Expected: only tirads_resolved
```

## §3 — Registry signoff

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_302', CURRENT_TIMESTAMP, 'cursor_composer_mig302',
 'mig_302: Repointed views_readable.Patient_Master_Canonical to drop legacy nlp_tirads_max_category and use tirads_resolved (mig_288 enum). Pre-snapshot to archive_pub_v1_0. Closes mig_294b downstream consumer audit.');
```

## §4 — Surgical git add

```
qc_framework_v1/migrations/302_views_readable_tirads_cleanup_20260504.sql
scripts/output/mig_302_apply_log.txt
cursor_prompts/CURSOR_PROMPT_MIG_302_VIEWS_READABLE_LEGACY_TIRADS_20260504.md
```

---

**End of mig_302 dispatch.**
