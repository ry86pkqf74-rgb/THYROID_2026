# Cursor Composer Dispatch — mig_288: M025 CPM TIRADS cleanup (mig_282 retry)

**Generated:** 2026-05-04 by Cowork.
**Lane:** mig_288 — mig_282 was authored 2026-05-03 to add a clean `tirads_resolved` enum col on CPM but **was not applied** (no signoff_migration row, no SQL file in qc_framework_v1/migrations/, only a probe-modified `qa/qa_script_cpm_tirads_partB.json` was left in working tree). Retry the cleanup with concrete derivation logic now that the cohort view is verified working post-mig_280.
**Recommended agent:** **Cursor Chat (Sonnet 4 / GPT-5) → Composer** — needs the source-of-truth probe before applying.
**Estimated runtime:** 45 min.
**Triggered by:** mig_282 not applied; M025 still consuming dirty CPM col via cohort-view workaround.
**Closes:** CF-M025-CPM-TIRADS-COL-DIRTY (still open from mig_282).

---

## §0 — First message to paste into Cursor Chat

> mig_288 dispatch (mig_282 retry). Read `cursor_prompts/CURSOR_PROMPT_MIG_288_M025_TIRADS_CPM_RETRY_20260504.md`. The original mig_282 prompt at `cursor_prompts/CURSOR_PROMPT_MIG_282_TIRADS_CPM_CLEANUP_20260503.md` was authored but NOT applied (signoff_migration check confirms). Apply Option C from that prompt: add `tirads_resolved` enum col on CPM, populate from cohort_m025 view derivation. MotherDuck DB is `thyroid_canonical_publication_v1_0`.

---

## §1 — Pre-task probe (must do this first)

```sql
-- 1.1 Confirm mig_282 was indeed not applied
SELECT mig_id FROM main.signoff_migration WHERE mig_id = 'mig_282';
-- If 1 row: STOP, mig_282 already applied; nothing to do.
-- If 0 rows: proceed.

-- 1.2 Find the cohort_m025 derivation logic
SELECT view_definition FROM information_schema.views
WHERE table_schema='manuscript_workspace' AND table_name='cohort_m025_tirads_performance_v1';
```

Identify which col / regex / lookup the cohort view uses to clean TIRADS. Replicate that in §2.

---

## §2 — Apply (mig_282 Option C path)

### §2a — Pre-snapshot

```sql
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.cpm_tirads_pre_mig288_20260504 AS
SELECT research_id, nlp_tirads_max_category FROM main.canonical_patient_master;
```

### §2b — Add new col + populate

```sql
ALTER TABLE main.canonical_patient_master ADD COLUMN tirads_resolved VARCHAR;

-- Strategy: pull clean TR-N from cohort view if it has it; else regex extract
UPDATE main.canonical_patient_master pm
SET tirads_resolved = (
  -- whichever col the cohort_m025 view exposes that has TR1-TR5 cleanly
  SELECT cm.<clean_col_name>
  FROM manuscript_workspace.cohort_m025_tirads_performance_v1 cm
  WHERE cm.research_id = pm.research_id
);

-- Belt-and-suspenders regex fallback for any pm.research_id not in cohort view:
UPDATE main.canonical_patient_master
SET tirads_resolved = CASE
  WHEN nlp_tirads_max_category REGEXP '^TR[1-5]$' THEN nlp_tirads_max_category
  ELSE NULL
END
WHERE tirads_resolved IS NULL;
```

### §2c — Verify

```sql
SELECT tirads_resolved, COUNT(*) AS n,
       COUNT_IF(is_malignant) AS n_malig,
       ROUND(100.0*COUNT_IF(is_malignant)/COUNT(*), 1) AS pct_malig
FROM main.canonical_patient_master
GROUP BY 1 ORDER BY 1;
-- Expected: NULL=~7,500 + TR1-TR5 buckets totalling ~3,375 (matches cohort_m025 row count)
```

### §2d — Registry signoff

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_288', CURRENT_TIMESTAMP, 'cursor_composer_mig288_retry_of_282',
 'mig_288: Added canonical_patient_master.tirads_resolved (VARCHAR enum: TR1-TR5 + NULL). Populated from cohort_m025 view derivation + regex fallback. mig_282 retry (was not applied 2026-05-03). Legacy nlp_tirads_max_category retained but DEPRECATED. Closes CF-M025-CPM-TIRADS-COL-DIRTY.');
```

---

## §3 — Surgical git add

```
qc_framework_v1/migrations/288_m025_tirads_cpm_retry_20260504.sql
scripts/output/mig_288_apply_log.txt
cursor_prompts/CURSOR_PROMPT_MIG_288_M025_TIRADS_CPM_RETRY_20260504.md
```

---

**End of mig_288 dispatch.**
