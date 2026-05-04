# Cursor Composer Dispatch — mig_294b: Drop legacy `nlp_tirads_max_category` (mig_294 retry)

**Generated:** 2026-05-04 by Cowork at HEAD `e590e40`.
**Lane:** mig_294b — Original mig_294 (drop legacy tirads col) was the only prompt from the prior batch of 4 that did not land. mig_293b/295/296 all completed. Retry the col drop after consumer audit.
**Recommended agent:** **Cursor Chat (Sonnet 4 / GPT-5) → Composer**.
**Estimated runtime:** 30 min.
**Severity:** LOW.
**Closes:** CF-mig282-LEGACY-NLP-COL.

---

## §0 — First message

> mig_294b dispatch (mig_294 retry). See `cursor_prompts/CURSOR_PROMPT_MIG_294_DROP_LEGACY_TIRADS_COL_20260504.md` for full scope. Same expected deliverable. MotherDuck DB is `thyroid_canonical_publication_v1_0`. `nlp_tirads_max_category` still exists on CPM (verified 2026-05-04 via `information_schema.columns`).

## §1 — Same scope as mig_294

Audit consumers, repoint to `tirads_resolved` (mig_288), pre-snapshot, drop col, verify, signoff.

## §2 — Registry signoff

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_294b', CURRENT_TIMESTAMP, 'cursor_composer_mig294b_retry_of_294',
 'mig_294b: Dropped canonical_patient_master.nlp_tirads_max_category (mig_294 retry). Pre-snapshot to archive. N consumers repointed before drop. Closes CF-mig282-LEGACY-NLP-COL.');
```

## §3 — Surgical git add

```
qc_framework_v1/migrations/294b_drop_legacy_tirads_col_20260504.sql
scripts/output/mig_294b_apply_log.txt
scripts/output/mig_294b_consumer_audit.md
cursor_prompts/CURSOR_PROMPT_MIG_294B_DROP_LEGACY_TIRADS_COL_RETRY_20260504.md
```

---

**End of mig_294b dispatch.**
