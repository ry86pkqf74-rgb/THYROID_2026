# Cursor Composer Dispatch — mig_293b: SF VALIDATION_RUN_LOG → MD mirror (mig_293 retry)

**Generated:** 2026-05-04 by Cowork at HEAD `7279f23`.
**Lane:** mig_293b — Original mig_293 (SF→MD validation log mirror) was the only prompt from the prior batch of 4 that did not land. mig_290/291/292 all completed with submission packages. Retry the small validation log mirror.
**Recommended agent:** **Cursor Composer** — mechanical script + table create.
**Estimated runtime:** 30 min.
**Severity:** LOW (cross-platform audit convenience).
**Closes:** CF-mig293-VALIDATION-LOG-MIRROR (still open).

---

## §0 — First message to paste into Cursor Composer

> mig_293b dispatch (mig_293 retry). See `cursor_prompts/CURSOR_PROMPT_MIG_293_VALIDATION_LOG_MD_MIRROR_20260504.md` for full scope. Same expected deliverable. MotherDuck DB is `thyroid_canonical_publication_v1_0`. SF source is `THYROID_VALIDATION.PUBLIC.VALIDATION_RUN_LOG_V1` (now has 17 checks per run, baseline v2).

---

## §1 — Why this dispatch exists

mig_293 was authored in round 16 (commit `e24a1e5`) but signoff_migration check 2026-05-04 confirms it never landed. mig_290/291/292 all completed with submission packages. Retry as mig_293b.

## §2 — Same scope as mig_293

See `cursor_prompts/CURSOR_PROMPT_MIG_293_VALIDATION_LOG_MD_MIRROR_20260504.md` §1-§2.

Build:
- `main.cowork_sf_validation_log_v1` table on MD (mirror of SF VALIDATION_RUN_LOG_v1)
- `snowflake_trial/scripts/35_pull_sf_validation_log.py` — script to refresh
- Add to refresh pipeline

## §3 — Updated baseline note

The SF SP was updated by Cowork to v2 baseline today. Now has 17 checks (was 10). New checks: M044_events_any_recurrence, M037_LN_pos_n, M025_malig_n, TIRADS_TR5_n, NLP_smoking_full_results, NLP_family_hx_full_results. Mirror schema unchanged.

## §4 — Registry signoff

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_293b', CURRENT_TIMESTAMP, 'cursor_composer_mig293b_retry_of_293',
 'mig_293b: Created main.cowork_sf_validation_log_v1 mirror + 35_pull_sf_validation_log.py (mig_293 retry). SF SP now baseline v2 (17 checks). Cross-platform audit trail enabled. Closes CF-mig293-VALIDATION-LOG-MIRROR.');
```

---

## §5 — Surgical git add

```
qc_framework_v1/migrations/293b_validation_log_md_mirror_20260504.sql
snowflake_trial/scripts/35_pull_sf_validation_log.py
scripts/output/mig_293b_apply_log.txt
cursor_prompts/CURSOR_PROMPT_MIG_293B_VALIDATION_LOG_MIRROR_RETRY_20260504.md
```

---

**End of mig_293b dispatch.**
