# Cursor Composer Dispatch — mig_305: Fix SF VALIDATE_ALL_COHORTS() v3 baseline SP

**Generated:** 2026-05-04 by Cowork at HEAD `d4bdebd`.
**Lane:** mig_305 — Cowork attempted to update `THYROID_VALIDATION.PUBLIC.VALIDATE_ALL_COHORTS()` to baseline v3 (20 checks; adds M004 cohort + manuscript-cell checks + sentinel for legacy_tirads_dropped). Updates landed but the SP execution hung when calling. Currently SP is back to baseline v1 (10 checks). Debug + redeploy v3.
**Recommended agent:** **Cursor Composer** — SQL SP debugging.
**Estimated runtime:** 30 min.
**Severity:** LOW (validation infrastructure; doesn't block manuscripts).
**Closes:** CF-VALIDATE-ALL-COHORTS-V3-HANG (newly opened).

---

## §0 — First message

> mig_305 dispatch. Read `snowflake_trial/scripts/sf_baseline_v3_post_mig_300.py` §4. SP definition is correct but execution hangs. Likely cause: the sentinel check `(SELECT IFF(COUNT(*) = 0, 'YES', 'NO') FROM information_schema.columns WHERE ...)` may not be allowed inside a SP context. Test by removing the information_schema check and rebuilding. SF account `qcc02515.us-east-1` / DB `THYROID_VALIDATION`.

## §1 — Test hypothesis

```sql
-- Verify the offending check works standalone
SELECT IFF(COUNT(*) = 0, 'YES', 'NO') AS legacy_dropped
FROM information_schema.columns
WHERE table_schema='PUBLIC' AND column_name='NLP_TIRADS_MAX_CATEGORY' AND table_name='CANONICAL_PATIENT_MASTER';
-- Expected: YES (post-mig_294b drop)
```

If the standalone query works, the issue is SP scope on information_schema. Either:
- (A) Move the check to a wrapper procedure that calls VALIDATE_ALL_COHORTS() then runs the sentinel separately
- (B) Cache the legacy-dropped status as a row in COWORK_PIPELINE_REGISTRY_V1 and read from there

## §2 — Apply

Modify the SP to remove the problematic sentinel + replace with a static row reference. Pattern:

```sql
-- ... existing checks ...
UNION ALL SELECT 'CPM_legacy_tirads_dropped', 'YES',
  (SELECT IFF(STATUS LIKE '%dropped%', 'YES', 'NO')
   FROM COWORK_PIPELINE_REGISTRY_V1
   WHERE COMPONENT = 'NLP_TIRADS_MAX_CATEGORY_DROPPED' LIMIT 1)
```

Or just delete that check and add it to a separate `VALIDATE_LEGACY_DROPS()` SP.

## §3 — Verify

```sql
CALL VALIDATE_ALL_COHORTS();
-- Expected: 19/19 or 20/20 PASS within 30 seconds (no hang)
```

## §4 — Registry signoff

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_305', CURRENT_TIMESTAMP, 'cursor_composer_mig305',
 'mig_305: Fixed SF VALIDATE_ALL_COHORTS() baseline v3 hang. Removed information_schema sentinel; substituted COWORK_PIPELINE_REGISTRY_V1 reference. SP now executes 30s with N/N PASS. Closes CF-VALIDATE-ALL-COHORTS-V3-HANG.');
```

## §5 — Surgical git add

```
snowflake_trial/scripts/sf_baseline_v3_post_mig_300.py  (corrected SP body)
qc_framework_v1/migrations/305_sf_sp_v3_fix_20260504.sql
scripts/output/mig_305_apply_log.txt
cursor_prompts/CURSOR_PROMPT_MIG_305_SF_BASELINE_V3_SP_FIX_20260504.md
```

---

**End of mig_305 dispatch.**
