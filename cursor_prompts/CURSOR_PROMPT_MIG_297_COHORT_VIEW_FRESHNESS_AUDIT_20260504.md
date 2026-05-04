# Cursor Composer Dispatch — mig_297: Cohort view freshness audit + repoint

**Generated:** 2026-05-04 by Cowork at HEAD `e590e40`.
**Lane:** mig_297 — Multiple cohort views in `manuscript_workspace` have been touched across mig_280 (BinderException fix), mig_283 (12 views fixed), mig_285 (M032 NLP augment), mig_286 (M037 NLP augment). Audit each cohort view's current row count + col count vs the most recent rebuild date. Identify any views with stale references to deprecated objects (e.g., `canonical_recurrence_v1` post-mig_284 deprecation, `nlp_tirads_max_category` if mig_294b still pending).
**Recommended agent:** **Cursor Chat (Sonnet 4 / GPT-5) → Composer** — needs scoped audit + repoint logic.
**Estimated runtime:** 45 min.
**Severity:** MED. Stale view definitions can silently produce wrong manuscript numbers.
**Closes:** CF-mig283-COHORT-FRESHNESS (newly opened).

---

## §0 — First message

> mig_297 dispatch. Audit all `manuscript_workspace.cohort_*` views for stale references to deprecated objects. MotherDuck DB is `thyroid_canonical_publication_v1_0`.

## §1 — Audit

```sql
-- 1.1 Inventory all cohort_* views with definition + row count
WITH views AS (
  SELECT table_name, view_definition
  FROM information_schema.views
  WHERE table_schema='manuscript_workspace' AND table_name LIKE 'cohort_%'
)
SELECT table_name,
       view_definition ILIKE '%canonical_recurrence_v1%' AS uses_legacy_recur_v1,
       view_definition ILIKE '%canonical_recurrence_resolved_v1%' AS uses_legacy_recur_resolved,
       view_definition ILIKE '%recurrence_event_clean_v1%' AS uses_legacy_recur_clean,
       view_definition ILIKE '%nlp_tirads_max_category%' AS uses_dirty_tirads,
       LENGTH(view_definition) AS def_len
FROM views ORDER BY table_name;
```

## §2 — Per-view disposition

For each view with a stale reference:
- **uses_legacy_recur_*** → REPOINT to `canonical_recurrence_patient_rollup_v1` (mig_269 SSOT) or `vw_recurrence_safe_VIEW_v1` (mig_284 stable wrapper)
- **uses_dirty_tirads** → REPOINT to `tirads_resolved` (mig_288 enum)

For each view, capture:
- Pre-state row count
- Post-state row count after CREATE OR REPLACE
- Drift assessment

## §3 — Apply

Standard pre-snapshot view-def + CREATE OR REPLACE pattern. Use mig_280/283 templates.

## §4 — Registry signoff

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_297', CURRENT_TIMESTAMP, 'cursor_composer_mig297',
 'mig_297: Cohort view freshness audit. Probed N views; M with stale refs to deprecated objects (canonical_recurrence_v1/_resolved_v1/recurrence_event_clean_v1, nlp_tirads_max_category); applied CREATE OR REPLACE to repoint each. Disposition table in scripts/output/mig_297_disposition.md. Closes CF-mig283-COHORT-FRESHNESS.');
```

## §5 — Surgical git add

```
qc_framework_v1/migrations/297_cohort_view_freshness_audit_20260504.sql
scripts/output/mig_297_disposition.md
scripts/output/mig_297_apply_log.txt
cursor_prompts/CURSOR_PROMPT_MIG_297_COHORT_VIEW_FRESHNESS_AUDIT_20260504.md
```

---

**End of mig_297 dispatch.**
