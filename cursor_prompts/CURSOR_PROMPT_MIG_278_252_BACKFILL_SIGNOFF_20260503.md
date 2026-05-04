# Cursor Composer Dispatch — mig_278: Backfill mig_252 signoff (no-op verified)

**Generated:** 2026-05-03 by Cowork at HEAD `2cfa535`.
**Lane:** mig_278 — Insert a `signoff_migration` row for mig_252 (`comp_*_confirmed rollup fix`). Cowork verified 2026-05-03 that the CPM rollup numbers already match the strict definition (`finding_status='present' AND evidence_strength IN ('definitive','probable')`) on `canonical_complications_events_v1`. The bug was fixed by a downstream rebuild (most likely mig_265 PMH definitive tier) but no signoff row was ever recorded. mig_278 closes the registry gap.
**Recommended agent:** **Cursor Composer** — mechanical 1-line INSERT.
**Estimated runtime:** 5 min.
**Closes:** CF-COMP-CONFIRMED-ROLLUP-BUG.

---

## §0 — First message to paste into Cursor Composer

> mig_278 dispatch. Read `cursor_prompts/CURSOR_PROMPT_MIG_278_252_BACKFILL_SIGNOFF_20260503.md`. This is a registry-only mig (no DML on canonicals). MotherDuck DB is `thyroid_canonical_publication_v1_0`.

---

## §1 — Pre-task verification (read-only)

Confirm the rollup is actually correct before signing off:

```sql
-- CPM rollup vs canonical_complications_events_v1 strict definition
WITH strict AS (
  SELECT LOWER(complication_type) AS comp,
         COUNT(DISTINCT research_id) AS strict_n
  FROM main.canonical_complications_events_v1
  WHERE finding_status='present' AND evidence_strength IN ('definitive','probable')
  GROUP BY 1
),
rollup AS (
  SELECT 'seroma' AS comp, SUM(CASE WHEN comp_seroma_confirmed THEN 1 ELSE 0 END) AS n FROM main.canonical_patient_master UNION ALL
  SELECT 'hematoma', SUM(CASE WHEN comp_hematoma_confirmed THEN 1 ELSE 0 END) FROM main.canonical_patient_master UNION ALL
  SELECT 'rln_injury', SUM(CASE WHEN comp_rln_injury_confirmed THEN 1 ELSE 0 END) FROM main.canonical_patient_master UNION ALL
  SELECT 'chyle_leak', SUM(CASE WHEN comp_chyle_leak_confirmed THEN 1 ELSE 0 END) FROM main.canonical_patient_master UNION ALL
  SELECT 'hypoparathyroidism', SUM(CASE WHEN comp_hypoparathyroidism_confirmed THEN 1 ELSE 0 END) FROM main.canonical_patient_master
)
SELECT r.comp, r.n AS rollup_n, s.strict_n, (r.n - s.strict_n) AS drift
FROM rollup r LEFT JOIN strict s USING (comp)
ORDER BY r.comp;
-- Expected (Cowork 2026-05-03 probe): drift=0 for all 5 named complications.
-- Cowork-confirmed: seroma 39=39, hematoma 68=68, rln 21=21, chyle 3=3, hypopara 296=296.
```

If drift != 0 anywhere, STOP and surface to Logan — do NOT sign off.

---

## §2 — Apply

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_252', CURRENT_TIMESTAMP, 'cursor_composer_mig278_backfill',
 'mig_252 (comp_*_confirmed rollup fix): NO-OP backfill signoff. CPM rollup numbers verified by Cowork 2026-05-03 to already match canonical_complications_events_v1 strict definition (finding_status=present AND evidence_strength IN (definitive,probable)). Drift=0 for seroma/hematoma/rln_injury/chyle_leak/hypoparathyroidism. Bug was fixed by downstream rebuild (likely mig_265 PMH definitive tier rebuild side-effect); SQL file qc_framework_v1/migrations/252_comp_confirmed_rollup_fix_20260501.sql was never applied but is no longer needed. Closes CF-COMP-CONFIRMED-ROLLUP-BUG.');
```

---

## §3 — Surgical git add

```
qc_framework_v1/migrations/278_mig_252_backfill_signoff_20260503.sql
scripts/output/mig_278_apply_log.txt
cursor_prompts/CURSOR_PROMPT_MIG_278_252_BACKFILL_SIGNOFF_20260503.md
```

Optional: rename `qc_framework_v1/migrations/252_comp_confirmed_rollup_fix_20260501.sql` to `252_DEPRECATED_no_op_see_mig_278.sql` for clarity.

Commit message:
```
chore(md): mig_278 backfill mig_252 signoff (no-op verified)

- CPM comp_*_confirmed rollup numbers already match strict definition on canonical_complications_events_v1
- Drift=0 for seroma/hematoma/rln_injury/chyle_leak/hypoparathyroidism
- Bug was fixed by downstream rebuild side-effect (likely mig_265)
- mig_252.sql in qc_framework_v1/migrations/ is dead code
- Closes CF-COMP-CONFIRMED-ROLLUP-BUG
```

---

**End of mig_278 dispatch.**
