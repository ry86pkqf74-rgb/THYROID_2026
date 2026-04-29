# CR vs CRR path_proven cross-SSOT reconcile — mig_153 close-out

**Date:** 2026-04-29  
**Lane:** 33c (Cowork / Cursor agent)  
**batch_id:** `mig_153_cr_vs_crr_path_proven_reconcile_20260429`  
**Repo migration:** `qc_framework_v1/migrations/153_cr_vs_crr_path_proven_reconcile_20260429.sql`

## Reproducer (unchanged)

```sql
WITH cr AS (
  SELECT CAST(research_id AS VARCHAR) AS rid, recurrence_confirmed
  FROM main.canonical_recurrence_v1
),
crr AS (
  SELECT CAST(research_id AS VARCHAR) AS rid, recurrence_status_final
  FROM main.canonical_recurrence_resolved_v1
)
SELECT cr.rid
FROM cr JOIN crr USING (rid)
WHERE COALESCE(cr.recurrence_confirmed,FALSE) = FALSE
  AND crr.recurrence_status_final = 'path_proven';
```

## Live MotherDuck result (pre-fix probe)

Connection: `scripts._md_connect.connect_locked()` → `thyroid_canonical_publication_v1_0`.

| Probe | Value |
|--------|--------|
| Rows matching reproducer | **0** (not 22) |
| `canonical_recurrence_resolved_v1` `path_proven` count | **145** |
| `canonical_recurrence_v1` `recurrence_confirmed` TRUE | **514** |
| Patients CR=TRUE but CRR ≠ `path_proven` | **369** (expected: CR encodes confirmed FNA + structural tiers; CRR `path_proven` is PME-/mig_62–filtered path-proven track only) |

## Per-rid disposition

**No rows required disposition:** the candidate set is empty on the publication database at probe time. The Cowork “22-patient drift” scenario does not reproduce against current `main.canonical_recurrence_v1` and `main.canonical_recurrence_resolved_v1`.

If a future rebuild reintroduces drift, re-run the reproducer; use `manuscript_workspace.cr_crr_reconcile_candidates_20260429` (repopulated by mig_153 Step 1) as the rid list for evidence pulls in §2b of the Lane 33c prompt.

## Actions taken

1. Added migration **153** — creates/replaces `manuscript_workspace.cr_crr_reconcile_candidates_20260429` with the join payload (empty). **No** `canonical_recurrence_v1` / `canonical_recurrence_resolved_v1` / PM UPDATE; **no** archive snapshot (no row-level mutation).

2. **verification_method:** `cross_ssot_reconcile_verified_zero_drift_live_20260429`

## Acceptance checklist

| Gate | Status |
|------|--------|
| Reproducer drift | **0** |
| Traceability table | Created (0 rows) |
| PM resync | N/A (CR unchanged) |
| Pre-snapshot archive | N/A (no UPDATE) |
| Per-rid table for Logan | N/A (0 candidates) |

## Note on SSOT semantics

`canonical_recurrence_v1` (203b / mig_123) and `canonical_recurrence_resolved_v1` (mig_62 / mig_125) use **different** path-proven definitions. Alignment is required only for the **explicit contradiction** in the Lane 33c predicate (CR not confirmed while CRR says path_proven). The broader pattern CR TRUE & CRR not path_proven is **not** a contradiction under current definitions.
