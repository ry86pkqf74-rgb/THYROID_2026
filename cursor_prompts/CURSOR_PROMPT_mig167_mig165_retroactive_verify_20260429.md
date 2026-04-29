# Cursor Prompt — mig_167 mig_165 Retroactive Path-C Verification

**Lane:** 55 / mig_167
**Batch_id:** `mig_167_mig165_retroactive_verification_20260429`
**Generated:** 2026-04-29 (late evening)
**Type:** Read-only audit + registry-notes-only writes. Path C apply via Cowork.

---

## §0 Why this lane exists — AGENTS governance violation closure

mig_165 (commit `cc0a07c`, Lane 53) **was applied directly to MotherDuck by the agent without Cowork running Path C verification**. This is the second AGENTS governance violation in this round (mig_155 was the first; mig_161 covered it retroactively).

mig_165 mass-flipped:
- **76 tables** column rows `not_started → na` (split: 30 main schema + 56 manuscript_workspace schema; bucketed into `auto_tier1_raw_mirror_skip` / `auto_governance_audit_table_skip` / `auto_registry_governance_skip`)
- **10 tables** got CF-only notes (columns stayed `not_started`; including `recurrence_event_clean_v1`)
- 1 new table registered: `note_entities_llm_presenting_symptoms` (23 cols na)

Post-apply state (Cowork live probe 2026-04-29 late evening):
- gate1 88 → **165**
- gate2/3/4 = 0
- gate5 = 21 (unchanged; mig_160 retype not yet applied)
- Status hist: 165 verified / 1 in_progress (PM) / 10 not_started / 176 total

This lane runs **independent verification** of the apply: did the agent classify each of the 76+10 tables correctly? Are any tables that should have been auto-na'd left as not_started, or vice versa? Are any of the 10 CF-only tables actually classifiable as tier-1 raw mirrors and should have been na'd? Does the schema_name split make sense (main vs manuscript_workspace)?

## §1 Governance posture

- **Read + author SQL only.** No `query_rw` from agent.
- Output: a single SQL file at `qc_framework_v1/migrations/167_mig165_retroactive_verification_20260429.sql` (registry-notes-only appendices and any retroactive na/verify reclassifications) PLUS a Markdown report at `qc_framework_v1/reports/mig_167_mig165_retroactive_verification_20260429.md`.
- Pre-snapshot the registry slices for any cell you propose to update.

## §2 Required pre-flight probes (paste into report)

```sql
-- §2a Confirm mig_165 batch_id cardinality — should match the 76+10+1 = 87 tables claimed
SELECT batch_id, verification_status, COUNT(DISTINCT table_name) AS n_tables, COUNT(*) AS n_cols
FROM main.canonical_column_verification_registry_v1
WHERE batch_id LIKE 'mig_165%'
GROUP BY 1, 2 ORDER BY 1, 2;

-- §2b Per-bucket disposition: which methodology strings did the agent use?
SELECT verification_method, COUNT(DISTINCT table_name) AS n_tables, COUNT(*) AS n_cols
FROM main.canonical_column_verification_registry_v1
WHERE batch_id LIKE 'mig_165%'
GROUP BY 1 ORDER BY 2 DESC;

-- §2c The 10 still-not_started tables — list them + verify why they were left
SELECT t.table_name, t.schema_name, t.n_columns_total, t.n_not_started, t.notes
FROM main.canonical_table_signoff_registry_v1 t
WHERE t.table_status='not_started'
ORDER BY t.schema_name, t.table_name;

-- §2d Schema_name distribution post-mig_165
SELECT schema_name, table_status, COUNT(*) AS n
FROM main.canonical_table_signoff_registry_v1
GROUP BY 1, 2 ORDER BY 1, 2;

-- §2e Probe each of the 10 CF-only tables: do any actually look like raw mirrors / governance / queue tables that should have been auto-na'd?
-- For each table_name in §2c, look at COUNT(*) and a sample of column names + dtypes.
-- Report whether each is genuinely "needs real verification" (analytic Tier-2 builder) or
-- whether the agent should have auto-na'd it.
```

## §3 Required findings to surface

For each of the 10 still-not_started tables, the report must classify:
- **VALID-DEFERRED** — really needs real verification (e.g., `recurrence_event_clean_v1`, mig_163 family). Open `CF-mig167-VALID-DEFER-<table>`.
- **MISCLASSIFIED-SHOULD-BE-NA** — actually a tier-1 / governance / queue table; the agent should have auto-na'd. Open `CF-mig167-MIG165-MISSED-AUTO-NA-<table>` and propose the correction in the SQL (Section B).

For each of the 76 auto-na'd tables, sample 5 randomly and confirm:
- The `verification_method` makes sense
- No analytic columns were silently na'd (e.g., a column with derivation lineage tagged `auto_governance_audit_table_skip`)
- Sample column names match the bucket (e.g., a `clinical_notes_long` table being auto_tier1_raw_mirror_skip is plausible; an `imaging_exam_master_v1` with derived analytic cols being skipped would be wrong)

For the new `note_entities_llm_presenting_symptoms` registration:
- Confirm 23 column rows were inserted
- Confirm methodology = `auto_tier1_raw_mirror_skip` per Tier-1 raw mirror precedent
- Spot-check the column list against `information_schema.columns` for the same table

## §4 SQL structure expected in `167_mig165_retroactive_verification_20260429.sql`

### Section A — Pre-snapshots
Snapshot any cell to be modified.

### Section B — Retroactive corrections (only if §3 finds misclassifications)
Per-table UPDATE blocks fixing the classification + appending `mig_167` notes. If no corrections needed, this section is empty (just a comment header).

### Section C — CF appendices
- `CF-mig167-MIG165-PATH-C-VERIFIED` global stamp on every mig_165 batch_id row (informational; "audit ran clean")
- Per-finding CFs from §3

### Section D — Methodology vocabulary additions (if any new methods emerge)
e.g., if the audit decides one of the 10 CF-only tables actually warrants a new methodology string like `auto_external_registry_passthrough_skip`.

## §5 Markdown report content

A 1-page summary with:
- mig_165 cardinality reconcile (claimed vs observed)
- The 10 not_started classification table
- Sample-of-5 sanity check from each bucket
- Net assessment: did the apply ship clean, or are there corrections to propose?
- Open CFs

## §6 Git workflow

- Files: 167 SQL + 167 report
- Commit: `qc: mig_167 retroactive Path-C verification of mig_165 (governance violation closure)`
- Push.

## §7 Out of scope

- Do NOT modify any base table or canonical data — registry-notes-only.
- Do NOT propose dropping any registry rows.
- Do NOT touch mig_166 (canonical_cleanup_audit_v1) or any other lane's batch_id.
