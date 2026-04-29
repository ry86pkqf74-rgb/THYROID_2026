# Apply Queue Plan — Cowork Path-C Execution (2026-04-29)

Standing context: all of the migrations below have agent-authored SQL committed to git. None have been applied to MotherDuck yet (except mig_165, which violated AGENTS governance and is being retro-verified by mig_167). Cowork applies via `query_rw` after Logan's go-signal, with explicit pre-snapshots per step.

---

## §1 Pre-flight invariants (must hold before any apply step)

- gate2 = 0, gate3 = 0, gate4 = 0
- PM cohort parity: 10,871 rows / 10,871 distinct research_id
- Live commit on origin/main matches local: `2395059` or later
- No unstaged changes touching `qc_framework_v1/migrations/*.sql` files in the queue

Re-run before each apply step — they're cheap.

---

## §2 Step 1 — Apply mig_161 (mig_155 retro-verify, registry notes only)

Risk: **lowest**. Notes-only updates to existing registry rows; no data writes.

### §2.1 Pre-snapshot (1 query_rw)

```sql
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_pre_mig161_20260429 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig161_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_column_verification_registry_v1
WHERE schema_name='main' AND table_name='canonical_patient_master'
  AND batch_id='mig_155_patient_master_risk_scoring_survival_genetics_cluster_20260429';
```

### §2.2 Apply Sections B0–B5 from `qc_framework_v1/migrations/161_mig155_independent_reverification_20260429.sql`

Each B-block is a separate `UPDATE` statement appending notes. Six total query_rw calls:

- B0 — global Path-C stamp on all 31 mig_155 cols
- B1 — Type-A near-uniform-TRUE CF on 3 cols (ata_response_calculable_flag, ata_response_is_provisional, ames_calculable_flag)
- B2 — degenerate single-value VARCHAR CF on resolved_layer_version
- B3 — scoring AJCC8 vs MACIS off-by-one CF on 2 cols
- B4 — recurrence proxy drift CFs on 4 cols
- B5 — surv_* vs LKA cross CF on 5 cols

### §2.3 Post-state verify (read-only)

```sql
SELECT COUNT(*) AS n_with_mig161_note
FROM main.canonical_column_verification_registry_v1
WHERE batch_id='mig_155_patient_master_risk_scoring_survival_genetics_cluster_20260429'
  AND notes ILIKE '%mig_161%';
-- Expect: 31 (every mig_155 row gets the B0 stamp)
```

Then re-run 5-gate audit; expect unchanged at 165 / 0 / 0 / 0 / 21.

---

## §3 Step 2 — Apply mig_161b (Cowork addendum: ATA-DUP CF gap closure)

Cowork's mig_161 review found Section B does not include a B-block for the `ata_initial_risk IS NOT DISTINCT FROM ata_risk_category` 100% duplication finding (§2h documents it but no per-col CF appendix). Adding a small mig_161b to close.

### §3.1 Author + commit + push first

Create `qc_framework_v1/migrations/161b_mig155_ata_initial_dup_cf_20260429.sql`:

```sql
-- mig_161b — ATA initial vs category 100% dup CF (gap from mig_161 §B)
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | mig_161b: CF-mig161-MIG155-ATA-INITIAL-RISK-DUP — ata_initial_risk '
            || 'IS NOT DISTINCT FROM ata_risk_category on 10,871/10,871 rows; '
            || 'ata_initial_risk is a redundant alias of ata_risk_category in mig_155 build. '
            || 'Manuscript pipeline should pick one canonical name (recommend ata_risk_category); '
            || 'consider deprecating ata_initial_risk in a future build, but not blocking.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND batch_id='mig_155_patient_master_risk_scoring_survival_genetics_cluster_20260429'
  AND column_name IN ('ata_initial_risk','ata_risk_category');
```

### §3.2 Apply (1 query_rw)

Just the UPDATE above. 2 rows touched.

---

## §4 Step 3 — Apply mig_159 (PM final residual, 27 cols)

Risk: **low**. Registry status flips only; no data writes.

### §4.1 Pre-snapshot

mig_159's Section A already creates `canonical_column_verification_registry_pre_mig159_20260429`. One query_rw.

### §4.2 Apply Sections 159a–h

Each section is a separate UPDATE. Eight query_rw calls:

- 159a — 10 molecular single-gene cols (BOOLEAN) → verified
- 159b — 2 completion thyroidectomy cols → verified
- 159c — 2 bilateral flag cols → verified
- 159d — 7 stim-Tg / Tg-span cols → verified
- 159e — 2 laryngoscopy timing cols → verified
- 159f — 4 misc cols (laterality, r_class_true, total_ln_positive_v10, date_traceability_status) → verified
- 159g — resync canonical_table_signoff_registry_v1 row for canonical_patient_master
- 159h — CF appendices for LN-3-source-drift + R-class divergence

### §4.3 Post-state verify

```sql
SELECT n_verified, n_na, n_not_started, n_failed, n_columns_total, table_status
FROM main.canonical_table_signoff_registry_v1 WHERE table_name='canonical_patient_master';
-- Expect: 1468 verified / 13 na / 117 not_started / 0 failed / 1598 total / in_progress
-- (1441 + 27 = 1468; 144 - 27 = 117)
```

5-gate audit unchanged (165 / 0 / 0 / 0 / 21).

---

## §5 Step 4 — Apply mig_160 (global clinical-date retype, 21 cols × 5 tables)

Risk: **medium-high**. Structural `ALTER COLUMN ... SET DATA TYPE` on 5 base tables; closes ~190 col-impact CFs and gate-5 21→0 in one shot.

### §5.1 Pre-snapshots (5 query_rw calls — one per base table)

mig_160 §A creates all 5 archive snapshots:
- `canonical_ete_event_resolved_v1_pre_mig160_20260429`
- `canonical_frozen_section_patient_rollup_v1_pre_mig160_20260429`
- `canonical_molecular_genetics_v2_pre_mig160_20260429`
- `canonical_path_malignant_patient_rollup_v1_pre_mig160_20260429`
- `canonical_recurrence_v1_pre_mig160_20260429`

### §5.2 Apply Sections C+D inside one transaction (or as DuckDB allows per-statement)

Per the SQL: `BEGIN TRANSACTION;` then 21 ALTER COLUMN statements + 21 UPDATE notes statements + `COMMIT;`. The MCP `query_rw` wrapper is one-statement-per-call, so this needs ~43 calls. Cowork will execute each sequentially; if any errors, rollback to the snapshot.

Order of ALTERs (matters because dependent views could choke if base type changes mid-flight):
1. `canonical_ete_event_resolved_v1.last_known_alive_date` (TIMESTAMP→DATE)
2. 14 frozen rollup cols (VARCHAR→DATE)
3. `canonical_molecular_genetics_v2.resolved_test_date` (VARCHAR→DATE)
4. `canonical_molecular_genetics_v2.test_date_native` (TIMESTAMP→DATE)
5. `canonical_path_malignant_patient_rollup_v1.earliest_malignant_path_date` (TIMESTAMP→DATE)
6. `canonical_path_malignant_patient_rollup_v1.latest_malignant_path_date` (TIMESTAMP→DATE)
7. `canonical_recurrence_v1.first_surgery_date` (TIMESTAMP→DATE)
8. `canonical_recurrence_v1.recurrence_date` (TIMESTAMP→DATE)

Then 21 registry-note UPDATEs (one per altered col).

### §5.3 Dependent-view recompile fallback

The 2 known dependent views (`molecular_fusions_unnested_VIEW_v2`, `molecular_variants_unnested_VIEW_v2`) project `test_date_native` through. DuckDB lazy view resolution should pick up the new DATE type transparently. If `BinderException` on next SELECT after apply, recompile via:

```sql
CREATE OR REPLACE VIEW main.molecular_fusions_unnested_VIEW_v2 AS <body from duckdb_views>;
CREATE OR REPLACE VIEW main.molecular_variants_unnested_VIEW_v2 AS <body from duckdb_views>;
```

(Body fetched fresh from `duckdb_views()` pre-apply so any changes are reflected.)

### §5.4 Post-state verify

```sql
-- Should now be 0
SELECT COUNT(*) FROM (
  WITH verified_tables AS (...) ...  -- mig_160 §E aggregate gate-5 query
);
-- Expect: 0

-- Spot-check the 5 retyped tables
SELECT table_name, column_name, data_type FROM information_schema.columns
WHERE table_name IN ('canonical_ete_event_resolved_v1','canonical_frozen_section_patient_rollup_v1',
                     'canonical_molecular_genetics_v2','canonical_path_malignant_patient_rollup_v1',
                     'canonical_recurrence_v1')
  AND column_name IN ('last_known_alive_date','frozen_1_date','frozen_section_first_date',
                      'resolved_test_date','test_date_native','earliest_malignant_path_date',
                      'latest_malignant_path_date','first_surgery_date','recurrence_date')
ORDER BY 1, 2;
-- All should now show data_type = 'DATE'
```

Re-run 5-gate audit; expect 165 / 0 / 0 / 0 / **0**.

---

## §6 Step 5 — Apply mig_166 (canonical_cleanup_audit_v1 column ledger refinement)

Risk: **low**. Registry-only updates; pre-snapshot snapshot already in mig_166 SQL.

### §6.1 Apply sequence

Per mig_166 SQL:
- §A pre-snapshot (1 query_rw)
- §C elevate 15 cols na→verified (1 query_rw with IN list)
- §D reaffirm 3 cols as na with new methodology + notes (1 query_rw with CASE)
- §E resync signoff registry (1 query_rw)

### §6.2 Post-state verify

```sql
SELECT n_verified, n_na, n_not_started, signoff_migration
FROM main.canonical_table_signoff_registry_v1 WHERE table_name='canonical_cleanup_audit_v1';
-- Expect: 15 / 3 / 0 / qc_framework_v1/migrations/166_canonical_cleanup_audit_v1_signoff_20260429.sql
```

5-gate audit: gate1 stays 165 (table was already verified), other gates unchanged.

---

## §7 Step 6 — Apply mig_164 (VIEW layer signoff, 4 views)

Risk: **medium**. Inserts new registry rows for 2 orphan VIEWs + flips signoff for all 4.

### §7.1 Pre-snapshot

mig_164 §A: snapshot signoff + column registries for the 4 view names.

### §7.2 Apply §B insert + §C UPDATE flip + §D resync

Approximately 8–12 query_rw calls. Read mig_164 SQL end-to-end first.

### §7.3 Post-state verify

```sql
SELECT table_name, table_status, n_verified, n_na, n_not_started
FROM main.canonical_table_signoff_registry_v1
WHERE table_name LIKE 'canonical_%_VIEW_v%'
ORDER BY table_name;
-- Expect: 4 rows, all table_status=verified
```

5-gate audit: gate1 165 → **169** (or whatever subset of the 4 flips successfully).

---

## §8 Step 7 — Resume after mig_152 NLP lands

mig_152 NLP (Cursor lane in flight) covers ~116 PM cols. Once it lands and Cowork verifies it via Path C:
- PM not_started: 117 (post-mig_159) → ~1 (post-mig_152, if covers all NLP cols)
- If PM not_started reaches 0, mig_162 PM finalization can apply
- Cohort sweep on mig_152 BOOLEAN cols (cluster-batch likely covers presence flags)

---

## §9 Step 8 — Resume after mig_167–170 Cursor reports land

- **mig_167** retroactive verify of mig_165: registry-notes-only; apply per Path C
- **mig_168** controlled-vocabulary audit: read-only; surface findings only
- **mig_169** dtype/units audit: read-only; surface findings
- **mig_170** cross-canonical dtype drift audit: read-only; surface findings

For each, Cowork verifies SQL on disk + applies via `query_rw` if registry-only (mig_167) or just consumes the report (168/169/170).

---

## §10 Step 9 — mig_163b HYBRID apply (after Cursor authors SQL)

mig_163b is ratified HYBRID. Once Cursor ships the SQL:

### §10.1 Pre-snapshot

mig_163b §A: snapshot PM `any_recurrence_flag` slice + registry row.

### §10.2 Apply §B (single transaction)

The HYBRID UPDATE + registry note appendix. Two query_rw calls (UPDATE PM + UPDATE registry).

### §10.3 Post-state verify

```sql
-- Should equal HYBRID UNION cardinality (514 today)
SELECT SUM(CASE WHEN any_recurrence_flag THEN 1 ELSE 0 END) FROM main.canonical_patient_master;
-- Expect: 514

-- Cross-check: 0 mismatch vs HYBRID set
WITH hybrid AS (
  SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid FROM main.canonical_recurrence_v1 WHERE recurrence_confirmed=TRUE
  UNION
  SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid FROM main.canonical_recurrence_resolved_v1 WHERE recurrence_status_final='path_proven'
)
SELECT
  SUM(CASE WHEN any_recurrence_flag AND NOT EXISTS (SELECT 1 FROM hybrid h WHERE h.rid = CAST(pm.research_id AS VARCHAR)) THEN 1 ELSE 0 END) AS pm_t_h_f,
  SUM(CASE WHEN NOT any_recurrence_flag AND EXISTS (SELECT 1 FROM hybrid h WHERE h.rid = CAST(pm.research_id AS VARCHAR)) THEN 1 ELSE 0 END) AS pm_f_h_t
FROM main.canonical_patient_master pm;
-- Expect: 0 / 0
```

---

## §11 Total query_rw plan (count + risk)

| Step | Migration | query_rw calls | Risk |
|---|---|---|---|
| 1 | mig_161 | 7 (1 snapshot + 6 update blocks) | lowest |
| 2 | mig_161b | 1 | lowest |
| 3 | mig_159 | 9 (1 snapshot + 8 sections) | low |
| 4 | mig_160 | ~48 (5 snapshots + 21 ALTERs + 21 notes + 1 commit) | medium-high |
| 5 | mig_166 | 4 | low |
| 6 | mig_164 | ~10 | medium |
| 7 | mig_163b | 3 (snapshot, UPDATE pm, UPDATE registry) | low (data write) |
| **Total** | | **~82** | mixed |

---

## §12 Logan's go-signal options

- **All-in**: Approve the full queue (Steps 1–6, plus Steps 7+ later as Cursor lanes land). Cowork executes sequentially with per-step verification.
- **Stepwise**: Approve one step at a time. Cowork pauses after each post-state verify and waits for next go.
- **Skip-gate-5**: Apply 1, 2, 3, 5, 6 first (registry-only); defer mig_160 (the structural one) until you can review the dependent-view recompile risk separately.
- **Apply-with-watch**: Approve all but ask Cowork to ping you between Step 4 (mig_160) and Step 5 for an interim 5-gate verify before continuing.
