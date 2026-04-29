# Cursor Agent Task — `canonical_patient_master` FINALIZATION + LAKEHOUSE COVERAGE AUDIT

**Generated:** 2026-04-29 (Cowork session)
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** `git fetch && git pull`
**Estimated effort:** 2-3 hours
**Run order:** Lane 50 of next 4-prompt batch (mig_162) — RUN ONLY AFTER mig_152/154/156/157/159/160/161 ALL LAND

---

## 0. Cleanliness & safety preamble (MUST READ)

This lane:
1. Flips `canonical_patient_master.table_status` from `in_progress` to `verified` (final lane closure).
2. Generates the manuscript-pipeline lakehouse coverage report.
3. Documents the open carry-forward inventory for v1.0 publication.

**Pre-conditions** — DO NOT START until ALL of these are true:
- mig_154 (pathology invasion, 38 cols) applied
- mig_156 (framework, 71 cols) applied
- mig_157 (clinical residual, ~56 cols) applied
- mig_159 (final residual, 27 cols) applied
- mig_152 (NLP cluster, ~116 cols) applied (or all NLP cols reclassified to na if scope shrinks)
- mig_160 (date retype) applied OR explicitly deferred
- mig_161 (mig_155 reverification) applied OR explicitly clean

**Pre-flight gate:**
```sql
SELECT n_verified, n_na, n_not_started, n_failed, n_columns_total, table_status
FROM main.canonical_table_signoff_registry_v1
WHERE table_name='canonical_patient_master';
-- Expect: n_not_started = 0 AND n_failed = 0 AND table_status='in_progress' (will flip in this lane)
```

If `n_not_started > 0`, STOP — list the residual cols and open a CF; do not flip table_status. Logan reviews before proceeding.

---

## 1. Goal — three deliverables

### 1a. PM finalization (table_status flip)

If pre-flight gate passes:
```sql
UPDATE main.canonical_table_signoff_registry_v1
SET table_status = 'verified',
    signed_off_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    signoff_migration = 'qc_framework_v1/migrations/162_patient_master_finalization_and_lakehouse_audit_20260429.sql',
    notes = COALESCE(notes,'') ||
            ' | mig_162: canonical_patient_master FINALIZED — all 1,598 cols verified or na. ' ||
            'Lakehouse v1.0 manuscript-ready (per coverage report appendix).'
WHERE table_name='canonical_patient_master';
```

### 1b. 5-gate audit verification

Re-run the standing audit (see `qc_framework_v1/migrations/127_audit_refinement_na_filter_20260429.sql` or COWORK_HANDOFF_PROMPT §11). Expected post-mig_162 state: **gate1=89, gate2=0, gate3=0, gate4=0, gate5=0** (gate1 ticks +1 for PM; gate5 should be 0 if mig_160 ran).

If any gate > 0 (other than gate1), enumerate violators and STOP — Cowork investigates before signoff.

### 1c. Lakehouse coverage report

Append to migration header (or write to `qc_framework_v1/reports/v1_0_manuscript_readiness_report_20260429.md`):

**Section 1 — Tier-2 canonical inventory:**
```sql
SELECT table_name, table_status, n_columns_total, n_verified, n_na, n_not_started, n_failed
FROM main.canonical_table_signoff_registry_v1
WHERE table_name LIKE 'canonical_%'
ORDER BY table_status, table_name;
```

**Section 2 — Verification methodology distribution (lakehouse-wide):**
```sql
SELECT verification_method, COUNT(*) AS n_cols
FROM main.canonical_column_verification_registry_v1
WHERE verification_status='verified'
GROUP BY 1 ORDER BY 2 DESC LIMIT 30;
```

**Section 3 — Carry-forward inventory:**
```sql
SELECT regexp_extract(notes, 'CF-[A-Za-z0-9_-]+', 0) AS cf_tag, COUNT(*) AS n_cols
FROM main.canonical_column_verification_registry_v1
WHERE notes ILIKE '%CF-%'
GROUP BY 1 ORDER BY 2 DESC;
```

**Section 4 — Date-type compliance:**
- All clinical event dates DATE-typed (gate-5 closure verified)
- Allowlist provenance/audit timestamps documented

**Section 5 — Cohort parity:**
```sql
SELECT
  (SELECT COUNT(*) FROM main.canonical_patient_master) AS pm_rows,
  (SELECT COUNT(DISTINCT research_id) FROM main.canonical_patient_master) AS pm_distinct_rids;
-- Expect: 10,871 / 10,871
```

**Section 6 — Open CFs requiring future work** (manually curated list of CF tags from §3 that still represent open work, not just informational):
- CF-mig151-RADTX-DERIVATION-GAP (5,431-pt gap; deferred unless manuscript radtx scope expands)
- CF-mig150-PTH-MULTI-SOURCE-DERIVATION (notes-PTH source restoration pending)
- CF-mig150-TP-UPSTREAM-NOT-IN-MAIN (live LN canonical pending)
- CF-mig142-RAI-AVIDITY-PLACEHOLDER-ZERO (V2 RAI NLP backfill pending)
- ... others as they appear

---

## 2. SQL file structure

File: `qc_framework_v1/migrations/162_patient_master_finalization_and_lakehouse_audit_20260429.sql`

Sections:
- **Section A — Pre-flight gate check** (commented; agent runs before drafting; aborts if fails)
- **Section B — PM finalization UPDATE** (single statement)
- **Section C — Re-run 5-gate audit query** (commented; agent runs and pastes results into header)
- **Section D — Coverage report queries** (commented; agent runs and writes Markdown report file)
- **Section E — Final commit message preview**

Pre-snapshot:
```sql
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_table_signoff_registry_pre_mig162_pm_20260429 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig162_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_table_signoff_registry_v1
WHERE table_name='canonical_patient_master';
```

---

## 3. Coverage report — separate file

Path: `qc_framework_v1/reports/v1_0_manuscript_readiness_report_20260429.md`

Should include:
- Title + date
- Executive summary (3-5 lines): canonical count / verified col count / cohort size / open CFs
- §1 Tier-2 canonical inventory (table)
- §2 Methodology distribution (table)
- §3 Carry-forward inventory by CF tag (table)
- §4 Date-type compliance (paragraph + count)
- §5 Cohort parity (sentence)
- §6 Open CFs requiring future work (curated list with disposition)
- §7 Recommended next steps for manuscript pipeline (e.g., baseline cohort definition, primary outcome derivation, etc.)

---

## 4. Required CFs (lane-specific)

- `CF-mig162-PM-FINALIZATION` — informational on success
- `CF-mig162-GATE-5-RESIDUAL` — open if gate5 > 0 after mig_160 supposedly ran
- `CF-mig162-COHORT-PARITY-VIOLATION` — open if PM != 10,871

---

## 5. Apply + verify (Logan-only)

NO MD writes from agent. Logan applies. Cowork independently re-runs all coverage queries to confirm report accuracy.

---

## 6. Git workflow

```bash
git add qc_framework_v1/migrations/162_patient_master_finalization_and_lakehouse_audit_20260429.sql qc_framework_v1/reports/v1_0_manuscript_readiness_report_20260429.md
git -c user.name="Logan Glosser" -c user.email="logan.glosser@gmail.com" commit -m "qc: mig_162 PM finalization + lakehouse v1.0 manuscript-readiness coverage report"
git push origin main
```

---

## 7. Done definition

- [ ] All pre-conditions met (mig_152/154/156/157/159/160/161 landed)
- [ ] Pre-flight gate check shows n_not_started=0 AND n_failed=0
- [ ] PM table_status flipped to 'verified' with signoff_migration set
- [ ] 5-gate audit returns gate1=89 / gate2=0 / gate3=0 / gate4=0 / gate5=0
- [ ] Lakehouse coverage report written to `qc_framework_v1/reports/`
- [ ] Open CF inventory curated for manuscript-pipeline disposition
- [ ] Pre-snapshot in archive_pub_v1_0
- [ ] SQL file + report file committed + pushed; NO MD writes from agent
