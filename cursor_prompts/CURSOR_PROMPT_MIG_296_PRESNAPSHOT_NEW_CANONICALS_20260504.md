# Cursor Composer Dispatch — mig_296: Pre-snapshot new canonicals + cohort views (Protocol v2 hygiene)

**Generated:** 2026-05-04 by Cowork at HEAD `7279f23`.
**Lane:** mig_296 — Several recently-built objects (canonical_recurrence_events_v1, canonical_recurrence_patient_rollup_v1, NLP-augmented cohort views from mig_285/286, tirads_resolved CPM col from mig_288) have not been pre-snapshotted to `archive_pub_v1_0`. Per Protocol v2, every Tier-2+ canonical needs a snapshot for rollback. Add belt-and-suspenders snapshots before the next major round of cleanup migs.
**Recommended agent:** **Cursor Composer** — mechanical CTAS sequence.
**Estimated runtime:** 20 min.
**Severity:** LOW (defensive hygiene; doesn't block anything).
**Triggered by:** Cowork audit of archive coverage.

---

## §0 — First message to paste into Cursor Composer

> mig_296 dispatch. Pre-snapshot recently-built canonicals + augmented views to `archive_pub_v1_0` for Protocol v2 rollback compliance. MotherDuck DB is `thyroid_canonical_publication_v1_0`.

---

## §1 — Apply

```sql
-- Recurrence canonicals (built in mig_269)
CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_recurrence_events_v1_baseline_20260504 AS
SELECT * FROM main.canonical_recurrence_events_v1;

CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_recurrence_patient_rollup_v1_baseline_20260504 AS
SELECT * FROM main.canonical_recurrence_patient_rollup_v1;

-- mig_285/286 augmented cohort views — snapshot the row-level data
CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.cohort_m032_descriptive_25yr_v1_baseline_20260504 AS
SELECT * FROM manuscript_workspace.cohort_m032_descriptive_25yr_v1;

CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.cohort_m037_ln_metastasis_v1_baseline_20260504 AS
SELECT * FROM manuscript_workspace.cohort_m037_ln_metastasis_v1;

-- mig_288 tirads_resolved CPM col snapshot
CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.cpm_tirads_resolved_baseline_20260504 AS
SELECT research_id, tirads_resolved, nlp_tirads_max_category
FROM main.canonical_patient_master;

-- mig_287 smoking enum snapshot
CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.cpm_smoking_clean_baseline_20260504 AS
SELECT research_id, pmhx_nlp_smoking_status, nsqip_smoker
FROM main.canonical_patient_master;
```

---

## §2 — Verify snapshots

```sql
SELECT table_name,
  (SELECT COUNT(*) FROM "Thyroid 2026 UPdated".archive_pub_v1_0.<table>) AS n_rows
FROM information_schema.tables
WHERE table_catalog='Thyroid 2026 UPdated' AND table_schema='archive_pub_v1_0'
  AND table_name LIKE '%baseline_20260504'
ORDER BY table_name;
-- Expected: 6 baseline tables, row counts match live source
```

---

## §3 — Registry signoff

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_296', CURRENT_TIMESTAMP, 'cursor_composer_mig296',
 'mig_296: Pre-snapshot baseline of 6 recently-built objects to archive_pub_v1_0 (canonical_recurrence_events_v1, canonical_recurrence_patient_rollup_v1, cohort_m032/m037 v1 augmented views, CPM tirads_resolved+nlp_tirads_max_category, CPM smoking enum). Defensive Protocol v2 hygiene before next major round.');
```

---

## §4 — Surgical git add

```
qc_framework_v1/migrations/296_presnapshot_new_canonicals_20260504.sql
scripts/output/mig_296_apply_log.txt
cursor_prompts/CURSOR_PROMPT_MIG_296_PRESNAPSHOT_NEW_CANONICALS_20260504.md
```

---

**End of mig_296 dispatch.**
