# Cursor Agent Task — `canonical_pmh_patient_rollup_v1` REBUILD + Sign-off (Protocol v2)

**Generated:** 2026-04-29 (Cowork session)
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** at handoff time — `git fetch && git pull` before starting
**Estimated effort:** 60-90 minutes (rebuild + verify + 79-col signoff)
**Run order:** Lane 7 (run last — biggest rollup, builds on patterns established by lanes 5+6)

---

## 1. Goal

Close the **PMH family** by REBUILDING then signing off `canonical_pmh_patient_rollup_v1`.

**Why rebuild (not just verify):** PMH events were verified by you in mig_107 (commit 2703153), but more importantly:
- Rollup `build_ts = 2026-04-22` (8 days stale)
- PMH events grew by **+252 rows** since rollup build:
  - +246 from mig_98 sub-mig closeouts (mig_98b/c/d/e/f) on 2026-04-28
  - +6 from mig_103 medications-classifier PMH-attribution on 2026-04-28
- Stale rollup ≠ current events — rebuild is required before signoff (parallel to mig_101 path_gland_rollup pattern)

---

## 2. Methodology — REBUILD then derivation re-derivation

Pattern: **mig_101 path_gland_rollup** — `qc_framework_v1/migrations/101_path_gland_patient_rollup_signoff.sql`. Adapt for PMH (more cols).

### 2a. Pre-rebuild snapshot
```sql
CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_pmh_patient_rollup_v1_pre_<mig-N>_20260429 AS
SELECT * FROM main.canonical_pmh_patient_rollup_v1;
```

### 2b. REBUILD via CREATE OR REPLACE TABLE
Find the build SQL in `scripts/365_psh_pmh_meds_consolidation.py` — the PMH rollup CTE. Copy verbatim into the migration with these adjustments:
- `build_script` → 'mig_<N>_via_365_pmh_rebuild'
- `build_ts` → `CAST(CURRENT_TIMESTAMP AS TIMESTAMP)` (memory: `reference_duckdb_timestamp_tz.md`)
- Source: `main.canonical_pmh_events_v1` (mig_107 verified — uses 12,696 rows now incl all 252 synthetic)

### 2c. Pre/post diff (sanity)
After rebuild, compare to pre-snapshot per-col:
```sql
WITH pre AS (SELECT * FROM "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_pmh_patient_rollup_v1_pre_<mig-N>_20260429),
     post AS (SELECT * FROM main.canonical_pmh_patient_rollup_v1)
SELECT SUM(CASE WHEN p.<col> IS DISTINCT FROM r.<col> THEN 1 ELSE 0 END) AS d_<col>, ...
FROM pre p JOIN post r USING (research_id);
```
Expected drift: small (~252 rows worth of new BOOL_OR=TRUE flips per affected phenotype). Anything else is a red flag.

### 2d. Derivation re-derivation (post-rebuild)
Verify rebuilt rollup matches fresh per-pt aggregation from current events. Should be 100% match (rollup was just rebuilt from events).

### 2e. Schema (79 cols total = 77 derivable + 2 na)
- 8 metadata: anchor_source, n_findings_any, n_findings_present, n_findings_definitive, n_findings_probable_or_better, first/last_finding_date, n_distinct_findings_norm
- 22 phenotypes × 3 tiers = 66 cols (diabetes, hypertension, cad, ckd, copd, depression, afib, asthma, gerd, obesity, osteoporosis, hyperthyroidism, hypothyroidism, autoimmune_thyroid_hx, breast_cancer, lung_cancer, radiation_exposure, prior_cancer_hx, coagulopathy, family_hx_cancer, family_hx_thyroid, men_syndrome)
- 3 plain BOOLs: pmh_smoking_status_{current, former, never}
- 2 na: research_id, build_ts

---

## 3. Acceptance gates

- Pre-rebuild snapshot in archive_pub_v1_0
- Rebuild row count = 10,871 (full cohort, CHANGE J)
- 77 not_started cols flipped via `verification_method='derivation_re_derivation_post_events_repair'` (analog of mig_101)
- 2 na cols carry over
- table_status='verified'
- Cross-validation: post-rebuild aggregate counts > pre-rebuild aggregate counts (because more PMH rows now exist) — small phenotype-flip increases expected
- Specifically: complication-attributed PMH (rln_injury, voice_change, etc.) now visible in pmh_*_any_evidence cols if they map to PMH phenotype taxonomy — note any cross-mapping mismatches

---

## 4. Don't touch (active parallel lanes)

- `canonical_pathology_clinical_events_v1` / `canonical_cervical_ln_clinical_events_v1` — Cowork's lane
- Any table touched by sibling Cursor lanes 5 + 6 (complications rollup, PSH rollup)

---

## 5. Reference reading

Required:
- Auto-memory: `project_path_gland_family_complete_2026-04-28.md` (rebuild-then-verify pattern)
- Auto-memory: `project_medications_parathyroid_families_complete_2026-04-29.md` (3 reusable patterns)
- Auto-memory: `feedback_motherduck_direct_check.md`, `feedback_surgical_git_add.md`
- Repo: `qc_framework_v1/migrations/101_path_gland_patient_rollup_signoff.sql` (rebuild template)
- Repo: `qc_framework_v1/migrations/105_medications_patient_rollup_signoff.sql` (signoff template post-rebuild)

---

## 6. File / commit conventions

- Author: `Logan Glosser <logan.glosser@gmail.com>`
- Migration filename: `qc_framework_v1/migrations/<next-N>_pmh_patient_rollup_signoff.sql`
- Pre-snapshot table: `"Thyroid 2026 UPdated".archive_pub_v1_0.canonical_pmh_patient_rollup_v1_pre_mig<N>_20260429`
- DuckDB CAST CURRENT_TIMESTAMP AS TIMESTAMP

---

## 7. If something unexpected surfaces

- Pre/post rollup diff > 500 patients on any phenotype → STOP, investigate (something is mapping incorrectly)
- The 6 mig_103 PMH rows are calcitriol/calcium_supplement (medication entries that got PMH-attributed). Check whether the PMH phenotype taxonomy in Script 365 maps these correctly. They likely DON'T map to any of the 22 phenotypes — that's expected; the rows still increment n_findings_any but no phenotype flag flips.
- The 246 mig_98 PMH rows are complication entries (rln_injury, vocal_cord_paralysis, hypocalcemia_clinical, hypoparathyroidism, hematoma, seroma, chyle_leak, mortality). These also don't map to PMH phenotypes — same situation. CF-mig107-PMH-PHENOTYPE-MAP open if you find this is actually not handled cleanly.
- New synthetic rows have anchor_source different from 'strict' / 'first_surgery_fallback' — adjust anchor_source verification logic to allow the additional values, or treat synthetic-row anchor_source as separate audit dimension.

---

End of prompt. Lane 7 of 3-prompt batch. Closes the PMH family. Update `MEMORY.md` with close-out entry following established pattern.
