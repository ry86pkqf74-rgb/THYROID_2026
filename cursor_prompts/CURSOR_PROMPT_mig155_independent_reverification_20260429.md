# Cursor Agent Task — mig_155 INDEPENDENT RE-VERIFICATION (Path C after-the-fact)

**Generated:** 2026-04-29 (Cowork session)
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** `git fetch && git pull`
**Estimated effort:** 1-2 hours
**Run order:** Lane 49 of next 4-prompt batch (mig_161)

---

## 0. Why this lane exists

mig_155 (Risk Scoring + Survival + Genetics-Residual cluster, 31 cols) was applied directly to MotherDuck by a Cursor agent at 2026-04-29 12:07 UTC despite the prompt's AGENTS-governance "no-write-from-agent" rule. Cowork didn't get to run pre-apply Path-C verification.

**This lane retroactively does the Path-C verification** — same protocol Cowork ran for mig_149/150 — to detect any agent-QA misses or methodology issues that need a `mig_155b` cleanup migration. It does NOT re-flip cols (they're already verified). It looks for issues to flag.

This is **read-only verification + (if issues found) a registry-note-only cleanup SQL**. No data writes against verified PM cols.

---

## 1. Goal

Independently verify the 31 cols flipped under `batch_id='mig_155_patient_master_risk_scoring_survival_genetics_cluster_20260429'` against the original prompt at `cursor_prompts/CURSOR_PROMPT_patient_master_risk_scoring_survival_genetics_cluster_20260429.md`.

### 1a. Scope confirmation

```sql
SELECT verification_method, COUNT(*) AS n
FROM main.canonical_column_verification_registry_v1
WHERE batch_id='mig_155_patient_master_risk_scoring_survival_genetics_cluster_20260429'
GROUP BY 1 ORDER BY 2 DESC;
```

Verify total count = 31.

### 1b. Cluster reminder (from original prompt)

7 ATA risk + ATA response, 4 MACIS, 3 AMES, 3 scoring eligibility flags, 5 survival aggregations, 4 recurrence proxies, 3 resolved-layer provenance, 2 genetics residual.

---

## 2. Verification protocol

### 2a. Live-table-name audit

For each `verification_method` in the registry, parse the upstream table name and verify it lives in `main`:

```sql
SELECT column_name, verification_method
FROM main.canonical_column_verification_registry_v1
WHERE batch_id='mig_155_patient_master_risk_scoring_survival_genetics_cluster_20260429'
  AND verification_method LIKE '%canonical%'
  AND verification_method NOT LIKE '%canonical_dynamic_risk_response%'
  AND verification_method NOT LIKE '%canonical_recurrence%'
  AND verification_method NOT LIKE '%canonical_path%'
  AND verification_method NOT LIKE '%canonical_molecular%'
ORDER BY verification_method;
```

Cross-check each named table exists:
```sql
SELECT '<table>' AS named, COUNT(*) AS exists_in_main
FROM information_schema.tables
WHERE table_catalog='thyroid_canonical_publication_v1_0' AND table_schema='main' AND table_name='<table>';
```

If any methodology names a non-live table, open `CF-mig161-MIG155-DEAD-TABLE-<col>` informational and document the live successor.

### 2b. Cohort-uniformity sweep on every BOOLEAN

11 BOOLEANs to sweep:
- ata_calculable_flag, ata_response_calculable_flag, ata_response_is_provisional, ata_risk_calculable_flag
- ames_calculable_flag, macis_calculable_flag
- biochemical_recurrence_flag, structural_recurrence_flag
- distant_mets_proxy, distant_mets_proxy_v2
- genetics_master_v1_link_flag
- scoring_ajcc8_flag, scoring_ata_flag, scoring_macis_flag

Use the standard template:
```sql
SELECT
  '<col>' AS col,
  SUM(CASE WHEN <col> THEN 1 ELSE 0 END) AS t,
  SUM(CASE WHEN NOT <col> THEN 1 ELSE 0 END) AS f,
  SUM(CASE WHEN <col> IS NULL THEN 1 ELSE 0 END) AS n
FROM main.canonical_patient_master;
```

Apply mig_142b decision rules:
- TRUE-only / 0 FALSE / NULL → CF-mig161-MIG155-COHORT-NEAR-UNIFORM-TRUE-<col> (Type-A; keep verified)
- 0 TRUE / FALSE-only / NULL → CF-mig161-MIG155-COHORT-UNIFORM-FALSE-<col> (Type-B; reclassify to na in mig_155b)
- TRUE > 99% or < 1% AND not Type-A → investigate clinical plausibility

### 2c. Single-value VARCHAR audit

```sql
SELECT 'resolved_layer_version' AS col, COUNT(DISTINCT resolved_layer_version) AS n_distinct, COUNT(*) FILTER (WHERE resolved_layer_version IS NULL) AS n_null FROM main.canonical_patient_master;
-- Repeat for: ata_initial_risk, ata_risk_category, ata_response_category, ames_risk, ames_risk_group, macis_risk_group, macis_missing_components, surv_recurrence_risk_band
```

Single-value VARCHARs → CF-mig161-MIG155-VALUE-DEGENERATE-UPSTREAM-<col>.

### 2d. Cross-canonical reconciliation (CRITICAL)

The original mig_155 prompt §7 requires cross-validation:
- `biochemical_recurrence_flag` should match patients with `recurrence_type='biochemical'` in canonical_recurrence_v1
- `structural_recurrence_flag` similar with `recurrence_type='structural'`
- `surv_n_events` should align with mig_141 outputs

Run these reconciliations:
```sql
-- Biochemical recurrence cross-check
WITH cr_biochem AS (
  SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid
  FROM main.canonical_recurrence_v1
  WHERE recurrence_confirmed=TRUE AND LOWER(recurrence_type) LIKE '%biochem%'
)
SELECT
  COUNT(*) AS n_pm,
  SUM(CASE WHEN pm.biochemical_recurrence_flag THEN 1 ELSE 0 END) AS pm_t,
  SUM(CASE WHEN cr.rid IS NOT NULL THEN 1 ELSE 0 END) AS canonical_t,
  SUM(CASE WHEN pm.biochemical_recurrence_flag AND cr.rid IS NULL THEN 1 ELSE 0 END) AS pm_only,
  SUM(CASE WHEN NOT pm.biochemical_recurrence_flag AND cr.rid IS NOT NULL THEN 1 ELSE 0 END) AS canon_only
FROM main.canonical_patient_master pm
LEFT JOIN cr_biochem cr ON CAST(pm.research_id AS VARCHAR)=cr.rid;
-- Repeat for structural, distant_mets_proxy
```

If drift > ~50 patients on any flag, open `CF-mig161-MIG155-RECURRENCE-PROXY-DRIFT-<col>`.

### 2e. Date-type check on the 1 date col

`resolved_at` is TIMESTAMP — allowlist (audit/provenance, OK). Verify it's not being treated as a clinical event date.

### 2f. Spot-check derivations

Pick 5 random rids with `ata_risk_category='high'`. Check underlying components (ETE, large size, distant mets) on the path/recurrence canonicals. Document evidence trace.

---

## 3. Output: mig_161 SQL file

File: `qc_framework_v1/migrations/161_mig155_independent_reverification_20260429.sql`

Two sections:
- **Section A — Verification report (commented header)** — full results of §2a-§2f probes
- **Section B — mig_161 cleanup UPDATEs** — append CF notes to the relevant mig_155 cols based on findings

If there are degenerate-FALSE BOOLEANs that need verified→na reclassification, include those flips. Include signoff registry resync.

If everything is clean (no Type-B degenerates, no dead-table methodologies, no drift > 50 pts), the migration is purely informational CF appendices — same shape as mig_149b.

---

## 4. Required CFs (enumerate even if none triggered)

- `CF-mig161-MIG155-DEAD-TABLE-<col>` — open / clear
- `CF-mig161-MIG155-COHORT-UNIFORM-FALSE-<col>` — list each
- `CF-mig161-MIG155-COHORT-NEAR-UNIFORM-TRUE-<col>` — list each
- `CF-mig161-MIG155-VALUE-DEGENERATE-UPSTREAM-<col>` — single-value VARCHARs
- `CF-mig161-MIG155-RECURRENCE-PROXY-DRIFT-<col>` — for each proxy with drift > 50
- `CF-mig161-MIG155-SURV-VS-MIG141-CROSS` — confirmation/divergence note

---

## 5. Apply + verify (Logan-only)

NO MD writes from agent. Cowork applies after review.

---

## 6. Git workflow

```bash
git add qc_framework_v1/migrations/161_mig155_independent_reverification_20260429.sql
git -c user.name="Logan Glosser" -c user.email="logan.glosser@gmail.com" commit -m "qc: mig_161 independent re-verification of agent-applied mig_155"
git push origin main
```

---

## 7. Done definition

- [ ] Live-table-name audit complete; all methodologies named live tables (or CF-DEAD-TABLE opened)
- [ ] Cohort-uniformity sweep complete on all 11 BOOLEANs
- [ ] Single-value VARCHAR audit complete on all relevant cols
- [ ] Cross-canonical reconciliation reports for biochemical/structural/distant proxies vs canonical_recurrence_v1
- [ ] surv_* cross-validation against mig_141 outputs
- [ ] 5-rid derivation spot-check evidence in migration header
- [ ] mig_161 SQL file committed + pushed (CF appendices + any verified→na flips)
- [ ] NO MD writes from agent
