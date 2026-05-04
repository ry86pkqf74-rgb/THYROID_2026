# Cursor Composer Dispatch — mig_277: NIFTP full reclassify (all 95 residual patients)

**Generated:** 2026-05-03 by Cowork at HEAD `be75bee` (post-mig_276 / mig_275 signoff).
**Lane:** mig_277 — Logan-ratified 2026-05-03: **NIFTP should NOT be counted as malignant, regardless of preoperative Bethesda category.** mig_264b only handled 22 NIFTPs at Bethesda 2; 95 NIFTPs remain flagged `is_malignant=TRUE`. Flip them to `FALSE`, NULL their AJCC stage cols (mig_271 cascade), and confirm cohort denominator drops to 4,018 / 36.96%.
**Recommended agent:** **Cursor Composer** — mechanical sweep with one decision rule.
**Estimated runtime:** 25-40 min.
**Triggered by:** Logan decision after Cowork audit found 95 NIFTP residuals in 2026-05-03 MD review.
**Severity:** MED. Affects every manuscript that cites the cohort malignancy denominator (M032 / M044 / M037 / M025 / M004 / M038).
**Closes carry-forward:** CF-mig264b-NIFTP-RESIDUAL-95.

---

## §0 — First message to paste into Cursor Composer

> mig_277 dispatch. Read `cursor_prompts/CURSOR_PROMPT_MIG_277_NIFTP_FULL_RECLASSIFY_20260503.md` end-to-end before any tool use. MotherDuck DB is `thyroid_canonical_publication_v1_0`. GitHub repo at `/Users/ros/THyroid 2026`. Use Desktop Commander for git ops.
>
> Logan-ratified rule: NIFTP is **never** malignant per WHO 2017 / AJCC 8 reclassification. Flip `is_malignant` from TRUE → FALSE for all 95 residual patients. Cascade NULL on `ajcc8_stage_group` / `_t` / `_n` / `_m` (mig_271 pattern). Pre-snapshot to `archive_pub_v1_0`. Insert signoff_migration row.

---

## §1 — Why this lane exists

mig_264b (2026-05-01) flipped 22 NIFTPs at Bethesda 2 + 2 follicular adenomas → `is_malignant=FALSE`. Audit on 2026-05-03 found 95 NIFTPs remain malignant-flagged at higher Bethesda values (and 51 at NULL Bethesda):

| `bethesda_final` | `n_niftp` | `is_malignant=TRUE` | `ajcc8_stage_group IS NOT NULL` |
|---:|---:|---:|---:|
| 1 | 2 | 2 | 2 |
| 2 | 22 | 0 | 0 (mig_264b cleared) |
| 3 | 23 | 23 | 23 |
| 4 | 9 | 9 | 9 |
| 5 | 3 | 3 | 3 |
| 6 | 7 | 7 | 7 |
| NULL | 51 | 51 | 49 |
| **Total NIFTP** | **117** | **95** | **93** |

Per WHO 2017 + AJCC 8, NIFTP is excluded from thyroid carcinoma classification regardless of Bethesda. The mig_264b convention (NIFTP→benign + stage NULL) is the established treatment; mig_277 just generalizes it to the long tail.

**Cohort impact:**
- Pre-277: 4,113 / 10,871 = **37.83%** malignant
- Post-277: 4,018 / 10,871 = **36.96%** malignant (Δ = -95 / -0.87 pp)

---

## §2 — Pre-task probes (read-only, run first)

```sql
-- 2.1 Confirm 95 NIFTP residuals before any DML
SELECT
  bethesda_final,
  COUNT(*) AS n,
  COUNT_IF(is_malignant) AS n_malig,
  COUNT_IF(ajcc8_stage_group IS NOT NULL) AS n_w_stage
FROM main.canonical_patient_master
WHERE histology_final ILIKE '%niftp%'
GROUP BY bethesda_final
ORDER BY bethesda_final;
-- Expected: total n=117, malig=95, with_stage=93

-- 2.2 Identify the 95 research_ids
CREATE OR REPLACE TEMP TABLE _mig277_targets AS
SELECT research_id, histology_final, bethesda_final, is_malignant, ajcc8_stage_group, ajcc8_t, ajcc8_n, ajcc8_m
FROM main.canonical_patient_master
WHERE histology_final ILIKE '%niftp%' AND is_malignant = TRUE;

SELECT COUNT(*) AS targets, COUNT(DISTINCT research_id) AS unique_pts FROM _mig277_targets;
-- Expected: 95 / 95

-- 2.3 Cross-check: any of these in M044 / M037 cohort views (which filter is_malignant=TRUE)?
SELECT 'cohort_m044' AS view, COUNT(*) AS n_to_drop
FROM _mig277_targets t
WHERE EXISTS (SELECT 1 FROM main.cohort_m044_ete_outcomes c WHERE c.research_id = t.research_id)
UNION ALL
SELECT 'cohort_m037', COUNT(*)
FROM _mig277_targets t
WHERE EXISTS (SELECT 1 FROM main.cohort_m037_ln_predictors c WHERE c.research_id = t.research_id);
-- Expected: small but non-zero. Cohort views auto-rebuild on next refresh; just document the drop.
```

---

## §3 — Apply (Protocol v2)

### §3a — Pre-snapshot to archive

```sql
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_pre_mig277_20260503 AS
SELECT research_id, histology_final, bethesda_final, is_malignant,
       ajcc8_stage_group, ajcc8_t, ajcc8_n, ajcc8_m,
       ajcc8_stage_group_resolved, ajcc8_t_resolved, ajcc8_n_resolved, ajcc8_m_resolved
FROM main.canonical_patient_master
WHERE histology_final ILIKE '%niftp%';
-- Expected: 117 rows
```

### §3b — UPDATE: flip is_malignant + NULL stage cols

```sql
UPDATE main.canonical_patient_master
SET
  is_malignant = FALSE,
  ajcc8_stage_group = NULL,
  ajcc8_t = NULL,
  ajcc8_n = NULL,
  ajcc8_m = NULL,
  ajcc8_stage_group_resolved = NULL,
  ajcc8_t_resolved = NULL,
  ajcc8_n_resolved = NULL,
  ajcc8_m_resolved = NULL
WHERE histology_final ILIKE '%niftp%' AND is_malignant = TRUE;
-- Expected: 95 rows updated
```

### §3c — Verify post-state

```sql
SELECT
  COUNT(*) AS n_total,
  COUNT_IF(is_malignant) AS n_malig,
  ROUND(100.0 * COUNT_IF(is_malignant) / COUNT(*), 2) AS pct_malig,
  COUNT_IF(histology_final ILIKE '%niftp%' AND is_malignant) AS n_niftp_still_malig,
  COUNT_IF(histology_final ILIKE '%niftp%' AND ajcc8_stage_group IS NOT NULL) AS n_niftp_still_staged
FROM main.canonical_patient_master;
-- Expected: 10871 / 4018 / 36.96 / 0 / 0
```

### §3d — Registry signoff

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_277', CURRENT_TIMESTAMP, 'cursor_composer_mig277',
 'mig_277: NIFTP full reclassify - 95 residual NIFTPs flipped is_malignant TRUE→FALSE; AJCC stage cols NULL''d (CPM + _resolved variants). Pre n_malig=4113 (37.83%); post n_malig=4018 (36.96%). Closes CF-mig264b-NIFTP-RESIDUAL-95. Generalizes mig_264b/271 NIFTP rule across all Bethesda categories per WHO 2017 / AJCC 8.');
```

---

## §4 — Downstream cohort views

The cohort views `cohort_m044_ete_outcomes`, `cohort_m037_ln_predictors`, `cohort_m032_25yr`, `cohort_m025_tirads`, `cohort_m004_autoimmune` all filter `WHERE is_malignant = TRUE`. They will auto-drop the 95 NIFTPs on next refresh. Confirm with:

```sql
SELECT 'm044' AS m, COUNT(*) AS n FROM main.cohort_m044_ete_outcomes
UNION ALL SELECT 'm037', COUNT(*) FROM main.cohort_m037_ln_predictors
UNION ALL SELECT 'm032', COUNT(*) FROM main.cohort_m032_25yr
UNION ALL SELECT 'm025', COUNT(*) FROM main.cohort_m025_tirads
UNION ALL SELECT 'm004', COUNT(*) FROM main.cohort_m004_autoimmune;
```

If any view CTAS-materialized (not VIEW), rebuild explicitly. Document new n's in mig_277 commit message.

---

## §5 — Snowflake re-verify

After mig_277 commit + push, Cowork will:
1. Re-export `canonical_patient_master.parquet` from MD
2. Reload to Snowflake `THYROID_VALIDATION.PUBLIC.CANONICAL_PATIENT_MASTER`
3. Rebuild flat view
4. Re-run M044 Table 1 (denominator drops 4,113 → 4,018)
5. Re-run M032 Table 1 (cohort malignancy rate footnote: 37.83% → 36.96%)

Cowork will handle this end of session — no Cursor action needed for SF.

---

## §6 — Carry-forwards

| ID | Status | Notes |
|---|---|---|
| CF-mig264b-NIFTP-RESIDUAL-95 | **CLOSED on apply** | All 95 reclassified |
| CF-mig277-MANUSCRIPT-RATE-FOOTNOTE | **OPEN** | M032/M044/M037/M025/M004 manuscripts citing 37.8% need bump to 37.0% (or 36.96% precise). Bulk find/replace lane. |

---

## §7 — Surgical git add (explicit paths only, never -A)

```
qc_framework_v1/migrations/277_niftp_full_reclassify_20260503.sql
scripts/output/mig_277_apply_log.txt
cursor_prompts/CURSOR_PROMPT_MIG_277_NIFTP_FULL_RECLASSIFY_20260503.md
```

Commit message:
```
feat(md): mig_277 NIFTP full reclassify (95 patients) per WHO 2017 / AJCC 8

- Flips is_malignant TRUE→FALSE for all 95 NIFTP residuals (Bethesda 1/3/4/5/6/NULL)
- NULLs ajcc8_stage_group + _t/_n/_m + _resolved variants
- Cohort: 4113 (37.83%) → 4018 (36.96%) malignant
- Generalizes mig_264b/271 rule across all preop Bethesda categories
- Closes CF-mig264b-NIFTP-RESIDUAL-95
- Opens CF-mig277-MANUSCRIPT-RATE-FOOTNOTE for downstream text updates
```

---

**End of mig_277 dispatch.**
