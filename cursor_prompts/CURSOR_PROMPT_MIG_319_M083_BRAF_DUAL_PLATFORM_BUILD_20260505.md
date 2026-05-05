# Cursor prompt — mig_319: cohort_m083 BRAF dual-platform discordance — flesh out the stub view

**Agent:** cursor_composer
**Estimated time:** 30–60 min (DDL + verification + signoff)
**Priority:** P3 — no active manuscript depends on this; build only when M083 enters the active queue
**Closes:** `CF-M083-STUB`

## Problem

`manuscript_workspace.cohort_m083_braf_dual_platform_discordance_v1` exists but is a **1-column stub**:

```sql
DESCRIBE manuscript_workspace.cohort_m083_braf_dual_platform_discordance_v1;
-- column_name | column_type
-- research_id | VARCHAR
-- (1 row, 167 patients)
```

The 167 research_ids are presumably patients with both Afirma + ThyroSeq (or other dual-platform) BRAF results. Without covariate columns, no analysis can proceed.

## Recipe

### Step 1 — Identify the dual-platform population

```bash
cd /Users/loganglosser/THYROID_2026
grep -rn "m083\|braf_dual\|dual_platform" scripts/ qc_framework_v1/ studies/ 2>/dev/null | head -20
```

Look for the original cohort definition — what makes a patient "dual-platform"? Typical answer: both `afirma_*` and `thyroseq_*` results present in `canonical_molecular_genetics_v2`.

### Step 2 — DDL (template; adapt to actual canonical fields)

```sql
CREATE OR REPLACE VIEW manuscript_workspace.cohort_m083_braf_dual_platform_discordance_v1 AS
WITH dual_platform AS (
  SELECT
    research_id,
    MAX(CASE WHEN platform='afirma'   THEN braf_status END) AS afirma_braf,
    MAX(CASE WHEN platform='afirma'   THEN braf_score  END) AS afirma_braf_score,
    MAX(CASE WHEN platform='thyroseq' THEN braf_status END) AS thyroseq_braf,
    MAX(CASE WHEN platform='thyroseq' THEN braf_vaf    END) AS thyroseq_braf_vaf,
    MAX(CASE WHEN platform IN ('afirma','thyroseq') THEN test_date END) AS latest_test_date
  FROM main.canonical_molecular_genetics_v2
  WHERE platform IN ('afirma','thyroseq')
  GROUP BY 1
  HAVING COUNT(DISTINCT platform) = 2  -- both platforms present
)
SELECT
  cpm.research_id,
  cpm.age_at_surgery,
  cpm.sex,
  cpm.histology_final,
  cpm.is_malignant,
  cpm.ajcc8_t_stage,
  cpm.ajcc8_n_stage,
  cpm.ajcc8_m_stage,
  cpm.ajcc8_stage_group,
  pme.tumor_size_cm,
  pme.lvi_grade,
  pme.ete_grade_resolved AS ete_grade_final,
  cpm.any_recurrence_flag,
  cpm.structural_recurrence_flag,
  cpm.followup_years,
  -- molecular
  dp.afirma_braf,
  dp.afirma_braf_score,
  dp.thyroseq_braf,
  dp.thyroseq_braf_vaf,
  dp.latest_test_date,
  -- discordance flag
  CASE
    WHEN dp.afirma_braf IS NULL OR dp.thyroseq_braf IS NULL THEN NULL
    WHEN dp.afirma_braf = dp.thyroseq_braf THEN FALSE
    ELSE TRUE
  END AS dual_platform_discordant_flag,
  -- canonical reference path (post-mig_313)
  pme.path_braf_status,
  -- discordance vs path
  CASE
    WHEN pme.path_braf_status IS NULL THEN NULL
    WHEN dp.afirma_braf IS NULL THEN NULL
    WHEN pme.path_braf_status = 'positive' AND dp.afirma_braf = 'negative' THEN 'afirma_false_negative'
    WHEN pme.path_braf_status = 'negative' AND dp.afirma_braf = 'positive' THEN 'afirma_false_positive'
    ELSE 'concordant'
  END AS afirma_vs_path_concordance,
  CASE
    WHEN pme.path_braf_status IS NULL THEN NULL
    WHEN dp.thyroseq_braf IS NULL THEN NULL
    WHEN pme.path_braf_status = 'positive' AND dp.thyroseq_braf = 'negative' THEN 'thyroseq_false_negative'
    WHEN pme.path_braf_status = 'negative' AND dp.thyroseq_braf = 'positive' THEN 'thyroseq_false_positive'
    ELSE 'concordant'
  END AS thyroseq_vs_path_concordance
FROM main.canonical_patient_master cpm
JOIN dual_platform dp USING (research_id)
LEFT JOIN main.canonical_path_malignant_events_v1 pme USING (research_id);
```

### Step 3 — Validation gates

```sql
-- Row count near 167 (existing stub count)
SELECT COUNT(*) FROM manuscript_workspace.cohort_m083_braf_dual_platform_discordance_v1;
-- Acceptance: 130 ≤ n ≤ 200 (cohort definition may evolve)

-- Discordance rate plausibility check
SELECT
  dual_platform_discordant_flag,
  COUNT(*) AS n,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
FROM manuscript_workspace.cohort_m083_braf_dual_platform_discordance_v1
GROUP BY 1;
-- Acceptance: discordant TRUE rate between 5% and 30% (literature is 10–25% for BRAF cross-platform)

-- Coverage of canonical path BRAF
SELECT
  COUNT(*) AS n_total,
  COUNT(path_braf_status) AS n_path_braf,
  ROUND(100.0 * COUNT(path_braf_status) / COUNT(*), 1) AS pct_path_braf
FROM manuscript_workspace.cohort_m083_braf_dual_platform_discordance_v1;
-- Acceptance: ≥40% have path_braf_status (limited by chart documentation)
```

### Step 4 — Signoff

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
SELECT 'mig_319', CURRENT_TIMESTAMP, 'cursor_composer_mig319',
  'mig_319: cohort_m083_braf_dual_platform_discordance_v1 fleshed out from 1-column stub. n=<X>; discordance rate <Y>%; path BRAF coverage <Z>%. Adds afirma_braf, thyroseq_braf, dual_platform_discordant_flag, plus afirma/thyroseq vs path concordance. Closes CF-M083-STUB.'
WHERE NOT EXISTS (SELECT 1 FROM main.signoff_migration WHERE mig_id='mig_319');
```

## Out of scope

- Do NOT build the M083 manuscript or any analysis script — view-build only.
- Do NOT modify `canonical_molecular_genetics_v2` or `canonical_path_malignant_events_v1`.
- Do NOT commit until validation gates pass.
