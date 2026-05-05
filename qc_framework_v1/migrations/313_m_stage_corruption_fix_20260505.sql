-- mig_313: M-stage corruption fix (applied 2026-05-05)
-- Root cause: m_stage_ajcc8_resolved was back-derived from stage_group_ajcc8
--   (age<55 + Stage II → M1; age>=55 + Stage IVB → M1)
--   originating from corrupt distant_mets_proxy = recurrence_flag (Issue 1, script 224)
-- Fix: use distant_mets_proxy_v2 (path_m_stage_raw='M1' OR pet_distant_mets_ever=TRUE)
-- Deployed by: scripts/mig_313_m_stage_corruption_fix.py --md
--
-- PRE-FIX:  M1=1816 (45.19%), PTC M1=44.23%, FC M1=57.82%, FA M1=100%
-- POST-FIX: M1=114  (2.84%),  PTC M1=2.24%,  FC M1=3.29%,  FA M1=0%
--
-- Stage group shifts (malignant, N=4019):
--   Stage I:   1537 → 2513 (+976)
--   Stage II:  1651 → 1159 (-492)
--   Stage IVB:  816 →   76 (-740)
--   Stage III:    9 →   81 (+72)
--   Stage IVA:    0 →   28 (+28)
--   Stage IVC:    0 →   11 (+11)
--
-- Downstream refreshed:
--   path_synoptics.tumor_{1-5}_m_stage_ajcc8: reset
--   path_synoptics.tumor_1_stage_group_ajcc8: reset
--   ata_initial_risk: re-derived with corrected distant_mets_proxy
--   m036_ata_2025_rss_v2: regenerated (high: 1642→1445, intermediate: 1143→2120)
--   All cohort views: auto-refreshed via CPM column update
--
-- Closes: CF-MSTAGE-CORRUPTION
-- Impacts requiring manuscript patch:
--   M044 v5: Stage IVB 816→76 — needs v6 patch (>5% change)
--   M025 v2: Stage IV% changes — needs recount
--   M032: Stage-by-era trend tables — needs re-run

USE thyroid_canonical_publication_v1_0;

-- §1 Verification: confirm post-fix M1 rates
SELECT 'ptc_m1_pct' AS metric,
       ROUND(100.0 * SUM(CASE WHEN ajcc8_m_stage='M1' THEN 1 ELSE 0 END)
             / COUNT(*), 2) AS value
FROM main.canonical_patient_master
WHERE LOWER(COALESCE(histology_final,'')) LIKE '%ptc%'
  AND is_malignant = TRUE;

SELECT 'cpm_m1_total' AS metric,
       SUM(CASE WHEN ajcc8_m_stage='M1' THEN 1 ELSE 0 END) AS value,
       COUNT(*) AS malignant_n,
       ROUND(100.0 * SUM(CASE WHEN ajcc8_m_stage='M1' THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct
FROM main.canonical_patient_master
WHERE is_malignant = TRUE;

-- §2 Stage group distribution post-fix
SELECT ajcc8_stage_group, COUNT(*) AS n,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
FROM main.canonical_patient_master
WHERE is_malignant = TRUE
GROUP BY 1 ORDER BY 2 DESC;

-- §3 Signoff confirmation
SELECT mig_id, signed_off_at, LEFT(summary, 200) AS summary_snippet
FROM main.signoff_migration
WHERE mig_id = 'mig_313';
