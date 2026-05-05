-- M025 v2 TI-RADS Analysis SQL Reference
-- Primary cohort: manuscript_workspace.cohort_m025_tirads_performance_v1 (N=3,375)
-- Primary TI-RADS column: tirads_worst_category_v12 (matches prompt distribution)
-- Numeric score: tirads_worst_score_v12

-- Distribution verification
SELECT tirads_worst_category_v12 as tirads, 
       SUM(CASE WHEN is_malignant IS TRUE THEN 1 ELSE 0 END) as malignant,
       SUM(CASE WHEN is_malignant IS FALSE THEN 1 ELSE 0 END) as benign,
       COUNT(*) as total,
       ROUND(100.0 * SUM(CASE WHEN is_malignant IS TRUE THEN 1 ELSE 0 END) / COUNT(*), 1) as rom_pct
FROM manuscript_workspace.cohort_m025_tirads_performance_v1
GROUP BY tirads_worst_category_v12
ORDER BY tirads_worst_category_v12;
