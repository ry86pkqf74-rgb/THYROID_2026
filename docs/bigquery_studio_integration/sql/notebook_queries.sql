-- Queries for the BigQuery Studio data-quality notebook ("Untitled notebook 2026-05-14").
-- Paste each into a notebook code cell prefixed with the %%bigquery magic.
-- (The Colab Enterprise code-cell editor is a cross-origin iframe that could not be
--  auto-populated by browser automation; these are a manual paste.)

-- Cell 1 - QC rule catalog (the 20 documented rules)
-- %%bigquery
SELECT rule_id, category, severity, source_object, description
FROM `thyroid-canonical-pub-2026.pub_workspace.qc_rules_v1`
ORDER BY CASE severity WHEN 'critical' THEN 1 WHEN 'warning' THEN 2 ELSE 3 END, rule_id;

-- Cell 2 - Workup census summary (modality coverage + cohort flags)
-- %%bigquery
SELECT * FROM `thyroid-canonical-pub-2026.pub_eval.vw_workup_census_summary_v1`
ORDER BY metric_group, metric;

-- Cell 3 - Nuclear-medicine date recovery check
-- %%bigquery
SELECT scandate_quality, COUNT(*) AS n
FROM `thyroid-canonical-pub-2026.pub_eval.vw_nuclear_med_dated_v1`
GROUP BY scandate_quality
ORDER BY n DESC;
