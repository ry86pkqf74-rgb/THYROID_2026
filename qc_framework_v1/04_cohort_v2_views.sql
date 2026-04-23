-- =====================================================================
-- qc_framework_v1 / 04_cohort_v2_views.sql
--
-- Target DB:     thyroid_canonical_publication_v1_0
-- Target schema: manuscript_workspace (does NOT touch main.manuscript_cohort_v1)
--
-- Run AFTER 03_qc_violations_populate.sql.
-- =====================================================================

-- Patients excluded because they violate any 'critical' rule
CREATE OR REPLACE VIEW manuscript_workspace.qc_critical_excluded_patients_v1 AS
SELECT DISTINCT research_id
FROM manuscript_workspace.qc_violations_v1
WHERE severity = 'critical';

-- cohort_v2 = main.manuscript_cohort_v1 MINUS critical violators
CREATE OR REPLACE VIEW manuscript_workspace.qc_manuscript_cohort_v2 AS
SELECT c.*
FROM main.manuscript_cohort_v1 c
WHERE c.research_id NOT IN (
    SELECT research_id FROM manuscript_workspace.qc_critical_excluded_patients_v1
);

-- PTC-only cohort_v2 with normalized histology
CREATE OR REPLACE VIEW manuscript_workspace.qc_manuscript_cohort_v2_ptc AS
SELECT *
FROM manuscript_workspace.qc_manuscript_cohort_v2
WHERE TRIM(COALESCE(histology_final,'')) IN (
    'PTC','PTC classical','PTC follicular variant','PTC tall cell variant',
    'PTC columnar cell variant','PTC diffuse sclerosing variant',
    'PTC hobnail variant','PTC oncocytic variant','PTC solid variant',
    'PTC cribriform-morular variant','PTMC'
);

-- Flight deck: counts
CREATE OR REPLACE VIEW manuscript_workspace.qc_cohort_flow_v1_to_v2 AS
SELECT
    (SELECT COUNT(*) FROM main.manuscript_cohort_v1)                             AS v1_patients,
    (SELECT COUNT(*) FROM manuscript_workspace.qc_critical_excluded_patients_v1) AS excluded_critical,
    (SELECT COUNT(*) FROM manuscript_workspace.qc_manuscript_cohort_v2)          AS v2_patients,
    (SELECT COUNT(*) FROM manuscript_workspace.qc_manuscript_cohort_v2_ptc)      AS v2_ptc_only,
    (SELECT COUNT(*) FROM manuscript_workspace.qc_violations_v1
                     WHERE severity='warning')                                   AS total_warnings,
    (SELECT COUNT(*) FROM manuscript_workspace.qc_event_issues_v1)               AS total_event_issues;

-- Per-rule attribution of cohort exclusions (for Methods section)
CREATE OR REPLACE VIEW manuscript_workspace.qc_cohort_exclusion_attribution_v1 AS
SELECT
    v.rule_id,
    r.description,
    r.severity,
    r.source_object,
    COUNT(DISTINCT v.research_id) AS n_patients
FROM manuscript_workspace.qc_violations_v1 v
JOIN manuscript_workspace.qc_rules_v1 r USING (rule_id)
WHERE r.severity = 'critical'
GROUP BY v.rule_id, r.description, r.severity, r.source_object
ORDER BY n_patients DESC;


-- Smoke test
SELECT * FROM manuscript_workspace.qc_cohort_flow_v1_to_v2;
SELECT * FROM manuscript_workspace.qc_cohort_exclusion_attribution_v1;
