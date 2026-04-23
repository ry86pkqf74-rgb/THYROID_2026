-- =====================================================================
-- qc_framework_v1 / 02_qc_violations_schema.sql
--
-- Target DB:     thyroid_canonical_publication_v1_0
-- Target schema: manuscript_workspace (all QC artifacts co-located here
--                alongside existing cpm_*, us_*, registry_* audit tables).
--                main.* is deliberately untouched so prod canonical stays
--                read-only from this framework's perspective.
--
-- Grain:
--   qc_violations_v1  - patient-grain (one row per research_id + rule_id).
--                       Drives cohort_v2 exclusion.
--   qc_event_issues_v1 - event-grain (research_id + source_table +
--                        source_pk_value). Drill-down detail so a chart
--                        reviewer can pull exact bad rows.
--
-- Severity:
--   'critical' - invalidates analyses. cohort_v2 drops these patients.
--   'warning'  - flag for review, don't drop.
--   'info'     - observational.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS manuscript_workspace;


-- ---- Rule registry ---------------------------------------------------
DROP TABLE IF EXISTS manuscript_workspace.qc_rules_v1;

CREATE TABLE manuscript_workspace.qc_rules_v1 (
    rule_id         VARCHAR PRIMARY KEY,
    category        VARCHAR NOT NULL,
    severity        VARCHAR NOT NULL,
    source_object   VARCHAR NOT NULL,   -- which table the check runs on
    description     VARCHAR NOT NULL,
    rationale       VARCHAR,
    exclusion_scope VARCHAR
);

INSERT INTO manuscript_workspace.qc_rules_v1 VALUES
    -- Lymph-node rules (manuscript_cohort_v1)
    ('LN01_POSITIVE_GT_EXAMINED',        'ln',         'critical',
     'manuscript_cohort_v1',
     'ln_positive_final > path_ln_examined_raw',
     'Impossible by construction; numerator / denominator mismatch.',
     'manuscript_cohort_v2'),

    ('LN02_POSITIVE_WITHOUT_EXAMINED',   'ln',         'critical',
     'manuscript_cohort_v1',
     'ln_positive_final > 0 AND COALESCE(path_ln_examined_raw,0) = 0',
     'Denominator missing while numerator present; staging uncomputable.',
     'manuscript_cohort_v2'),

    ('LN03_RAW_VS_FINAL_DISAGREE',       'ln',         'warning',
     'manuscript_cohort_v1',
     'path_ln_positive_raw <> ln_positive_final when both present',
     'Two LN-positive sources of truth disagree; pick one canonically.',
     NULL),

    ('LN04_LN_DATA_MISSING',             'ln',         'info',
     'manuscript_cohort_v1',
     'ln_positive_final IS NULL AND path_ln_examined_raw IS NULL',
     'Non-random missingness; assess selection bias before models.',
     NULL),

    -- Recurrence rules (manuscript_cohort_v1 + recurrence_event_clean_v1)
    ('REC01_RECURRENCE_BEFORE_SURGERY',  'recurrence', 'critical',
     'recurrence_event_clean_v1',
     'ANY recurrence_date < first_surgery_date',
     'Hard temporal violation; survival/time-to-event models invalid.',
     'manuscript_cohort_v2'),

    ('REC02_FLAG_WITHOUT_DATE',          'recurrence', 'warning',
     'manuscript_cohort_v1',
     'any_recurrence_flag = TRUE but recurrence_date IS NULL',
     'Recurrence flagged without a date; time-to-event drops or miscodes.',
     NULL),

    ('REC03_DATE_WITHOUT_FLAG',          'recurrence', 'warning',
     'manuscript_cohort_v1',
     'recurrence_date IS NOT NULL but any_recurrence_flag IS NOT TRUE',
     'Date present without confirm flag; disambiguate suspected vs confirmed.',
     NULL),

    -- Surgery date rules
    ('SURG01_DATE_DIVERGENCE',           'surgery',    'critical',
     'manuscript_cohort_v1',
     'first_surgery_date / surg_first_date / surgery_date disagree when populated',
     'Three competing surgery-date columns must never disagree.',
     'manuscript_cohort_v2'),

    ('SURG02_TRIPLE_DATE_DUPLICATE',     'surgery',    'info',
     'manuscript_cohort_v1',
     'All three surgery-date columns populated and identical',
     'Schema smell: collapse to one canonical column, drop the others.',
     NULL),

    -- Histology rules
    ('HIST01_WHITESPACE',                'histology',  'warning',
     'manuscript_cohort_v1',
     'histology_final <> TRIM(histology_final)',
     'Silent cohort inconsistency.',
     NULL),

    ('HIST02_UNNORMALIZED_VARIANT',      'histology',  'warning',
     'manuscript_cohort_v1',
     'histology_final matches %PTC% but not in canonical variant list',
     'Subtype analyses become noisy.',
     NULL),

    ('HIST03_METASTATIC_PREFIX',         'histology',  'warning',
     'manuscript_cohort_v1',
     'histology_final starts with "metastatic "',
     'Route to cohort-histology + is_metastatic flag.',
     NULL),

    -- FNA / imaging temporal (cross-table)
    ('FNA01_FNA_AFTER_SURGERY',          'temporal',   'critical',
     'canonical_fna_events_v1',
     'fna_date_resolved > first_surgery_date',
     'Pre-op FNA by definition; post-op FNAs must not feed preop covariates.',
     'manuscript_cohort_v2'),

    -- TIRADS rules (canonical_us_nodule_v2; aggregated to patient)
    ('TIR01_POINTS_CATEGORY_MISMATCH',   'tirads',     'warning',
     'canonical_us_nodule_v2',
     'acr2017_tirads_points inconsistent with acr2017_tirads_category per ACR 2017 bands',
     'ACR bands: 0=TR1, 2=TR2, 3=TR3, 4-6=TR4, 7+=TR5. Mismatches break FNA-recommendation logic.',
     NULL),

    ('TIR02_CONCORDANCE_FLAG_WRONG',     'tirads',     'warning',
     'canonical_us_nodule_v2',
     'acr2017_vs_updated_concordant=FALSE but acr2017_tirads_category = updated_tirads_category',
     'Concordance flag is literally inconsistent with the two category columns.',
     NULL),

    ('TIR03_MULTI_NODULE_UNDEREXPLODED', 'tirads',     'critical',
     'canonical_us_nodule_v2',
     'Per exam: many non-aggregate nodules, many distinct reported TIRADS numbers, but collapsed computed categories',
     'Upstream parsing likely failed to split a multi-nodule/multi-TIRADS report into rows; treat affected patient imaging as unreliable.',
     'manuscript_cohort_v2'),

    -- Pathology ETE rules
    ('ETE01_NONNORMALIZED_STRING',       'pathology',  'warning',
     'canonical_path_malignant_events_v1',
     'extrathyroidal_extension not in controlled vocabulary',
     'Free-text ETE ("x", "yes", "extesive", "microscopiic" etc.) breaks derivation.',
     NULL),

    ('ETE02_GROSS_FLAG_VS_STRING_INCONSISTENT', 'pathology', 'critical',
     'canonical_path_malignant_events_v1',
     'gross_ete = 1 but extrathyroidal_extension ILIKE "minimal"/"microscopic"/"focal"',
     'Gross ETE by definition excludes microscopic-only; invalidates ETE-grade-derived cohorts.',
     'manuscript_cohort_v2'),

    -- AJCC rule
    ('AJCC01_CALCULABLE_BUT_N_NULL',     'staging',    'warning',
     'canonical_path_malignant_events_v1',
     'ajcc8_stage_calculable_flag = TRUE but n_stage_ajcc8 IS NULL',
     'calculable flag is unreliable; do not trust stage_group rows where flag=TRUE without components.',
     NULL),

    ('AJCC02_COHORT_CALCULABLE_BUT_N_NULL', 'staging', 'warning',
     'manuscript_cohort_v1',
     'ajcc8_calculable_flag = TRUE but ajcc8_n_stage IS NULL',
     'Same rule applied at patient-grain on the cohort rollup.',
     NULL)
;


-- ---- Patient-grain violations table ---------------------------------
DROP TABLE IF EXISTS manuscript_workspace.qc_violations_v1;

CREATE TABLE manuscript_workspace.qc_violations_v1 (
    research_id     BIGINT  NOT NULL,
    rule_id         VARCHAR NOT NULL,
    severity        VARCHAR NOT NULL,
    category        VARCHAR NOT NULL,
    n_events        INTEGER DEFAULT 1,
    details         VARCHAR,
    detected_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (research_id, rule_id)
);


-- ---- Event-grain drill-down table -----------------------------------
DROP TABLE IF EXISTS manuscript_workspace.qc_event_issues_v1;

CREATE TABLE manuscript_workspace.qc_event_issues_v1 (
    research_id     BIGINT  NOT NULL,
    rule_id         VARCHAR NOT NULL,
    source_table    VARCHAR NOT NULL,
    source_pk       VARCHAR,            -- e.g. 'nodule_id=abc, us_exam_id=xyz'
    details         VARCHAR,
    detected_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- ---- Summary view ---------------------------------------------------
CREATE OR REPLACE VIEW manuscript_workspace.qc_violations_summary_v1 AS
SELECT
    r.rule_id,
    r.category,
    r.severity,
    r.source_object,
    COUNT(v.research_id)          AS n_patients_flagged,
    COUNT(DISTINCT v.research_id) AS n_distinct_patients,
    SUM(v.n_events)               AS n_events_total,
    r.description,
    r.exclusion_scope
FROM manuscript_workspace.qc_rules_v1 r
LEFT JOIN manuscript_workspace.qc_violations_v1 v USING (rule_id)
GROUP BY r.rule_id, r.category, r.severity, r.source_object, r.description, r.exclusion_scope
ORDER BY
    CASE r.severity WHEN 'critical' THEN 0 WHEN 'warning' THEN 1 ELSE 2 END,
    r.category,
    r.rule_id;
