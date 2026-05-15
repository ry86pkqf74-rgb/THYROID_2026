-- manuscript_column_source_assessment.sql
-- Step 4 of the manuscript iterative-build protocol
-- (.cowork/skills/manuscript-iterative-build/SKILL.md).
--
-- Purpose: when a manuscript cohort is (re)built from BigQuery, automatically assess every
-- column the cohort uses against the competing-source register, and have Gemini write a
-- plain-language advisory for any contested column. Run as a step in the manuscript build
-- pipeline / iteration notebook so the check happens automatically, not by memory.
--
-- Usage:
--   1. Set cohort_dataset / cohort_table below to the manuscript's cohort.
--   2. Run the whole script. The first result set (deterministic) is the reliable core;
--      the second (Gemini) requires a Vertex AI connection - set vertex_connection.
--
-- Update the competing_source_register CTE whenever a THY-xx source-of-truth decision lands.

DECLARE cohort_dataset     STRING DEFAULT 'pub_canonical';
DECLARE cohort_table       STRING DEFAULT 'manuscript_cohort_v1';   -- <-- set per manuscript
DECLARE vertex_connection  STRING DEFAULT 'us.vertex_ai';           -- <-- set to the project's Vertex AI connection

-- 1. Competing-source register: the known contested / watch columns.
--    'contested' = a canonical-source decision is OPEN; do not lock numbers on it silently.
--    'watch'     = known QC rules touch it; verify before use.
CREATE TEMP TABLE competing_source_register AS
SELECT * FROM UNNEST([
  STRUCT('surgery date'      AS concept, 'first_surgery_date'   AS column_name, 'THY-87'    AS linear_issue, 'contested' AS status,
         'surg_first_date and surgery_date are identical duplicates; first_surgery_date is most complete but diverges in 171 patients; no canonical pick yet' AS note),
  ('surgery date',      'surg_first_date',     'THY-87',    'contested', 'identical duplicate of surgery_date; candidate for deprecation'),
  ('surgery date',      'surgery_date',        'THY-87',    'contested', 'identical duplicate of surg_first_date; candidate for deprecation'),
  ('ln positive count', 'path_ln_positive_raw','THY-89',    'contested', 'raw pathology extract; 51 disagreements with ln_positive_final'),
  ('ln positive count', 'ln_positive_final',   'THY-89',    'contested', 'derived final value; 38 impossible rows (LN01 positive>examined, LN02 positive w/o examined)'),
  ('histology',         'histology_final',     'HIST01-03', 'watch',     'whitespace / unnormalized PTC variants / metastatic-prefix QC rules apply'),
  ('recurrence',        'any_recurrence_flag', 'REC01-03',  'watch',     'recurrence flag/date mismatch QC rules apply'),
  ('recurrence',        'recurrence_date',     'REC01-03',  'watch',     'recurrence flag/date mismatch QC rules apply')
]);

-- 2. Pull the cohort's column list (dynamic so it works for any manuscript cohort table).
EXECUTE IMMEDIATE FORMAT("""
  CREATE TEMP TABLE cohort_columns AS
  SELECT column_name, data_type
  FROM `thyroid-canonical-pub-2026.%s.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = '%s'
""", cohort_dataset, cohort_table);

-- 3. DETERMINISTIC ASSESSMENT (reliable core) — every column, flagged against the register.
--    Contested/watch columns sort to the top.
SELECT
  cc.column_name,
  cc.data_type,
  r.concept,
  r.linear_issue,
  COALESCE(r.status, 'ok')                       AS source_of_truth_status,
  r.note,
  IF(r.status = 'contested',
     'BLOCK: a canonical-source decision is open for this column. Do not lock manuscript numbers on it without a methods caveat. See ' || r.linear_issue,
     IF(r.status = 'watch',
        'CHECK: known QC rules touch this column; verify in the cohort-scoped QC step.',
        'ok')) AS build_guidance
FROM cohort_columns cc
LEFT JOIN competing_source_register r USING (column_name)
ORDER BY (r.status IS NULL), r.status, cc.column_name;

-- 4. GEMINI ASSESSMENT (optional) — plain-language advisory for the flagged columns only.
--    Requires a Vertex AI connection (set vertex_connection above). If no connection is
--    configured this statement errors; the deterministic assessment above is unaffected.
EXECUTE IMMEDIATE FORMAT("""
  SELECT
    f.column_name,
    f.concept,
    f.linear_issue,
    AI.GENERATE(
      CONCAT(
        'A thyroid-research manuscript is being built from the BigQuery database and uses the ',
        'column "', f.column_name, '" (clinical concept: ', f.concept, '). This column is a ',
        'competing source of truth tracked in Linear ', f.linear_issue, '. Context: ', f.note,
        '. In 2-3 sentences, advise the manuscript author: what to verify before locking any ',
        'numbers that depend on this column, and whether the methods section needs a caveat.'),
      connection_id => '%s'
    ).result AS gemini_assessment
  FROM (
    SELECT cc.column_name, r.concept, r.linear_issue, r.note
    FROM cohort_columns cc
    JOIN competing_source_register r USING (column_name)
  ) f
""", vertex_connection);
