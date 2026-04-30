"""Fix and run Template 04 — complication rate by surgery type.
Column mapping: timing_days → computed from finding_date - first_surgery_date; onset_class used as fallback window.
"""
import toml, duckdb, csv
from pathlib import Path

d = toml.load("motherduck.local.toml")
tok = d["MD_SA_TOKEN"]
con = duckdb.connect(f"md:thyroid_canonical_publication_v1_0?motherduck_token={tok}")
con.execute("USE thyroid_canonical_publication_v1_0")

SQL04 = """
WITH
malignant_pts AS (
  SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id
  FROM main.canonical_path_malignant_events_v1
),
indeterminate_only_rid AS (
  SELECT DISTINCT CAST(i.research_id AS VARCHAR) AS research_id
  FROM main.canonical_path_indeterminate_events_v1 AS i
  WHERE NOT EXISTS (
    SELECT 1 FROM main.canonical_path_malignant_events_v1 AS m
    WHERE CAST(m.research_id AS VARCHAR) = CAST(i.research_id AS VARCHAR)
  )
),
step1_pool AS (
  SELECT CAST(research_id AS VARCHAR) AS research_id
  FROM main.canonical_patient_master WHERE is_malignant IS TRUE
),
step2_pool AS (
  SELECT p.research_id FROM step1_pool p
  WHERE NOT EXISTS (SELECT 1 FROM indeterminate_only_rid i WHERE i.research_id = p.research_id)
),
step3_pool AS (
  SELECT p.research_id FROM step2_pool p
  INNER JOIN main.canonical_patient_master c ON p.research_id = CAST(c.research_id AS VARCHAR)
  WHERE NOT (c.histology_final IS NULL AND c.ajcc8_t_stage_resolved IS NULL)
),
step4_pool AS (
  SELECT p.research_id FROM step3_pool p
  INNER JOIN main.canonical_patient_master c ON p.research_id = CAST(c.research_id AS VARCHAR)
  WHERE c.last_contact_date IS NOT NULL
),
analytic_rid AS (
  SELECT DISTINCT p.research_id FROM step4_pool p
  INNER JOIN malignant_pts m ON m.research_id = p.research_id
),
patient_surgery AS (
  SELECT
    CAST(c.research_id AS VARCHAR) AS research_id,
    CAST(c.first_surgery_date AS DATE) AS first_surgery_date,
    CASE
      WHEN COALESCE(c.surg_total_thyroidectomy, FALSE) IS TRUE THEN 'total_thyroidectomy'
      WHEN COALESCE(c.surg_hemithyroidectomy, FALSE) IS TRUE THEN 'hemithyroidectomy'
      ELSE 'other_or_unknown_primary_procedure'
    END || ' x ' ||
    CASE
      WHEN (COALESCE(c.lateral_neck_dissected_structured_or_nlp, FALSE) IS TRUE
            OR COALESCE(c.lateral_neck_dissected, FALSE) IS TRUE
            OR COALESCE(c.cnln_img_lateral_neck_present, FALSE) IS TRUE)
           AND (COALESCE(c.cnln_img_central_present, FALSE) IS TRUE
                OR COALESCE(c.ln_rollup_central_examined, 0) > 0)
        THEN 'ND_both'
      WHEN COALESCE(c.lateral_neck_dissected_structured_or_nlp, FALSE) IS TRUE
        OR COALESCE(c.lateral_neck_dissected, FALSE) IS TRUE
        OR COALESCE(c.cnln_img_lateral_neck_present, FALSE) IS TRUE
        THEN 'ND_lateral'
      WHEN COALESCE(c.cnln_img_central_present, FALSE) IS TRUE
        OR COALESCE(c.ln_rollup_central_examined, 0) > 0
        THEN 'ND_central'
      ELSE 'ND_none_signal'
    END AS surgery_type_label
  FROM main.canonical_patient_master c
  INNER JOIN analytic_rid ar ON ar.research_id = CAST(c.research_id AS VARCHAR)
),
comp_long AS (
  SELECT
    CAST(ce.research_id AS VARCHAR) AS research_id,
    ce.complication_type,
    ce.finding_date,
    ce.onset_class,
    ce.finding_status,
    DATE_DIFF('day', ps.first_surgery_date, CAST(ce.finding_date AS DATE)) AS timing_days_computed,
    CASE ce.complication_type
      WHEN 'hypocalcemia_clinical' THEN 'hypocalcemia'
      WHEN 'hypoparathyroidism'    THEN 'hypoparathyroidism'
      WHEN 'vocal_cord_paralysis'  THEN 'vocal_cord_palsy'
      WHEN 'rln_injury'            THEN 'vocal_cord_palsy'
      WHEN 'chyle_leak'            THEN 'chyle_leak'
      WHEN 'hematoma'              THEN 'hematoma'
      WHEN 'mortality'             THEN 'mortality'
      ELSE NULL
    END AS complication_category,
    CASE
      WHEN ce.complication_type IN ('hypoparathyroidism', 'mortality') THEN 'any_time_followup'
      WHEN ce.finding_date IS NOT NULL AND ps.first_surgery_date IS NOT NULL
           AND DATE_DIFF('day', ps.first_surgery_date, CAST(ce.finding_date AS DATE)) BETWEEN 0 AND 30
        THEN 'acute_0_30d'
      WHEN LOWER(COALESCE(ce.onset_class, '')) IN ('acute', 'perioperative', 'immediate')
        THEN 'acute_0_30d'
      ELSE 'outside_acute_window'
    END AS analysis_window
  FROM main.canonical_complications_events_v1 ce
  INNER JOIN analytic_rid ar ON ar.research_id = CAST(ce.research_id AS VARCHAR)
  LEFT JOIN patient_surgery ps ON ps.research_id = CAST(ce.research_id AS VARCHAR)
  WHERE LOWER(TRIM(COALESCE(ce.finding_status, ''))) = 'present'
),
comp_flag AS (
  SELECT DISTINCT research_id, complication_category, analysis_window
  FROM comp_long
  WHERE complication_category IS NOT NULL
    AND (analysis_window = 'any_time_followup' OR analysis_window = 'acute_0_30d')
),
cats AS (
  SELECT * FROM (VALUES
    ('hypocalcemia'), ('hypoparathyroidism'), ('vocal_cord_palsy'),
    ('chyle_leak'), ('hematoma'), ('mortality')
  ) AS v(complication_category)
),
sx    AS (SELECT DISTINCT surgery_type_label FROM patient_surgery),
grid  AS (SELECT sx.surgery_type_label, cats.complication_category FROM sx CROSS JOIN cats),
denom AS (SELECT surgery_type_label, COUNT(*)::BIGINT AS denom_n FROM patient_surgery GROUP BY 1),
numer AS (
  SELECT ps.surgery_type_label, cf.complication_category,
         COUNT(DISTINCT ps.research_id)::BIGINT AS numer_n
  FROM patient_surgery ps
  INNER JOIN comp_flag cf ON cf.research_id = ps.research_id
  WHERE (cf.complication_category IN ('hypoparathyroidism', 'mortality') AND cf.analysis_window = 'any_time_followup')
     OR (cf.complication_category NOT IN ('hypoparathyroidism', 'mortality') AND cf.analysis_window = 'acute_0_30d')
  GROUP BY 1, 2
)
SELECT
  g.surgery_type_label,
  g.complication_category,
  CASE WHEN g.complication_category IN ('hypoparathyroidism', 'mortality') THEN 'any_time_followup'
       ELSE 'acute_0_30d' END AS analysis_window,
  COALESCE(n.numer_n, 0)::BIGINT AS n_with_complication,
  d.denom_n AS n_patients_in_surgery_bucket,
  ROUND(COALESCE(n.numer_n, 0)::DOUBLE / NULLIF(d.denom_n, 0), 5) AS proportion,
  ROUND(GREATEST(
    COALESCE(n.numer_n, 0)::DOUBLE / NULLIF(d.denom_n, 0)
    - 1.96 * SQRT((COALESCE(n.numer_n, 0)::DOUBLE / NULLIF(d.denom_n, 0))
      * (1.0 - COALESCE(n.numer_n, 0)::DOUBLE / NULLIF(d.denom_n, 0)) / NULLIF(d.denom_n, 0)),
    0.0), 5) AS ci95_lower_wald,
  ROUND(LEAST(
    COALESCE(n.numer_n, 0)::DOUBLE / NULLIF(d.denom_n, 0)
    + 1.96 * SQRT((COALESCE(n.numer_n, 0)::DOUBLE / NULLIF(d.denom_n, 0))
      * (1.0 - COALESCE(n.numer_n, 0)::DOUBLE / NULLIF(d.denom_n, 0)) / NULLIF(d.denom_n, 0)),
    1.0), 5) AS ci95_upper_wald
FROM grid g
INNER JOIN denom d ON d.surgery_type_label = g.surgery_type_label
LEFT JOIN numer n
  ON n.surgery_type_label = g.surgery_type_label
 AND n.complication_category = g.complication_category
ORDER BY g.surgery_type_label, g.complication_category
"""

print("Running Template 04 (fixed)...")
rel = con.execute(SQL04)
rows = rel.fetchall()
cols = [c[0] for c in rel.description]
print(f"Template 04: {len(rows)} rows")

out = Path("qc_framework_v1/manuscript/analytic_templates/previews/04_complication_rate_by_surgery_type_preview.csv")
out.parent.mkdir(parents=True, exist_ok=True)
with open(out, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=cols)
    w.writeheader()
    w.writerows([dict(zip(cols, r)) for r in rows])
print(f"Written: {out}")
for r in rows[:4]:
    print(dict(zip(cols, r)))

con.close()
print("Done.")
