"""Prompt 9: NLP Tier-1 flag coverage + cross-validation vs Tier-2 canonicals.

Snowflake-side mirror of the MD-direct validation done on 2026-05-01.
Runs against CANONICAL_PATIENT_MASTER_FLAT + the canonical detail FLAT views.

Note: when Logan re-exports MD→Snowflake, the live publication-DB CPM has
only 5 NLP TIRADS columns (vs 28+ in current Snowflake mirror). Several CPM
columns referenced in this script may be missing post-re-export. See
reports/10_imaging_validation.md §6 for migration paths.
"""
import sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import get_cursor, md_table

OUT = Path("/Users/ros/THyroid 2026/snowflake_trial/reports/09_nlp_validation.md")

ctx, cur = get_cursor()
report = ["# Snowflake Cortex Validation — Prompt 9: NLP Tier-1 Flag Coverage (Snowflake re-run)\n",
          f"**Generated:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n",
          "**Source:** CANONICAL_PATIENT_MASTER_FLAT + canonical detail FLAT views\n\n---\n"]

# 1. Domain coverage
print("=== NLP domain coverage ===")
flags = ['nlp_synoptic_has_data','nlp_ne_procedures_has_data','nlp_ne_problemlist_has_data',
         'nlp_ne_operative_has_data','nlp_parathyroid_has_data','nlp_path_has_data',
         'nlp_survfu_has_data','nlp_frozensec_has_data','nlp_ne_complications_has_data',
         'nlp_ne_medications_has_data','nlp_pshx_has_data','nlp_imaging_has_data',
         'nlp_tirads_has_data','nlp_cervln_has_data','nlp_ne_staging_has_data',
         'nlp_airway_has_data','nlp_funcoutcome_has_data','nlp_ln_has_data',
         'nlp_labs_has_data','nlp_vasc_has_data','nlp_raidetail_has_data',
         'nlp_ne_genetics_has_data','nlp_physexam_has_data','nlp_ptdecision_has_data',
         'nlp_pmhx_has_data','nlp_radtx_has_data','nlp_rec_has_data',
         'nlp_symptoms_has_data','nlp_esoph_has_data','nlp_tg_has_data',
         'nlp_dynrisk_has_data','nlp_usnodule_has_data']
counts_sql = " UNION ALL ".join(
    f"SELECT '{f}' AS flag, COUNT_IF({f}) AS n FROM CANONICAL_PATIENT_MASTER_FLAT" for f in flags)
cur.execute(f"""
WITH counts AS ({counts_sql}),
     coh AS (SELECT COUNT(*) AS n FROM CANONICAL_PATIENT_MASTER_FLAT)
SELECT c.flag, c.n, ROUND(100.0*c.n/coh.n, 1) AS pct
FROM counts c CROSS JOIN coh ORDER BY c.n DESC
""")
rows = cur.fetchall(); cols = [d[0] for d in cur.description]
report.append("## NLP domain coverage\n\n" + md_table(rows, cols) + "\n")

# 2. NLP flag vs canonical detail (representative subset; expand as needed)
print("=== NLP vs canonical cross-validation ===")
cur.execute("""
WITH cpm AS (SELECT research_id, nlp_path_has_data, nlp_pmhx_has_data, nlp_pshx_has_data,
                    nlp_imaging_has_data, nlp_synoptic_has_data, nlp_ne_complications_has_data,
                    nlp_parathyroid_has_data, nlp_frozensec_has_data, nlp_ne_genetics_has_data
             FROM CANONICAL_PATIENT_MASTER_FLAT)
SELECT 'pmhx' AS domain,
  (SELECT COUNT(*) FROM cpm WHERE nlp_pmhx_has_data) AS nlp_n,
  -- canonical PMH detail FLAT view loaded separately; placeholder
  0 AS canon_n,
  0 AS overlap_n
""")
report.append("## NLP flag vs canonical (placeholder; expand once detail FLAT views land)\n\n")

# 3. Confidence tier distribution
cur.execute("""
SELECT 'nlp_path' AS domain, nlp_path_confidence_tier AS tier, COUNT(*) AS n FROM CANONICAL_PATIENT_MASTER_FLAT GROUP BY 2
UNION ALL SELECT 'nlp_cervln', nlp_cervln_confidence_tier, COUNT(*) FROM CANONICAL_PATIENT_MASTER_FLAT GROUP BY 2
UNION ALL SELECT 'nlp_esoph', nlp_esoph_confidence_tier, COUNT(*) FROM CANONICAL_PATIENT_MASTER_FLAT GROUP BY 2
UNION ALL SELECT 'nlp_vasc', nlp_vasc_confidence_tier, COUNT(*) FROM CANONICAL_PATIENT_MASTER_FLAT GROUP BY 2
UNION ALL SELECT 'nlp_rec', nlp_rec_confidence_tier, COUNT(*) FROM CANONICAL_PATIENT_MASTER_FLAT GROUP BY 2
""")
rows = cur.fetchall(); cols = [d[0] for d in cur.description]
report.append("## Confidence tier distribution\n\n" + md_table(rows, cols) + "\n")

# 4. Recurrence sanity
cur.execute("""
SELECT
  COUNT(*) AS n_total,
  COUNT_IF(nlp_rec_any_mentioned) AS nlp_rec_any,
  COUNT_IF(recurrence_flag_v2) AS recur_flag_n,
  COUNT_IF(nlp_rec_any_mentioned AND recurrence_flag_v2) AS both_n,
  COUNT_IF(nlp_rec_any_mentioned AND NOT COALESCE(recurrence_flag_v2,FALSE)) AS nlp_only,
  COUNT_IF(NOT COALESCE(nlp_rec_any_mentioned,FALSE) AND recurrence_flag_v2) AS canon_only,
  COUNT_IF(nlp_rec_earliest_days_from_surg < 0) AS nlp_rec_pre_surgery
FROM CANONICAL_PATIENT_MASTER_FLAT
""")
rows = cur.fetchall(); cols = [d[0] for d in cur.description]
report.append("## NLP recurrence vs canonical recurrence_flag_v2\n\n" + md_table(rows, cols) + "\n")

OUT.write_text("".join(report))
print(f"\n[saved] {OUT}  ({OUT.stat().st_size:,} bytes)")
ctx.close()
