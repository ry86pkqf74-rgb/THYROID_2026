"""mig_280: repair cohort_m037 + cohort_m025 (DATE drift + stale CUPM column ref).

Companion SQL: qc_framework_v1/migrations/280_cohort_view_date_retype_20260503.sql
Logs: scripts/output/mig_280_apply_log.txt
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).resolve().parent))

from _md_connect import connect_locked  # noqa: E402


def main() -> int:
    log_lines: list[str] = []
    lg = log_lines.append

    con = connect_locked()

    lg("mig_280 start UTC (local connection time is server-side CURRENT_TIMESTAMP)")
    lg("s1 archive cohort_m037 snapshot")
    con.execute(
        """
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.view_def_cohort_m037_ln_metastasis_v1_pre_mig280_20260503 AS
SELECT
  database_name AS view_catalog,
  schema_name   AS view_schema,
  view_name,
  sql           AS view_definition,
  CURRENT_TIMESTAMP AS snapshot_at
FROM duckdb_views()
WHERE database_name = 'thyroid_canonical_publication_v1_0'
  AND schema_name = 'manuscript_workspace'
  AND view_name = 'cohort_m037_ln_metastasis_v1'
"""
    )

    lg("s1 archive cohort_m025 snapshot")
    con.execute(
        """
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.view_def_cohort_m025_tirads_performance_v1_pre_mig280_20260503 AS
SELECT
  database_name AS view_catalog,
  schema_name   AS view_schema,
  view_name,
  sql           AS view_definition,
  CURRENT_TIMESTAMP AS snapshot_at
FROM duckdb_views()
WHERE database_name = 'thyroid_canonical_publication_v1_0'
  AND schema_name = 'manuscript_workspace'
  AND view_name = 'cohort_m025_tirads_performance_v1'
"""
    )

    lg("s2 REPLACE cohort_m037_ln_metastasis_v1")
    con.execute(
        """
CREATE OR REPLACE VIEW manuscript_workspace.cohort_m037_ln_metastasis_v1 AS
SELECT
  p.research_id,
  p.age_at_surgery,
  p.sex,
  p.histology_final,
  p.is_malignant,
  p.path_tumor_size_cm AS tumor_size_cm,
  p.multifocal_flag_path,
  p.ete_grade_final,
  p.gross_ete_flag,
  p.ln_positive_flag,
  p.ln_positive_final,
  p.ln_total_examined,
  p.ln_total_positive,
  p.ln_ratio,
  p.ln_ene_status,
  p.ln_burden_band,
  p.ln_lateral_dissected,
  p.lateral_neck_dissected,
  p.ln_rollup_central_examined,
  p.ln_rollup_central_positive,
  p.ln_rollup_lateral_right_examined,
  p.ln_rollup_lateral_right_positive,
  p.ln_rollup_lateral_left_examined,
  p.ln_rollup_lateral_left_positive,
  p.ln_rollup_total_levels_involved,
  p.ln_level_i_positive,
  p.ln_level_ii_positive,
  p.ln_level_iii_positive,
  p.ln_level_iv_positive,
  p.ln_level_v_positive,
  p.ln_level_vi_positive,
  p.ajcc8_n_stage,
  p.ajcc8_stage_group,
  p.braf_positive_final,
  p.tert_positive_final,
  p.any_recurrence_flag,
  p.structural_recurrence_flag,
  p.followup_years,
  p.surg_procedure_type,
  p.surg_first_date
FROM main.canonical_patient_master AS p
WHERE ((p.is_malignant = CAST('t' AS BOOLEAN))
   AND ((p.ln_total_examined > 0)
    OR (p.ln_positive_flag = CAST('t' AS BOOLEAN))))
"""
    )

    lg("s2 REPLACE cohort_m025_tirads_performance_v1")
    con.execute(
        """
CREATE OR REPLACE VIEW manuscript_workspace.cohort_m025_tirads_performance_v1 AS
SELECT
  p.research_id,
  p.age_at_surgery,
  p.sex,
  p.race,
  cupm.tirads_category_at_last_preop_exam AS preop_tirads_category,
  cupm.tirads_category_at_first_exam AS tirads_best_category_v12,
  cupm.max_tirads_category_ever AS tirads_worst_category_v12,
  CAST(substr(cupm.tirads_category_at_first_exam, 3) AS BIGINT) AS tirads_best_score_v12,
  CAST(substr(cupm.max_tirads_category_ever, 3) AS BIGINT) AS tirads_worst_score_v12,
  CAST(NULL AS VARCHAR) AS tirads_worst_rank_source,
  cupm.n_us_exams AS n_us_exams,
  p.dominant_nodule_size_cm AS imaging_nodule_size_cm,
  p.dominant_nodule_size_cm,
  p.bethesda_final,
  p.bethesda_final_name,
  p.histology_final,
  p.is_malignant,
  p.path_tumor_size_cm AS tumor_size_cm,
  p.path_tumor_size_cm,
  p.fna_path_concordance_category,
  p.fna_path_concordant,
  p.surg_procedure_type,
  p.surg_first_date
FROM main.canonical_patient_master AS p
LEFT JOIN main.canonical_us_patient_master_VIEW_v2 AS cupm USING (research_id)
WHERE cupm.tirads_category_at_last_preop_exam IS NOT NULL
   OR cupm.tirads_category_at_first_exam IS NOT NULL
"""
    )

    lg("s3 verify LIMIT 1 + COUNT(*)")
    for v in ("cohort_m037_ln_metastasis_v1", "cohort_m025_tirads_performance_v1"):
        con.execute(f"SELECT * FROM manuscript_workspace.{v} LIMIT 1").fetchone()
        n = con.execute(f"SELECT COUNT(*) FROM manuscript_workspace.{v}").fetchone()[0]
        lg(f"  {v} count={n}")

    n37 = con.execute(
        "SELECT COUNT(*) FROM manuscript_workspace.cohort_m037_ln_metastasis_v1"
    ).fetchone()[0]
    n25 = con.execute(
        "SELECT COUNT(*) FROM manuscript_workspace.cohort_m025_tirads_performance_v1"
    ).fetchone()[0]

    summary = (
        "mig_280: CREATE OR REPLACE VIEW for manuscript_workspace.cohort_m037_ln_metastasis_v1 "
        "+ cohort_m025_tirads_performance_v1. Fixed catalog DATE drift (m037; post-mig_160b) "
        "and stale cupm.tirads_worst_rank_source ref (NULL placeholder). Post-state: resolves. "
        f"Cohort counts: M037 n={n37}, M025 n={n25}. "
        "Closes CF-mig277-COHORT-VIEW-BINDER + CF-mig160b-COHORT-VIEW-CASCADE."
    )

    lg("s4 signoff_migration INSERT")
    con.execute(
        """
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
VALUES (?, CAST(CURRENT_TIMESTAMP AS TIMESTAMP), ?, ?)
""",
        ["mig_280", "cursor_agent_mig280", summary],
    )

    lg("mig_280 COMPLETE")
    lg(summary)

    out = REPO_ROOT / "scripts/output/mig_280_apply_log.txt"
    out.write_text("\n".join(log_lines) + "\n", encoding="utf-8")
    print(f"Wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
