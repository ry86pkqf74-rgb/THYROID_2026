"""Read-only sizing probe for Phase 1 UPDATE candidates."""
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts"))

from _md_connect import connect_locked  # type: ignore

HERE = Path(__file__).resolve().parent
OUT = HERE / "phase1_dryrun_probe.json"


def main() -> int:
    con = connect_locked()
    out: dict = {}

    # 1.5a: episode-driven candidates
    out["1_5a_episode_candidates"] = con.execute(
        """
        WITH ep AS (
          SELECT research_id, MAX(dose_mci) AS max_dose
          FROM main.rai_treatment_episode_v2
          GROUP BY research_id
        )
        SELECT COUNT(*)
        FROM main.canonical_patient_master cpm
        LEFT JOIN ep USING(research_id)
        WHERE (cpm.rai_max_dose_mci = 0 OR cpm.rai_max_dose_mci IS NULL)
          AND COALESCE(ep.max_dose, cpm.rai_dose_v9) > 0
        """
    ).fetchone()[0]

    # 1.5b: rai_dose_v9-only fallback candidates (no episode match)
    out["1_5b_v9_only_candidates"] = con.execute(
        """
        SELECT COUNT(*) FROM main.canonical_patient_master cpm
        LEFT JOIN (
          SELECT DISTINCT research_id FROM main.rai_treatment_episode_v2
          WHERE dose_mci IS NOT NULL
        ) ep USING(research_id)
        WHERE ep.research_id IS NULL
          AND (cpm.rai_max_dose_mci = 0 OR cpm.rai_max_dose_mci IS NULL)
          AND cpm.rai_dose_v9 > 0
        """
    ).fetchone()[0]

    # 1.5: episode-distinct rids in CPM
    out["1_5_episode_distinct_rids"] = con.execute(
        "SELECT COUNT(DISTINCT research_id) FROM main.rai_treatment_episode_v2 "
        "WHERE dose_mci IS NOT NULL"
    ).fetchone()[0]

    # 1.4 dry-run candidates (TR promotion)
    out["1_4_promotion_candidates"] = con.execute(
        """
        WITH per_rid AS (
          SELECT research_id,
                 GREATEST(MAX(tirads_reported), MAX(tirads_acr_recalculated)) AS new_max
          FROM main.canonical_us_nodule_characteristics_v1
          WHERE tirads_reported IS NOT NULL OR tirads_acr_recalculated IS NOT NULL
          GROUP BY research_id
        )
        SELECT COUNT(*)
        FROM main.canonical_patient_master cpm
        JOIN per_rid USING(research_id)
        WHERE per_rid.new_max IS NOT NULL
          AND (cpm.max_tirads_ever IS NULL OR per_rid.new_max > cpm.max_tirads_ever)
        """
    ).fetchone()[0]

    # 1.3 dry-run candidates (lateral ND OR-in)
    out["1_3_lateral_orin_candidates"] = con.execute(
        """
        WITH oed AS (
          SELECT research_id, BOOL_OR(lateral_neck_dissection_flag) AS f
          FROM main.operative_episode_detail_v2
          GROUP BY research_id
        )
        SELECT COUNT(*)
        FROM main.canonical_patient_master cpm
        JOIN oed USING(research_id)
        WHERE oed.f IS TRUE
          AND (cpm.lateral_neck_dissected IS NULL
               OR cpm.lateral_neck_dissected IS NOT TRUE)
        """
    ).fetchone()[0]

    # 1.6 distinct rids in Tg lab
    out["1_6_tg_distinct_rids"] = con.execute(
        "SELECT COUNT(DISTINCT research_id) FROM main.thyroglobulin_lab_canonical_v1"
    ).fetchone()[0]

    # 1.6 expected new TG counts/peak/nadir using classifier
    classifier = (
        "CASE WHEN LOWER(analyte) LIKE '%antibod%' OR LOWER(analyte) LIKE 'tgab%' "
        "THEN 'TGAB' WHEN LOWER(analyte) LIKE 'thyroglobulin%' OR LOWER(analyte) = 'tg' "
        "THEN 'TG' ELSE 'OTHER' END"
    )
    out["1_6_classifier_dist"] = con.execute(
        f"SELECT {classifier} AS cls, COUNT(*) FROM main.thyroglobulin_lab_canonical_v1 "
        "GROUP BY 1 ORDER BY 1"
    ).fetchall()

    # 1.6 expected number of patients newly with values
    out["1_6_n_with_tg_pre"] = con.execute(
        "SELECT COUNT(*) FROM main.canonical_patient_master "
        "WHERE n_tg_measurements_structured > 0"
    ).fetchone()[0]
    out["1_6_n_with_tg_after"] = con.execute(
        f"""
        SELECT COUNT(DISTINCT research_id)
        FROM main.thyroglobulin_lab_canonical_v1
        WHERE {classifier} = 'TG'
        """
    ).fetchone()[0]
    out["1_6_n_with_tgab_pre"] = con.execute(
        "SELECT COUNT(*) FROM main.canonical_patient_master WHERE n_tgab_measurements > 0"
    ).fetchone()[0]
    out["1_6_n_with_tgab_after"] = con.execute(
        f"""
        SELECT COUNT(DISTINCT research_id)
        FROM main.thyroglobulin_lab_canonical_v1
        WHERE {classifier} = 'TGAB'
        """
    ).fetchone()[0]
    out["1_6_n_with_peak_pre"] = con.execute(
        "SELECT COUNT(*) FROM main.canonical_patient_master WHERE tg_peak IS NOT NULL"
    ).fetchone()[0]
    out["1_6_n_with_nadir_pre"] = con.execute(
        "SELECT COUNT(*) FROM main.canonical_patient_master WHERE tg_nadir IS NOT NULL"
    ).fetchone()[0]
    out["1_6_n_with_peak_after"] = con.execute(
        f"""
        SELECT COUNT(DISTINCT research_id)
        FROM main.thyroglobulin_lab_canonical_v1
        WHERE {classifier} = 'TG' AND result_numeric IS NOT NULL
        """
    ).fetchone()[0]
    # peak == nadir candidates: same count (rids with at least one numeric Tg)

    OUT.write_text(json.dumps(out, indent=2, default=str))
    print(json.dumps(out, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
