"""Read-only schema recon of all Phase 1 source tables.

Captures: column names + dtypes for the tables Phase 1 reads from / writes to,
and small probe queries for source-of-truth precedence values.

Output: studies/canonical_cleanup_20260417/schema_recon.json
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
sys.path.insert(0, str(REPO / "scripts"))

from _md_connect import connect_locked  # type: ignore

OUT = HERE / "schema_recon.json"


TABLES = [
    "complication_phenotype_v1",
    "operative_episode_detail_v2",
    "fna_episode_master_v2",
    "rai_treatment_episode_v2",
    "canonical_us_nodule_characteristics_v1",
    "thyroglobulin_lab_canonical_v1",
    "synoptic_tumor_long_v1",
    "imaging_nodule_master_v1",
    "vc_paralysis_recalibration_v236",  # in manuscript_workspace
]


def main() -> int:
    con = connect_locked()
    out: dict = {"columns": {}, "probes": {}}

    for t in TABLES:
        if t == "vc_paralysis_recalibration_v236":
            schema = "manuscript_workspace"
        else:
            schema = "main"
        rows = con.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_catalog='thyroid_canonical_publication_v1_0' "
            f"AND table_schema='{schema}' AND table_name='{t}' "
            "ORDER BY ordinal_position"
        ).fetchall()
        out["columns"][f"{schema}.{t}"] = [{"name": r[0], "type": r[1]} for r in rows]

    # Probe Phase 1.1 — count of VC s236 candidates
    out["probes"]["phase_1_1_vc_s236_candidates"] = {}
    try:
        rows = con.execute(
            """
            SELECT complication_entity, COUNT(*) AS n,
                   COUNT(*) FILTER (WHERE confirmed_flag IS TRUE) AS already_confirmed,
                   COUNT(*) FILTER (WHERE confirmed_flag IS FALSE OR confirmed_flag IS NULL)
                       AS to_promote
            FROM main.complication_phenotype_v1
            WHERE complication_entity IN ('vocal_cord_paralysis','vocal_cord_paresis')
              AND status_v2 = 'confirmed_from_rln_crossref'
            GROUP BY 1 ORDER BY 1
            """
        ).fetchall()
        out["probes"]["phase_1_1_vc_s236_candidates"] = [
            {"entity": r[0], "n_total": r[1], "already_confirmed": r[2], "to_promote": r[3]}
            for r in rows
        ]
    except Exception as e:  # noqa: BLE001
        out["probes"]["phase_1_1_vc_s236_candidates"] = f"ERR: {e}"

    # Probe Phase 1.3 — lateral ND structured TRUE counts
    try:
        out["probes"]["phase_1_3_lateral_nd"] = {
            "oed_lateral_TRUE_distinct_rids": con.execute(
                "SELECT COUNT(DISTINCT research_id) FROM main.operative_episode_detail_v2 "
                "WHERE lateral_neck_dissection_flag IS TRUE"
            ).fetchone()[0],
            "cpm_current_lateral_TRUE": con.execute(
                "SELECT COUNT(*) FROM main.canonical_patient_master "
                "WHERE lateral_neck_dissected IS TRUE"
            ).fetchone()[0],
        }
        # NLP-only positives (CPM TRUE but oed FALSE/NULL): count
        out["probes"]["phase_1_3_lateral_nd"]["cpm_TRUE_oed_not_TRUE_distinct"] = con.execute(
            """
            WITH oed AS (
              SELECT research_id, BOOL_OR(lateral_neck_dissection_flag) AS f
              FROM main.operative_episode_detail_v2 GROUP BY 1
            )
            SELECT COUNT(*)
            FROM main.canonical_patient_master cpm
            LEFT JOIN oed USING(research_id)
            WHERE cpm.lateral_neck_dissected IS TRUE
              AND (oed.f IS NULL OR oed.f IS NOT TRUE)
            """
        ).fetchone()[0]
    except Exception as e:  # noqa: BLE001
        out["probes"]["phase_1_3_lateral_nd"] = f"ERR: {e}"

    # Probe Phase 1.4 — TIRADS columns sample, current max_tirads_ever distribution
    try:
        out["probes"]["phase_1_4_tirads"] = {
            "us_rows_with_either_tirads_present": con.execute(
                "SELECT COUNT(*) FROM main.canonical_us_nodule_characteristics_v1 "
                "WHERE tirads_reported IS NOT NULL OR tirads_acr_recalculated IS NOT NULL"
            ).fetchone()[0],
            "max_tirads_ever_current_dist": con.execute(
                "SELECT max_tirads_ever, COUNT(*) "
                "FROM main.canonical_patient_master GROUP BY 1 ORDER BY 1"
            ).fetchall(),
        }
    except Exception as e:  # noqa: BLE001
        out["probes"]["phase_1_4_tirads"] = f"ERR: {e}"

    # Probe Phase 1.5 — RAI dose
    try:
        out["probes"]["phase_1_5_rai"] = {
            "rai_episodes_with_dose": con.execute(
                "SELECT COUNT(*) FROM main.rai_treatment_episode_v2 WHERE dose_mci IS NOT NULL"
            ).fetchone()[0],
            "cpm_rai_max_dose_zero_or_null": con.execute(
                "SELECT COUNT(*) FROM main.canonical_patient_master "
                "WHERE rai_max_dose_mci = 0 OR rai_max_dose_mci IS NULL"
            ).fetchone()[0],
        }
    except Exception as e:  # noqa: BLE001
        out["probes"]["phase_1_5_rai"] = f"ERR: {e}"

    # Probe Phase 1.8 — distinct complication_entity values in phenotype_v1
    try:
        out["probes"]["phase_1_8_complication_entities"] = con.execute(
            "SELECT complication_entity, COUNT(*) AS n, "
            "COUNT(*) FILTER (WHERE confirmed_flag IS TRUE) AS confirmed_n "
            "FROM main.complication_phenotype_v1 GROUP BY 1 ORDER BY 1"
        ).fetchall()
    except Exception as e:  # noqa: BLE001
        out["probes"]["phase_1_8_complication_entities"] = f"ERR: {e}"

    OUT.write_text(json.dumps(out, indent=2, default=str))
    print(f"Wrote {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
