"""Phase 2 hypopara probe (read-only)."""
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts"))

from _md_connect import connect_locked  # type: ignore

OUT = Path(__file__).resolve().parent / "phase2_probe.json"


def main() -> int:
    con = connect_locked()
    out: dict = {}

    # Required CPM columns exist?
    cpm_cols = {
        r[0] for r in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_catalog='thyroid_canonical_publication_v1_0' "
            "AND table_schema='main' AND table_name='canonical_patient_master'"
        ).fetchall()
    }
    out["columns_present"] = {
        c: (c in cpm_cols) for c in (
            "comp_hypoparathyroidism_permanent",
            "comp_hypopara_permanent_limitation_note",
            "comp_hypopara_permanent_source",
            "prm_hypoparathyroidism_lab_flag",
        )
    }
    # Find any column with 'hypopara' in the name
    out["all_hypopara_cols"] = sorted(c for c in cpm_cols if "hypopara" in c.lower())
    out["all_lab_flag_cols"] = sorted(
        c for c in cpm_cols
        if c.lower().endswith("_lab_flag") and "hypo" in c.lower()
    )

    # Phase 2.1 candidates: CPM permanent=TRUE AND phenotype final_complication_status='confirmed_duration_unknown'
    if "comp_hypoparathyroidism_permanent" in cpm_cols:
        out["phase_2_1_candidates"] = con.execute(
            """
            SELECT cpm.research_id, cpm.comp_hypoparathyroidism_permanent,
                   p.final_complication_status, p.permanent_flag, p.transient_flag
            FROM main.canonical_patient_master cpm
            JOIN (
              SELECT CAST(research_id AS VARCHAR) AS research_id,
                     final_complication_status, permanent_flag, transient_flag
              FROM main.complication_phenotype_v1
              WHERE complication_entity = 'hypoparathyroidism'
            ) p USING (research_id)
            WHERE cpm.comp_hypoparathyroidism_permanent IS TRUE
              AND p.final_complication_status = 'confirmed_duration_unknown'
            """
        ).fetchall()

        # Phase 2.2 contradictions
        out["phase_2_2_contradictions"] = con.execute(
            """
            SELECT cpm.research_id, cpm.comp_hypoparathyroidism_permanent,
                   p.final_complication_status, p.permanent_flag, p.transient_flag
            FROM main.canonical_patient_master cpm
            JOIN (
              SELECT CAST(research_id AS VARCHAR) AS research_id,
                     final_complication_status, permanent_flag, transient_flag
              FROM main.complication_phenotype_v1
              WHERE complication_entity = 'hypoparathyroidism'
            ) p USING (research_id)
            WHERE cpm.comp_hypoparathyroidism_permanent IS TRUE
              AND p.final_complication_status = 'confirmed_transient'
            """
        ).fetchall()

        # All other final_complication_status distributions for hypopara where CPM permanent=TRUE
        out["phase_2_status_dist"] = con.execute(
            """
            SELECT p.final_complication_status, COUNT(*) AS n
            FROM main.canonical_patient_master cpm
            LEFT JOIN (
              SELECT CAST(research_id AS VARCHAR) AS research_id,
                     final_complication_status
              FROM main.complication_phenotype_v1
              WHERE complication_entity = 'hypoparathyroidism'
            ) p USING (research_id)
            WHERE cpm.comp_hypoparathyroidism_permanent IS TRUE
            GROUP BY 1 ORDER BY 2 DESC
            """
        ).fetchall()

    OUT.write_text(json.dumps(out, indent=2, default=str))
    print(json.dumps(out, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
