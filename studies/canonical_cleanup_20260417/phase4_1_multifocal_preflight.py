"""Phase 4.1 multifocal pre-flight COUNT (read-only, per Logan's SQL).

Per-surgery grain (research_id, surg_date) for STL counts. Patient is
'single-tumor' for this cleanup ONLY if ALL their surgeries show exactly one
tumor in synoptic_tumor_long_v1. STOPS before UPDATE.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts"))

from _md_connect import connect_locked  # type: ignore

OUT = Path(__file__).resolve().parent / "phase4_1_multifocal_preflight.json"


def main() -> int:
    con = connect_locked()
    out: dict = {}

    # Schema sanity check
    cols = {
        r[0] for r in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_catalog='thyroid_canonical_publication_v1_0' "
            "AND table_schema='main' AND table_name='canonical_patient_master'"
        ).fetchall()
    }
    needed = ["multifocal_flag_path", "nlp_path_multifocal_mentioned"]
    out["columns_needed"] = {c: (c in cols) for c in needed}
    deprecated = "DEPRECATED__path_multifocal_flag"
    out["deprecated_col_present"] = (deprecated in cols)

    # Logan's per-(rid, surg_date) SQL.
    # synoptic_tumor_long_v1.research_id is BIGINT; cpm.research_id is VARCHAR
    sql = """
        WITH stl_counts AS (
          SELECT CAST(research_id AS VARCHAR) AS research_id,
                 surg_date,
                 COUNT(*) AS n_tumors
          FROM main.synoptic_tumor_long_v1
          WHERE research_id IS NOT NULL
          GROUP BY 1, 2
        ),
        per_rid_max AS (
          -- A patient is 'single-tumor' only if EVERY surgery has exactly 1 tumor.
          SELECT research_id,
                 MAX(n_tumors) AS max_tumors_any_surgery,
                 MIN(n_tumors) AS min_tumors_any_surgery,
                 COUNT(*)      AS n_surgeries
          FROM stl_counts
          GROUP BY 1
        ),
        single_tumor_rids AS (
          SELECT research_id
          FROM per_rid_max
          WHERE max_tumors_any_surgery = 1
        )
        SELECT
          COUNT(*) FILTER (
            WHERE cpm.multifocal_flag_path IS TRUE
              AND cpm.research_id IN (SELECT research_id FROM single_tumor_rids)
              AND (cpm.nlp_path_multifocal_mentioned IS NULL
                   OR cpm.nlp_path_multifocal_mentioned IS NOT TRUE)
          ) AS downgrade_candidates,
          COUNT(*) FILTER (
            WHERE cpm.multifocal_flag_path IS TRUE
              AND cpm.nlp_path_multifocal_mentioned IS TRUE
          ) AS preserved_nlp_supported,
          COUNT(*) FILTER (
            WHERE cpm.multifocal_flag_path IS TRUE
          ) AS total_multifocal_flag_path_TRUE
        FROM main.canonical_patient_master cpm
    """
    row = con.execute(sql).fetchone()
    out["downgrade_candidates"] = row[0]
    out["preserved_nlp_supported"] = row[1]
    out["total_multifocal_flag_path_TRUE"] = row[2]

    # Distribution of patients by max_tumors_any_surgery (sanity-check the spine)
    out["per_rid_max_tumor_distribution"] = con.execute(
        """
        WITH stl_counts AS (
          SELECT CAST(research_id AS VARCHAR) AS research_id, surg_date,
                 COUNT(*) AS n_tumors
          FROM main.synoptic_tumor_long_v1
          WHERE research_id IS NOT NULL
          GROUP BY 1, 2
        ),
        per_rid_max AS (
          SELECT research_id, MAX(n_tumors) AS max_tumors
          FROM stl_counts GROUP BY 1
        )
        SELECT max_tumors, COUNT(*) AS n_patients
        FROM per_rid_max GROUP BY 1 ORDER BY 1
        """
    ).fetchall()

    # Sample 10 downgrade candidates (so Logan can spot-check)
    out["downgrade_candidate_sample"] = con.execute(
        """
        WITH stl_counts AS (
          SELECT CAST(research_id AS VARCHAR) AS research_id, surg_date,
                 COUNT(*) AS n_tumors
          FROM main.synoptic_tumor_long_v1 WHERE research_id IS NOT NULL
          GROUP BY 1, 2
        ),
        per_rid_max AS (
          SELECT research_id, MAX(n_tumors) AS max_tumors
          FROM stl_counts GROUP BY 1
        ),
        single_tumor_rids AS (
          SELECT research_id FROM per_rid_max WHERE max_tumors = 1
        )
        SELECT cpm.research_id,
               cpm.multifocal_flag_path,
               cpm.nlp_path_multifocal_mentioned,
               (SELECT COUNT(*) FROM stl_counts s
                WHERE s.research_id = cpm.research_id) AS n_surgeries
        FROM main.canonical_patient_master cpm
        WHERE cpm.multifocal_flag_path IS TRUE
          AND cpm.research_id IN (SELECT research_id FROM single_tumor_rids)
          AND (cpm.nlp_path_multifocal_mentioned IS NULL
               OR cpm.nlp_path_multifocal_mentioned IS NOT TRUE)
        LIMIT 10
        """
    ).fetchall()

    OUT.write_text(json.dumps(out, indent=2, default=str))
    print(json.dumps(out, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
