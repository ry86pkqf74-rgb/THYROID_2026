#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

import duckdb
from motherduck_client import get_token
from urllib.parse import quote_plus
import os

DEV = "Thyroid 2026 Molecular Dev 20260407"


def qi(s: str) -> str:
    return '"' + s.replace('"', '""') + '"'


def main() -> None:
    token = get_token(prefer_service_account=False)
    qs = f"motherduck_token={quote_plus(token)}"
    ua = os.getenv("MOTHERDUCK_CUSTOM_USER_AGENT", "THYROID_2026_agent_validate/1")
    con = duckdb.connect(f"md:?{qs}&custom_user_agent={quote_plus(ua)}")
    try:
        con.execute(f"USE {qi(DEV)}")
        stmts = [
            (
                "row_counts",
                """
                SELECT 'note_entities_genetics' AS slice, COUNT(*) AS n FROM main.note_entities_genetics
                UNION ALL SELECT 'molecular_results', COUNT(*) FROM main.molecular_results
                UNION ALL SELECT 'molecular_variant_long', COUNT(*) FROM main.molecular_variant_long
                UNION ALL SELECT 'molecular_assay_dictionary', COUNT(*) FROM main.molecular_assay_dictionary
                UNION ALL SELECT 'molecular_code_crosswalk', COUNT(*) FROM main.molecular_code_crosswalk
                """,
            ),
            (
                "genetics_verification_status",
                """
                SELECT verification_status, COUNT(*) AS n
                FROM main.note_entities_genetics
                GROUP BY 1 ORDER BY n DESC NULLS LAST
                """,
            ),
            (
                "assay_by_platform",
                """
                SELECT platform, COUNT(*) AS n
                FROM main.molecular_assay_dictionary
                GROUP BY 1 ORDER BY n DESC
                """,
            ),
            (
                "top_genetics_entities",
                """
                SELECT entity_value_norm, entity_type, COUNT(*) AS n
                FROM main.note_entities_genetics
                GROUP BY 1, 2
                ORDER BY n DESC
                LIMIT 20
                """,
            ),
            (
                "mr_qc_flags",
                """
                SELECT COUNT(*) AS mr_rows,
                    COUNT(*) FILTER (
                        WHERE qc_flags IS NOT NULL
                        AND CAST(qc_flags AS VARCHAR) NOT IN ('[]', '{}', 'null', '')
                    ) AS mr_with_qc_flags
                FROM main.molecular_results
                """,
            ),
            (
                "mvl_qc_flags",
                """
                SELECT COUNT(*) AS mvl_rows,
                    COUNT(*) FILTER (
                        WHERE qc_flags IS NOT NULL
                        AND CAST(qc_flags AS VARCHAR) NOT IN ('[]', '{}', 'null', '')
                    ) AS mvl_with_qc_flags
                FROM main.molecular_variant_long
                """,
            ),
            (
                "assay_outsiders",
                """
                SELECT COUNT(DISTINCT assay_name) AS distinct_assay_names_not_in_dict
                FROM main.molecular_results r
                WHERE r.assay_name IS NOT NULL
                  AND NOT EXISTS (
                    SELECT 1 FROM main.molecular_assay_dictionary d
                    WHERE COALESCE(d.assay_name, '') = COALESCE(r.assay_name, '')
                  )
                """,
            ),
        ]
        for label, sql in stmts:
            print(f"\n=== {label} ===\n")
            print(con.execute(sql.strip()).fetchdf().to_string(index=False))
    finally:
        con.close()


if __name__ == "__main__":
    main()
