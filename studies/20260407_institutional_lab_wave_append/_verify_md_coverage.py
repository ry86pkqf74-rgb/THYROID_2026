#!/usr/bin/env python3
"""One-off verification queries after institutional lab append."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from utils.md_connect import connect_md_or_file  # noqa: E402

WAVE = "final_institutional_20260407"


def main() -> None:
    con = connect_md_or_file(ROOT / "thyroid_master.duckdb", md=True, fail_closed=True, prefer_service_account=True)
    try:
        print("=== ingestion_wave counts (canonical) ===")
        print(
            con.execute(
                """
                SELECT ingestion_wave, COUNT(*) n, COUNT(DISTINCT research_id) pts
                FROM main.longitudinal_lab_canonical_v1
                GROUP BY 1 ORDER BY 2 DESC
                """
            )
            .fetchdf()
            .to_string(index=False)
        )
        print("\n=== final_institutional analyte breakdown ===")
        print(
            con.execute(
                """
                SELECT lab_name_standardized, COUNT(*) n
                FROM main.longitudinal_lab_canonical_v1
                WHERE ingestion_wave = ?
                GROUP BY 1 ORDER BY 2 DESC
                """,
                [WAVE],
            )
            .fetchdf()
            .to_string(index=False)
        )
        checks = [
            ("TSH", "lab_name_standardized ILIKE '%tsh%' AND lab_name_standardized NOT ILIKE '%tgab%'"),
            ("PTH", "lab_name_standardized ILIKE '%pth%'"),
            (
                "calcium",
                "(lab_name_standardized ILIKE '%calcium%' OR lab_name_standardized = 'calcium')",
            ),
            (
                "vitamin D",
                "lab_name_standardized ILIKE '%vitamin%d%' OR lab_name_standardized = 'vitamin_d'",
            ),
        ]
        print("\n=== longitudinal_lab_deduped_v ===")
        for label, cond in checks:
            q = f"SELECT COUNT(*) n, COUNT(DISTINCT research_id) pts FROM main.longitudinal_lab_deduped_v WHERE {cond}"
            row = con.execute(q).fetchone()
            print(f"  {label}: rows={row[0]:,} pts={row[1]:,}")
        row = con.execute(
            """
            SELECT COUNT(*) n, COUNT(DISTINCT research_id) pts FROM main.longitudinal_lab_deduped_v
            WHERE analyte_group = 'thyroid_tumor_markers'
               OR lab_name_standardized ILIKE '%thyroglobulin%'
               OR lab_name_standardized ILIKE '%tgab%'
            """
        ).fetchone()
        print(f"  Tg axis (broad): rows={row[0]:,} pts={row[1]:,}")

        print("\n=== provenance (new wave) ===")
        print(
            con.execute(
                """
                SELECT source_table, source_script, COUNT(*) n,
                       SUM(CASE WHEN provenance_note ILIKE '%lineage_key=%' THEN 1 ELSE 0 END) has_lineage_token
                FROM main.longitudinal_lab_canonical_v1
                WHERE ingestion_wave = ?
                GROUP BY 1, 2
                """,
                [WAVE],
            )
            .fetchdf()
            .to_string(index=False)
        )

        print("\n=== join canonical facts (MD5 lineage in provenance vs cfact note_row_id hash mismatch check) ===")
        print(
            con.execute(
                """
                SELECT COUNT(*) AS rows_institutional_cfact
                FROM main.longitudinal_lab_canonical_v1 lab
                WHERE lab.ingestion_wave = ?
                  AND lab.source_table = 'canonical_extracted_fact_long_v2'
                """,
                [WAVE],
            ).fetchdf().to_string(index=False)
        )

        jm = con.execute(
            """
            SELECT COUNT(*) AS lab_rows_with_matching_fact
            FROM main.longitudinal_lab_canonical_v1 lab
            WHERE lab.ingestion_wave = ?
              AND lab.source_table = 'canonical_extracted_fact_long_v2'
              AND EXISTS (
                SELECT 1
                FROM main.canonical_extracted_fact_long_v2 cf
                WHERE lab.research_id = cf.research_id
                  AND cf.fact_domain = 'labs'
                  AND cf.present_or_negated = 'present'
                  AND cf.note_row_id = regexp_extract(lab.provenance_note, 'note_row_id=([^|]+)', 1)
                  AND cf.extraction_run_id = regexp_extract(lab.provenance_note, 'extraction_run_id=([^|]+)', 1)
                  AND LOWER(TRIM(cf.entity_type)) = regexp_extract(lab.provenance_note, 'entity_type=([^|]+)', 1)
              )
            """,
            [WAVE],
        ).fetchone()[0]
        print(
            f"\n=== NLP lab rows with ≥1 matching canonical fact (join keys in provenance_note) ===\n  {jm:,}"
        )
    finally:
        con.close()


if __name__ == "__main__":
    main()
