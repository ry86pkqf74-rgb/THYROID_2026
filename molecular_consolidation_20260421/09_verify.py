"""Step 8 - Verification queries (STOP checkpoint)."""
from __future__ import annotations

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(HERE, ".."))
sys.path.insert(0, REPO_ROOT)

import duckdb
from motherduck_client import get_token  # type: ignore


CHECKS = [
    (
        "8a row count sanity (master + notes + flat views)",
        """
        SELECT 'master'       AS table_name, COUNT(*) AS rows, COUNT(DISTINCT research_id) AS distinct_pts FROM molecular_genetics_test_v2
        UNION ALL SELECT 'notes',        COUNT(*), COUNT(DISTINCT research_id) FROM molecular_genetics_from_notes_v2
        UNION ALL SELECT 'variant_flat', COUNT(*), COUNT(DISTINCT research_id) FROM molecular_variant_flat_v2
        UNION ALL SELECT 'fusion_flat',  COUNT(*), COUNT(DISTINCT research_id) FROM molecular_fusion_flat_v2
        ORDER BY table_name
        """,
    ),
    (
        "8b parse quality distribution",
        """
        SELECT parse_status, COUNT(*) AS n,
               ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
        FROM molecular_genetics_test_v2
        GROUP BY parse_status
        ORDER BY n DESC
        """,
    ),
    (
        "8c platform/parser split",
        """
        SELECT platform, parser, COUNT(*) AS n
        FROM molecular_genetics_test_v2
        GROUP BY 1, 2
        ORDER BY n DESC
        """,
    ),
    (
        "8d ROM coverage (header fields, ThyroSeq only)",
        """
        SELECT
            SUM(CASE WHEN rom_percent_point IS NOT NULL THEN 1 ELSE 0 END) AS with_rom_numeric,
            SUM(CASE WHEN rom_descriptor    IS NOT NULL THEN 1 ELSE 0 END) AS with_rom_descriptor,
            SUM(CASE WHEN rom_description   IS NOT NULL THEN 1 ELSE 0 END) AS with_rom_description,
            SUM(CASE WHEN test_result_summary IS NOT NULL THEN 1 ELSE 0 END) AS with_test_result_summary,
            COUNT(*) AS total
        FROM molecular_genetics_test_v2
        WHERE parser = 'thyroseq'
        """,
    ),
    (
        "8e TERT distribution",
        """
        SELECT tert_present, tert_promoter_variant, COUNT(*) AS n
        FROM molecular_genetics_test_v2
        GROUP BY 1, 2
        ORDER BY n DESC
        """,
    ),
    (
        "8f top variants (validates UNNEST)",
        """
        SELECT gene, protein, COUNT(*) AS n
        FROM molecular_variant_flat_v2
        GROUP BY gene, protein
        ORDER BY n DESC
        LIMIT 20
        """,
    ),
    (
        "8g legacy parity - positive mutation count",
        """
        SELECT
            (SELECT COUNT(*) FROM molecular_genetics_test_v2 WHERE gene_mutations_status = 'Positive') AS new_positive_count,
            (SELECT COUNT(DISTINCT research_id) FROM molecular_variant_long)                          AS legacy_distinct_ids_with_variants,
            (SELECT COUNT(*) FROM molecular_variant_long)                                             AS legacy_variant_rows,
            (SELECT COUNT(*) FROM canonical_molecular_tested_v1 WHERE braf_positive_canonical)        AS legacy_braf_positive_patients,
            (SELECT COUNT(*) FROM molecular_genetics_test_v2 WHERE gene_mutations_variants IS NOT NULL
                AND len(gene_mutations_variants) > 0
                AND list_contains(list_transform(gene_mutations_variants, x -> x.gene), 'BRAF'))      AS new_braf_positive_tests
        """,
    ),
    (
        "8h status distributions across all DETAILED RESULTS sub-fields",
        """
        SELECT 'gene_mutations'  AS field, gene_mutations_status  AS status, COUNT(*) AS n FROM molecular_genetics_test_v2 GROUP BY 1,2
        UNION ALL SELECT 'gene_fusions',   gene_fusions_status,   COUNT(*) FROM molecular_genetics_test_v2 GROUP BY 1,2
        UNION ALL SELECT 'cna',            cna_status,            COUNT(*) FROM molecular_genetics_test_v2 GROUP BY 1,2
        UNION ALL SELECT 'gep',            gep_status,            COUNT(*) FROM molecular_genetics_test_v2 GROUP BY 1,2
        UNION ALL SELECT 'parathyroid',    parathyroid_status,    COUNT(*) FROM molecular_genetics_test_v2 GROUP BY 1,2
        UNION ALL SELECT 'medullary',      medullary_status,      COUNT(*) FROM molecular_genetics_test_v2 GROUP BY 1,2
        ORDER BY 1, 3 DESC
        """,
    ),
    (
        "8i text source distribution (which column contributed the parsed text)",
        """
        SELECT report_text_source, COUNT(*) AS n,
               AVG(report_text_length)::INTEGER AS avg_text_len
        FROM molecular_genetics_test_v2
        GROUP BY report_text_source
        ORDER BY n DESC
        """,
    ),
    (
        "8j Afirma-specific results coverage",
        """
        SELECT
            COUNT(*)                                                            AS total_afirma,
            SUM(CASE WHEN afirma_braf_result      IS NOT NULL THEN 1 ELSE 0 END) AS with_braf,
            SUM(CASE WHEN afirma_mtc_result       IS NOT NULL THEN 1 ELSE 0 END) AS with_mtc,
            SUM(CASE WHEN afirma_tert_c228t_result IS NOT NULL THEN 1 ELSE 0 END) AS with_tert228,
            SUM(CASE WHEN afirma_retptc_result    IS NOT NULL THEN 1 ELSE 0 END) AS with_retptc
        FROM molecular_genetics_test_v2
        WHERE platform = 'Afirma'
        """,
    ),
    (
        "8k notes table sanity",
        """
        SELECT entity_type, present_or_negated, COUNT(*) AS n
        FROM molecular_genetics_from_notes_v2
        GROUP BY 1, 2
        ORDER BY n DESC
        LIMIT 15
        """,
    ),
]


def main() -> None:
    os.environ["motherduck_token"] = get_token() or ""
    con = duckdb.connect("md:thyroid_canonical_publication_v1_0")

    pass_fail = []
    for label, sql in CHECKS:
        print(f"\n========== {label} ==========")
        try:
            df = con.execute(sql).fetchdf()
            with __import__("pandas").option_context("display.max_rows", 200, "display.width", 200,
                                                    "display.max_colwidth", 80):
                print(df.to_string(index=False))
            pass_fail.append((label, "OK", len(df)))
        except Exception as e:
            print(f"ERROR: {e}")
            pass_fail.append((label, "ERR", str(e)))

    print("\n========== verification summary ==========")
    for lab, status, info in pass_fail:
        print(f"  [{status}] {lab}  ({info})")


if __name__ == "__main__":
    main()
