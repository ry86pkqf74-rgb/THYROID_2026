"""Diagnostic-only: rebuild tier2.frozen_section_event_v2_test, dump every gate
and a per-source breakdown, then drop. Read-only with respect to v1 / wide tables."""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "scripts"))

import importlib.util  # noqa: E402

spec = importlib.util.spec_from_file_location(
    "s360", REPO / "scripts" / "360_frozen_section_cleanup.py"
)
s360 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(s360)


def main() -> int:
    con = s360.connect()
    tbl = "tier2.frozen_section_event_v2_test"
    con.execute(f"DROP TABLE IF EXISTS {tbl}")
    con.execute(s360.build_event_v2_sql(tbl))

    def q(sql: str) -> list:
        return con.execute(sql).fetchall()

    print("\n=== high-level counts ===")
    for label, sql in [
        ("rows", f"SELECT COUNT(*) FROM {tbl}"),
        ("distinct_patients", f"SELECT COUNT(DISTINCT research_id) FROM {tbl}"),
        ("rows_surgery_null", f"SELECT COUNT(*) FROM {tbl} WHERE surgery_n IS NULL"),
    ]:
        print(f"  {label}: {q(sql)[0][0]}")

    print("\n=== rows by source_of_data ===")
    for src, n, np in q(
        f"SELECT source_of_data, COUNT(*), COUNT(DISTINCT research_id) "
        f"FROM {tbl} GROUP BY 1 ORDER BY 2 DESC"
    ):
        print(f"  {src:35s}  rows={n:>6}  patients={np:>5}")

    print("\n=== rows by source_priority x source_column ===")
    for sp, col, n in q(
        f"SELECT source_priority, source_column, COUNT(*) FROM {tbl} "
        f"GROUP BY 1,2 ORDER BY 1,3 DESC"
    ):
        print(f"  p={sp}  col={col!r:55s}  rows={n}")

    print("\n=== synoptic_match_key collapses ===")
    nk, n_keyed_rows = q(
        f"SELECT COUNT(DISTINCT synoptic_match_key), "
        f"COUNT(*) FILTER (WHERE synoptic_match_key IS NOT NULL) FROM {tbl}"
    )[0]
    print(f"  distinct keyed survivors: {nk}")
    print(f"  rows carrying a key:      {n_keyed_rows}")

    print("\n=== regression: duplicate non-null synoptic_match_key ===")
    dup = q(
        f"SELECT synoptic_match_key, COUNT(*) c FROM {tbl} "
        f"WHERE synoptic_match_key IS NOT NULL GROUP BY 1 HAVING COUNT(*)>1 "
        f"ORDER BY c DESC LIMIT 10"
    )
    print(f"  offenders: {len(dup)}  sample: {dup[:5]}")

    print(
        "\n=== regression: same (research_id, fs_day) with both p1 and p2 "
        "fs_pathology_frozen_section ==="
    )
    dual = q(
        f"""
        SELECT research_id,
               TRY_CAST(SUBSTRING(COALESCE(frozen_section_date,''),1,10) AS DATE) AS d,
               COUNT(*) FILTER (WHERE source_priority=1) p1,
               COUNT(*) FILTER (WHERE source_priority=2) p2
        FROM {tbl}
        WHERE source_column='fs_pathology_frozen_section'
        GROUP BY 1,2
        HAVING COUNT(*) FILTER (WHERE source_priority=1)>0
           AND COUNT(*) FILTER (WHERE source_priority=2)>0
        LIMIT 10
        """
    )
    print(f"  offenders: {len(dual)}  sample: {dual[:5]}")

    print("\n=== excel_corroborated_flag distribution (LLM survivors only) ===")
    for flag, n in q(
        f"SELECT excel_corroborated_flag, COUNT(*) FROM {tbl} "
        f"WHERE source_priority=2 AND synoptic_match_key IS NOT NULL "
        f"GROUP BY 1 ORDER BY 1"
    ):
        print(f"  flag={flag}  rows={n}")

    print("\n=== Excel-only synoptic survivors (Excel rows still in v2) ===")
    n_excel_only = q(
        f"SELECT COUNT(*) FROM {tbl} "
        f"WHERE source_priority=1 AND synoptic_match_key IS NOT NULL "
        f"  AND source_of_data='synoptic_excel_parsed_column'"
    )[0][0]
    print(f"  Excel rows kept (no LLM match): {n_excel_only}  (expected ~1,700)")

    print("\n=== source-side raw counts (sanity) ===")
    for label, sql in [
        (
            "path_synoptics rows with fs_pathology_frozen_section non-null",
            "SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main.path_synoptics "
            "WHERE NULLIF(TRIM(CAST(fs_pathology_frozen_section AS VARCHAR)),'') IS NOT NULL",
        ),
        (
            "LLM detail rows with at least one entity",
            "SELECT COUNT(*), COUNT(DISTINCT research_id) "
            "FROM main.note_entities_llm_frozen_section_detail "
            "WHERE result_json IS NOT NULL "
            "AND json_type(json_extract(CAST(result_json AS JSON), '$.entities'))='ARRAY' "
            "AND json_array_length(json_extract(CAST(result_json AS JSON), '$.entities'))>0",
        ),
    ]:
        r = q(sql)[0]
        print(f"  {label}: rows={r[0]}  patients={r[1]}")

    print("\n=== LLM detail rows by note_type / source_column buckets ===")
    for nt, sc, n in q(
        """
        SELECT note_type, source_column, COUNT(*)
        FROM main.note_entities_llm_frozen_section_detail
        GROUP BY 1,2
        ORDER BY 3 DESC
        LIMIT 15
        """
    ):
        print(f"  note_type={nt!r:30s}  source_column={sc!r:55s}  rows={n}")

    con.execute(f"DROP TABLE IF EXISTS {tbl}")
    print("\n(dropped test table)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
