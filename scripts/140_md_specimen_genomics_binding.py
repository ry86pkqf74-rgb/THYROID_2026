#!/usr/bin/env python3
"""Materialize specimen–genomics assay bindings on MotherDuck (fail-closed) or local file.

Uses existing v3 linkage only (no direct molecular→surgery). Optional ``genetic_testing`` and
``thyroseq_molecular_enrichment`` sections are applied only when those tables exist.

Rules:
  * connect_md_or_file(..., fail_closed=True) when --md
  * :func:`specimen_fhir_release_writer_attribution` for UA / session hint
  * RW token (see motherduck_client / .env.motherduck)

Usage:
  .venv/bin/python scripts/140_md_specimen_genomics_binding.py --md [--dry-run] [--skip-snapshot]
  .venv/bin/python scripts/140_md_specimen_genomics_binding.py --db-path ./thyroid_master.duckdb
"""
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DEFAULT_DB = ROOT / "thyroid_master.duckdb"
DDL_PATH = ROOT / "scripts" / "sql" / "140_specimen_genomics_binding_ddl.sql"

PREREQ_MAIN_TABLES: tuple[str, ...] = (
    "molecular_test_episode_v2",
    "fna_molecular_linkage_v3",
    "preop_surgery_linkage_v3",
    "surgery_pathology_linkage_v3",
    "specimen_tumor_focus_v1",
)


def _git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True
        ).strip()
    except (subprocess.CalledProcessError, OSError):
        return "unknown"


def _prod_database_name() -> str:
    return (
        os.environ.get("MOTHERDUCK_DATABASE")
        or os.environ.get("MOTHERDUCK_DB")
        or "Thyroid 2026"
    ).strip()


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def try_named_snapshot(con, *, snapshot_name: str, prod: str) -> tuple[str, str]:
    sql = f"CREATE SNAPSHOT {_quote_ident(snapshot_name)} OF {_quote_ident(prod)};"
    try:
        con.execute(sql)
        return ("ok", sql)
    except Exception as e:
        msg = str(e).lower()
        if (
            "ducklake" in msg
            or ("snapshot" in msg and "not supported" in msg)
            or "does not have snapshots" in msg
            or "not a native duckdb" in msg
        ):
            return ("skipped", f"{e!r} — {sql}")
        return ("failed", f"{e!r} — {sql}")


def _table_exists(con, schema: str, name: str) -> bool:
    try:
        r = con.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ?",
            [schema, name],
        ).fetchone()
        return r is not None
    except Exception:
        return False


def missing_prereq_tables(con) -> list[str]:
    return [t for t in PREREQ_MAIN_TABLES if not _table_exists(con, "main", t)]


def strip_optional_sql(sql: str, *, has_genetic: bool, has_thyroseq: bool) -> str:
    if not has_genetic:
        sql = re.sub(
            r"-- @OPTIONAL_GENETIC_BODY_START.*?-- @OPTIONAL_GENETIC_BODY_END\s*",
            "",
            sql,
            flags=re.DOTALL,
        )
        sql = re.sub(
            r"-- @OPTIONAL_UNION_GENETIC\s*\nUNION ALL\s*\nSELECT \* FROM genetic_rows\s*\n",
            "",
            sql,
        )
    if not has_thyroseq:
        sql = re.sub(
            r"-- @OPTIONAL_THYROSEQ_BODY_START.*?-- @OPTIONAL_THYROSEQ_BODY_END\s*",
            "",
            sql,
            flags=re.DOTALL,
        )
        sql = re.sub(
            r"-- @OPTIONAL_UNION_THYROSEQ\s*\nUNION ALL\s*\nSELECT \* FROM thy_rows\s*\n",
            "",
            sql,
        )
    return sql


def apply_specimen_genomics_binding(
    con,
    *,
    has_genetic: bool | None = None,
    has_thyroseq: bool | None = None,
) -> str:
    """Execute DDL; return the SQL actually run (after optional stripping)."""

    if has_genetic is None:
        has_genetic = _table_exists(con, "main", "genetic_testing")
    if has_thyroseq is None:
        has_thyroseq = _table_exists(con, "main", "thyroseq_molecular_enrichment")

    raw = DDL_PATH.read_text(encoding="utf-8")
    sql = strip_optional_sql(raw, has_genetic=has_genetic, has_thyroseq=has_thyroseq)
    con.execute(sql)
    return sql


def run_validation(con) -> list[tuple[str, str, str]]:
    out: list[tuple[str, str, str]] = []

    def run(name: str, q: str, expect_true: bool) -> None:
        try:
            val = con.execute(q).fetchone()
            ok = bool(val and val[0] is True)
            if not expect_true:
                ok = bool(val and val[0] is not None)
            out.append((name, "PASS" if ok else "FAIL", f"{val}"))
        except Exception as e:
            out.append((name, "FAIL", str(e)))

    run(
        "genomic_assay_id_unique",
        "SELECT COALESCE(COUNT(*) = COUNT(DISTINCT genomic_assay_id), FALSE)"
        " FROM main.specimen_genomic_assay_v1",
        True,
    )
    if _table_exists(con, "main", "specimen_master_v1"):
        run(
            "specimen_master_fk_when_present",
            """SELECT COALESCE(NOT EXISTS (
              SELECT 1 FROM main.specimen_genomic_assay_v1 g
              LEFT JOIN main.specimen_master_v1 m
                ON g.specimen_id = m.specimen_id AND g.research_id = m.research_id
              WHERE g.specimen_id IS NOT NULL AND m.specimen_id IS NULL
            ), FALSE)""",
            True,
        )
    else:
        out.append(("specimen_master_fk_when_present", "SKIP", "specimen_master_v1 absent"))

    run(
        "high_tier_null_specimen_guard",
        """SELECT COALESCE(NOT EXISTS (
              SELECT 1 FROM main.specimen_genomic_assay_v1
              WHERE linkage_confidence_tier IN ('exact', 'high_confidence')
                AND specimen_id IS NULL
            ), FALSE)""",
        True,
    )
    run(
        "specimen_focus_fk_when_populated",
        """SELECT COALESCE(NOT EXISTS (
              SELECT 1 FROM main.specimen_genomic_assay_v1 g
              LEFT JOIN main.specimen_tumor_focus_v1 f
                ON g.specimen_focus_id = f.specimen_focus_id
              WHERE g.specimen_focus_id IS NOT NULL
                AND f.specimen_focus_id IS NULL
            ), FALSE)""",
        True,
    )
    run(
        "thyroseq_explode_ordinality_dense",
        r"""SELECT COALESCE(NOT EXISTS (
              SELECT 1 FROM (
                SELECT
                  research_id,
                  regexp_replace(source_row_key, ':\d+$', '') AS explode_group,
                  payload_field,
                  COUNT(*)::BIGINT AS c,
                  MIN(payload_explode_ord)::BIGINT AS lo,
                  MAX(payload_explode_ord)::BIGINT AS hi
                FROM main.specimen_genomic_assay_v1
                WHERE source_table = 'thyroseq_molecular_enrichment+json_each'
                  AND payload_field IN ('fusion_genes_json', 'allele_fractions_json')
                GROUP BY 1, 2, 3
                HAVING COUNT(*) > 0 AND (MIN(payload_explode_ord) <> 1
                  OR MAX(payload_explode_ord) <> COUNT(*))
              ) bad
            ), FALSE)""",
        True,
    )
    if _table_exists(con, "main", "thyroseq_molecular_enrichment"):
        # Parity only for rows that thy_pick would ingest (ThyroSeq molecular episode match);
        # orphan enrichment rows without a platform match are intentionally absent from sga_v1.
        _thy_eligible = """
            SELECT
              te.research_id,
              te.source_row_hash,
              te.fusion_genes_json,
              te.allele_fractions_json
            FROM (
              SELECT
                t.research_id,
                CAST(t.source_row_hash AS VARCHAR) AS source_row_hash,
                t.fusion_genes_json,
                t.allele_fractions_json
              FROM main.thyroseq_molecular_enrichment t
              INNER JOIN main.molecular_test_episode_v2 m
                ON t.research_id = m.research_id
               AND (
                 LOWER(COALESCE(m.platform, '')) LIKE '%thyroseq%'
                 OR LOWER(COALESCE(m.platform, '')) = 'thyroseq'
               )
              QUALIFY ROW_NUMBER() OVER (
                PARTITION BY t.research_id, t.source_row_hash
                ORDER BY m.test_date_native DESC NULLS LAST, m.molecular_episode_id DESC
              ) = 1
            ) te
        """
        run(
            "thyroseq_fusion_array_parity",
            f"""SELECT COALESCE(NOT EXISTS (
              SELECT 1 FROM (
                SELECT
                  t.research_id,
                  t.source_row_hash,
                  CASE
                    WHEN json_valid(CAST(t.fusion_genes_json AS VARCHAR))
                      AND LENGTH(TRIM(CAST(t.fusion_genes_json AS VARCHAR))) > 2
                    THEN json_array_length(CAST(t.fusion_genes_json AS VARCHAR))::BIGINT
                    ELSE 0::BIGINT
                  END AS expected_n,
                  (
                    SELECT COUNT(*)::BIGINT
                    FROM main.specimen_genomic_assay_v1 g
                    WHERE g.source_table = 'thyroseq_molecular_enrichment+json_each'
                      AND g.payload_field = 'fusion_genes_json'
                      AND g.research_id = t.research_id
                      AND starts_with(
                        g.source_row_key,
                        CAST(t.research_id AS VARCHAR) || ':'
                          || CAST(t.source_row_hash AS VARCHAR) || ':fusion_genes_json:'
                      )
                  ) AS actual_n
                FROM ({_thy_eligible.strip()}) t
              ) x
              WHERE x.expected_n > 0 AND x.actual_n IS DISTINCT FROM x.expected_n
            ), FALSE)""",
            True,
        )
        run(
            "thyroseq_allele_array_parity",
            f"""SELECT COALESCE(NOT EXISTS (
              SELECT 1 FROM (
                SELECT
                  t.research_id,
                  t.source_row_hash,
                  CASE
                    WHEN json_valid(CAST(t.allele_fractions_json AS VARCHAR))
                      AND LENGTH(TRIM(CAST(t.allele_fractions_json AS VARCHAR))) > 2
                    THEN json_array_length(CAST(t.allele_fractions_json AS VARCHAR))::BIGINT
                    ELSE 0::BIGINT
                  END AS expected_n,
                  (
                    SELECT COUNT(*)::BIGINT
                    FROM main.specimen_genomic_assay_v1 g
                    WHERE g.source_table = 'thyroseq_molecular_enrichment+json_each'
                      AND g.payload_field = 'allele_fractions_json'
                      AND g.research_id = t.research_id
                      AND starts_with(
                        g.source_row_key,
                        CAST(t.research_id AS VARCHAR) || ':'
                          || CAST(t.source_row_hash AS VARCHAR) || ':allele_fractions_json:'
                      )
                  ) AS actual_n
                FROM ({_thy_eligible.strip()}) t
              ) y
              WHERE y.expected_n > 0 AND y.actual_n IS DISTINCT FROM y.expected_n
            ), FALSE)""",
            True,
        )
    else:
        out.append(("thyroseq_fusion_array_parity", "SKIP", "thyroseq_molecular_enrichment absent"))
        out.append(("thyroseq_allele_array_parity", "SKIP", "thyroseq_molecular_enrichment absent"))

    run(
        "thyroseq_payload_fingerprint_unique_per_slice",
        """SELECT COALESCE(NOT EXISTS (
              SELECT 1 FROM (
                SELECT
                  research_id,
                  molecular_episode_id,
                  payload_field,
                  payload_explode_ord,
                  COUNT(*)::BIGINT AS c
                FROM main.specimen_genomic_assay_v1
                WHERE source_table = 'thyroseq_molecular_enrichment+json_each'
                GROUP BY 1, 2, 3, 4
                HAVING COUNT(*) > 1
              ) d
            ), FALSE)""",
        True,
    )
    return out


def persist_validation(con, rows: list[tuple[str, str, str]]) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS qa")
    con.execute(
        "CREATE TABLE IF NOT EXISTS qa.val_specimen_genomic_binding_v1 ("
        "check_name VARCHAR NOT NULL, status VARCHAR NOT NULL, detail VARCHAR, measured_at TIMESTAMP NOT NULL)"
    )
    con.execute("DELETE FROM qa.val_specimen_genomic_binding_v1 WHERE 1=1")
    con.executemany(
        "INSERT INTO qa.val_specimen_genomic_binding_v1 "
        "(check_name, status, detail, measured_at) VALUES (?, ?, ?, current_timestamp)",
        [(a, b, c) for a, b, c in rows],
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Specimen genomics binding layer on MotherDuck.")
    p.add_argument("--md", action="store_true", help="MotherDuck fail-closed.")
    p.add_argument("--db-path", default=str(DEFAULT_DB), help="Local DuckDB path when not --md.")
    p.add_argument("--dry-run", action="store_true", help="Print plan only.")
    p.add_argument("--skip-snapshot", action="store_true", help="Skip CREATE SNAPSHOT preamble.")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if args.dry_run:
        has_g = "[check genetic_testing on target]"
        has_t = "[check thyroseq_molecular_enrichment on target]"
        print(f"[dry-run] Would apply {DDL_PATH} optional genetic={has_g} thyroseq={has_t}")
        return

    from utils.md_connect import connect_md_or_file
    from utils.md_pipeline_attribution import specimen_fhir_release_writer_attribution

    ua, hint = specimen_fhir_release_writer_attribution()
    con = connect_md_or_file(
        Path(args.db_path),
        md=args.md,
        fail_closed=args.md,
        prefer_service_account=True,
        custom_user_agent=ua,
        motherduck_session_hint=hint,
    )

    missing = missing_prereq_tables(con)
    if missing:
        print(
            "FATAL: missing prerequisites:\n  - " + "\n  - ".join(missing),
            file=sys.stderr,
        )
        con.close()
        sys.exit(1)

    snap_detail = "not_attempted"
    if args.md and not args.skip_snapshot:
        prod = _prod_database_name()
        snap_name = f"specimen_genomics_pre_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}"
        st, detail = try_named_snapshot(con, snapshot_name=snap_name, prod=prod)
        print(f"  Snapshot {snap_name}: {st} — {detail[:200]}")
        snap_detail = detail

    try:
        hg = _table_exists(con, "main", "genetic_testing")
        ht = _table_exists(con, "main", "thyroseq_molecular_enrichment")
        print(f"  Optional: genetic_testing={hg}, thyroseq_molecular_enrichment={ht}")
        apply_specimen_genomics_binding(con, has_genetic=hg, has_thyroseq=ht)
    except Exception:
        con.close()
        raise

    rows = run_validation(con)
    persist_validation(con, rows)
    for name, st, det in rows:
        print(f"  [{st}] {name}: {det[:200]}")

    con.close()
    print(f"Done. UA={ua} snapshot_detail={snap_detail[:120]}")


if __name__ == "__main__":
    main()
