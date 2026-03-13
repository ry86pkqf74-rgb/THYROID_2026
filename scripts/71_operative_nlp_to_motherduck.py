#!/usr/bin/env python3
"""
71_operative_nlp_to_motherduck.py -- Targeted operative NLP enrichment to MotherDuck

Closes the operative-note propagation gap identified in
docs/final_repo_verification_20260313.md and
docs/operative_note_path_linkage_audit_20260313.md.

Root cause: script 22 creates operative_episode_detail_v2 via SQL (NLP
fields = FALSE/NULL), then enrich_from_v2_extractors() UPDATEs them
from NLP.  But MotherDuck was materialized from the base table BEFORE
enrichment ran against it.

This script:
  1. Connects to MotherDuck RW
  2. Loads clinical_notes_long (note_text >10 chars)
  3. Runs OperativeDetailExtractor on each note
  4. Stages enrichment in _v2_operative_enrichment
  5. UPDATEs operative_episode_detail_v2 in-place
  6. Recreates md_oper_episode_detail_v2 mirror
  7. Prints before/after validation counts

PHI safety: note_text never printed; only counts and short snippets.

Usage:
    .venv/bin/python scripts/71_operative_nlp_to_motherduck.py --md
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from notes_extraction.extract_operative_v2 import OperativeDetailExtractor


def get_md_connection() -> duckdb.DuckDBPyConnection:
    try:
        import toml
        token = os.environ.get("MOTHERDUCK_TOKEN") or toml.load(
            str(ROOT / ".streamlit" / "secrets.toml")
        )["MOTHERDUCK_TOKEN"]
    except Exception:
        token = os.environ["MOTHERDUCK_TOKEN"]
    return duckdb.connect(f"md:thyroid_research_2026?motherduck_token={token}")


def count_nlp(con: duckdb.DuckDBPyConnection, table: str = "operative_episode_detail_v2") -> dict:
    metrics = {}
    queries = {
        "total_rows": f"SELECT COUNT(*) FROM {table}",
        "rln_monitoring_flag": f"SELECT COUNT(*) FROM {table} WHERE rln_monitoring_flag",
        "rln_finding_raw": f"SELECT COUNT(*) FROM {table} WHERE rln_finding_raw IS NOT NULL",
        "drain_flag": f"SELECT COUNT(*) FROM {table} WHERE drain_flag",
        "operative_findings_raw": f"SELECT COUNT(*) FROM {table} WHERE operative_findings_raw IS NOT NULL AND operative_findings_raw != ''",
        "gross_ete_flag": f"SELECT COUNT(*) FROM {table} WHERE gross_ete_flag",
        "strap_muscle_flag": f"SELECT COUNT(*) FROM {table} WHERE strap_muscle_involvement_flag",
        "parathyroid_autograft_flag": f"SELECT COUNT(*) FROM {table} WHERE parathyroid_autograft_flag",
        "reoperative_field_flag": f"SELECT COUNT(*) FROM {table} WHERE reoperative_field_flag",
        "local_invasion_flag": f"SELECT COUNT(*) FROM {table} WHERE local_invasion_flag",
        "tracheal_involvement_flag": f"SELECT COUNT(*) FROM {table} WHERE tracheal_involvement_flag",
        "esophageal_involvement_flag": f"SELECT COUNT(*) FROM {table} WHERE esophageal_involvement_flag",
        "has_nlp_parse": f"""SELECT COUNT(*) FROM {table}
            WHERE rln_monitoring_flag OR gross_ete_flag OR drain_flag
                  OR reoperative_field_flag OR tracheal_involvement_flag
                  OR esophageal_involvement_flag OR strap_muscle_involvement_flag
                  OR local_invasion_flag
                  OR rln_finding_raw IS NOT NULL
                  OR (operative_findings_raw IS NOT NULL AND operative_findings_raw != '')""",
    }
    for label, sql in queries.items():
        try:
            metrics[label] = con.execute(sql).fetchone()[0]
        except Exception as e:
            metrics[label] = f"ERROR: {e}"
    return metrics


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--md", action="store_true", required=True,
                        help="Target MotherDuck (required)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Extract but do not UPDATE")
    args = parser.parse_args()

    print("\n" + "=" * 70)
    print("  71 — Operative NLP Enrichment → MotherDuck")
    print("=" * 70)

    con = get_md_connection()
    print("  Connected to MotherDuck (RW)")

    before = count_nlp(con)
    print("\n  BEFORE counts:")
    for k, v in before.items():
        print(f"    {k}: {v}")

    print("\n  Loading clinical notes from MotherDuck...")
    t0 = time.time()
    notes_df = con.execute(
        "SELECT note_row_id, CAST(research_id AS INTEGER) AS research_id, "
        "note_type, note_text, note_date "
        "FROM clinical_notes_long "
        "WHERE note_text IS NOT NULL AND LENGTH(note_text) > 10"
    ).fetchdf()
    print(f"  Loaded {len(notes_df):,} clinical notes in {time.time()-t0:.1f}s")

    print("  Running OperativeDetailExtractor...")
    t0 = time.time()
    op_ext = OperativeDetailExtractor()
    op_results: list[dict] = []

    for _, row in notes_df.iterrows():
        rid = row["research_id"]
        nrid = str(row["note_row_id"])
        ntype = str(row["note_type"] or "")
        ntext = str(row["note_text"] or "")
        ndate = str(row["note_date"]) if pd.notna(row["note_date"]) else None

        for em in op_ext.extract(nrid, rid, ntype, ntext, ndate):
            op_results.append(em.to_dict())

    elapsed = time.time() - t0
    print(f"  Extracted {len(op_results):,} operative entities in {elapsed:.1f}s")

    if not op_results:
        print("  WARNING: No operative entities extracted. Nothing to update.")
        con.close()
        return

    if args.dry_run:
        print("  DRY RUN — skipping UPDATE. Entity counts by type:")
        op_df = pd.DataFrame(op_results)
        print(op_df.groupby("entity_type").size().to_string())
        con.close()
        return

    print("  Staging enrichment data on MotherDuck...")
    op_df = pd.DataFrame(op_results)
    con.register("_op_v2_raw", op_df)

    con.execute("""
        CREATE OR REPLACE TABLE _v2_operative_enrichment AS
        SELECT
            CAST(research_id AS INTEGER) AS research_id,
            note_date,
            BOOL_OR(entity_type = 'nerve_monitoring'
                AND present_or_negated = 'present') AS rln_monitoring_flag,
            MAX(CASE WHEN entity_type = 'rln_finding'
                AND present_or_negated = 'present'
                THEN entity_value_norm END) AS rln_finding_raw,
            BOOL_OR(entity_type = 'parathyroid_autograft'
                AND present_or_negated = 'present') AS parathyroid_autograft_flag,
            BOOL_OR(entity_type = 'gross_invasion'
                AND present_or_negated = 'present'
                AND entity_value_norm IN ('gross_ete', 'ete_present')
            ) AS gross_ete_flag,
            BOOL_OR(entity_type = 'gross_invasion'
                AND present_or_negated = 'present') AS local_invasion_flag,
            BOOL_OR(entity_type = 'tracheal_involvement'
                AND present_or_negated = 'present'
                AND entity_value_norm != 'trachea_intact'
            ) AS tracheal_involvement_flag,
            BOOL_OR(entity_type = 'esophageal_involvement'
                AND present_or_negated = 'present'
                AND entity_value_norm != 'esophagus_intact'
            ) AS esophageal_involvement_flag,
            BOOL_OR(entity_type = 'strap_muscle'
                AND present_or_negated = 'present'
                AND entity_value_norm IN ('strap_resected', 'strap_invaded')
            ) AS strap_muscle_involvement_flag,
            BOOL_OR(entity_type = 'reoperative_field'
                AND present_or_negated = 'present') AS reoperative_field_flag,
            BOOL_OR(entity_type = 'drain_placement'
                AND present_or_negated = 'present'
                AND entity_value_norm != 'no_drain') AS drain_flag,
            STRING_AGG(DISTINCT CASE WHEN entity_type IN (
                    'gross_invasion', 'rln_finding', 'tracheal_involvement',
                    'esophageal_involvement', 'strap_muscle', 'intraop_complication')
                AND present_or_negated = 'present'
                THEN entity_value_norm END, '; ') AS operative_findings_raw
        FROM _op_v2_raw
        GROUP BY CAST(research_id AS INTEGER), note_date
    """)
    con.unregister("_op_v2_raw")

    staging_count = con.execute(
        "SELECT COUNT(*) FROM _v2_operative_enrichment"
    ).fetchone()[0]
    print(f"  Staging table: {staging_count:,} enrichment rows")

    print("  Applying UPDATE to operative_episode_detail_v2...")
    t0 = time.time()
    con.execute("""
        UPDATE operative_episode_detail_v2 o
        SET rln_monitoring_flag = COALESCE(e.rln_monitoring_flag, o.rln_monitoring_flag),
            rln_finding_raw = COALESCE(e.rln_finding_raw, o.rln_finding_raw),
            parathyroid_autograft_flag = COALESCE(e.parathyroid_autograft_flag, o.parathyroid_autograft_flag),
            gross_ete_flag = COALESCE(e.gross_ete_flag, o.gross_ete_flag),
            local_invasion_flag = COALESCE(e.local_invasion_flag, o.local_invasion_flag),
            tracheal_involvement_flag = COALESCE(e.tracheal_involvement_flag, o.tracheal_involvement_flag),
            esophageal_involvement_flag = COALESCE(e.esophageal_involvement_flag, o.esophageal_involvement_flag),
            strap_muscle_involvement_flag = COALESCE(e.strap_muscle_involvement_flag, o.strap_muscle_involvement_flag),
            reoperative_field_flag = COALESCE(e.reoperative_field_flag, o.reoperative_field_flag),
            drain_flag = COALESCE(e.drain_flag, o.drain_flag),
            operative_findings_raw = COALESCE(e.operative_findings_raw, o.operative_findings_raw)
        FROM (
            SELECT DISTINCT ON (o2.research_id, o2.surgery_episode_id)
                o2.research_id, o2.surgery_episode_id, e2.*
            FROM operative_episode_detail_v2 o2
            CROSS JOIN _v2_operative_enrichment e2
            WHERE o2.research_id = e2.research_id
              AND (e2.rln_monitoring_flag
                   OR e2.rln_finding_raw IS NOT NULL
                   OR e2.operative_findings_raw IS NOT NULL)
            ORDER BY o2.research_id, o2.surgery_episode_id,
                     ABS(DATEDIFF('day',
                         COALESCE(o2.surgery_date_native, DATE '2099-01-01'),
                         COALESCE(TRY_CAST(e2.note_date AS DATE), DATE '2099-01-01')))
        ) e
        WHERE o.research_id = e.research_id
          AND o.surgery_episode_id = e.surgery_episode_id
    """)
    print(f"  UPDATE completed in {time.time()-t0:.1f}s")

    print("  Recreating md_oper_episode_detail_v2 mirror...")
    con.execute("""
        CREATE OR REPLACE TABLE md_oper_episode_detail_v2 AS
        SELECT * FROM operative_episode_detail_v2
    """)
    md_count = con.execute(
        "SELECT COUNT(*) FROM md_oper_episode_detail_v2"
    ).fetchone()[0]
    print(f"  md_oper_episode_detail_v2: {md_count:,} rows")

    con.execute("DROP TABLE IF EXISTS _v2_operative_enrichment")

    after = count_nlp(con)
    print("\n  AFTER counts:")
    for k, v in after.items():
        print(f"    {k}: {v}")

    print("\n  DELTA (after - before):")
    for k in after:
        b = before.get(k, 0)
        a = after.get(k, 0)
        if isinstance(b, int) and isinstance(a, int):
            delta = a - b
            print(f"    {k}: {b} → {a}  (Δ {'+' if delta >= 0 else ''}{delta})")

    con.close()
    print("\n  Done.\n")


if __name__ == "__main__":
    main()
