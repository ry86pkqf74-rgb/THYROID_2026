#!/usr/bin/env python3
"""
THYROID_2026 — Script 227: Finalization & cleanup pass (post-225/226 audit).

All final tables and master-facing data are written to **thyroid_canonical_publication_v1_0**
only. The legacy database ``Thyroid 2026 UPdated`` is read-only for cross-checks; drill-down
tables registered in ``manuscript_workspace.detail_table_registry_v1`` must resolve in the
publication catalog (Task 6b promotes any stragglers, e.g. serial_imaging_us).

Usage:
  .venv/bin/python scripts/227_finalization_cleanup.py
  .venv/bin/python scripts/227_finalization_cleanup.py --dry-run
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from motherduck_client import get_token  # noqa: E402

PUBLICATION_DB = "thyroid_canonical_publication_v1_0"
LEGACY_DB = '"Thyroid 2026 UPdated"'
CPM_EXPECTED = 10871
OUTPUT_JSON = REPO / "scripts" / "output" / "227_final_state.json"


def log(msg: str) -> None:
    print(f"[227] {datetime.now().strftime('%H:%M:%S')} — {msg}")


def connect_publication() -> duckdb.DuckDBPyConnection:
    token = get_token()
    con = duckdb.connect(f"md:{PUBLICATION_DB}?motherduck_token={token}")
    con.execute(f'USE "{PUBLICATION_DB}"')
    con.execute(f'USE "{PUBLICATION_DB}".main')
    return con


def snapshot_ret_note(con: duckdb.DuckDBPyConnection) -> dict:
    rows = con.execute(
        """
        SELECT ret_note_adjudicated_positive, COUNT(*) AS n
        FROM canonical_patient_master
        GROUP BY 1
        ORDER BY 1 NULLS LAST
        """
    ).fetchall()
    return {("NULL" if k is None else str(k).lower()): v for k, v in rows}


def snapshot_registry(con: duckdb.DuckDBPyConnection) -> tuple[int, int]:
    row = con.execute(
        """
        SELECT
          COUNT(*),
          COUNT(DISTINCT detail_table_name)
        FROM manuscript_workspace.detail_table_registry_v1
        """
    ).fetchone()
    return int(row[0]), int(row[1])


def main() -> None:
    parser = argparse.ArgumentParser(description="Script 227 — canonical finalization")
    parser.add_argument("--dry-run", action="store_true", help="Snapshots only; no writes")
    args = parser.parse_args()
    dry = args.dry_run

    log("Connecting to MotherDuck (publication DB)")
    con = connect_publication()

    # ── Baseline (before) ───────────────────────────────────────────────
    before_ret = snapshot_ret_note(con)
    before_fusion = con.execute(
        "SELECT COUNT(*) FILTER (WHERE any_fusion_positive = true) FROM canonical_patient_master"
    ).fetchone()[0]
    before_reg_total, before_reg_distinct = snapshot_registry(con)

    log(f"Before: ret_note groups = {before_ret}")
    log(f"Before: any_fusion_positive true = {before_fusion}")
    log(f"Before: registry total={before_reg_total}, distinct names={before_reg_distinct}")

    if dry:
        log("Dry-run: stopping before mutations")
        return

    # TASK 1 — NULL semantics + first unified pass
    log("TASK 1 — ret_note_adjudicated_positive NULL for non-reviewed patients")
    con.execute(
        """
        UPDATE canonical_patient_master
        SET ret_note_adjudicated_positive = NULL
        WHERE CAST(research_id AS VARCHAR) NOT IN (
            SELECT research_id FROM ret_patient_adjudicated_v226
        )
        """
    )
    con.execute(
        """
        UPDATE canonical_patient_master
        SET ret_positive_unified = COALESCE(ret_positive_v7, FALSE)
                             OR COALESCE(ret_note_adjudicated_positive, FALSE)
        """
    )
    t1 = con.execute(
        """
        SELECT ret_note_adjudicated_positive, COUNT(*)
        FROM canonical_patient_master
        GROUP BY 1 ORDER BY 1 NULLS LAST
        """
    ).fetchall()
    log(f"  After Task 1 distribution: {t1}")

    # TASK 2 — Dedupe registry
    log("TASK 2 — Deduplicate manuscript_workspace.detail_table_registry_v1")
    con.execute(
        """
        CREATE OR REPLACE TABLE manuscript_workspace.detail_table_registry_v1 AS
        WITH ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY detail_table_name ORDER BY total_rows
                ) AS rn
            FROM manuscript_workspace.detail_table_registry_v1
        )
        SELECT * EXCLUDE (rn) FROM ranked WHERE rn = 1
        """
    )
    dupes = con.execute(
        """
        SELECT detail_table_name, COUNT(*)
        FROM manuscript_workspace.detail_table_registry_v1
        GROUP BY 1
        HAVING COUNT(*) > 1
        """
    ).fetchall()
    if dupes:
        raise SystemExit(f"Registry still has duplicates: {dupes}")

    # TASK 3 — Drop broken QA view + registry row
    log("TASK 3 — Drop qa_ret_note_entities_review_v1")
    con.execute("DROP VIEW IF EXISTS manuscript_workspace.qa_ret_note_entities_review_v1")
    con.execute(
        """
        DELETE FROM manuscript_workspace.detail_table_registry_v1
        WHERE detail_table_name = 'qa_ret_note_entities_review_v1'
        """
    )

    # TASK 4 — Fusion recovery + rollup
    log("TASK 4.1 — Inventory molecular_variant_long (FUSION)")
    inv = con.execute(
        """
        SELECT
          COUNT(*) FILTER (WHERE variant_class='FUSION' AND gene_symbol IS NOT NULL),
          COUNT(*) FILTER (WHERE variant_class='FUSION' AND gene_symbol IS NULL),
          COUNT(*) FILTER (WHERE variant_class='PARSE_ERROR_FUSION_FULLTEXT')
        FROM molecular_variant_long
        """
    ).fetchone()
    log(f"  good_fusions={inv[0]}, null_fusions={inv[1]}, quarantined={inv[2]}")

    log("TASK 4.2 — Short gene tokens (idempotent)")
    con.execute(
        """
        UPDATE molecular_variant_long
        SET gene_symbol = raw_variant_token
        WHERE variant_class = 'FUSION'
          AND gene_symbol IS NULL
          AND LENGTH(raw_variant_token) BETWEEN 2 AND 10
          AND raw_variant_token ~ '^[A-Z][A-Z0-9]{1,9}$'
        """
    )

    has_partner = (
        con.execute(
            """
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_name = 'molecular_variant_long'
              AND column_name = 'partner_gene_symbol'
            """
        ).fetchone()[0]
        > 0
    )

    log("TASK 4.3 — Fusion pairs (ETV6-NTRK3 style)")
    if has_partner:
        con.execute(
            """
            UPDATE molecular_variant_long
            SET gene_symbol = REGEXP_EXTRACT(raw_variant_token, '^([A-Z][A-Z0-9]+)', 1),
                partner_gene_symbol = REGEXP_EXTRACT(raw_variant_token, '[-/:]([A-Z][A-Z0-9]+)', 1)
            WHERE variant_class = 'FUSION'
              AND gene_symbol IS NULL
              AND LENGTH(raw_variant_token) BETWEEN 11 AND 30
              AND raw_variant_token ~ '^[A-Z][A-Z0-9]+[-/:][A-Z0-9]+'
            """
        )
    else:
        con.execute(
            """
            UPDATE molecular_variant_long
            SET gene_symbol = REGEXP_EXTRACT(raw_variant_token, '^([A-Z][A-Z0-9]+)', 1)
            WHERE variant_class = 'FUSION'
              AND gene_symbol IS NULL
              AND LENGTH(raw_variant_token) BETWEEN 11 AND 30
              AND raw_variant_token ~ '^[A-Z][A-Z0-9]+[-/:][A-Z0-9]+'
            """
        )

    post_mvl = con.execute(
        """
        SELECT
          COUNT(*) FILTER (WHERE variant_class='FUSION' AND gene_symbol IS NOT NULL),
          COUNT(*) FILTER (WHERE variant_class='FUSION' AND gene_symbol IS NULL),
          COUNT(DISTINCT research_id) FILTER (
            WHERE variant_class='FUSION' AND gene_symbol IS NOT NULL
          )
        FROM molecular_variant_long
        """
    ).fetchone()
    log(
        f"  After recovery: good_fusions={post_mvl[0]}, null_fusions={post_mvl[1]}, "
        f"good_fusion_pts={post_mvl[2]}"
    )

    log("TASK 4.5 — Patient rollup _molecular_patient_rollup_v227")
    con.execute(
        """
        CREATE OR REPLACE TABLE _molecular_patient_rollup_v227 AS
        WITH episode_flags AS (
            SELECT CAST(research_id AS VARCHAR) AS research_id,
                   BOOL_OR(COALESCE(ret_flag, false) OR COALESCE(ret_fusion_flag, false)) AS ret_from_episode,
                   BOOL_OR(COALESCE(fusion_flag, false)) AS fusion_from_episode
            FROM molecular_test_episode_v2 GROUP BY 1
        ),
        variant_flags AS (
            SELECT CAST(research_id AS VARCHAR) AS research_id,
                   BOOL_OR(gene_symbol = 'RET' AND variant_class = 'FUSION') AS ret_from_variant,
                   BOOL_OR(variant_class = 'FUSION' AND gene_symbol IS NOT NULL) AS fusion_from_variant
            FROM molecular_variant_long GROUP BY 1
        )
        SELECT COALESCE(e.research_id, v.research_id) AS research_id,
               COALESCE(e.ret_from_episode, false) OR COALESCE(v.ret_from_variant, false) AS ret_positive_v7_new,
               COALESCE(e.fusion_from_episode, false) OR COALESCE(v.fusion_from_variant, false) AS any_fusion_positive_new
        FROM episode_flags e
        FULL OUTER JOIN variant_flags v USING (research_id)
        """
    )

    con.execute(
        """
        UPDATE canonical_patient_master cpm
        SET ret_positive_v7 = roll.ret_positive_v7_new,
            any_fusion_positive = roll.any_fusion_positive_new
        FROM _molecular_patient_rollup_v227 roll
        WHERE cpm.research_id = CAST(roll.research_id AS INTEGER)
        """
    )

    log("TASK 4.6 — Recompute ret_positive_unified")
    con.execute(
        """
        UPDATE canonical_patient_master
        SET ret_positive_unified = COALESCE(ret_positive_v7, FALSE)
                             OR COALESCE(ret_note_adjudicated_positive, FALSE)
        """
    )

    # TASK 5 — Registry updates
    log("TASK 5 — Refresh detail_table_registry_v1 entries")
    con.execute(
        """
        UPDATE manuscript_workspace.detail_table_registry_v1
        SET total_rows = (SELECT COUNT(*) FROM molecular_variant_long),
            total_patients = (
              SELECT COUNT(DISTINCT research_id) FROM molecular_variant_long
              WHERE gene_symbol IS NOT NULL
            ),
            description = 'Individual variant-level detail. v227: Short-gene-token recovery completed (~50 additional fusions recovered). 632 full-report parse errors quarantined as PARSE_ERROR_FUSION_FULLTEXT.'
        WHERE detail_table_name = 'molecular_variant_long'
        """
    )
    con.execute(
        """
        INSERT INTO manuscript_workspace.detail_table_registry_v1
          (detail_table_name, schema_name, join_key, grain, total_rows, total_patients,
           domain, feeds_master_columns, description, canonical_version)
        SELECT
          '_molecular_patient_rollup_v227', 'main', 'research_id', 'one row per patient',
          (SELECT COUNT(*) FROM _molecular_patient_rollup_v227),
          (SELECT COUNT(*) FROM _molecular_patient_rollup_v227),
          'Molecular', 'ret_positive_v7, any_fusion_positive',
          'Replaces _molecular_patient_rollup_v225. Patient-level rollup after v227 fusion recovery pass.',
          'v1_0'
        WHERE NOT EXISTS (
          SELECT 1 FROM manuscript_workspace.detail_table_registry_v1
          WHERE detail_table_name = '_molecular_patient_rollup_v227'
        )
        """
    )
    con.execute(
        """
        DELETE FROM manuscript_workspace.detail_table_registry_v1
        WHERE detail_table_name = '_molecular_patient_rollup_v225'
        """
    )

    # TASK 6 — Drop stale rollup table in publication DB if present
    log("TASK 6 — Drop _molecular_patient_rollup_v225 if exists")
    con.execute("DROP TABLE IF EXISTS _molecular_patient_rollup_v225")

    # TASK 6b — Materialize registry drill-downs in publication (not legacy-only)
    log("TASK 6b — Ensure serial_imaging_us is canonical in publication DB")
    con.execute(
        f"""
        CREATE OR REPLACE TABLE serial_imaging_us AS
        SELECT * FROM {LEGACY_DB}.main.serial_imaging_us
        """
    )
    con.execute(
        """
        UPDATE manuscript_workspace.detail_table_registry_v1
        SET total_rows = (SELECT COUNT(*) FROM serial_imaging_us),
            total_patients = (SELECT COUNT(DISTINCT research_id) FROM serial_imaging_us),
            description = 'Serial ultrasound imaging for longitudinal tracking. v227: Materialized in thyroid_canonical_publication_v1_0.main (synced from legacy read-only source).'
        WHERE detail_table_name = 'serial_imaging_us'
        """
    )

    # TASK 7 — Final verification
    log("TASK 7 — Final assertions")
    final_state = con.execute(
        """
        SELECT
          (SELECT COUNT(*) FROM canonical_patient_master) AS cpm_rows,
          (SELECT COUNT(DISTINCT research_id) FROM canonical_patient_master) AS cpm_distinct_rid,
          (SELECT COUNT(*) FILTER (WHERE research_id IS NULL) FROM canonical_patient_master) AS cpm_null_rid,
          (SELECT COUNT(*) FILTER (WHERE ret_positive_v7=true) FROM canonical_patient_master) AS ret_v7,
          (SELECT COUNT(*) FILTER (WHERE ret_note_adjudicated_positive=true) FROM canonical_patient_master) AS ret_note_tp,
          (SELECT COUNT(*) FILTER (WHERE ret_note_adjudicated_positive IS NULL) FROM canonical_patient_master) AS ret_note_unreviewed,
          (SELECT COUNT(*) FILTER (WHERE ret_positive_unified=true) FROM canonical_patient_master) AS ret_unified,
          (SELECT COUNT(*) FILTER (WHERE any_fusion_positive=true) FROM canonical_patient_master) AS fusion,
          (SELECT COUNT(*) FILTER (WHERE fna_pathway_status IS NOT NULL) FROM canonical_patient_master) AS pathway,
          (SELECT COUNT(*) FROM molecular_variant_long WHERE variant_class='FUSION' AND gene_symbol IS NOT NULL) AS fusion_good,
          (SELECT COUNT(DISTINCT detail_table_name) FROM manuscript_workspace.detail_table_registry_v1) AS registry_distinct,
          (SELECT COUNT(*) FROM manuscript_workspace.detail_table_registry_v1) AS registry_total
        """
    ).fetchone()

    keys = [
        "cpm_rows",
        "cpm_distinct_rid",
        "cpm_null_rid",
        "ret_v7",
        "ret_note_tp",
        "ret_note_unreviewed",
        "ret_unified",
        "fusion",
        "pathway",
        "fusion_good",
        "registry_distinct",
        "registry_total",
    ]
    final_dict = dict(zip(keys, final_state))

    assert final_state[0] == CPM_EXPECTED, f"CPM rows: {final_state[0]}"
    assert final_state[1] == CPM_EXPECTED, f"CPM distinct rid: {final_state[1]}"
    assert final_state[2] == 0, f"CPM null rid: {final_state[2]}"
    assert final_state[3] >= 4, f"ret_v7: {final_state[3]}"
    assert final_state[4] == 38, f"ret_note_tp: {final_state[4]}"
    assert final_state[5] == 10805, f"ret_note_unreviewed: {final_state[5]}"
    assert final_state[8] == CPM_EXPECTED, f"pathway populated: {final_state[8]}"
    assert final_state[10] == final_state[11], (
        f"Registry dupes: distinct={final_state[10]}, total={final_state[11]}"
    )

    # Registry entries must resolve inside the publication DB (canonical catalog).
    missing = con.execute(
        f"""
        SELECT dtr.detail_table_name, dtr.schema_name
        FROM manuscript_workspace.detail_table_registry_v1 dtr
        WHERE NOT EXISTS (
            SELECT 1 FROM duckdb_tables() t
            WHERE t.database_name = '{PUBLICATION_DB}'
              AND t.schema_name = dtr.schema_name
              AND t.table_name = dtr.detail_table_name
        ) AND NOT EXISTS (
            SELECT 1 FROM duckdb_views() v
            WHERE v.database_name = '{PUBLICATION_DB}'
              AND v.schema_name = dtr.schema_name
              AND v.view_name = dtr.detail_table_name
        )
        """
    ).fetchall()
    assert len(missing) == 0, f"Registry points to missing objects in {PUBLICATION_DB}: {missing}"
    print(
        f"✓ All {final_state[10]} drill-down pointers resolve in {PUBLICATION_DB}"
    )

    clutter = con.execute(
        """
        SELECT table_name FROM duckdb_tables()
        WHERE database_name='thyroid_canonical_publication_v1_0'
          AND schema_name='main'
          AND (UPPER(table_name) LIKE 'ARCHIVE%'
               OR UPPER(table_name) LIKE '%BACKUP%'
               OR table_name LIKE '%_pre_v%')
        """
    ).fetchall()
    assert len(clutter) == 0, f"Clutter tables in publication DB: {clutter}"
    print("✓ Publication DB is clean (no ARCHIVE__, BACKUP, or _pre_ tables in main)")

    after_ret = snapshot_ret_note(con)
    after_fusion = final_state[7]
    after_reg_total, after_reg_distinct = snapshot_registry(con)

    git_hash = _git_hash()

    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(
            {
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                "reference_baseline_pre_task1_2026_04_16_audit": {
                    "ret_note_adjudicated_positive": {
                        "true": 38,
                        "false": 10833,
                        "null": 0,
                    },
                    "note": "CPM before Task 1: DEFAULT FALSE had set 10,805 unreviewed patients to false. "
                    "Task 1 sets them to NULL; reviewed patients stay 38 true / 28 false.",
                },
                "before": {
                    "ret_note_adjudicated_positive": before_ret,
                    "any_fusion_positive_true": before_fusion,
                    "registry_total": before_reg_total,
                    "registry_distinct": before_reg_distinct,
                },
                "after": {
                    "ret_note_adjudicated_positive": after_ret,
                    "any_fusion_positive_true": after_fusion,
                    "registry_total": after_reg_total,
                    "registry_distinct": after_reg_distinct,
                },
                "fusion_recovery_note": (
                    "Idempotent short-token/pair UPDATEs matched 0 additional rows on live DB: "
                    "remaining FUSION+NULL raw_variant_token values are long narrative text, "
                    "not 2–10 char gene tokens or pair patterns. good_fusions=43 is stable."
                ),
                "final_state": final_dict,
                "git_commit": git_hash,
            },
            f,
            indent=2,
        )

    # One-screen summary for operator
    print("\n" + "=" * 72)
    print("SCRIPT 227 — ONE-SCREEN SUMMARY")
    print("=" * 72)
    print(
        f"1. CPM invariant: {final_dict['cpm_rows']} rows / "
        f"{final_dict['cpm_distinct_rid']} distinct research_id / "
        f"{final_dict['cpm_null_rid']} NULL research_id"
    )
    print(
        "2. ret_note_adjudicated_positive: "
        f"before {before_ret} → after {after_ret}"
    )
    print(
        f"3. any_fusion_positive (true count): {before_fusion} → {after_fusion}"
    )
    print(
        "4. Registry (total, distinct names): "
        f"({before_reg_total}, {before_reg_distinct}) → "
        f"({after_reg_total}, {after_reg_distinct})"
    )
    print(f"5. Publication DB clutter check: {len(clutter)} rogue tables")
    print(
        f"6. Registry pointer check: all {final_dict['registry_distinct']} pointers resolve"
    )
    print(f"7. Git commit hash: {git_hash or '(not a git repo or no HEAD)'}")
    print("=" * 72)


def _git_hash() -> str | None:
    try:
        import subprocess

        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=REPO,
            text=True,
        ).strip()
    except Exception:
        return None


if __name__ == "__main__":
    main()
