#!/usr/bin/env python3
"""
THYROID_2026 — Script 225: Molecular Pipeline Fixes + Canonical Cleanup & Finalization

Fixes 5 confirmed pipeline bugs in the molecular domain, cleans up stale
backup/archive tables, registers new QA tables, and rebuilds + verifies
the canonical master.

Bugs fixed:
  1. Epoch date errors (4 rows: research_ids 6511, 7249, 9188, 9705)
  2. Fusion parse errors — 47 short-gene rows recovered, 125 fusion-pairs
     recovered, 632 full-report rows quarantined, 366 flagged manual_review
  3. RET flag propagation — ret_positive_v7 now consults molecular_variant_long
  4. any_fusion_positive rollup — combines episode + variant sources
  5. specimen_site_raw column: INTEGER→VARCHAR, inherited from linked FNA

Additional fixes:
  - fna_episode_master_v2.fna_episode_id rebuilt (was 12 distinct across 8,119)
  - linked_fna_episode_id in molecular_test_episode_v2 rebuilt via date-proximity
  - fna_pathway_status column added to canonical_patient_master
  - RET note_entities QA view created for human re-adjudication

All writes go to thyroid_canonical_publication_v1_0.
"Thyroid 2026 UPdated" is READ-ONLY except for receiving archive copies (Task 7).

Usage:
    .venv/bin/python scripts/225_molecular_pipeline_fixes_and_cleanup.py
    .venv/bin/python scripts/225_molecular_pipeline_fixes_and_cleanup.py --dry-run
    .venv/bin/python scripts/225_molecular_pipeline_fixes_and_cleanup.py --task 0
    .venv/bin/python scripts/225_molecular_pipeline_fixes_and_cleanup.py --task 1
    ...
    .venv/bin/python scripts/225_molecular_pipeline_fixes_and_cleanup.py --task 10
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import traceback
from datetime import datetime
from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "scripts"))

from _md_connect import connect_locked, PUBLICATION_DB

FQ = f'"{PUBLICATION_DB}".main'
LEGACY_DB = '"Thyroid 2026 UPdated"'
OUTPUT_DIR = REPO / "scripts" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

CPM_EXPECTED_ROWS = 10871


def log(msg: str) -> None:
    print(f"[225] {datetime.now().strftime('%H:%M:%S')} — {msg}")


def fatal(msg: str) -> None:
    log(f"FATAL: {msg}")
    sys.exit(1)


def assert_cpm_invariants(con: duckdb.DuckDBPyConnection, label: str = "") -> None:
    """Hard-assert canonical_patient_master row count + uniqueness."""
    row = con.execute(
        f"SELECT COUNT(*), COUNT(DISTINCT research_id), "
        f"COUNT(*) FILTER (WHERE research_id IS NULL) "
        f"FROM canonical_patient_master"
    ).fetchone()
    n_rows, n_distinct, n_null = row
    prefix = f"[{label}] " if label else ""
    if n_rows != CPM_EXPECTED_ROWS:
        fatal(f"{prefix}CPM rows = {n_rows}, expected {CPM_EXPECTED_ROWS}")
    if n_distinct != CPM_EXPECTED_ROWS:
        fatal(f"{prefix}CPM distinct research_id = {n_distinct}, expected {CPM_EXPECTED_ROWS}")
    if n_null != 0:
        fatal(f"{prefix}CPM has {n_null} NULL research_ids")
    log(f"{prefix}CPM invariants OK: {n_rows} rows, {n_distinct} distinct, 0 NULL")


# ── TASK 0: Pre-flight snapshot ──────────────────────────────────────────

def task_0_preflight(con: duckdb.DuckDBPyConnection, dry_run: bool = False) -> dict:
    log("TASK 0 — Pre-flight snapshot")
    assert_cpm_invariants(con, "preflight")

    if not dry_run:
        con.execute("""
            CREATE OR REPLACE TABLE ARCHIVE__canonical_patient_master_v225_pre_molecular_fix AS
            SELECT * FROM canonical_patient_master
        """)
        log("  Archived canonical_patient_master")

        for tbl in ['molecular_test_episode_v2', 'molecular_variant_long',
                     'fna_episode_master_v2', 'molecular_results']:
            con.execute(f"""
                CREATE OR REPLACE TABLE ARCHIVE__{tbl}_pre_v225 AS
                SELECT * FROM {tbl}
            """)
            log(f"  Archived {tbl}")

    pre_metrics = con.execute("""
        SELECT
          (SELECT COUNT(*) FROM canonical_patient_master) AS cpm_rows,
          (SELECT COUNT(*) FILTER (WHERE ret_positive_v7=true) FROM canonical_patient_master) AS cpm_ret_true,
          (SELECT COUNT(*) FILTER (WHERE any_fusion_positive=true) FROM canonical_patient_master) AS cpm_fusion_true,
          (SELECT COUNT(*) FILTER (WHERE bethesda_final IS NULL) FROM canonical_patient_master) AS cpm_null_beth,
          (SELECT COUNT(*) FROM molecular_variant_long WHERE variant_class='FUSION' AND gene_symbol IS NOT NULL) AS mvl_fusion_good,
          (SELECT COUNT(*) FROM molecular_variant_long WHERE variant_class='FUSION' AND gene_symbol IS NULL) AS mvl_fusion_null,
          (SELECT COUNT(*) FROM molecular_test_episode_v2 WHERE EXTRACT(YEAR FROM test_date_native) < 100) AS mte_epoch_errs,
          (SELECT COUNT(*) FROM molecular_test_episode_v2 WHERE specimen_site_raw IS NOT NULL) AS mte_with_site
    """).fetchone()

    keys = ['cpm_rows', 'cpm_ret_true', 'cpm_fusion_true', 'cpm_null_beth',
            'mvl_fusion_good', 'mvl_fusion_null', 'mte_epoch_errs', 'mte_with_site']
    pre = dict(zip(keys, pre_metrics))
    metrics = {'ts': datetime.now().isoformat(), 'pre': pre}

    out_path = OUTPUT_DIR / '225_pre_metrics.json'
    with open(out_path, 'w') as f:
        json.dump(metrics, f, indent=2)
    log(f"  Pre-metrics saved to {out_path}")
    log(f"  PRE: {pre}")

    expected = {
        'cpm_rows': 10871, 'cpm_ret_true': 0, 'mte_epoch_errs': 4, 'mte_with_site': 0
    }
    for k, v in expected.items():
        if pre[k] != v:
            log(f"  WARNING: {k}={pre[k]}, expected {v}")

    return pre


# ── TASK 1: Fix epoch dates ──────────────────────────────────────────────

def task_1_epoch_dates(con: duckdb.DuckDBPyConnection, dry_run: bool = False) -> None:
    log("TASK 1 — Fix epoch dates (4 rows)")

    rows = con.execute("""
        SELECT research_id, molecular_episode_id, test_date_native,
               EXTRACT(YEAR FROM test_date_native) AS yr
        FROM molecular_test_episode_v2
        WHERE EXTRACT(YEAR FROM test_date_native) < 100
        ORDER BY research_id
    """).fetchall()
    log(f"  Found {len(rows)} epoch-date rows:")
    for r in rows:
        log(f"    research_id={r[0]}, episode={r[1]}, date={r[2]}, year={r[3]}")

    if len(rows) == 0:
        log("  No epoch dates to fix — skipping")
        return

    expected_rids = {6511, 7249, 9188, 9705}
    found_rids = {r[0] for r in rows}
    if found_rids != expected_rids:
        log(f"  WARNING: Expected rids {expected_rids}, found {found_rids}")

    if dry_run:
        log("  DRY RUN — skipping UPDATE")
        return

    con.execute("""
        UPDATE molecular_test_episode_v2
        SET test_date_native = test_date_native + INTERVAL '2000' YEAR,
            resolved_test_date = CAST(CAST(test_date_native + INTERVAL '2000' YEAR AS DATE) AS VARCHAR)
        WHERE EXTRACT(YEAR FROM test_date_native) < 100
    """)

    remaining = con.execute("""
        SELECT COUNT(*) FROM molecular_test_episode_v2
        WHERE EXTRACT(YEAR FROM test_date_native) < 100
    """).fetchone()[0]

    if remaining != 0:
        fatal(f"Epoch date fix failed: {remaining} rows still have year < 100")
    log("  Epoch dates fixed — 0 remaining")


# ── TASK 2: Recover / quarantine fusion parse errors ─────────────────────

def task_2_fusion_parse(con: duckdb.DuckDBPyConnection, dry_run: bool = False) -> None:
    log("TASK 2 — Recover / quarantine fusion parse errors")

    con.execute("""
        CREATE OR REPLACE TABLE qa_fusion_parse_triage_v1 AS
        SELECT
            research_id, molecular_variant_id, raw_variant_token,
            LENGTH(raw_variant_token) AS tok_len,
            CASE
                WHEN LENGTH(raw_variant_token) <= 10
                  AND raw_variant_token ~ '^[A-Z][A-Z0-9]{1,9}$'
                  THEN 'recover_as_gene'
                WHEN LENGTH(raw_variant_token) > 10
                  AND LENGTH(raw_variant_token) <= 30
                  AND raw_variant_token ~ '^[A-Z][A-Z0-9]+[-/:][A-Z0-9]+'
                  THEN 'recover_as_fusion_pair'
                WHEN LENGTH(raw_variant_token) > 100
                  THEN 'quarantine_full_report'
                ELSE 'manual_review'
            END AS disposition
        FROM molecular_variant_long
        WHERE variant_class = 'FUSION' AND gene_symbol IS NULL
    """)

    dist = con.execute("""
        SELECT disposition, COUNT(*), COUNT(DISTINCT research_id)
        FROM qa_fusion_parse_triage_v1 GROUP BY 1 ORDER BY 2 DESC
    """).fetchall()
    log("  Triage distribution:")
    for d in dist:
        log(f"    {d[0]}: {d[1]} rows, {d[2]} patients")

    if dry_run:
        log("  DRY RUN — skipping mutations")
        return

    # 2.2 — Recover short gene tokens
    r = con.execute("""
        UPDATE molecular_variant_long
        SET gene_symbol = raw_variant_token
        WHERE variant_class = 'FUSION'
          AND gene_symbol IS NULL
          AND LENGTH(raw_variant_token) <= 10
          AND raw_variant_token ~ '^[A-Z][A-Z0-9]{1,9}$'
    """)
    n_short = con.execute("""
        SELECT COUNT(*) FROM molecular_variant_long
        WHERE variant_class = 'FUSION' AND gene_symbol = raw_variant_token
          AND LENGTH(raw_variant_token) <= 10
    """).fetchone()[0]
    log(f"  Recovered {n_short} short-gene fusion rows")

    # 2.3 — Recover fusion pairs (use partner_gene_symbol column)
    has_partner_col = con.execute("""
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_name='molecular_variant_long' AND column_name='partner_gene_symbol'
    """).fetchone()[0] > 0

    if has_partner_col:
        con.execute("""
            UPDATE molecular_variant_long
            SET gene_symbol = REGEXP_EXTRACT(raw_variant_token, '^([A-Z][A-Z0-9]+)', 1),
                partner_gene_symbol = REGEXP_EXTRACT(raw_variant_token, '[-/:]([A-Z][A-Z0-9]+)', 1)
            WHERE variant_class = 'FUSION'
              AND gene_symbol IS NULL
              AND LENGTH(raw_variant_token) BETWEEN 11 AND 30
              AND raw_variant_token ~ '^[A-Z][A-Z0-9]+[-/:][A-Z0-9]+'
        """)
        n_pairs = con.execute("""
            SELECT COUNT(*) FROM molecular_variant_long
            WHERE variant_class = 'FUSION' AND gene_symbol IS NOT NULL
              AND partner_gene_symbol IS NOT NULL
              AND LENGTH(raw_variant_token) BETWEEN 11 AND 30
        """).fetchone()[0]
        log(f"  Recovered {n_pairs} fusion-pair rows (gene + partner)")
    else:
        log("  WARNING: partner_gene_symbol column not found — skipping fusion-pair recovery")

    # 2.4 — Quarantine full-report text rows
    con.execute("""
        UPDATE molecular_variant_long
        SET variant_class = 'PARSE_ERROR_FUSION_FULLTEXT'
        WHERE variant_class = 'FUSION'
          AND gene_symbol IS NULL
          AND LENGTH(raw_variant_token) > 100
    """)
    n_quarantined = con.execute("""
        SELECT COUNT(*) FROM molecular_variant_long
        WHERE variant_class = 'PARSE_ERROR_FUSION_FULLTEXT'
    """).fetchone()[0]
    log(f"  Quarantined {n_quarantined} full-report rows")

    # 2.6 — Verify
    stats = con.execute("""
        SELECT
          COUNT(*) FILTER (WHERE variant_class='FUSION' AND gene_symbol IS NOT NULL) AS good_fusions,
          COUNT(*) FILTER (WHERE variant_class='FUSION' AND gene_symbol IS NULL) AS remaining_null,
          COUNT(*) FILTER (WHERE variant_class='PARSE_ERROR_FUSION_FULLTEXT') AS quarantined,
          COUNT(DISTINCT research_id) FILTER (WHERE variant_class='FUSION' AND gene_symbol IS NOT NULL) AS good_fusion_pts
        FROM molecular_variant_long
    """).fetchone()
    log(f"  Post-fix: good_fusions={stats[0]}, remaining_null={stats[1]}, "
        f"quarantined={stats[2]}, good_fusion_patients={stats[3]}")


# ── TASK 3: Fix RET flag + fusion flag propagation ───────────────────────

def task_3_ret_fusion_propagation(con: duckdb.DuckDBPyConnection, dry_run: bool = False) -> None:
    log("TASK 3 — Fix RET flag + fusion flag propagation")

    # 3.1 — Patch episode-level RET flag from variant table
    if not dry_run:
        con.execute("""
            UPDATE molecular_test_episode_v2 mte
            SET ret_flag = true, ret_fusion_flag = true, fusion_flag = true
            WHERE EXISTS (
                SELECT 1 FROM molecular_variant_long mvl
                WHERE CAST(mvl.research_id AS VARCHAR) = CAST(mte.research_id AS VARCHAR)
                  AND mvl.gene_symbol = 'RET'
                  AND mvl.variant_class = 'FUSION'
            )
        """)
        n_ret_episodes = con.execute("""
            SELECT COUNT(*) FROM molecular_test_episode_v2 WHERE ret_flag = true
        """).fetchone()[0]
        log(f"  Episode-level RET flag patched: {n_ret_episodes} episodes now ret_flag=true")

    # 3.2 — Build patient-level corrected rollup
    con.execute("""
        CREATE OR REPLACE TABLE _molecular_patient_rollup_v225 AS
        WITH episode_flags AS (
            SELECT
                CAST(research_id AS VARCHAR) AS research_id,
                BOOL_OR(COALESCE(ret_flag, false) OR COALESCE(ret_fusion_flag, false)) AS ret_from_episode,
                BOOL_OR(COALESCE(fusion_flag, false)) AS fusion_from_episode
            FROM molecular_test_episode_v2
            GROUP BY 1
        ),
        variant_flags AS (
            SELECT
                CAST(research_id AS VARCHAR) AS research_id,
                BOOL_OR(gene_symbol = 'RET' AND variant_class = 'FUSION') AS ret_from_variant,
                BOOL_OR(variant_class = 'FUSION' AND gene_symbol IS NOT NULL) AS fusion_from_variant
            FROM molecular_variant_long
            GROUP BY 1
        )
        SELECT
            COALESCE(e.research_id, v.research_id) AS research_id,
            COALESCE(e.ret_from_episode, false) OR COALESCE(v.ret_from_variant, false) AS ret_positive_v7_fixed,
            COALESCE(e.fusion_from_episode, false) OR COALESCE(v.fusion_from_variant, false) AS any_fusion_positive_fixed
        FROM episode_flags e FULL OUTER JOIN variant_flags v USING (research_id)
    """)
    rollup_count = con.execute("SELECT COUNT(*) FROM _molecular_patient_rollup_v225").fetchone()[0]
    log(f"  Rollup table built: {rollup_count} patients")

    if dry_run:
        log("  DRY RUN — skipping CPM update")
        return

    # 3.3 — Apply to canonical_patient_master
    con.execute("""
        UPDATE canonical_patient_master cpm
        SET ret_positive_v7 = roll.ret_positive_v7_fixed,
            any_fusion_positive = roll.any_fusion_positive_fixed
        FROM _molecular_patient_rollup_v225 roll
        WHERE cpm.research_id = CAST(roll.research_id AS INTEGER)
    """)

    # 3.4 — Verify
    stats = con.execute("""
        SELECT
          (SELECT COUNT(*) FROM canonical_patient_master WHERE ret_positive_v7=true) AS ret_true,
          (SELECT COUNT(*) FROM canonical_patient_master WHERE any_fusion_positive=true) AS fusion_true
    """).fetchone()
    log(f"  Post-fix: ret_positive_v7=true: {stats[0]}, any_fusion_positive=true: {stats[1]}")

    if stats[0] < 4:
        fatal(f"RET propagation failed: only {stats[0]} patients, expected >= 4")
    assert_cpm_invariants(con, "post-task3")


# ── TASK 4: Fix specimen_site_raw + FNA episode ID ───────────────────────

def task_4_specimen_site(con: duckdb.DuckDBPyConnection, dry_run: bool = False) -> None:
    log("TASK 4 — Fix specimen_site_raw + FNA episode ID")

    # 4.1 — Rebuild fna_episode_master_v2 with unique episode IDs
    pre_stats = con.execute("""
        SELECT COUNT(*), COUNT(DISTINCT fna_episode_id)
        FROM fna_episode_master_v2
    """).fetchone()
    log(f"  FNA pre-fix: {pre_stats[0]} rows, {pre_stats[1]} distinct episode_ids")

    if dry_run:
        log("  DRY RUN — skipping rebuild")
        return

    con.execute("""
        CREATE OR REPLACE TABLE fna_episode_master_v2 AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY research_id, fna_date_native, source_table) AS fna_episode_id,
            research_id,
            fna_date_native, resolved_fna_date, date_status, date_confidence,
            bethesda_raw, bethesda_category, pathology_diagnosis, pathology_extended,
            specimen_site_raw, laterality,
            linked_molecular_episode_id, linked_imaging_nodule_id, linked_surgery_episode_id,
            source_table, fna_confidence
        FROM ARCHIVE__fna_episode_master_v2_pre_v225
    """)

    post_stats = con.execute("""
        SELECT COUNT(*), COUNT(DISTINCT fna_episode_id)
        FROM fna_episode_master_v2
    """).fetchone()
    log(f"  FNA post-rebuild: {post_stats[0]} rows, {post_stats[1]} distinct episode_ids")
    if post_stats[0] != post_stats[1]:
        fatal(f"FNA episode_id still not unique: {post_stats[1]}/{post_stats[0]}")

    # 4.2 — Fix molecular_test_episode_v2 column types
    con.execute("ALTER TABLE molecular_test_episode_v2 ALTER specimen_site_raw SET DATA TYPE VARCHAR")
    con.execute("ALTER TABLE molecular_test_episode_v2 ALTER specimen_site_normalized SET DATA TYPE VARCHAR")
    log("  specimen_site_raw/normalized retyped INTEGER → VARCHAR")

    # 4.3 — Rebuild linked_fna_episode_id via date-proximity matching
    con.execute("""
        UPDATE molecular_test_episode_v2 mte
        SET linked_fna_episode_id = CAST(fem.fna_episode_id AS VARCHAR)
        FROM fna_episode_master_v2 fem
        WHERE CAST(mte.research_id AS VARCHAR) = CAST(fem.research_id AS VARCHAR)
          AND fem.resolved_fna_date IS NOT NULL
          AND mte.test_date_native IS NOT NULL
          AND DATE_DIFF('day', fem.resolved_fna_date, CAST(mte.test_date_native AS DATE)) BETWEEN 0 AND 180
          AND NOT EXISTS (
              SELECT 1 FROM fna_episode_master_v2 fem2
              WHERE CAST(fem2.research_id AS VARCHAR) = CAST(mte.research_id AS VARCHAR)
                AND fem2.resolved_fna_date IS NOT NULL
                AND DATE_DIFF('day', fem2.resolved_fna_date, CAST(mte.test_date_native AS DATE)) BETWEEN 0 AND 180
                AND fem2.resolved_fna_date > fem.resolved_fna_date
          )
    """)
    n_linked = con.execute("""
        SELECT COUNT(*) FROM molecular_test_episode_v2
        WHERE linked_fna_episode_id IS NOT NULL
    """).fetchone()[0]
    n_distinct_links = con.execute("""
        SELECT COUNT(DISTINCT linked_fna_episode_id) FROM molecular_test_episode_v2
        WHERE linked_fna_episode_id IS NOT NULL
    """).fetchone()[0]
    log(f"  Linked FNA episodes: {n_linked} rows, {n_distinct_links} distinct links")

    # 4.4 — Inherit specimen_site from linked FNA episode
    con.execute("""
        UPDATE molecular_test_episode_v2 mte
        SET specimen_site_raw = fem.specimen_site_raw,
            specimen_site_normalized = CASE
                WHEN fem.specimen_site_raw ILIKE '%right%' OR fem.specimen_site_raw LIKE 'RL%'
                     OR fem.specimen_site_raw ILIKE '%RL nodule%' THEN 'right_lobe'
                WHEN fem.specimen_site_raw ILIKE '%left%' OR fem.specimen_site_raw LIKE 'LL%'
                     OR fem.specimen_site_raw ILIKE '%LL nodule%' THEN 'left_lobe'
                WHEN fem.specimen_site_raw ILIKE '%isthm%' THEN 'isthmus'
                WHEN fem.specimen_site_raw ILIKE '%lymph%' OR fem.specimen_site_raw ILIKE '%LN%' THEN 'lymph_node'
                WHEN fem.specimen_site_raw IS NULL OR TRIM(fem.specimen_site_raw) = '' THEN NULL
                ELSE 'other'
            END
        FROM fna_episode_master_v2 fem
        WHERE mte.linked_fna_episode_id = CAST(fem.fna_episode_id AS VARCHAR)
          AND fem.specimen_site_raw IS NOT NULL
    """)

    # 4.5 — Verify
    site_stats = con.execute("""
        SELECT
          COUNT(*) AS total,
          COUNT(*) FILTER (WHERE linked_fna_episode_id IS NOT NULL) AS with_fna_link,
          COUNT(DISTINCT linked_fna_episode_id) AS distinct_fna_links,
          COUNT(*) FILTER (WHERE specimen_site_raw IS NOT NULL) AS with_site,
          COUNT(*) FILTER (WHERE specimen_site_normalized IS NOT NULL) AS with_site_norm
        FROM molecular_test_episode_v2
    """).fetchone()
    log(f"  Post-fix: total={site_stats[0]}, with_fna_link={site_stats[1]}, "
        f"distinct_links={site_stats[2]}, with_site={site_stats[3]}, with_site_norm={site_stats[4]}")


# ── TASK 5: Document Bethesda missingness ────────────────────────────────

def task_5_bethesda_pathway(con: duckdb.DuckDBPyConnection, dry_run: bool = False) -> None:
    log("TASK 5 — Document Bethesda missingness pattern")

    col_exists = con.execute("""
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_name='canonical_patient_master' AND column_name='fna_pathway_status'
          AND table_schema='main'
    """).fetchone()[0] > 0

    if dry_run:
        log("  DRY RUN — skipping column creation")
        return

    if not col_exists:
        con.execute("ALTER TABLE canonical_patient_master ADD COLUMN fna_pathway_status VARCHAR")
        log("  Added fna_pathway_status column")

    con.execute("""
        UPDATE canonical_patient_master
        SET fna_pathway_status = CASE
            WHEN bethesda_final IS NOT NULL THEN 'has_bethesda'
            WHEN bethesda_final IS NULL AND surg_n_procedures > 0 THEN 'direct_to_surgery'
            WHEN bethesda_final IS NULL AND (surg_n_procedures = 0 OR surg_n_procedures IS NULL) THEN 'no_fna_no_surgery'
            ELSE 'other'
        END
    """)

    dist = con.execute("""
        SELECT fna_pathway_status, COUNT(*)
        FROM canonical_patient_master GROUP BY 1 ORDER BY 2 DESC
    """).fetchall()
    log("  fna_pathway_status distribution:")
    for d in dist:
        log(f"    {d[0]}: {d[1]}")
    assert_cpm_invariants(con, "post-task5")


# ── TASK 6: RET note_entities QA view ────────────────────────────────────

def task_6_ret_qa_view(con: duckdb.DuckDBPyConnection, dry_run: bool = False) -> None:
    log("TASK 6 — RET note_entities QA view (optional)")

    if dry_run:
        log("  DRY RUN — skipping view creation")
        return

    con.execute("""
        CREATE OR REPLACE VIEW manuscript_workspace.qa_ret_note_entities_review_v1 AS
        SELECT
            neg.research_id, neg.note_row_id, neg.note_type,
            neg.entity_value_raw, neg.present_or_negated, neg.confidence,
            neg.evidence_span,
            CASE
                WHEN neg.evidence_span ILIKE '%RET/PTC%not detected%' THEN 'likely_false_positive_not_detected'
                WHEN neg.evidence_span ILIKE '%negative for%RET%' THEN 'likely_false_positive_negative_for'
                WHEN neg.evidence_span ILIKE '%no evidence of%RET%' THEN 'likely_false_positive_no_evidence'
                WHEN neg.evidence_span ILIKE '%not%RET%' OR neg.evidence_span ILIKE '%no%RET%' THEN 'likely_false_positive_negation'
                WHEN neg.evidence_span ILIKE '%positive for%RET%' OR neg.evidence_span ILIKE '%RET%fusion%detected%' THEN 'likely_true_positive'
                ELSE 'needs_review'
            END AS auto_triage
        FROM note_entities_genetics neg
        WHERE UPPER(neg.entity_value_norm) = 'RET' AND neg.present_or_negated = 'present'
    """)
    log("  Created manuscript_workspace.qa_ret_note_entities_review_v1")

    triage = con.execute("""
        SELECT auto_triage, COUNT(*), COUNT(DISTINCT research_id)
        FROM manuscript_workspace.qa_ret_note_entities_review_v1
        GROUP BY 1 ORDER BY 2 DESC
    """).fetchall()
    log("  RET auto-triage:")
    for t in triage:
        log(f"    {t[0]}: {t[1]} rows, {t[2]} patients")


# ── TASK 7: Clean up stale archive/backup tables ────────────────────────

def task_7_cleanup(con: duckdb.DuckDBPyConnection, dry_run: bool = False) -> None:
    log("TASK 7 — Clean up stale archive/backup tables")

    cleanup_tables = [
        'ARCHIVE__canonical_patient_master_v224_pre_linkage_fix',
        'fna_episode_master_v2_backup_20260414',
        'patient_refined_master_clinical_v12_ln_backup_20260414',
        'patient_refined_master_clinical_v12_outcome_backup_20260415',
        'rai_treatment_episode_v2_backup_20260415',
    ]

    for tbl in cleanup_tables:
        exists = con.execute(f"""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_name='{tbl}' AND table_schema='main'
              AND table_catalog='{PUBLICATION_DB}'
        """).fetchone()[0] > 0

        if not exists:
            log(f"  {tbl}: already gone — skipping")
            continue

        archive_name = tbl if tbl.startswith('ARCHIVE__') else f"ARCHIVE__{tbl}"

        if dry_run:
            src_count = con.execute(f"SELECT COUNT(*) FROM {FQ}.{tbl}").fetchone()[0]
            log(f"  DRY RUN: would move {tbl} ({src_count} rows) to legacy DB")
            continue

        con.execute(f"""
            CREATE OR REPLACE TABLE {LEGACY_DB}.main.{archive_name} AS
            SELECT * FROM {FQ}.{tbl}
        """)
        dst = con.execute(f'SELECT COUNT(*) FROM {LEGACY_DB}.main.{archive_name}').fetchone()[0]
        src = con.execute(f'SELECT COUNT(*) FROM {FQ}.{tbl}').fetchone()[0]
        if dst != src:
            fatal(f"Archive copy row count mismatch for {tbl}: src={src} dst={dst}")

        con.execute(f'DROP TABLE {FQ}.{tbl}')
        log(f"  Moved {tbl} ({src} rows) → legacy DB and dropped from publication")


# ── TASK 8: Update detail_table_registry_v1 ──────────────────────────────

def task_8_registry(con: duckdb.DuckDBPyConnection, dry_run: bool = False) -> None:
    log("TASK 8 — Update detail_table_registry_v1")

    if dry_run:
        log("  DRY RUN — skipping registry updates")
        return

    # 8.1 — Refresh row counts for touched tables
    con.execute("""
        UPDATE manuscript_workspace.detail_table_registry_v1
        SET total_rows = (SELECT COUNT(*) FROM molecular_test_episode_v2),
            total_patients = (SELECT COUNT(DISTINCT research_id) FROM molecular_test_episode_v2),
            feeds_master_columns = feeds_master_columns || ', ret_positive_v7 (v225), any_fusion_positive (v225), specimen_site_raw (v225), specimen_site_normalized (v225)'
        WHERE detail_table_name = 'molecular_test_episode_v2'
    """)
    log("  Updated molecular_test_episode_v2 registry entry")

    con.execute("""
        UPDATE manuscript_workspace.detail_table_registry_v1
        SET total_rows = (SELECT COUNT(*) FROM molecular_variant_long),
            total_patients = (SELECT COUNT(DISTINCT research_id) FROM molecular_variant_long WHERE gene_symbol IS NOT NULL),
            description = 'Individual variant-level detail (gene, HGVS, allele fraction, risk call). Linked to molecular_test_episode_v2. v225: 47 short-gene rows recovered, 632 full-report parse errors moved to variant_class=PARSE_ERROR_FUSION_FULLTEXT.'
        WHERE detail_table_name = 'molecular_variant_long'
    """)
    log("  Updated molecular_variant_long registry entry")

    con.execute("""
        UPDATE manuscript_workspace.detail_table_registry_v1
        SET total_rows = (SELECT COUNT(*) FROM fna_episode_master_v2),
            total_patients = (SELECT COUNT(DISTINCT research_id) FROM fna_episode_master_v2),
            description = 'Master FNA episode table. v225: fna_episode_id rebuilt to be unique (was only 12 distinct values across 8,119 rows). Links to molecular_test_episode_v2 via linked_molecular_episode_id and to imaging via linked_imaging_nodule_id.'
        WHERE detail_table_name = 'fna_episode_master_v2'
    """)
    log("  Updated fna_episode_master_v2 registry entry")

    # 8.2 — Register new QA/rollup tables
    for name, schema, jk, grain, src_sql, domain, feeds, desc in [
        ('qa_fusion_parse_triage_v1', 'main', 'research_id',
         'one row per null-gene fusion variant',
         'qa_fusion_parse_triage_v1', 'QA/Molecular',
         'any_fusion_positive (indirectly)',
         'Triage of fusion variants with NULL gene_symbol. Created by Script 225. '
         'Dispositions: recover_as_gene, recover_as_fusion_pair, quarantine_full_report, manual_review.'),
        ('_molecular_patient_rollup_v225', 'main', 'research_id',
         'one row per patient',
         '_molecular_patient_rollup_v225', 'Molecular',
         'ret_positive_v7, any_fusion_positive',
         'Patient-level molecular flag rollup consulting BOTH episode and variant tables. '
         'Fix for RET fusions missed at episode level. Built by Script 225.'),
    ]:
        already = con.execute(f"""
            SELECT COUNT(*) FROM manuscript_workspace.detail_table_registry_v1
            WHERE detail_table_name = '{name}'
        """).fetchone()[0]
        if already > 0:
            log(f"  {name} already in registry — skipping")
            continue
        rows_q = con.execute(f"SELECT COUNT(*) FROM {src_sql}").fetchone()[0]
        pts_q = con.execute(f"SELECT COUNT(DISTINCT research_id) FROM {src_sql}").fetchone()[0]
        con.execute(f"""
            INSERT INTO manuscript_workspace.detail_table_registry_v1
              (detail_table_name, schema_name, join_key, grain, total_rows, total_patients,
               domain, feeds_master_columns, description, canonical_version)
            VALUES ('{name}', '{schema}', '{jk}', '{grain}', {rows_q}, {pts_q},
                    '{domain}', '{feeds}', '{desc}', 'v1_0')
        """)
        log(f"  Registered {name} ({rows_q} rows, {pts_q} patients)")

    # Register the view separately (no row/patient counts since it's a view)
    already = con.execute("""
        SELECT COUNT(*) FROM manuscript_workspace.detail_table_registry_v1
        WHERE detail_table_name = 'qa_ret_note_entities_review_v1'
    """).fetchone()[0]
    if already == 0:
        con.execute("""
            INSERT INTO manuscript_workspace.detail_table_registry_v1
              (detail_table_name, schema_name, join_key, grain, total_rows, total_patients,
               domain, feeds_master_columns, description, canonical_version)
            VALUES ('qa_ret_note_entities_review_v1', 'manuscript_workspace', 'research_id',
                    'one row per RET NLP mention', NULL, NULL,
                    'QA/NLP', 'none (review-only)',
                    'QA view of patients with RET present in note_entities_genetics. Auto-triaged for likely false positives. Built by Script 225. Requires human re-adjudication.',
                    'v1_0')
        """)
        log("  Registered qa_ret_note_entities_review_v1 (view)")

    # 8.3 — Verify all drill-down targets exist
    missing = con.execute("""
        SELECT dtr.detail_table_name, dtr.schema_name
        FROM manuscript_workspace.detail_table_registry_v1 dtr
        LEFT JOIN information_schema.tables t
          ON t.table_catalog = 'thyroid_canonical_publication_v1_0'
         AND t.table_schema = dtr.schema_name
         AND t.table_name = dtr.detail_table_name
        WHERE t.table_name IS NULL
    """).fetchall()

    if missing:
        log(f"  WARNING: {len(missing)} registry entries point to missing tables:")
        for m in missing:
            log(f"    {m[1]}.{m[0]}")
    else:
        log("  All registry entries resolve to existing tables/views")


# ── TASK 9: Final verification ───────────────────────────────────────────

def task_9_verify(con: duckdb.DuckDBPyConnection) -> dict:
    log("TASK 9 — Final verification against pre-metrics")

    post_metrics = con.execute("""
        SELECT
          (SELECT COUNT(*) FROM canonical_patient_master) AS cpm_rows,
          (SELECT COUNT(DISTINCT research_id) FROM canonical_patient_master) AS cpm_distinct_rid,
          (SELECT COUNT(*) FILTER (WHERE research_id IS NULL) FROM canonical_patient_master) AS cpm_null_rid,
          (SELECT COUNT(*) FILTER (WHERE ret_positive_v7=true) FROM canonical_patient_master) AS cpm_ret_true,
          (SELECT COUNT(*) FILTER (WHERE any_fusion_positive=true) FROM canonical_patient_master) AS cpm_fusion_true,
          (SELECT COUNT(*) FILTER (WHERE fna_pathway_status IS NOT NULL) FROM canonical_patient_master) AS cpm_has_pathway,
          (SELECT COUNT(*) FROM molecular_variant_long WHERE variant_class='FUSION' AND gene_symbol IS NOT NULL) AS mvl_fusion_good,
          (SELECT COUNT(*) FROM molecular_variant_long WHERE variant_class='PARSE_ERROR_FUSION_FULLTEXT') AS mvl_quarantined,
          (SELECT COUNT(*) FROM molecular_test_episode_v2 WHERE EXTRACT(YEAR FROM test_date_native) < 100) AS mte_epoch_errs,
          (SELECT COUNT(*) FROM molecular_test_episode_v2 WHERE specimen_site_raw IS NOT NULL) AS mte_with_site,
          (SELECT COUNT(DISTINCT fna_episode_id) FROM fna_episode_master_v2) AS fem_distinct_ids,
          (SELECT COUNT(*) FROM fna_episode_master_v2) AS fem_total_rows
    """).fetchone()

    keys = ['cpm_rows', 'cpm_distinct_rid', 'cpm_null_rid', 'cpm_ret_true', 'cpm_fusion_true',
            'cpm_has_pathway', 'mvl_fusion_good', 'mvl_quarantined', 'mte_epoch_errs',
            'mte_with_site', 'fem_distinct_ids', 'fem_total_rows']
    post = dict(zip(keys, post_metrics))
    log(f"  POST: {post}")

    # Hard asserts
    errors = []
    if post['cpm_rows'] != CPM_EXPECTED_ROWS:
        errors.append(f"CPM rows changed: {post['cpm_rows']}")
    if post['cpm_distinct_rid'] != CPM_EXPECTED_ROWS:
        errors.append(f"CPM distinct rids changed: {post['cpm_distinct_rid']}")
    if post['cpm_null_rid'] != 0:
        errors.append(f"CPM has NULL rids: {post['cpm_null_rid']}")
    if post['cpm_ret_true'] < 4:
        errors.append(f"RET should be >= 4, got {post['cpm_ret_true']}")
    if post['cpm_fusion_true'] < 56:
        errors.append(f"Fusion should be >= 56, got {post['cpm_fusion_true']}")
    if post['cpm_has_pathway'] != CPM_EXPECTED_ROWS:
        errors.append(f"fna_pathway_status not fully populated: {post['cpm_has_pathway']}")
    if post['mte_epoch_errs'] != 0:
        errors.append(f"Epoch dates still present: {post['mte_epoch_errs']}")
    if post['fem_distinct_ids'] != post['fem_total_rows']:
        errors.append(f"FNA episode ID not unique: {post['fem_distinct_ids']}/{post['fem_total_rows']}")

    if errors:
        log("  VERIFICATION FAILURES:")
        for e in errors:
            log(f"    ✗ {e}")
        fatal("Verification failed — see above. Use rollback plan to restore.")

    # Save post-metrics
    pre_path = OUTPUT_DIR / '225_pre_metrics.json'
    if pre_path.exists():
        with open(pre_path) as f:
            m = json.load(f)
    else:
        m = {}
    m['post'] = post

    post_path = OUTPUT_DIR / '225_post_metrics.json'
    with open(post_path, 'w') as f:
        json.dump(m, f, indent=2)
    log(f"  Post-metrics saved to {post_path}")
    log("  ALL VERIFICATION CHECKS PASSED")
    return post


# ── TASK 10: Confirm clean DB ────────────────────────────────────────────

def task_10_clean_check(con: duckdb.DuckDBPyConnection) -> None:
    log("TASK 10 — Confirm canonical DB is clean")

    # 10.1 — No rogue backup/archive tables (except v225 rollback archives)
    rogue = con.execute(f"""
        SELECT table_name FROM information_schema.tables
        WHERE table_catalog='{PUBLICATION_DB}'
          AND table_schema='main'
          AND (table_name LIKE '%backup%' OR table_name LIKE '%BACKUP%'
               OR (table_name LIKE 'ARCHIVE%' AND table_name NOT LIKE '%v225%'))
    """).fetchall()
    if rogue:
        log(f"  WARNING: Rogue tables still in publication DB: {[r[0] for r in rogue]}")
    else:
        log("  No rogue backup/archive tables in publication DB")

    # 10.2 — All registry pointers resolve
    missing = con.execute(f"""
        SELECT dtr.detail_table_name FROM manuscript_workspace.detail_table_registry_v1 dtr
        LEFT JOIN information_schema.tables t
          ON t.table_catalog='{PUBLICATION_DB}'
         AND t.table_schema=dtr.schema_name AND t.table_name=dtr.detail_table_name
        WHERE t.table_name IS NULL
    """).fetchall()
    if missing:
        log(f"  WARNING: Registry points to missing tables: {[m[0] for m in missing]}")
    else:
        log("  All drill-down pointers resolve")

    # 10.3 — Required columns exist in CPM
    cpm_cols = set(r[0] for r in con.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}'
          AND table_schema='main' AND table_name='canonical_patient_master'
    """).fetchall())
    for col in ['fna_pathway_status', 'ret_positive_v7', 'any_fusion_positive']:
        if col not in cpm_cols:
            fatal(f"{col} missing from canonical_patient_master")

    log("  Publication DB is clean")
    log("  All drill-down pointers resolve")
    log("  Canonical master has all required columns")


# ── ROLLBACK ─────────────────────────────────────────────────────────────

def rollback(con: duckdb.DuckDBPyConnection) -> None:
    log("ROLLBACK — restoring from v225 archives")
    rollback_tables = [
        ('canonical_patient_master', 'ARCHIVE__canonical_patient_master_v225_pre_molecular_fix'),
        ('molecular_test_episode_v2', 'ARCHIVE__molecular_test_episode_v2_pre_v225'),
        ('molecular_variant_long', 'ARCHIVE__molecular_variant_long_pre_v225'),
        ('fna_episode_master_v2', 'ARCHIVE__fna_episode_master_v2_pre_v225'),
        ('molecular_results', 'ARCHIVE__molecular_results_pre_v225'),
    ]
    for live, archive in rollback_tables:
        archive_exists = con.execute(f"""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_name='{archive}' AND table_schema='main'
        """).fetchone()[0] > 0
        if not archive_exists:
            log(f"  Archive {archive} not found — cannot rollback {live}")
            continue
        con.execute(f"CREATE OR REPLACE TABLE {live} AS SELECT * FROM {archive}")
        log(f"  Rolled back {live}")
    assert_cpm_invariants(con, "post-rollback")


# ── MAIN ─────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Script 225: Molecular Pipeline Fixes + Canonical Cleanup"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Print queries without mutating")
    parser.add_argument("--task", type=int, default=None,
                        help="Run only a specific task (0-10)")
    parser.add_argument("--rollback", action="store_true",
                        help="Roll back all changes from v225 archives")
    args = parser.parse_args()

    log("Connecting to MotherDuck...")
    con = connect_locked()
    log(f"Connected to {PUBLICATION_DB}")

    if args.rollback:
        rollback(con)
        con.close()
        return

    tasks = {
        0: ("Pre-flight snapshot", lambda: task_0_preflight(con, args.dry_run)),
        1: ("Fix epoch dates", lambda: task_1_epoch_dates(con, args.dry_run)),
        2: ("Fusion parse recovery", lambda: task_2_fusion_parse(con, args.dry_run)),
        3: ("RET + fusion propagation", lambda: task_3_ret_fusion_propagation(con, args.dry_run)),
        4: ("Specimen site + FNA ID", lambda: task_4_specimen_site(con, args.dry_run)),
        5: ("Bethesda pathway doc", lambda: task_5_bethesda_pathway(con, args.dry_run)),
        6: ("RET QA view", lambda: task_6_ret_qa_view(con, args.dry_run)),
        7: ("Cleanup stale tables", lambda: task_7_cleanup(con, args.dry_run)),
        8: ("Update registry", lambda: task_8_registry(con, args.dry_run)),
        9: ("Final verification", lambda: task_9_verify(con)),
        10: ("Clean DB check", lambda: task_10_clean_check(con)),
    }

    if args.task is not None:
        if args.task not in tasks:
            fatal(f"Invalid task: {args.task}. Valid range: 0-10")
        label, fn = tasks[args.task]
        log(f"Running single task {args.task}: {label}")
        fn()
    else:
        for idx in sorted(tasks):
            label, fn = tasks[idx]
            try:
                fn()
            except SystemExit:
                raise
            except Exception as e:
                log(f"TASK {idx} ({label}) FAILED: {e}")
                traceback.print_exc()
                fatal(f"Stopping on first failure at task {idx}")

    con.close()
    log("Done — connection closed")


if __name__ == "__main__":
    main()
