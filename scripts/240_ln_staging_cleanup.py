"""
Script 240: LN/Staging Data Quality Remediation & Canonical Publication DB Finalization.

Database: thyroid_canonical_publication_v1_0 on MotherDuck (AUTHORITATIVE reads + writes).

Fixes five data quality issues identified in the Prompt 5 LN/staging audit:

1. 949 microscopic-ETE patients wrongly staged as T3b under AJCC 8th (mic ETE was
   eliminated as an upstaging criterion in AJCC 8th DTC). New cols:
   ``ajcc8_t_stage_corrected``, ``ajcc8_stage_group_corrected``,
   ``microscopic_ete_t3b_corrected``.

2. ``ln_positive_flag`` is a count (range 0-51), not a boolean. 51 mismatches
   with ``ln_total_positive`` and 457 gap-fill candidates. New cols:
   ``ln_positive_binary``, ``ln_positive_count_raw``, ``ln_count_reconciled``.

3. ENE-positive patients with zero/NULL positive nodes classified by source.
   New col: ``ene_ln_concordance_status``.

4. 8 N1b + age>=55 patients wrongly coded as Stage III (DTC-only, non-DTC uses
   other staging rules). New col: ``stage_discordance_note``.

5. Level-specific impossible values (positive > examined) — Level II
   transposition fix + global positive-without-exam-count flag. New col:
   ``ln_data_quality_flag``.

The script is idempotent: ALTER ADD COLUMN IF NOT EXISTS guards, UPDATE
statements that overwrite deterministically on each run.

It preserves the ORIGINAL ``ajcc8_t_stage`` and ``ajcc8_stage_group`` columns
untouched for provenance — corrections go into new ``_corrected`` columns.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from motherduck_client import get_token  # noqa: E402

OUTPUT_DIR = REPO / "scripts" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
REPORT_PATH = OUTPUT_DIR / "script_240_report.md"

DB = "thyroid_canonical_publication_v1_0"
N_EXPECTED = 10_871


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def banner(text: str) -> None:
    print("\n" + "=" * 78)
    print(text)
    print("=" * 78)


def check_invariants(con: duckdb.DuckDBPyConnection, label: str = "") -> tuple:
    row = con.execute(
        """
        SELECT COUNT(*) AS total_rows,
               COUNT(DISTINCT research_id) AS distinct_rids,
               COUNT(*) FILTER (WHERE research_id IS NULL) AS null_rids,
               COUNT(*) FILTER (WHERE fna_path_outcome IS NULL) AS null_fna
        FROM canonical_patient_master
        """
    ).fetchone()
    print(
        f"  [invariants {label}] total={row[0]} distinct_rid={row[1]} "
        f"null_rid={row[2]} null_fna={row[3]}"
    )
    assert row == (N_EXPECTED, N_EXPECTED, 0, 0), f"INVARIANT BROKEN: {row}"
    return row


def column_exists(con: duckdb.DuckDBPyConnection, col: str) -> bool:
    result = con.execute(
        """
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_catalog = ?
          AND table_name = 'canonical_patient_master'
          AND column_name = ?
        """,
        [DB, col],
    ).fetchone()
    return result[0] > 0


def column_count(con: duckdb.DuckDBPyConnection) -> int:
    return con.execute(
        """
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_catalog = ?
          AND table_name = 'canonical_patient_master'
        """,
        [DB],
    ).fetchone()[0]


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------


def step0_preflight(con: duckdb.DuckDBPyConnection) -> dict:
    banner("STEP 0 — Pre-flight checks")
    db_name = con.execute("SELECT current_database()").fetchone()[0]
    print(f"  database = {db_name}")
    assert db_name == DB, f"Wrong database attached: {db_name}"

    check_invariants(con, "pre-run")
    n_cols = column_count(con)
    print(f"  column count = {n_cols}")

    fix_cols = [
        "ajcc8_t_stage_v2",
        "ajcc8_stage_group_v2",
        "ajcc8_t_stage_corrected",
        "ajcc8_stage_group_corrected",
        "microscopic_ete_t3b_corrected",
        "ln_positive_binary",
        "ln_positive_count_raw",
        "ln_count_reconciled",
        "ene_ln_concordance_status",
        "stage_discordance_note",
        "ln_data_quality_flag",
    ]
    existing = {c for c in fix_cols if column_exists(con, c)}
    print(f"  already-present fix columns = {sorted(existing)}")
    return {"n_cols_pre": n_cols, "existing_fix_cols": existing}


def step1_backup(con: duckdb.DuckDBPyConnection) -> None:
    banner("STEP 1 — Snapshot backup")
    con.execute(
        """
        CREATE OR REPLACE TABLE canonical_patient_master_pre240_backup AS
        SELECT * FROM canonical_patient_master
        """
    )
    n = con.execute(
        "SELECT COUNT(*) FROM canonical_patient_master_pre240_backup"
    ).fetchone()[0]
    assert n == N_EXPECTED, f"Backup row count wrong: {n}"
    print(f"  canonical_patient_master_pre240_backup rows = {n}")


def step2_issue1_micete(con: duckdb.DuckDBPyConnection) -> dict:
    banner("STEP 2 — Issue 1: Microscopic ETE T3b correction")

    n_mic_t3b = con.execute(
        """
        SELECT COUNT(*) FROM canonical_patient_master
        WHERE ete_grade = 'microscopic' AND ajcc8_t_stage = 'T3b'
        """
    ).fetchone()[0]
    print(f"  microscopic-ETE + T3b cohort = {n_mic_t3b}")

    # Diagnose existing v2 columns for the cohort.
    v2_dist = con.execute(
        """
        SELECT ajcc8_t_stage_v2, COUNT(*) AS n
        FROM canonical_patient_master
        WHERE ete_grade = 'microscopic' AND ajcc8_t_stage = 'T3b'
        GROUP BY 1 ORDER BY 2 DESC
        """
    ).fetchall()
    print(f"  existing ajcc8_t_stage_v2 distribution for cohort: {v2_dist}")

    # Add correction columns (idempotent).
    con.execute(
        "ALTER TABLE canonical_patient_master "
        "ADD COLUMN IF NOT EXISTS ajcc8_t_stage_corrected VARCHAR"
    )
    con.execute(
        "ALTER TABLE canonical_patient_master "
        "ADD COLUMN IF NOT EXISTS ajcc8_stage_group_corrected VARCHAR"
    )
    con.execute(
        "ALTER TABLE canonical_patient_master "
        "ADD COLUMN IF NOT EXISTS microscopic_ete_t3b_corrected BOOLEAN"
    )

    # Reset the flag so UPDATE is deterministic on re-runs.
    con.execute(
        "UPDATE canonical_patient_master SET microscopic_ete_t3b_corrected = FALSE"
    )

    # AJCC 8 DTC rule: re-derive T stage by tumor size for mic-ETE + T3b (DTC only).
    con.execute(
        """
        UPDATE canonical_patient_master
        SET microscopic_ete_t3b_corrected = TRUE,
            ajcc8_t_stage_corrected = CASE
                WHEN tumor_size_cm IS NULL THEN 'T3a'
                WHEN tumor_size_cm <= 1.0 THEN 'T1a'
                WHEN tumor_size_cm <= 2.0 THEN 'T1b'
                WHEN tumor_size_cm <= 4.0 THEN 'T2'
                ELSE 'T3a'
            END
        WHERE ete_grade = 'microscopic'
          AND ajcc8_t_stage = 'T3b'
          AND diagnosis_primary NOT IN ('MTC', 'ATC')
        """
    )

    # Passthrough for patients NOT in the cohort: copy original T into corrected.
    con.execute(
        """
        UPDATE canonical_patient_master
        SET ajcc8_t_stage_corrected = ajcc8_t_stage
        WHERE microscopic_ete_t3b_corrected = FALSE
        """
    )

    # Re-derive stage group for corrected rows using AJCC 8 DTC rules.
    # For corrected (DTC) patients only:
    con.execute(
        """
        UPDATE canonical_patient_master
        SET ajcc8_stage_group_corrected = CASE
            WHEN age_at_surgery < 55 AND ajcc8_m_stage = 'M1' THEN 'II'
            WHEN age_at_surgery < 55 THEN 'I'
            WHEN ajcc8_m_stage = 'M1' THEN 'IVB'
            WHEN ajcc8_t_stage_corrected IN ('T1a','T1b','T2')
                 AND (ajcc8_n_stage IS NULL OR ajcc8_n_stage IN ('N0','N0a','N0b','NX'))
                 THEN 'I'
            WHEN ajcc8_t_stage_corrected IN ('T1a','T1b','T2')
                 AND ajcc8_n_stage IN ('N1','N1a','N1b')
                 THEN 'II'
            WHEN ajcc8_t_stage_corrected IN ('T3a','T3b') THEN 'II'
            WHEN ajcc8_t_stage_corrected = 'T4a' THEN 'III'
            WHEN ajcc8_t_stage_corrected = 'T4b' THEN 'IVA'
            ELSE ajcc8_stage_group
        END
        WHERE microscopic_ete_t3b_corrected = TRUE
        """
    )

    # Passthrough for non-corrected rows.
    con.execute(
        """
        UPDATE canonical_patient_master
        SET ajcc8_stage_group_corrected = ajcc8_stage_group
        WHERE microscopic_ete_t3b_corrected = FALSE
        """
    )

    corrected_t_dist = con.execute(
        """
        SELECT ajcc8_t_stage_corrected, COUNT(*) AS n
        FROM canonical_patient_master
        WHERE microscopic_ete_t3b_corrected = TRUE
        GROUP BY 1 ORDER BY 2 DESC
        """
    ).fetchall()
    print(f"  corrected T-stage distribution: {corrected_t_dist}")

    stage_migration = con.execute(
        """
        SELECT ajcc8_stage_group AS original,
               ajcc8_stage_group_corrected AS corrected,
               COUNT(*) AS n
        FROM canonical_patient_master
        WHERE microscopic_ete_t3b_corrected = TRUE
          AND COALESCE(ajcc8_stage_group,'') <> COALESCE(ajcc8_stage_group_corrected,'')
        GROUP BY 1, 2 ORDER BY 3 DESC
        """
    ).fetchall()
    print(f"  stage group migrations (orig -> corrected): {stage_migration}")

    n_corrected = con.execute(
        "SELECT COUNT(*) FROM canonical_patient_master "
        "WHERE microscopic_ete_t3b_corrected = TRUE"
    ).fetchone()[0]
    print(f"  microscopic_ete_t3b_corrected = TRUE rows: {n_corrected}")

    return {
        "n_mic_t3b": n_mic_t3b,
        "n_corrected": n_corrected,
        "corrected_t_dist": corrected_t_dist,
        "stage_migration": stage_migration,
    }


def step3_issue2_ln_flag(con: duckdb.DuckDBPyConnection) -> dict:
    banner("STEP 3 — Issue 2: ln_positive_flag reconciliation")

    con.execute(
        "ALTER TABLE canonical_patient_master "
        "ADD COLUMN IF NOT EXISTS ln_positive_binary BOOLEAN"
    )
    con.execute(
        "ALTER TABLE canonical_patient_master "
        "ADD COLUMN IF NOT EXISTS ln_positive_count_raw INTEGER"
    )
    con.execute(
        "ALTER TABLE canonical_patient_master "
        "ADD COLUMN IF NOT EXISTS ln_count_reconciled INTEGER"
    )

    n_mismatch = con.execute(
        """
        SELECT COUNT(*) FROM canonical_patient_master
        WHERE ln_positive_flag IS NOT NULL
          AND ln_total_positive IS NOT NULL
          AND ln_positive_flag <> ln_total_positive
        """
    ).fetchone()[0]
    n_gap = con.execute(
        """
        SELECT COUNT(*) FROM canonical_patient_master
        WHERE ln_positive_flag IS NOT NULL AND ln_total_positive IS NULL
        """
    ).fetchone()[0]
    print(f"  mismatches flag<>total: {n_mismatch}")
    print(f"  gap-fill candidates (flag set, total NULL): {n_gap}")

    # Binary flag: TRUE if any positive LN, FALSE if examined but none positive,
    # NULL if no evidence of LN examination at all.
    con.execute(
        """
        UPDATE canonical_patient_master
        SET ln_positive_binary = CASE
            WHEN ln_total_positive > 0 THEN TRUE
            WHEN ln_rollup_total_positive > 0 THEN TRUE
            WHEN ln_rollup_any_positive = TRUE THEN TRUE
            WHEN ln_total_positive = 0 THEN FALSE
            WHEN ln_rollup_total_positive = 0 THEN FALSE
            WHEN ln_rollup_any_positive = FALSE THEN FALSE
            ELSE NULL
        END
        """
    )

    # Preserve the raw count from the misnamed column.
    con.execute(
        """
        UPDATE canonical_patient_master
        SET ln_positive_count_raw = ln_positive_flag
        """
    )

    # Reconciled count: prefer cross-validated rollup, then canonical total,
    # then the raw flag value (ignoring implausible extremes).
    con.execute(
        """
        UPDATE canonical_patient_master
        SET ln_count_reconciled = COALESCE(
            ln_rollup_total_positive,
            ln_total_positive,
            CASE WHEN ln_positive_flag BETWEEN 0 AND 200 THEN ln_positive_flag ELSE NULL END
        )
        """
    )

    summary = con.execute(
        """
        SELECT
            COUNT(*) FILTER (WHERE ln_positive_binary IS NOT NULL) AS binary_filled,
            COUNT(*) FILTER (WHERE ln_positive_binary = TRUE)      AS binary_true,
            COUNT(*) FILTER (WHERE ln_positive_binary = FALSE)     AS binary_false,
            COUNT(*) FILTER (WHERE ln_count_reconciled IS NOT NULL) AS reconciled_filled,
            COUNT(*) FILTER (WHERE ln_positive_count_raw IS NOT NULL) AS raw_filled
        FROM canonical_patient_master
        """
    ).fetchone()
    print(
        f"  binary_filled={summary[0]} true={summary[1]} false={summary[2]} "
        f"reconciled_filled={summary[3]} raw_filled={summary[4]}"
    )
    return {
        "n_mismatch": n_mismatch,
        "n_gap": n_gap,
        "binary_filled": summary[0],
        "binary_true": summary[1],
        "binary_false": summary[2],
        "reconciled_filled": summary[3],
        "raw_filled": summary[4],
    }


def step4_issue3_ene_ln(con: duckdb.DuckDBPyConnection) -> dict:
    banner("STEP 4 — Issue 3: ENE/LN concordance")

    con.execute(
        "ALTER TABLE canonical_patient_master "
        "ADD COLUMN IF NOT EXISTS ene_ln_concordance_status VARCHAR"
    )

    # Reset for deterministic re-run.
    con.execute(
        "UPDATE canonical_patient_master SET ene_ln_concordance_status = NULL"
    )

    con.execute(
        """
        UPDATE canonical_patient_master
        SET ene_ln_concordance_status = CASE
            WHEN ene_positive IS NOT TRUE THEN NULL
            WHEN COALESCE(ln_count_reconciled, ln_total_positive, 0) > 0 THEN 'concordant'
            WHEN (ene_path_synoptic = TRUE OR ene_path_nlp = TRUE)
                THEN 'path_ene_missing_ln_count'
            WHEN (ene_ct = TRUE OR ene_us = TRUE OR ene_pet = TRUE OR ene_rai_scan = TRUE)
                AND ene_path_synoptic IS NOT TRUE
                AND ene_path_nlp IS NOT TRUE
                THEN 'imaging_only_ene'
            WHEN ln_count_reconciled IS NULL AND ln_total_positive IS NULL
                THEN 'ene_positive_ln_unknown'
            ELSE 'discordant_unresolved'
        END
        WHERE ene_positive IS NOT NULL
        """
    )

    # Attempt to recover LN counts from detail tables for path-ENE cases.
    # Cast research_id from source (BIGINT) to VARCHAR.
    recovery_rows = con.execute(
        """
        SELECT COUNT(*) FROM canonical_patient_master c
        WHERE c.ene_ln_concordance_status = 'path_ene_missing_ln_count'
        """
    ).fetchone()[0]
    print(f"  path_ene_missing_ln_count cases: {recovery_rows}")

    if recovery_rows:
        # Recover using ln_master_rollup_v1.ln_total_positive (cross-validated)
        # or tumor_pathology.primary_ln_ln_total_positive / histology_1_ln_positive.
        # Both tables already have research_id as VARCHAR, but we CAST defensively.
        con.execute(
            """
            UPDATE canonical_patient_master AS c
            SET ln_count_reconciled = sub.best_pos
            FROM (
                SELECT CAST(rid AS VARCHAR) AS rid, MAX(best_pos) AS best_pos FROM (
                    SELECT research_id AS rid, ln_total_positive AS best_pos
                    FROM ln_master_rollup_v1
                    WHERE ln_total_positive IS NOT NULL
                    UNION ALL
                    SELECT research_id AS rid,
                           GREATEST(
                               COALESCE(primary_ln_ln_total_positive, 0),
                               COALESCE(histology_1_ln_positive, 0),
                               COALESCE(ln_total_positive_from_locations, 0)
                           ) AS best_pos
                    FROM tumor_pathology
                ) u
                GROUP BY 1
            ) sub
            WHERE sub.rid = c.research_id
              AND c.ene_ln_concordance_status = 'path_ene_missing_ln_count'
              AND sub.best_pos > 0
              AND (c.ln_count_reconciled IS NULL OR c.ln_count_reconciled = 0)
            """
        )

        # Reclassify rows that now have a positive count.
        con.execute(
            """
            UPDATE canonical_patient_master
            SET ene_ln_concordance_status = 'count_recovered'
            WHERE ene_ln_concordance_status = 'path_ene_missing_ln_count'
              AND COALESCE(ln_count_reconciled, 0) > 0
            """
        )

    dist = con.execute(
        """
        SELECT ene_ln_concordance_status, COUNT(*) AS n
        FROM canonical_patient_master
        WHERE ene_positive = TRUE
        GROUP BY 1 ORDER BY 2 DESC
        """
    ).fetchall()
    print(f"  ENE-positive concordance distribution: {dist}")

    return {"concordance_dist": dist, "recovery_candidates": recovery_rows}


def step5_issue4_stage_discordance(con: duckdb.DuckDBPyConnection) -> dict:
    banner("STEP 5 — Issue 4: N1b stage-III discordances")

    con.execute(
        "ALTER TABLE canonical_patient_master "
        "ADD COLUMN IF NOT EXISTS stage_discordance_note VARCHAR"
    )

    rows = con.execute(
        """
        SELECT research_id, age_at_surgery, diagnosis_primary,
               ajcc8_t_stage, ajcc8_n_stage, ajcc8_m_stage, ajcc8_stage_group
        FROM canonical_patient_master
        WHERE age_at_surgery >= 55
          AND ajcc8_n_stage LIKE '%N1b%'
          AND ajcc8_stage_group = 'III'
        """
    ).fetchall()
    print(f"  discordance candidates ({len(rows)}):")
    for r in rows:
        print(f"    {r}")

    # DTC: correct to II. MTC/ATC: keep, annotate non-DTC rules.
    con.execute(
        """
        UPDATE canonical_patient_master
        SET stage_discordance_note = CASE
            WHEN diagnosis_primary IN ('MTC', 'ATC')
                THEN 'non_DTC_staging_rules_apply'
            ELSE 'n1b_incorrectly_upstaged_to_III_corrected_to_II'
        END,
        ajcc8_stage_group_corrected = CASE
            WHEN diagnosis_primary IN ('MTC', 'ATC')
                THEN ajcc8_stage_group_corrected
            ELSE 'II'
        END
        WHERE age_at_surgery >= 55
          AND ajcc8_n_stage LIKE '%N1b%'
          AND ajcc8_stage_group = 'III'
        """
    )

    note_dist = con.execute(
        """
        SELECT stage_discordance_note, COUNT(*) AS n
        FROM canonical_patient_master
        WHERE stage_discordance_note IS NOT NULL
        GROUP BY 1 ORDER BY 2 DESC
        """
    ).fetchall()
    print(f"  stage_discordance_note distribution: {note_dist}")

    return {"n_candidates": len(rows), "note_dist": note_dist}


def step6_issue5_level_impossible(con: duckdb.DuckDBPyConnection) -> dict:
    banner("STEP 6 — Issue 5: Level-specific impossible values")

    con.execute(
        "ALTER TABLE canonical_patient_master "
        "ADD COLUMN IF NOT EXISTS ln_data_quality_flag VARCHAR"
    )
    # Reset so re-runs produce deterministic flags.
    con.execute(
        "UPDATE canonical_patient_master SET ln_data_quality_flag = NULL"
    )

    levels = ["i", "ii", "iii", "iv", "v", "vi", "vii"]
    pre = {}
    for lvl in levels:
        pre[lvl] = con.execute(
            f"""
            SELECT research_id, ln_level_{lvl}_examined, ln_level_{lvl}_positive
            FROM canonical_patient_master
            WHERE ln_level_{lvl}_positive > ln_level_{lvl}_examined
              AND ln_level_{lvl}_examined IS NOT NULL
            """
        ).fetchall()
        if pre[lvl]:
            print(f"  Level {lvl.upper()} impossible cases: {pre[lvl]}")

    # Fix the Level II transposition. DuckDB evaluates SET RHS before assignment
    # commits, so column1 = column2, column2 = column1 is a safe atomic swap.
    con.execute(
        """
        UPDATE canonical_patient_master
        SET ln_level_ii_positive = ln_level_ii_examined,
            ln_level_ii_examined = ln_level_ii_positive,
            ln_data_quality_flag = 'level_ii_transposition_corrected'
        WHERE ln_level_ii_positive > ln_level_ii_examined
          AND ln_level_ii_examined IS NOT NULL
        """
    )

    # Scan other levels — should be none per audit, but fix if present.
    for lvl in [x for x in levels if x != "ii"]:
        still = con.execute(
            f"""
            SELECT COUNT(*) FROM canonical_patient_master
            WHERE ln_level_{lvl}_positive > ln_level_{lvl}_examined
              AND ln_level_{lvl}_examined IS NOT NULL
            """
        ).fetchone()[0]
        if still:
            print(f"  Level {lvl.upper()} still has {still} cases — flagging")
            con.execute(
                f"""
                UPDATE canonical_patient_master
                SET ln_data_quality_flag = COALESCE(ln_data_quality_flag || '; ','')
                    || 'level_{lvl}_positive_gt_examined'
                WHERE ln_level_{lvl}_positive > ln_level_{lvl}_examined
                  AND ln_level_{lvl}_examined IS NOT NULL
                """
            )

    # Global: positive counts without exam counts.
    con.execute(
        """
        UPDATE canonical_patient_master
        SET ln_data_quality_flag = COALESCE(ln_data_quality_flag || '; ','')
            || 'positive_without_exam_count'
        WHERE ln_total_positive > 0
          AND (ln_total_examined IS NULL OR ln_total_examined = 0)
        """
    )

    # Verify Level II fix (use post-swap audit).
    post_ii = con.execute(
        """
        SELECT COUNT(*) FROM canonical_patient_master
        WHERE ln_level_ii_positive > ln_level_ii_examined
          AND ln_level_ii_examined IS NOT NULL
        """
    ).fetchone()[0]
    print(f"  Level II impossible cases after fix: {post_ii}")
    assert post_ii == 0, f"Level II fix failed ({post_ii} remain)"

    flag_dist = con.execute(
        """
        SELECT ln_data_quality_flag, COUNT(*) AS n
        FROM canonical_patient_master
        WHERE ln_data_quality_flag IS NOT NULL
        GROUP BY 1 ORDER BY 2 DESC
        """
    ).fetchall()
    print(f"  ln_data_quality_flag distribution: {flag_dist}")

    return {
        "pre_level_issues": {k: len(v) for k, v in pre.items()},
        "flag_dist": flag_dist,
    }


def step7_data_dictionary(con: duckdb.DuckDBPyConnection) -> dict:
    banner("STEP 7 — Data dictionary v240")

    new_col_descriptions = {
        "ajcc8_t_stage_corrected": (
            "AJCC 8th T-stage with microscopic ETE correction "
            "(mic ETE no longer upstages to T3b; falls through tumor-size rules)"
        ),
        "ajcc8_stage_group_corrected": (
            "AJCC 8th stage group re-derived after T-stage correction; "
            "also applied to the 8 N1b+age>=55 DTC discordances"
        ),
        "microscopic_ete_t3b_corrected": (
            "Boolean: TRUE if patient had microscopic ETE wrongly staged as T3b "
            "(DTC only) and now re-derived by AJCC 8th DTC rules"
        ),
        "ln_positive_binary": (
            "Clean boolean: TRUE if any positive LN, FALSE if examined but none "
            "positive, NULL if not examined or unknown"
        ),
        "ln_positive_count_raw": (
            "Raw count from original ln_positive_flag column "
            "(misnamed as flag; range 0-51)"
        ),
        "ln_count_reconciled": (
            "Best available positive LN count: coalesces "
            "ln_rollup_total_positive > ln_total_positive > "
            "ln_positive_flag (bounded)"
        ),
        "ene_ln_concordance_status": (
            "ENE/LN concordance: concordant | count_recovered | "
            "path_ene_missing_ln_count | imaging_only_ene | "
            "ene_positive_ln_unknown | discordant_unresolved"
        ),
        "stage_discordance_note": (
            "Explanation for 8 N1b+age>=55 stage-III discordances "
            "(non-DTC rules vs DTC correction to Stage II)"
        ),
        "ln_data_quality_flag": (
            "LN data quality annotations (e.g. level_ii_transposition_corrected, "
            "positive_without_exam_count)"
        ),
    }

    con.execute("DROP TABLE IF EXISTS data_dictionary_v240")
    con.execute(
        """
        CREATE TABLE data_dictionary_v240 (
            column_name VARCHAR,
            data_type VARCHAR,
            ordinal_position INTEGER,
            n_non_null BIGINT,
            pct_non_null DOUBLE,
            n_distinct BIGINT,
            description VARCHAR
        )
        """
    )

    cols = con.execute(
        f"""
        SELECT column_name, data_type, ordinal_position
        FROM information_schema.columns
        WHERE table_catalog = '{DB}'
          AND table_name = 'canonical_patient_master'
        ORDER BY ordinal_position
        """
    ).fetchall()
    print(f"  dictionary entries to write: {len(cols)}")

    rows_to_insert = []
    for name, dtype, ordinal in cols:
        stats = con.execute(
            f'''
            SELECT COUNT(*) FILTER (WHERE "{name}" IS NOT NULL) AS nn,
                   COUNT(DISTINCT "{name}") AS nd
            FROM canonical_patient_master
            '''
        ).fetchone()
        nn, nd = stats[0], stats[1]
        pct = round(nn * 100.0 / N_EXPECTED, 2)
        desc = new_col_descriptions.get(name, "")
        rows_to_insert.append((name, dtype, ordinal, nn, pct, nd, desc))

    con.executemany(
        "INSERT INTO data_dictionary_v240 VALUES (?, ?, ?, ?, ?, ?, ?)",
        rows_to_insert,
    )

    n = con.execute("SELECT COUNT(*) FROM data_dictionary_v240").fetchone()[0]
    print(f"  data_dictionary_v240 row count: {n}")
    return {"dd_rows": n}


def step8_verify_views(con: duckdb.DuckDBPyConnection) -> dict:
    banner("STEP 8 — Drill-down registry + cohort views verification")

    registry_issues: list[tuple[str, str, str]] = []
    try:
        registry = con.execute(
            """
            SELECT detail_table_name, schema_name, total_rows
            FROM manuscript_workspace.detail_table_registry_v1
            """
        ).fetchall()
    except Exception as exc:  # noqa: BLE001
        print(f"  WARNING: detail_table_registry_v1 unavailable: {exc}")
        registry = []

    print(f"  registry rows: {len(registry)}")
    for table_name, schema_name, total_rows in registry:
        fq = f"{schema_name}.{table_name}" if schema_name else table_name
        try:
            actual = con.execute(f"SELECT COUNT(*) FROM {fq}").fetchone()[0]
            if total_rows is not None and actual != total_rows:
                msg = f"row count changed registry={total_rows} actual={actual}"
                registry_issues.append((fq, msg, ""))
                print(f"    WARN {fq}: {msg}")
        except Exception as exc:  # noqa: BLE001
            registry_issues.append((fq, "missing_or_error", str(exc)))
            print(f"    ERR  {fq}: {exc}")

    try:
        pointer_n = con.execute(
            "SELECT COUNT(*) FROM manuscript_workspace.canonical_detail_pointer_v1"
        ).fetchone()[0]
        print(f"  canonical_detail_pointer_v1 rows: {pointer_n}")
    except Exception as exc:  # noqa: BLE001
        pointer_n = None
        print(f"  canonical_detail_pointer_v1: {exc}")

    cohort_views = [
        r[0]
        for r in con.execute(
            f"""
            SELECT table_name FROM information_schema.tables
            WHERE table_catalog = '{DB}'
              AND table_schema = 'manuscript_workspace'
              AND table_name LIKE 'cohort_%'
            """
        ).fetchall()
    ]
    print(f"  cohort_* views to test: {len(cohort_views)}")

    broken = []
    for v in cohort_views:
        try:
            con.execute(f"SELECT COUNT(*) FROM manuscript_workspace.{v}").fetchone()
        except Exception as exc:  # noqa: BLE001
            broken.append((v, str(exc)))
            print(f"    BROKEN: {v} -- {exc}")

    print(
        f"  cohort views: {len(cohort_views) - len(broken)}/{len(cohort_views)} healthy"
    )
    return {
        "registry_issues": registry_issues,
        "pointer_rows": pointer_n,
        "cohort_views": len(cohort_views),
        "broken_views": broken,
    }


def step9_stale_objects(con: duckdb.DuckDBPyConnection) -> dict:
    banner("STEP 9 — Stale-object check")
    info: dict = {}
    try:
        v1_count = con.execute(
            "SELECT COUNT(*) FROM canonical_patient_master_v1"
        ).fetchone()[0]
        print(f"  WARNING: canonical_patient_master_v1 exists ({v1_count} rows)")
        info["v1_exists"] = True
        info["v1_count"] = v1_count
    except Exception:  # noqa: BLE001
        print("  OK: canonical_patient_master_v1 not present in publication DB")
        info["v1_exists"] = False
    return info


def step10_final_validation(con: duckdb.DuckDBPyConnection) -> dict:
    banner("STEP 10 — Final validation battery")
    check_invariants(con, "final")

    still_mic_t3b = con.execute(
        """
        SELECT COUNT(*) FROM canonical_patient_master
        WHERE ete_grade = 'microscopic'
          AND ajcc8_t_stage_corrected = 'T3b'
          AND diagnosis_primary NOT IN ('MTC', 'ATC')
        """
    ).fetchone()[0]
    print(f"  mic-ETE still T3b in corrected column (DTC only): {still_mic_t3b}")
    assert still_mic_t3b == 0, "Microscopic ETE T3b correction incomplete"

    for lvl in ["i", "ii", "iii", "iv", "v", "vi", "vii"]:
        bad = con.execute(
            f"""
            SELECT COUNT(*) FROM canonical_patient_master
            WHERE ln_level_{lvl}_positive > ln_level_{lvl}_examined
              AND ln_level_{lvl}_examined IS NOT NULL
            """
        ).fetchone()[0]
        assert bad == 0, f"Level {lvl.upper()} still has impossible values"

    n_final = column_count(con)
    print(f"  final column count: {n_final}")

    coverage = {}
    for col in [
        "ajcc8_t_stage_corrected",
        "ajcc8_stage_group_corrected",
        "microscopic_ete_t3b_corrected",
        "ln_positive_binary",
        "ln_positive_count_raw",
        "ln_count_reconciled",
        "ene_ln_concordance_status",
        "stage_discordance_note",
        "ln_data_quality_flag",
    ]:
        if column_exists(con, col):
            n = con.execute(
                f'SELECT COUNT(*) FROM canonical_patient_master WHERE "{col}" IS NOT NULL'
            ).fetchone()[0]
            pct = n * 100.0 / N_EXPECTED
            coverage[col] = (n, pct)
            print(f"  {col}: {n} non-null ({pct:.1f}%)")
        else:
            coverage[col] = None
            print(f"  {col}: MISSING")

    return {"final_cols": n_final, "coverage": coverage, "still_mic_t3b": still_mic_t3b}


def step11_report(
    con: duckdb.DuckDBPyConnection,
    s0: dict,
    s2: dict,
    s3: dict,
    s4: dict,
    s5: dict,
    s6: dict,
    s7: dict,
    s8: dict,
    s9: dict,
    s10: dict,
) -> None:
    banner("STEP 11 — Summary report")
    report = f"""# Script 240 — LN/Staging Data Quality Remediation Report

Generated: {datetime.utcnow().isoformat(timespec='seconds')}Z
Database: `{DB}` on MotherDuck

## Pre-run state
- canonical_patient_master: 10,871 × {s0['n_cols_pre']} columns
- Pre-existing fix columns: {sorted(s0['existing_fix_cols']) or 'none'}

## Issue 1 — Microscopic ETE T3b correction
- cohort: {s2['n_mic_t3b']} patients
- corrected rows: {s2['n_corrected']}
- corrected T-stage distribution: `{s2['corrected_t_dist']}`
- stage-group migrations (original -> corrected): `{s2['stage_migration']}`
- New columns: `ajcc8_t_stage_corrected`, `ajcc8_stage_group_corrected`, `microscopic_ete_t3b_corrected`

## Issue 2 — ln_positive_flag reconciliation
- flag<>total mismatches: {s3['n_mismatch']}
- gap-fill candidates (flag set, total NULL): {s3['n_gap']}
- `ln_positive_binary` filled: {s3['binary_filled']} (TRUE={s3['binary_true']}, FALSE={s3['binary_false']})
- `ln_count_reconciled` filled: {s3['reconciled_filled']}
- `ln_positive_count_raw` filled: {s3['raw_filled']}
- New columns: `ln_positive_binary`, `ln_positive_count_raw`, `ln_count_reconciled`

## Issue 3 — ENE/LN concordance
- concordance distribution among ENE-positive:
  `{s4['concordance_dist']}`
- New column: `ene_ln_concordance_status`

## Issue 4 — Stage discordance (N1b + age>=55 at Stage III)
- candidates: {s5['n_candidates']}
- note distribution: `{s5['note_dist']}`
- New column: `stage_discordance_note`

## Issue 5 — Level-specific impossible values
- pre-run counts by level: `{s6['pre_level_issues']}`
- ln_data_quality_flag distribution: `{s6['flag_dist']}`
- New column: `ln_data_quality_flag`

## Data dictionary
- data_dictionary_v240 entries: {s7['dd_rows']}

## Registry + cohort views
- cohort views tested: {s8['cohort_views']}
- broken views: {len(s8['broken_views'])}
- registry issues: {len(s8['registry_issues'])}
- canonical_detail_pointer_v1 rows: {s8['pointer_rows']}

## Stale objects
- canonical_patient_master_v1 in publication DB: {s9.get('v1_exists')}

## Final state
- canonical_patient_master: 10,871 × {s10['final_cols']} columns
- All invariants pass
- mic-ETE still T3b (DTC, corrected col): {s10['still_mic_t3b']}
- New-column coverage:
"""
    for col, cov in s10["coverage"].items():
        if cov is None:
            report += f"  - {col}: MISSING\n"
        else:
            report += f"  - {col}: {cov[0]} ({cov[1]:.1f}%)\n"

    REPORT_PATH.write_text(report)
    print(f"  wrote {REPORT_PATH}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    token = get_token()
    con = duckdb.connect(f"md:{DB}?motherduck_token={token}")
    print(f"Connected to {DB}")

    s0 = step0_preflight(con)
    step1_backup(con)

    s2 = step2_issue1_micete(con)
    check_invariants(con, "after-issue1")

    s3 = step3_issue2_ln_flag(con)
    check_invariants(con, "after-issue2")

    s4 = step4_issue3_ene_ln(con)
    check_invariants(con, "after-issue3")

    s5 = step5_issue4_stage_discordance(con)
    check_invariants(con, "after-issue4")

    s6 = step6_issue5_level_impossible(con)
    check_invariants(con, "after-issue5")

    s7 = step7_data_dictionary(con)
    s8 = step8_verify_views(con)
    s9 = step9_stale_objects(con)
    s10 = step10_final_validation(con)

    step11_report(con, s0, s2, s3, s4, s5, s6, s7, s8, s9, s10)

    banner("SCRIPT 240 COMPLETE")


if __name__ == "__main__":
    main()
