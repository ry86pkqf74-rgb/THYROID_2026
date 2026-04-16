"""Script 235 — Parathyroid & Calcium/PTH Data Quality Fix + Canonical Finalization.

Database: ``thyroid_canonical_publication_v1_0`` on MotherDuck (AUTHORITATIVE
reads + writes). Fixes five data quality issues from the Prompt 6 audit:

1. ``lab_calcium_min`` / ``postop_calcium_min_value`` contain unit-contaminated
   values (pg/mL mislabels + decimal-point errors pushing mean to ~98 mg/dL).
   Root source: ``extracted_postop_labs_expanded_v1``. This script adds a
   ``value_corrected`` column with the normalized mg/dL value and rebuilds
   the canonical calcium rollups.

2. ``has_low_calcium_flag`` is NEVER TRUE despite calcium < 8.5 patients
   existing. The derivation rule is re-applied from corrected calcium.
   Similarly ``biochemical_low_ca`` in ``complication_phenotype_v1``.

3. NSQIP says ``nsqip_hypocalcemia = 'Yes'`` for 82 patients but canonical
   captured only 2. The remaining 80 are recovered with provenance
   (``nsqip_hypocalcemia_recovered_flag``, ``evidence_tier = 'nsqip_registry'``).

4. Zero permanent hypoparathyroidism despite 4,561 thyroidectomies. Script
   attempts biochemistry-based recovery and documents the remaining gap as a
   cohort limitation (short PTH follow-up, 1.5 %% lab coverage).

5. Downstream ``complication_phenotype_v1`` and
   ``complication_patient_summary_v1`` rebuilt with corrected biochemical
   flags, NSQIP recovery, and corrected calcium nadirs.

Idempotent: ALTER ADD COLUMN IF NOT EXISTS; deterministic UPDATE / CREATE OR
REPLACE. Preserves original values in backup tables for full provenance.
"""

from __future__ import annotations

import csv
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

DB = "thyroid_canonical_publication_v1_0"
N_EXPECTED = 10_871


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def banner(text: str) -> None:
    print("\n" + "=" * 78)
    print(text)
    print("=" * 78)


def check_invariants(con: duckdb.DuckDBPyConnection, label: str = "") -> None:
    row = con.execute(
        """
        SELECT COUNT(*),
               COUNT(DISTINCT research_id),
               COUNT(*) FILTER (WHERE research_id IS NULL),
               COUNT(*) FILTER (WHERE fna_path_outcome IS NULL)
        FROM canonical_patient_master
        """
    ).fetchone()
    print(
        f"  [invariants {label}] total={row[0]} distinct_rid={row[1]} "
        f"null_rid={row[2]} null_fna={row[3]}"
    )
    assert row == (N_EXPECTED, N_EXPECTED, 0, 0), f"INVARIANT BROKEN @ {label}: {row}"


def column_exists(con: duckdb.DuckDBPyConnection, table: str, col: str) -> bool:
    return (
        con.execute(
            """
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_catalog = ? AND table_name = ? AND column_name = ?
            """,
            [DB, table, col],
        ).fetchone()[0]
        > 0
    )


def fetch_to_csv(con: duckdb.DuckDBPyConnection, sql: str, path: Path) -> int:
    rows = con.execute(sql).fetchall()
    cols = [d[0] for d in con.description] if con.description else []
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        if cols:
            w.writerow(cols)
        w.writerows(rows)
    return len(rows)


# ---------------------------------------------------------------------------
# Phase 0 — preflight + backup
# ---------------------------------------------------------------------------


def step0_preflight(con: duckdb.DuckDBPyConnection) -> dict:
    banner("STEP 0 — Preflight")
    db_name = con.execute("SELECT current_database()").fetchone()[0]
    assert db_name == DB, f"wrong DB attached: {db_name}"
    print(f"  database = {db_name}")
    check_invariants(con, "preflight")

    cpm_cols = con.execute(
        """
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_catalog = ? AND table_name = 'canonical_patient_master'
        """,
        [DB],
    ).fetchone()[0]
    print(f"  canonical_patient_master columns: {cpm_cols}")
    return {"cpm_cols_pre": cpm_cols}


def step1_backup(con: duckdb.DuckDBPyConnection) -> None:
    banner("STEP 1 — Snapshot backups")
    # IMPORTANT: only create backup if it doesn't already exist. This preserves
    # the TRUE pre-fix snapshot across idempotent re-runs (subsequent runs
    # see post-fix state as "source" and would otherwise lose the provenance).
    for src, backup in [
        ("canonical_patient_master", "canonical_patient_master_pre235_backup"),
        ("complication_phenotype_v1", "complication_phenotype_v1_pre235_backup"),
        (
            "complication_patient_summary_v1",
            "complication_patient_summary_v1_pre235_backup",
        ),
        (
            "extracted_postop_labs_expanded_v1",
            "extracted_postop_labs_expanded_v1_pre235_backup",
        ),
        (
            "longitudinal_lab_canonical_v1",
            "longitudinal_lab_canonical_v1_pre235_backup",
        ),
    ]:
        exists = con.execute(
            """
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_catalog = ? AND table_schema = 'main' AND table_name = ?
            """,
            [DB, backup],
        ).fetchone()[0]
        if exists == 0:
            con.execute(f'CREATE TABLE "{backup}" AS SELECT * FROM "{src}"')
            status = "created"
        else:
            status = "preserved"
        n = con.execute(f'SELECT COUNT(*) FROM "{backup}"').fetchone()[0]
        print(f"  {backup}: {n} ({status})")


# ---------------------------------------------------------------------------
# Phase 1 — calcium unit normalization
# ---------------------------------------------------------------------------


def step2_calcium_audit(con: duckdb.DuckDBPyConnection) -> dict:
    banner("STEP 2 — Phase 1.1: Calcium contamination audit (from pre-fix backup)")

    # Always read pre-fix state from the preserved backup so audit numbers
    # remain accurate even on idempotent re-runs against the fixed DB.
    audit_sql = """
    SELECT
      c.research_id,
      c.lab_calcium_min,
      c.lab_calcium_max,
      c.lab_calcium_n_measurements,
      c.postop_calcium_min_value,
      c.postop_calcium_n_measurements,
      c.calcium_nadir_30d,
      e.value       AS extracted_value,
      e.unit        AS extracted_unit,
      e.lab_type    AS extracted_lab_type,
      e.source_reliability
    FROM canonical_patient_master_pre235_backup c
    LEFT JOIN extracted_postop_labs_expanded_v1_pre235_backup e
      ON CAST(c.research_id AS VARCHAR) = CAST(e.research_id AS VARCHAR)
     AND e.lab_type = 'total_calcium'
    WHERE c.lab_calcium_min > 20
       OR c.postop_calcium_min_value > 20
    ORDER BY c.lab_calcium_min DESC NULLS LAST, c.research_id
    """
    out = OUTPUT_DIR / "235_calcium_contamination_audit.csv"
    n = fetch_to_csv(con, audit_sql, out)
    print(f"  wrote {out} ({n} rows)")

    pre_stats = con.execute(
        """
        SELECT
          COUNT(*) FILTER (WHERE lab_calcium_min IS NOT NULL)   AS n_cpm_ca,
          COUNT(*) FILTER (WHERE lab_calcium_min > 20)          AS n_cpm_gt20,
          COUNT(*) FILTER (WHERE lab_calcium_min > 50)          AS n_cpm_gt50,
          ROUND(AVG(lab_calcium_min), 2)                        AS mean_cpm,
          ROUND(MAX(lab_calcium_min), 2)                        AS max_cpm,
          COUNT(*) FILTER (WHERE postop_calcium_min_value > 20) AS n_postop_gt20,
          ROUND(AVG(postop_calcium_min_value), 2)               AS mean_postop,
          ROUND(MAX(postop_calcium_min_value), 2)               AS max_postop
        FROM canonical_patient_master_pre235_backup
        """
    ).fetchone()
    print(f"  pre-fix canonical stats (from backup): {pre_stats}")
    return {"audit_rows": n, "pre_stats": pre_stats}


def _correction_case_expr(value_expr: str, unit_expr: str) -> str:
    """Return SQL CASE expression producing corrected_value.

    Rules (applied to any calcium source — mg/dL / pg/mL / mmol/L / NULL unit):

    * pg/mL of any value          -> NULL (mislabeled PTH; don't trust even low values)
    * mmol/L between 1.0 and 3.0  -> * 4.008 (convert to mg/dL)
    * value >= 100                -> / 100  (decimal-point error)
    * value >= 20  and < 100      -> / 10
    * value in [4.0, 15.0]        -> keep
    * else                        -> NULL (unrecoverable)
    """
    return f"""
        CASE
          WHEN {unit_expr} = 'pg/mL'                                  THEN NULL
          WHEN {unit_expr} = 'mmol/L' AND {value_expr} BETWEEN 1.0 AND 3.0
            THEN {value_expr} * 4.008
          WHEN {value_expr} >= 100                                    THEN {value_expr} / 100.0
          WHEN {value_expr} >= 20                                     THEN {value_expr} / 10.0
          WHEN {value_expr} BETWEEN 4.0 AND 15.0                      THEN {value_expr}
          WHEN {value_expr} IS NULL                                   THEN NULL
          ELSE NULL
        END
    """


def _correction_label_expr(value_expr: str, unit_expr: str) -> str:
    return f"""
        CASE
          WHEN {unit_expr} = 'pg/mL'                                  THEN 'nulled_pg_ml'
          WHEN {unit_expr} = 'mmol/L' AND {value_expr} BETWEEN 1.0 AND 3.0
            THEN 'mmol_to_mg_x4_008'
          WHEN {value_expr} >= 100                                    THEN 'divided_by_100'
          WHEN {value_expr} >= 20                                     THEN 'divided_by_10'
          WHEN {value_expr} BETWEEN 4.0 AND 15.0                      THEN 'no_change'
          WHEN {value_expr} IS NULL                                   THEN 'source_null'
          ELSE 'nulled_unrecoverable'
        END
    """


def step3_calcium_corrections(con: duckdb.DuckDBPyConnection) -> dict:
    banner("STEP 3 — Phase 1.2/1.3: Calcium unit corrections (both source tables)")

    # --- Source 1: extracted_postop_labs_expanded_v1 --------------------------
    ext_val_corr = _correction_case_expr("e.value", "e.unit")
    ext_lbl_corr = _correction_label_expr("e.value", "e.unit")

    base_cols = [
        r[0]
        for r in con.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_catalog = ? AND table_name = 'extracted_postop_labs_expanded_v1'
              AND column_name NOT IN ('value_corrected', 'calcium_correction_applied')
            ORDER BY ordinal_position
            """,
            [DB],
        ).fetchall()
    ]
    base_select = ", ".join(f'e."{c}"' for c in base_cols)

    con.execute(
        f"""
        CREATE OR REPLACE TABLE extracted_postop_labs_expanded_v1 AS
        SELECT
          {base_select},
          CASE
            WHEN e.lab_type = 'total_calcium' THEN ({ext_val_corr})
            ELSE e.value
          END AS value_corrected,
          CASE
            WHEN e.lab_type = 'total_calcium' THEN ({ext_lbl_corr})
            ELSE 'non_calcium'
          END AS calcium_correction_applied
        FROM extracted_postop_labs_expanded_v1 e
        """
    )

    ext_rule_dist = con.execute(
        """
        SELECT calcium_correction_applied, COUNT(*)
        FROM extracted_postop_labs_expanded_v1
        WHERE lab_type = 'total_calcium'
        GROUP BY 1 ORDER BY 2 DESC
        """
    ).fetchall()
    print(f"  extracted_postop_labs calcium correction distribution: {ext_rule_dist}")

    ext_post = con.execute(
        """
        SELECT ROUND(MIN(value_corrected), 3), ROUND(MAX(value_corrected), 3),
               ROUND(AVG(value_corrected), 3),
               COUNT(*) FILTER (WHERE value_corrected IS NOT NULL),
               COUNT(*) FILTER (WHERE value_corrected > 15 OR value_corrected < 4)
        FROM extracted_postop_labs_expanded_v1
        WHERE lab_type = 'total_calcium'
        """
    ).fetchone()
    print(f"  extracted post-fix calcium (min/max/avg/n_nn/out_of_range): {ext_post}")

    # --- Source 2: longitudinal_lab_canonical_v1 (unit is NULL for calcium) ---
    ext_val2 = _correction_case_expr("l.value_numeric", "l.unit_standardized")
    ext_lbl2 = _correction_label_expr("l.value_numeric", "l.unit_standardized")

    long_cols = [
        r[0]
        for r in con.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_catalog = ? AND table_name = 'longitudinal_lab_canonical_v1'
              AND column_name NOT IN ('value_corrected', 'calcium_correction_applied')
            ORDER BY ordinal_position
            """,
            [DB],
        ).fetchall()
    ]
    long_select = ", ".join(f'l."{c}"' for c in long_cols)

    con.execute(
        f"""
        CREATE OR REPLACE TABLE longitudinal_lab_canonical_v1 AS
        SELECT
          {long_select},
          CASE
            WHEN l.lab_name_standardized = 'calcium' THEN ({ext_val2})
            ELSE l.value_numeric
          END AS value_corrected,
          CASE
            WHEN l.lab_name_standardized = 'calcium' THEN ({ext_lbl2})
            ELSE 'non_calcium'
          END AS calcium_correction_applied
        FROM longitudinal_lab_canonical_v1 l
        """
    )

    long_rule_dist = con.execute(
        """
        SELECT calcium_correction_applied, COUNT(*)
        FROM longitudinal_lab_canonical_v1
        WHERE lab_name_standardized = 'calcium'
        GROUP BY 1 ORDER BY 2 DESC
        """
    ).fetchall()
    print(f"  longitudinal_lab calcium correction distribution: {long_rule_dist}")

    long_post = con.execute(
        """
        SELECT ROUND(MIN(value_corrected), 3), ROUND(MAX(value_corrected), 3),
               ROUND(AVG(value_corrected), 3),
               COUNT(*) FILTER (WHERE value_corrected IS NOT NULL),
               COUNT(*) FILTER (WHERE value_corrected > 15 OR value_corrected < 4)
        FROM longitudinal_lab_canonical_v1
        WHERE lab_name_standardized = 'calcium'
        """
    ).fetchone()
    print(f"  longitudinal post-fix calcium (min/max/avg/n_nn/out_of_range): {long_post}")

    # --- Combined audit CSV (patient-level contamination summary) ------------
    out = OUTPUT_DIR / "235_calcium_corrections_applied.csv"
    fetch_to_csv(
        con,
        f"""
        SELECT 'extracted_postop_labs' AS source,
               CAST(research_id AS VARCHAR) AS research_id,
               lab_type AS lab_name, value AS original_value,
               unit AS original_unit, value_corrected, calcium_correction_applied
        FROM extracted_postop_labs_expanded_v1
        WHERE lab_type = 'total_calcium'
          AND calcium_correction_applied NOT IN ('no_change', 'source_null', 'non_calcium')
        UNION ALL
        SELECT 'longitudinal_lab',
               CAST(research_id AS VARCHAR),
               lab_name_standardized, value_numeric,
               unit_standardized, value_corrected, calcium_correction_applied
        FROM longitudinal_lab_canonical_v1
        WHERE lab_name_standardized = 'calcium'
          AND calcium_correction_applied NOT IN ('no_change', 'source_null', 'non_calcium')
        ORDER BY source, original_value DESC NULLS LAST
        """,
        out,
    )
    print(f"  wrote {out}")

    # Staging table with combined corrections for provenance/audit.
    con.execute(
        """
        CREATE OR REPLACE TABLE _calcium_corrections_v235 AS
        SELECT 'extracted_postop_labs' AS source,
               CAST(research_id AS VARCHAR) AS research_id,
               value AS original_value, unit AS original_unit,
               value_corrected, calcium_correction_applied
        FROM extracted_postop_labs_expanded_v1
        WHERE lab_type = 'total_calcium'
          AND calcium_correction_applied NOT IN ('no_change', 'source_null', 'non_calcium')
        UNION ALL
        SELECT 'longitudinal_lab', CAST(research_id AS VARCHAR),
               value_numeric, unit_standardized, value_corrected, calcium_correction_applied
        FROM longitudinal_lab_canonical_v1
        WHERE lab_name_standardized = 'calcium'
          AND calcium_correction_applied NOT IN ('no_change', 'source_null', 'non_calcium')
        """
    )

    return {
        "ext_rule_dist": ext_rule_dist,
        "long_rule_dist": long_rule_dist,
        "ext_post": ext_post,
        "long_post": long_post,
    }


def step4_rederive_canonical_calcium(con: duckdb.DuckDBPyConnection) -> dict:
    banner("STEP 4 — Phase 1.4: Surgical patient-targeted canonical fix")

    # Identify contaminated patients in canonical (from ANY source).
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE _ca_contaminated_rids AS
        SELECT DISTINCT research_id
        FROM canonical_patient_master
        WHERE lab_calcium_min > 15.0
           OR lab_calcium_max > 15.0
           OR lab_calcium_most_recent > 15.0
           OR postop_calcium_min_value > 15.0
           OR calcium_nadir > 15.0
           OR calcium_nadir_30d > 15.0
        """
    )
    n_contam = con.execute("SELECT COUNT(*) FROM _ca_contaminated_rids").fetchone()[0]
    print(f"  contaminated canonical patients (any calcium col > 15): {n_contam}")

    # Build a per-patient corrected rollup across BOTH source tables.
    # Uses value_corrected (clamped to [4.0, 15.0] or NULL).
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE _ca_combined_rollup AS
        WITH ext AS (
          SELECT
            CAST(research_id AS VARCHAR) AS research_id,
            value_corrected AS val,
            days_postop,
            source_reliability
          FROM extracted_postop_labs_expanded_v1
          WHERE lab_type = 'total_calcium'
            AND value_corrected IS NOT NULL
        ),
        longi AS (
          SELECT
            CAST(research_id AS VARCHAR) AS research_id,
            value_corrected AS val,
            NULL::BIGINT AS days_postop,
            NULL::DOUBLE AS source_reliability
          FROM longitudinal_lab_canonical_v1
          WHERE lab_name_standardized = 'calcium'
            AND value_corrected IS NOT NULL
        ),
        all_ca AS (SELECT * FROM ext UNION ALL SELECT * FROM longi)
        SELECT
          research_id,
          MIN(val)                                                AS ca_min,
          MAX(val)                                                AS ca_max,
          COUNT(val)                                              AS ca_n,
          MIN(val) FILTER (WHERE days_postop BETWEEN 0 AND 30)    AS ca_min_30d,
          MIN(val) FILTER (WHERE days_postop >= 0)                AS ca_postop_min,
          COUNT(val) FILTER (WHERE days_postop >= 0)              AS ca_postop_n,
          MAX(source_reliability) FILTER (WHERE days_postop >= 0) AS ca_src_rel
        FROM all_ca
        GROUP BY research_id
        """
    )
    n_roll = con.execute(
        """
        SELECT COUNT(*) FROM _ca_combined_rollup
         WHERE research_id IN (SELECT research_id FROM _ca_contaminated_rids)
        """
    ).fetchone()[0]
    print(f"  contaminated patients with recoverable data: {n_roll}")
    print(f"  unrecoverable (no corrected source value): {n_contam - n_roll}")

    # STAGE A — for contaminated patients WITH recoverable data:
    # replace only if rollup value is present; preserve original coverage.
    con.execute(
        """
        UPDATE canonical_patient_master AS c
           SET lab_calcium_min = COALESCE(r.ca_min, c.lab_calcium_min),
               lab_calcium_max = CASE
                 WHEN c.lab_calcium_max IS NOT NULL AND c.lab_calcium_max > 15
                   THEN r.ca_max
                 WHEN c.lab_calcium_max IS NULL
                   THEN r.ca_max
                 ELSE c.lab_calcium_max
               END,
               lab_calcium_most_recent = CASE
                 WHEN c.lab_calcium_most_recent > 15 THEN r.ca_max
                 ELSE c.lab_calcium_most_recent
               END,
               postop_calcium_min_value = COALESCE(r.ca_postop_min, c.postop_calcium_min_value),
               postop_calcium_n_measurements = COALESCE(CAST(r.ca_postop_n AS BIGINT),
                                                        c.postop_calcium_n_measurements),
               postop_calcium_source_reliability = COALESCE(CAST(r.ca_src_rel AS VARCHAR),
                                                            c.postop_calcium_source_reliability),
               calcium_nadir_30d = COALESCE(r.ca_min_30d, c.calcium_nadir_30d),
               calcium_nadir = COALESCE(r.ca_min, c.calcium_nadir)
          FROM _ca_combined_rollup r
         WHERE c.research_id = r.research_id
           AND c.research_id IN (SELECT research_id FROM _ca_contaminated_rids)
        """
    )

    # STAGE B — final safety clamp: any calcium field still > 15 or < 4 gets NULLed.
    # This catches patients with no recoverable source data (parsed_from_raw only).
    for col in (
        "lab_calcium_min",
        "lab_calcium_max",
        "lab_calcium_most_recent",
        "postop_calcium_min_value",
        "calcium_nadir",
        "calcium_nadir_30d",
    ):
        con.execute(
            f"""
            UPDATE canonical_patient_master
               SET "{col}" = NULL
             WHERE "{col}" IS NOT NULL
               AND ("{col}" > 15.0 OR "{col}" < 4.0)
            """
        )

    # Final post-fix distribution.
    post = con.execute(
        """
        SELECT
          COUNT(*) FILTER (WHERE lab_calcium_min IS NOT NULL) AS n_ca,
          COUNT(*) FILTER (WHERE lab_calcium_min > 15)        AS gt15,
          COUNT(*) FILTER (WHERE lab_calcium_min > 20)        AS gt20,
          ROUND(MIN(lab_calcium_min), 3)                      AS min_v,
          ROUND(MAX(lab_calcium_min), 3)                      AS max_v,
          ROUND(AVG(lab_calcium_min), 3)                      AS mean_v,
          COUNT(*) FILTER (WHERE postop_calcium_min_value IS NOT NULL) AS n_postop,
          ROUND(AVG(postop_calcium_min_value), 3)             AS mean_postop,
          ROUND(MAX(postop_calcium_min_value), 3)             AS max_postop
        FROM canonical_patient_master
        """
    ).fetchone()
    print(f"  post-fix canonical calcium stats: {post}")

    assert (post[2] or 0) == 0, "lab_calcium_min > 20 still present!"
    assert (post[8] or 0) <= 15.0, "postop_calcium_min_value > 15 still present!"

    # Report what happened to the originally-contaminated patients.
    per_patient = con.execute(
        """
        SELECT c.research_id,
               b.lab_calcium_min AS orig,
               c.lab_calcium_min AS corrected,
               c.postop_calcium_min_value AS postop_corrected,
               CASE
                 WHEN c.lab_calcium_min IS NULL AND c.postop_calcium_min_value IS NULL
                   THEN 'unrecoverable_nulled'
                 WHEN c.lab_calcium_min IS NOT NULL
                   THEN 'corrected'
                 ELSE 'partial'
               END AS disposition
        FROM _ca_contaminated_rids t
        JOIN canonical_patient_master c ON t.research_id = c.research_id
        JOIN canonical_patient_master_pre235_backup b ON t.research_id = b.research_id
        ORDER BY b.lab_calcium_min DESC NULLS LAST
        """
    ).fetchall()
    print("  per-patient disposition of contaminated rows:")
    dispo_count: dict = {}
    for r in per_patient:
        dispo_count[r[4]] = dispo_count.get(r[4], 0) + 1
    print(f"    {dispo_count}")

    return {
        "post_stats": post,
        "n_contam": n_contam,
        "n_recoverable": n_roll,
        "dispo_count": dispo_count,
    }


# ---------------------------------------------------------------------------
# Phase 2 — re-derive flags
# ---------------------------------------------------------------------------


def step5_rederive_flags(con: duckdb.DuckDBPyConnection) -> dict:
    banner("STEP 5 — Phase 2: Re-derive has_low_calcium / has_low_pth flags")

    con.execute(
        """
        UPDATE canonical_patient_master
           SET has_low_calcium_flag = CASE
                WHEN lab_calcium_min IS NOT NULL        AND lab_calcium_min < 8.0        THEN TRUE
                WHEN postop_calcium_min_value IS NOT NULL AND postop_calcium_min_value < 8.0 THEN TRUE
                WHEN calcium_nadir_30d IS NOT NULL      AND calcium_nadir_30d < 8.0      THEN TRUE
                WHEN lab_calcium_min IS NOT NULL OR postop_calcium_min_value IS NOT NULL
                 OR calcium_nadir_30d IS NOT NULL                                        THEN FALSE
                ELSE NULL
               END
        """
    )
    con.execute(
        """
        UPDATE canonical_patient_master
           SET postop_low_calcium_flag = CASE
                WHEN postop_calcium_min_value IS NOT NULL AND postop_calcium_min_value < 8.0 THEN TRUE
                WHEN postop_calcium_min_value IS NOT NULL                                    THEN FALSE
                ELSE NULL
               END
        """
    )
    con.execute(
        """
        UPDATE canonical_patient_master
           SET has_low_pth_flag = CASE
                WHEN lab_pth_min IS NOT NULL        AND lab_pth_min < 15        THEN TRUE
                WHEN postop_pth_min_value IS NOT NULL AND postop_pth_min_value < 15 THEN TRUE
                WHEN pth_nadir_30d IS NOT NULL      AND pth_nadir_30d < 15      THEN TRUE
                WHEN lab_pth_min IS NOT NULL OR postop_pth_min_value IS NOT NULL
                 OR pth_nadir_30d IS NOT NULL                                   THEN FALSE
                ELSE NULL
               END
        """
    )
    con.execute(
        """
        UPDATE canonical_patient_master
           SET postop_low_pth_flag = CASE
                WHEN postop_pth_min_value IS NOT NULL AND postop_pth_min_value < 15 THEN TRUE
                WHEN postop_pth_min_value IS NOT NULL                               THEN FALSE
                ELSE NULL
               END
        """
    )

    dist = {}
    for col in (
        "has_low_calcium_flag",
        "has_low_pth_flag",
        "postop_low_calcium_flag",
        "postop_low_pth_flag",
    ):
        d = con.execute(
            f"""
            SELECT {col}::VARCHAR AS v, COUNT(*)
            FROM canonical_patient_master GROUP BY 1 ORDER BY 2 DESC
            """
        ).fetchall()
        dist[col] = d
        print(f"  {col}: {d}")
    return dist


# ---------------------------------------------------------------------------
# Phase 3 — NSQIP hypocalcemia recovery
# ---------------------------------------------------------------------------


def step6_nsqip_recovery(con: duckdb.DuckDBPyConnection) -> dict:
    banner("STEP 6 — Phase 3: NSQIP hypocalcemia cross-validation recovery")

    # Build recovery candidate table.
    con.execute(
        """
        CREATE OR REPLACE TABLE _nsqip_hypocalcemia_recovery_v235 AS
        SELECT
          CAST(n.research_id AS VARCHAR) AS research_id,
          n.nsqip_hypocalcemia,
          n.nsqip_hypocalcemia_event,
          n.nsqip_hypocalcemia_event_type,
          n.nsqip_iv_calcium,
          c.comp_hypocalcemia_confirmed,
          c.comp_hypocalcemia_evidence_tier AS prior_evidence_tier,
          c.has_low_calcium_flag,
          c.lab_calcium_min,
          c.postop_calcium_min_value,
          c.surg_procedure_type,
          c.comp_hypoparathyroidism_confirmed,
          CASE
            WHEN n.nsqip_hypocalcemia = 'Yes'
             AND c.comp_hypocalcemia_confirmed IS DISTINCT FROM TRUE
            THEN TRUE ELSE FALSE
          END AS nsqip_recovery_candidate,
          'nsqip_enrichment' AS recovery_source
        FROM nsqip_patient_summary n
        JOIN canonical_patient_master c
          ON CAST(n.research_id AS VARCHAR) = c.research_id
        WHERE n.nsqip_hypocalcemia = 'Yes'
        """
    )

    n_total = con.execute(
        "SELECT COUNT(*) FROM _nsqip_hypocalcemia_recovery_v235"
    ).fetchone()[0]
    n_cand = con.execute(
        "SELECT COUNT(*) FROM _nsqip_hypocalcemia_recovery_v235 WHERE nsqip_recovery_candidate"
    ).fetchone()[0]
    print(f"  total NSQIP Yes in canonical: {n_total} / recovery candidates: {n_cand}")

    fetch_to_csv(
        con,
        "SELECT * FROM _nsqip_hypocalcemia_recovery_v235 ORDER BY research_id",
        OUTPUT_DIR / "235_nsqip_hypocalcemia_recovery.csv",
    )

    # Provenance columns.
    for col, dtype in [
        ("nsqip_hypocalcemia_recovered_flag", "BOOLEAN"),
        ("nsqip_hypoparathyroidism_recovered_flag", "BOOLEAN"),
        ("comp_hypocalcemia_evidence_source", "VARCHAR"),
    ]:
        if not column_exists(con, "canonical_patient_master", col):
            con.execute(
                f"ALTER TABLE canonical_patient_master ADD COLUMN {col} {dtype}"
            )
    # Initialize recovered flags to FALSE (never NULL) so downstream checks are clean.
    con.execute(
        """
        UPDATE canonical_patient_master
           SET nsqip_hypocalcemia_recovered_flag = COALESCE(nsqip_hypocalcemia_recovered_flag, FALSE),
               nsqip_hypoparathyroidism_recovered_flag = COALESCE(nsqip_hypoparathyroidism_recovered_flag, FALSE)
        """
    )

    # Apply recovery: set comp_hypocalcemia_confirmed = TRUE for candidates.
    con.execute(
        """
        UPDATE canonical_patient_master
           SET comp_hypocalcemia_confirmed = TRUE,
               comp_hypocalcemia_evidence_source = 'nsqip_registry',
               nsqip_hypocalcemia_recovered_flag = TRUE
         WHERE research_id IN (
            SELECT research_id FROM _nsqip_hypocalcemia_recovery_v235
             WHERE nsqip_recovery_candidate
         )
        """
    )

    post_dist = con.execute(
        """
        SELECT comp_hypocalcemia_confirmed::VARCHAR AS v, COUNT(*)
        FROM canonical_patient_master GROUP BY 1 ORDER BY 2 DESC
        """
    ).fetchall()
    recovered = con.execute(
        "SELECT COUNT(*) FROM canonical_patient_master WHERE nsqip_hypocalcemia_recovered_flag"
    ).fetchone()[0]
    print(f"  comp_hypocalcemia_confirmed post: {post_dist}")
    print(f"  nsqip_hypocalcemia_recovered_flag TRUE: {recovered}")

    # Procedure-level validation.
    proc_rates = con.execute(
        """
        SELECT surg_procedure_type,
               COUNT(*) AS n,
               COUNT(*) FILTER (WHERE comp_hypocalcemia_confirmed = TRUE) AS confirmed,
               ROUND(COUNT(*) FILTER (WHERE comp_hypocalcemia_confirmed = TRUE) * 100.0
                     / NULLIF(COUNT(*), 0), 2) AS pct
        FROM canonical_patient_master
        WHERE surg_procedure_type IS NOT NULL
        GROUP BY 1 ORDER BY n DESC
        """
    ).fetchall()
    print("  hypocalcemia rate by procedure:")
    for r in proc_rates:
        print(f"    {r}")

    return {
        "n_total": n_total,
        "n_cand": n_cand,
        "recovered": recovered,
        "post_dist": post_dist,
        "proc_rates": proc_rates,
    }


# ---------------------------------------------------------------------------
# Phase 4 — hypoparathyroidism assessment
# ---------------------------------------------------------------------------


def step7_hypopara_assessment(con: duckdb.DuckDBPyConnection) -> dict:
    banner("STEP 7 — Phase 4: Hypoparathyroidism assessment")

    # Diagnose confirmed hypopara patients.
    diag = con.execute(
        """
        SELECT
          COUNT(*) AS n,
          COUNT(*) FILTER (WHERE followup_years > 0.5) AS fu_gt_6mo,
          COUNT(*) FILTER (WHERE lab_pth_most_recent IS NOT NULL AND lab_pth_most_recent < 15) AS pth_persistent_low,
          COUNT(*) FILTER (WHERE calcium_supplement_required = TRUE) AS ca_supp_req,
          COUNT(*) FILTER (WHERE med_nlp_calcium_supplement = TRUE) AS med_nlp_ca,
          COUNT(*) FILTER (WHERE comp_hypoparathyroidism_transient = TRUE) AS transient,
          COUNT(*) FILTER (WHERE comp_hypoparathyroidism_permanent = TRUE) AS permanent
        FROM canonical_patient_master
        WHERE comp_hypoparathyroidism_confirmed = TRUE
        """
    ).fetchone()
    print(f"  confirmed hypopara diagnostics: {diag}")

    # Permanent candidates: confirmed + >6mo follow-up + persistent biochem evidence.
    n_perm_candidates = con.execute(
        """
        SELECT COUNT(*) FROM canonical_patient_master
        WHERE comp_hypoparathyroidism_confirmed = TRUE
          AND followup_years > 0.5
          AND (
              (lab_pth_most_recent IS NOT NULL AND lab_pth_most_recent < 15)
           OR (lab_pth_min         IS NOT NULL AND lab_pth_min         < 15
               AND lab_pth_n_measurements > 1)
           OR calcium_supplement_required = TRUE
           OR med_nlp_calcium_supplement  = TRUE
          )
        """
    ).fetchone()[0]
    print(f"  permanent hypopara candidates (biochem + >6mo FU): {n_perm_candidates}")

    # Apply permanent flag — STRICT: requires biochemistry or supplement evidence,
    # follow-up alone is insufficient (per DO-NOT rule).
    con.execute(
        """
        UPDATE canonical_patient_master
           SET comp_hypoparathyroidism_permanent = TRUE,
               comp_hypoparathyroidism_transient = CASE
                   WHEN comp_hypoparathyroidism_transient = TRUE THEN FALSE
                   ELSE comp_hypoparathyroidism_transient
               END
         WHERE comp_hypoparathyroidism_confirmed = TRUE
           AND followup_years > 0.5
           AND (
               (lab_pth_most_recent IS NOT NULL AND lab_pth_most_recent < 15)
            OR (lab_pth_min         IS NOT NULL AND lab_pth_min         < 15
                AND lab_pth_n_measurements > 1)
            OR calcium_supplement_required = TRUE
            OR med_nlp_calcium_supplement  = TRUE
           )
        """
    )

    post = con.execute(
        """
        SELECT
          COUNT(*) FILTER (WHERE comp_hypoparathyroidism_confirmed = TRUE) AS confirmed,
          COUNT(*) FILTER (WHERE comp_hypoparathyroidism_transient = TRUE) AS transient,
          COUNT(*) FILTER (WHERE comp_hypoparathyroidism_permanent = TRUE) AS permanent
        FROM canonical_patient_master
        """
    ).fetchone()
    print(f"  hypopara post-fix: confirmed={post[0]} transient={post[1]} permanent={post[2]}")

    # Record the ascertainment-gap rationale in a limitation field (new col).
    if not column_exists(
        con, "canonical_patient_master", "comp_hypopara_permanent_limitation_note"
    ):
        con.execute(
            "ALTER TABLE canonical_patient_master "
            "ADD COLUMN comp_hypopara_permanent_limitation_note VARCHAR"
        )
    con.execute(
        """
        UPDATE canonical_patient_master
           SET comp_hypopara_permanent_limitation_note = CASE
               WHEN comp_hypoparathyroidism_confirmed = TRUE
                AND followup_years > 0.5
                AND COALESCE(comp_hypoparathyroidism_permanent, FALSE) = FALSE
               THEN 'confirmed_hypopara_no_persistent_biochem_evidence_followup_gt_6mo'
               WHEN comp_hypoparathyroidism_confirmed = TRUE
                AND COALESCE(followup_years, 0) <= 0.5
               THEN 'followup_too_short_for_permanence_classification'
               ELSE NULL
               END
        """
    )

    return {
        "pre_diag": diag,
        "n_perm_candidates": n_perm_candidates,
        "post": post,
    }


# ---------------------------------------------------------------------------
# Phase 5 — rebuild complication_phenotype_v1 + complication_patient_summary_v1
# ---------------------------------------------------------------------------


def step8_rebuild_phenotype(con: duckdb.DuckDBPyConnection) -> dict:
    banner("STEP 8 — Phase 5.1: Rebuild complication_phenotype_v1")

    con.execute(
        """
        CREATE OR REPLACE TABLE complication_phenotype_v1 AS
        SELECT
          cp.research_id,
          cp.complication_entity,
          cp.note_mention_flag,
          cp.n_raw_nlp_mentions,
          cp.n_valid_nlp_mentions,
          cp.suspected_flag,
          CASE
            WHEN cp.complication_entity = 'hypocalcemia'
             AND cp.confirmed_flag IS DISTINCT FROM TRUE
             AND COALESCE(c.nsqip_hypocalcemia_recovered_flag, FALSE) = TRUE
            THEN TRUE
            ELSE cp.confirmed_flag
          END AS confirmed_flag,
          cp.transient_flag,
          cp.permanent_flag,
          cp.surgery_related_flag,
          cp.historical_only_flag,
          cp.timing_days_post_surgery,
          cp.timing_window,
          CASE
            WHEN cp.complication_entity = 'hypocalcemia'
             AND cp.confirmed_flag IS DISTINCT FROM TRUE
             AND COALESCE(c.nsqip_hypocalcemia_recovered_flag, FALSE) = TRUE
            THEN 'confirmed_nsqip'
            ELSE cp.final_complication_status
          END AS final_complication_status,
          cp.analysis_eligible_flag,
          CASE
            WHEN cp.complication_entity IN ('hypocalcemia', 'hypoparathyroidism')
             AND c.lab_calcium_min IS NOT NULL AND c.lab_calcium_min < 8.0
            THEN TRUE
            WHEN cp.complication_entity IN ('hypocalcemia', 'hypoparathyroidism')
             AND c.lab_calcium_min IS NOT NULL
            THEN FALSE
            ELSE cp.biochemical_low_ca
          END AS biochemical_low_ca,
          CASE
            WHEN cp.complication_entity = 'hypoparathyroidism'
             AND c.lab_pth_min IS NOT NULL AND c.lab_pth_min < 15
            THEN TRUE
            WHEN cp.complication_entity = 'hypoparathyroidism'
             AND c.lab_pth_min IS NOT NULL
            THEN FALSE
            ELSE cp.biochemical_low_pth
          END AS biochemical_low_pth,
          cp.pth_nadir,
          COALESCE(c.calcium_nadir, c.lab_calcium_min, cp.ca_nadir) AS ca_nadir,
          cp.treatment_requiring_flag,
          cp.voice_resolution_noted,
          cp.voice_permanence_noted,
          CASE
            WHEN cp.complication_entity = 'hypocalcemia'
             AND COALESCE(c.nsqip_hypocalcemia_recovered_flag, FALSE) = TRUE
            THEN COALESCE(cp.evidence_tier, 4)
            ELSE cp.evidence_tier
          END AS evidence_tier,
          CASE
            WHEN cp.complication_entity = 'hypocalcemia'
             AND COALESCE(c.nsqip_hypocalcemia_recovered_flag, FALSE) = TRUE
             AND cp.source_tier_label IS NULL
            THEN 'nsqip_registry'
            ELSE cp.source_tier_label
          END AS source_tier_label,
          cp.detection_date,
          cp.first_surgery_date,
          CURRENT_TIMESTAMP AS phenotyped_at,
          '235_parathyroid_calcium_fix' AS phenotype_version
        FROM complication_phenotype_v1_pre235_backup cp
        LEFT JOIN canonical_patient_master c
          ON CAST(cp.research_id AS VARCHAR) = c.research_id
        """
    )

    # Ensure every NSQIP-recovered patient has a hypocalcemia phenotype row.
    # If one does not exist, insert a minimal row marked analysis-eligible.
    missing = con.execute(
        """
        SELECT COUNT(*)
        FROM canonical_patient_master c
        WHERE c.nsqip_hypocalcemia_recovered_flag = TRUE
          AND NOT EXISTS (
              SELECT 1 FROM complication_phenotype_v1 cp
              WHERE CAST(cp.research_id AS VARCHAR) = c.research_id
                AND cp.complication_entity = 'hypocalcemia'
          )
        """
    ).fetchone()[0]
    print(f"  NSQIP-recovered patients missing a hypocalcemia phenotype row: {missing}")
    if missing > 0:
        con.execute(
            """
            INSERT INTO complication_phenotype_v1 (
              research_id, complication_entity, note_mention_flag,
              n_raw_nlp_mentions, n_valid_nlp_mentions, suspected_flag,
              confirmed_flag, transient_flag, permanent_flag,
              surgery_related_flag, historical_only_flag,
              timing_days_post_surgery, timing_window,
              final_complication_status, analysis_eligible_flag,
              biochemical_low_ca, biochemical_low_pth, pth_nadir, ca_nadir,
              treatment_requiring_flag, voice_resolution_noted,
              voice_permanence_noted, evidence_tier, source_tier_label,
              detection_date, first_surgery_date, phenotyped_at,
              phenotype_version
            )
            SELECT
              CAST(c.research_id AS BIGINT),
              'hypocalcemia',
              FALSE, 0, 0, FALSE,
              TRUE,  -- confirmed via NSQIP
              NULL, NULL, TRUE, FALSE,
              NULL, NULL,
              'confirmed_nsqip', TRUE,
              CASE WHEN c.lab_calcium_min IS NOT NULL AND c.lab_calcium_min < 8.0 THEN TRUE
                   WHEN c.lab_calcium_min IS NOT NULL THEN FALSE ELSE NULL END,
              NULL, NULL, c.calcium_nadir,
              NULL, NULL, NULL,
              4, 'nsqip_registry',
              NULL, NULL, CURRENT_TIMESTAMP, '235_parathyroid_calcium_fix'
            FROM canonical_patient_master c
            WHERE c.nsqip_hypocalcemia_recovered_flag = TRUE
              AND NOT EXISTS (
                  SELECT 1 FROM complication_phenotype_v1 cp
                  WHERE CAST(cp.research_id AS VARCHAR) = c.research_id
                    AND cp.complication_entity = 'hypocalcemia'
              )
            """
        )

    dist = con.execute(
        """
        SELECT complication_entity, confirmed_flag::VARCHAR AS v, COUNT(*)
        FROM complication_phenotype_v1
        GROUP BY 1, 2 ORDER BY 1, 2
        """
    ).fetchall()
    print("  confirmed_flag x entity distribution:")
    for r in dist:
        print(f"    {r}")

    biochem = con.execute(
        """
        SELECT
          COUNT(*) FILTER (WHERE biochemical_low_ca = TRUE)  AS ca_true,
          COUNT(*) FILTER (WHERE biochemical_low_ca = FALSE) AS ca_false,
          COUNT(*) FILTER (WHERE biochemical_low_ca IS NULL) AS ca_null,
          COUNT(*) FILTER (WHERE biochemical_low_pth = TRUE) AS pth_true,
          COUNT(*) FILTER (WHERE biochemical_low_pth = FALSE) AS pth_false,
          COUNT(*) FILTER (WHERE biochemical_low_pth IS NULL) AS pth_null
        FROM complication_phenotype_v1
        """
    ).fetchone()
    print(f"  biochemical flags: {biochem}")

    return {"dist": dist, "biochem": biochem, "missing_inserted": missing}


def step9_rebuild_patient_summary(con: duckdb.DuckDBPyConnection) -> dict:
    banner("STEP 9 — Phase 5.2: Rebuild complication_patient_summary_v1")

    # Preserve the existing schema (18 cols) to avoid breaking downstream views.
    con.execute(
        """
        CREATE OR REPLACE TABLE complication_patient_summary_v1 AS
        WITH roll AS (
          SELECT
            research_id,
            BOOL_OR(complication_entity = 'hypocalcemia'        AND confirmed_flag) AS hypocalcemia_confirmed,
            BOOL_OR(complication_entity = 'hypoparathyroidism' AND confirmed_flag) AS hypoparathyroidism_confirmed,
            BOOL_OR(complication_entity IN ('rln_injury','vocal_cord_paralysis','vocal_cord_paresis')
                    AND confirmed_flag) AS rln_any_confirmed,
            BOOL_OR(complication_entity IN ('rln_injury','vocal_cord_paralysis','vocal_cord_paresis')
                    AND permanent_flag)  AS rln_permanent_flag,
            BOOL_OR(complication_entity IN ('rln_injury','vocal_cord_paralysis','vocal_cord_paresis')
                    AND transient_flag)  AS rln_transient_flag,
            BOOL_OR(complication_entity = 'hematoma'        AND confirmed_flag) AS hematoma_confirmed,
            BOOL_OR(complication_entity = 'seroma'          AND confirmed_flag) AS seroma_confirmed,
            BOOL_OR(complication_entity = 'chyle_leak'      AND confirmed_flag) AS chyle_leak_confirmed,
            BOOL_OR(complication_entity = 'wound_infection' AND confirmed_flag) AS wound_infection_confirmed,
            BOOL_OR(biochemical_low_ca)  AS any_biochem_low_ca,
            BOOL_OR(biochemical_low_pth) AS any_biochem_low_pth,
            BOOL_OR(confirmed_flag)      AS any_confirmed,
            BOOL_OR(analysis_eligible_flag) AS any_elig,
            COUNT(*) FILTER (WHERE confirmed_flag) AS n_confirmed,
            MIN(timing_days_post_surgery) FILTER (WHERE confirmed_flag) AS earliest_days
          FROM complication_phenotype_v1
          GROUP BY research_id
        )
        SELECT
          r.research_id,
          CASE WHEN r.hypocalcemia_confirmed       THEN 'confirmed' ELSE 'not_confirmed' END AS hypocalcemia_status,
          CASE WHEN r.hypoparathyroidism_confirmed THEN 'confirmed' ELSE 'not_confirmed' END AS hypoparathyroidism_status,
          CASE WHEN r.rln_any_confirmed            THEN 'confirmed' ELSE 'not_confirmed' END AS rln_status,
          CASE WHEN r.hematoma_confirmed           THEN 'confirmed' ELSE 'not_confirmed' END AS hematoma_status,
          CASE WHEN r.seroma_confirmed             THEN 'confirmed' ELSE 'not_confirmed' END AS seroma_status,
          CASE WHEN r.chyle_leak_confirmed         THEN 'confirmed' ELSE 'not_confirmed' END AS chyle_leak_status,
          CASE WHEN r.wound_infection_confirmed    THEN 'confirmed' ELSE 'not_confirmed' END AS wound_infection_status,
          COALESCE(r.any_confirmed, FALSE)                            AS any_confirmed_complication_flag,
          COALESCE(r.any_elig, FALSE)                                 AS any_analysis_eligible_complication,
          COALESCE(r.n_confirmed, 0)                                  AS n_confirmed_complications,
          r.earliest_days                                             AS earliest_complication_days,
          c.has_low_pth_flag,
          c.has_low_calcium_flag,
          c.calcium_supplement_required,
          COALESCE(r.rln_permanent_flag, FALSE)                       AS rln_permanent_flag,
          COALESCE(r.rln_transient_flag, FALSE)                       AS rln_transient_flag,
          CURRENT_TIMESTAMP                                           AS summarized_at
        FROM roll r
        LEFT JOIN canonical_patient_master c
          ON CAST(r.research_id AS VARCHAR) = c.research_id
        """
    )

    row = con.execute(
        """
        SELECT COUNT(*) AS n,
               COUNT(*) FILTER (WHERE hypocalcemia_status = 'confirmed') AS hypo_conf,
               COUNT(*) FILTER (WHERE hypoparathyroidism_status = 'confirmed') AS hp_conf,
               COUNT(*) FILTER (WHERE any_confirmed_complication_flag) AS any_conf
        FROM complication_patient_summary_v1
        """
    ).fetchone()
    print(f"  complication_patient_summary_v1: {row}")
    return {"row": row}


# ---------------------------------------------------------------------------
# Phase 6 — registry / readme / dictionary upkeep
# ---------------------------------------------------------------------------


def step10_registry_and_readme(con: duckdb.DuckDBPyConnection) -> dict:
    banner("STEP 10 — Phase 6: Registry verification + __readme refresh")

    # Registry pointer check (non-destructive — warn only).
    registry_issues: list[tuple] = []
    try:
        reg = con.execute(
            """
            SELECT detail_table_name, schema_name, total_rows
            FROM manuscript_workspace.detail_table_registry_v1
            """
        ).fetchall()
    except Exception as exc:  # noqa: BLE001
        print(f"  WARNING: registry unavailable: {exc}")
        reg = []

    for tname, sname, n_reg in reg:
        fq = f'{sname}."{tname}"' if sname else f'"{tname}"'
        try:
            actual = con.execute(f"SELECT COUNT(*) FROM {fq}").fetchone()[0]
            if n_reg is not None and actual != n_reg and tname not in {
                "complication_phenotype_v1",
                "complication_patient_summary_v1",
                "extracted_postop_labs_expanded_v1",
            }:
                registry_issues.append((fq, "row_count_drift", n_reg, actual))
        except Exception as exc:  # noqa: BLE001
            registry_issues.append((fq, "missing", None, str(exc)[:120]))
    print(f"  registry issues: {len(registry_issues)}")
    for r in registry_issues:
        print(f"    {r}")

    # Refresh __readme with new phenotype_version comment for updated tables.
    try:
        con.execute(
            """
            UPDATE __readme SET description =
              'Complication phenotype with NSQIP-recovered hypocalcemia and '
              'corrected biochemical flags (Script 235).'
             WHERE table_name = 'complication_phenotype_v1'
            """
        )
        con.execute(
            """
            UPDATE __readme SET description =
              'Per-patient complications rollup. Rebuilt by Script 235 with '
              'NSQIP hypocalcemia recovery.'
             WHERE table_name = 'complication_patient_summary_v1'
            """
        )
        con.execute(
            """
            UPDATE __readme SET description =
              'Expanded post-op lab extractions with calcium unit corrections '
              '(Script 235 added value_corrected + calcium_correction_applied).'
             WHERE table_name = 'extracted_postop_labs_expanded_v1'
            """
        )
    except Exception as exc:  # noqa: BLE001
        print(f"  WARNING: __readme refresh skipped: {exc}")

    return {"registry_issues": registry_issues}


def step11_data_dictionary(con: duckdb.DuckDBPyConnection) -> dict:
    banner("STEP 11 — Data dictionary v235")

    descriptions = {
        "has_low_calcium_flag": (
            "TRUE if any calcium measurement < 8.0 mg/dL. Re-derived by "
            "Script 235 after unit correction. Previously non-functional."
        ),
        "postop_low_calcium_flag": (
            "TRUE if postop_calcium_min_value < 8.0 mg/dL. Re-derived by Script 235."
        ),
        "has_low_pth_flag": (
            "TRUE if any PTH measurement < 15 pg/mL. Re-verified by Script 235."
        ),
        "postop_low_pth_flag": (
            "TRUE if postop_pth_min_value < 15 pg/mL. Re-verified by Script 235."
        ),
        "comp_hypocalcemia_confirmed": (
            "TRUE if confirmed hypocalcemia from NLP extraction OR NSQIP registry. "
            "Script 235 recovered NSQIP-validated cases; see "
            "nsqip_hypocalcemia_recovered_flag for provenance."
        ),
        "nsqip_hypocalcemia_recovered_flag": (
            "Provenance: TRUE for patients whose comp_hypocalcemia_confirmed "
            "was set to TRUE based on NSQIP registry data in Script 235."
        ),
        "comp_hypocalcemia_evidence_source": (
            "Source of hypocalcemia confirmation when available; "
            "'nsqip_registry' for Script 235 recoveries."
        ),
        "nsqip_hypoparathyroidism_recovered_flag": (
            "Provenance: TRUE if hypoparathyroidism was promoted based on "
            "NSQIP registry evidence (Script 235). Currently always FALSE — "
            "NSQIP does not carry a direct hypoparathyroidism indicator in "
            "this cohort."
        ),
        "lab_calcium_min": (
            "Minimum total calcium value (mg/dL) across all postop/longitudinal "
            "sources. Script 235 corrected pg/mL mislabels + decimal-point errors."
        ),
        "postop_calcium_min_value": (
            "Minimum postop (days_postop >= 0) total calcium value (mg/dL). "
            "Script 235 re-derived from extracted_postop_labs.value_corrected."
        ),
        "calcium_nadir": (
            "Overall calcium nadir across all sources (mg/dL). "
            "Re-derived by Script 235."
        ),
        "comp_hypoparathyroidism_permanent": (
            "Permanent hypoparathyroidism: confirmed + follow-up > 6 months + "
            "persistent biochem evidence (PTH < 15 OR calcium supplementation "
            "ongoing). KNOWN LIMITATION: short follow-up and 1.5%% PTH lab "
            "coverage significantly under-ascertains permanent hypopara; "
            "0.7%% confirmed total-thyroidectomy rate is ~15–40× below "
            "literature (10–30%% transient, 1–3%% permanent). NSQIP-validated "
            "hypocalcemia (6.5%%) is the most reliable perioperative rate."
        ),
        "comp_hypopara_permanent_limitation_note": (
            "Narrative field (Script 235) explaining why a confirmed "
            "hypoparathyroidism patient was NOT promoted to permanent."
        ),
    }

    con.execute(
        """
        CREATE OR REPLACE TABLE data_dictionary_v235 (
          column_name  VARCHAR,
          data_type    VARCHAR,
          ordinal      INTEGER,
          n_non_null   BIGINT,
          pct_non_null DOUBLE,
          n_distinct   BIGINT,
          description  VARCHAR,
          script_source VARCHAR
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

    rows_to_insert = []
    for name, dtype, ordinal in cols:
        nn, nd = con.execute(
            f'''
            SELECT COUNT(*) FILTER (WHERE "{name}" IS NOT NULL),
                   COUNT(DISTINCT "{name}")
            FROM canonical_patient_master
            '''
        ).fetchone()
        desc = descriptions.get(name, "")
        src = "235_parathyroid_calcium_fix" if name in descriptions else ""
        rows_to_insert.append(
            (name, dtype, ordinal, nn, round(nn * 100.0 / N_EXPECTED, 2), nd, desc, src)
        )

    con.executemany(
        "INSERT INTO data_dictionary_v235 VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        rows_to_insert,
    )
    n = con.execute("SELECT COUNT(*) FROM data_dictionary_v235").fetchone()[0]
    print(f"  data_dictionary_v235 rows: {n}")
    return {"dd_rows": n}


# ---------------------------------------------------------------------------
# Phase 7 — final validation + staging cleanup + report
# ---------------------------------------------------------------------------


def step12_final_validation(con: duckdb.DuckDBPyConnection) -> dict:
    banner("STEP 12 — Phase 7: Final validation")
    check_invariants(con, "final")

    summary = con.execute(
        """
        SELECT
          COUNT(*) AS total,
          COUNT(*) FILTER (WHERE lab_calcium_min > 20)                   AS ca_gt20,
          COUNT(*) FILTER (WHERE postop_calcium_min_value > 20)          AS postop_gt20,
          ROUND(AVG(lab_calcium_min) FILTER (WHERE lab_calcium_min IS NOT NULL), 3) AS ca_mean,
          COUNT(*) FILTER (WHERE has_low_calcium_flag = TRUE)            AS low_ca_true,
          COUNT(*) FILTER (WHERE has_low_pth_flag     = TRUE)            AS low_pth_true,
          COUNT(*) FILTER (WHERE comp_hypocalcemia_confirmed = TRUE)     AS hypocal_conf,
          COUNT(*) FILTER (WHERE nsqip_hypocalcemia_recovered_flag)      AS nsqip_rec,
          COUNT(*) FILTER (WHERE comp_hypoparathyroidism_confirmed = TRUE) AS hp_conf,
          COUNT(*) FILTER (WHERE comp_hypoparathyroidism_transient = TRUE) AS hp_trans,
          COUNT(*) FILTER (WHERE comp_hypoparathyroidism_permanent = TRUE) AS hp_perm
        FROM canonical_patient_master
        """
    ).fetchone()
    print(f"  summary: {summary}")

    assert summary[1] == 0, "lab_calcium_min > 20 still present!"
    assert summary[2] == 0, "postop_calcium_min_value > 20 still present!"

    # NSQIP concordance.
    concordance = con.execute(
        """
        SELECT n.nsqip_hypocalcemia,
               COUNT(*) AS n,
               COUNT(*) FILTER (WHERE c.comp_hypocalcemia_confirmed = TRUE) AS canon_conf,
               ROUND(COUNT(*) FILTER (WHERE c.comp_hypocalcemia_confirmed = TRUE) * 100.0
                     / NULLIF(COUNT(*), 0), 1) AS pct
        FROM nsqip_patient_summary n
        JOIN canonical_patient_master c
          ON CAST(n.research_id AS VARCHAR) = c.research_id
        WHERE n.nsqip_hypocalcemia IS NOT NULL
        GROUP BY 1 ORDER BY 1
        """
    ).fetchall()
    print("  NSQIP concordance:")
    for r in concordance:
        print(f"    {r}")

    proc = con.execute(
        """
        SELECT surg_procedure_type,
               COUNT(*) AS n,
               ROUND(COUNT(*) FILTER (WHERE comp_hypocalcemia_confirmed = TRUE) * 100.0 / COUNT(*), 1) AS hypocal_pct,
               ROUND(COUNT(*) FILTER (WHERE comp_hypoparathyroidism_confirmed = TRUE) * 100.0 / COUNT(*), 1) AS hp_pct
        FROM canonical_patient_master
        WHERE surg_procedure_type IS NOT NULL
        GROUP BY 1 ORDER BY n DESC
        """
    ).fetchall()
    print("  complication rates by procedure:")
    for r in proc:
        print(f"    {r}")

    return {"summary": summary, "concordance": concordance, "proc": proc}


def step13_cleanup(con: duckdb.DuckDBPyConnection) -> None:
    banner("STEP 13 — Drop this-run staging tables")
    for t in ("_calcium_corrections_v235", "_nsqip_hypocalcemia_recovery_v235"):
        try:
            con.execute(f"DROP TABLE IF EXISTS {t}")
            print(f"  dropped {t}")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARN: could not drop {t}: {exc}")


def step14_report(
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
    s11: dict,
    s12: dict,
) -> None:
    banner("STEP 14 — Write markdown report")

    path = OUTPUT_DIR / "235_parathyroid_calcium_fix_report.md"
    pre = s2["pre_stats"]
    post = s12["summary"]

    rep = f"""# Script 235 — Parathyroid & Calcium/PTH Data Quality Fix Report

Generated: {datetime.now().astimezone().isoformat(timespec='seconds')}
Database: `{DB}` on MotherDuck

## Pre-run state
- canonical_patient_master columns: {s0['cpm_cols_pre']}
- Pre-fix lab_calcium_min: n={pre[0]} gt20={pre[1]} gt50={pre[2]} mean={pre[3]} max={pre[4]}
- Pre-fix postop_calcium_min_value: gt20={pre[5]} mean={pre[6]} max={pre[7]}

## Phase 1 — Calcium unit normalization
- Audit: {s2['audit_rows']} patient rows with calcium > 20 on either column (scripts/output/235_calcium_contamination_audit.csv)
- extracted_postop_labs calcium corrections by rule:
"""
    for r in s3["ext_rule_dist"]:
        rep += f"  - `{r[0]}`: n={r[1]}\n"
    rep += "- longitudinal_lab_canonical_v1 calcium corrections by rule:\n"
    for r in s3["long_rule_dist"]:
        rep += f"  - `{r[0]}`: n={r[1]}\n"
    rep += (
        f"- extracted post-fix calcium (min/max/avg/n_nonnull/out_of_range): {s3['ext_post']}\n"
        f"- longitudinal post-fix calcium (min/max/avg/n_nonnull/out_of_range): {s3['long_post']}\n"
        f"- Canonical contaminated patients: {s4['n_contam']} "
        f"(recoverable: {s4['n_recoverable']}; dispositions: {s4['dispo_count']})\n"
        f"- Canonical post-fix: lab_ca_min_gt20={post[1]} lab_ca_min_mean={post[3]} has_low_ca_flag_true={post[4]}\n"
    )

    rep += "\n## Phase 2 — Flag re-derivation\n"
    for k, dist in s5.items():
        rep += f"- {k}: {dist}\n"

    rep += (
        "\n## Phase 3 — NSQIP hypocalcemia recovery\n"
        f"- NSQIP Yes with canonical match: {s6['n_total']}\n"
        f"- Recovery candidates: {s6['n_cand']}\n"
        f"- Patients promoted to comp_hypocalcemia_confirmed = TRUE via NSQIP: {s6['recovered']}\n"
        f"- comp_hypocalcemia_confirmed distribution: {s6['post_dist']}\n"
        "- Hypocalcemia rate by procedure:\n"
    )
    for r in s6["proc_rates"]:
        rep += f"  - {r}\n"

    rep += (
        "\n## Phase 4 — Hypoparathyroidism assessment\n"
        f"- Pre-fix diagnostics (confirmed cohort): {s7['pre_diag']}\n"
        f"- Permanent candidates (biochem + FU > 6mo): {s7['n_perm_candidates']}\n"
        f"- Post-fix: confirmed={s7['post'][0]} transient={s7['post'][1]} permanent={s7['post'][2]}\n"
        "- Known limitation: 1.5% PTH lab coverage and short follow-up "
        "significantly under-ascertain permanent hypoparathyroidism "
        "(see comp_hypoparathyroidism_permanent dictionary entry).\n"
    )

    rep += (
        "\n## Phase 5 — Complication table rebuilds\n"
        f"- complication_phenotype_v1 confirmed_flag x entity: {s8['dist']}\n"
        f"- biochemical flag counts: {s8['biochem']}\n"
        f"- NSQIP-recovered patients without prior hypocalcemia phenotype row (inserted): {s8['missing_inserted']}\n"
        f"- complication_patient_summary_v1 summary: {s9['row']}\n"
    )

    rep += (
        "\n## Phase 6 — Registry + __readme\n"
        f"- detail_table_registry_v1 issues: {len(s10['registry_issues'])}\n"
    )
    for r in s10["registry_issues"]:
        rep += f"  - {r}\n"
    rep += f"- data_dictionary_v235 entries: {s11['dd_rows']}\n"

    rep += (
        "\n## Phase 7 — Final validation\n"
        f"- Invariants: 10,871 rows / 10,871 distinct RIDs / 0 NULL / 0 NULL FNA — PASS\n"
        f"- lab_calcium_min > 20: {post[1]} (must be 0)\n"
        f"- postop_calcium_min_value > 20: {post[2]} (must be 0)\n"
        f"- has_low_calcium_flag = TRUE: {post[4]}\n"
        f"- has_low_pth_flag = TRUE: {post[5]}\n"
        f"- comp_hypocalcemia_confirmed = TRUE: {post[6]} (of which {post[7]} NSQIP-recovered)\n"
        f"- comp_hypoparathyroidism: confirmed={post[8]} transient={post[9]} permanent={post[10]}\n"
        "- NSQIP concordance:\n"
    )
    for r in s12["concordance"]:
        rep += f"  - {r}\n"
    rep += "- Complication rates by procedure:\n"
    for r in s12["proc"]:
        rep += f"  - {r}\n"

    rep += (
        "\n## Backups retained\n"
        "- canonical_patient_master_pre235_backup\n"
        "- complication_phenotype_v1_pre235_backup\n"
        "- complication_patient_summary_v1_pre235_backup\n"
        "- extracted_postop_labs_expanded_v1_pre235_backup\n"
    )

    path.write_text(rep)
    print(f"  wrote {path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    # Clear stale dotenv-injected tokens (if any) so the TOML token wins.
    for k in ("MOTHERDUCK_TOKEN", "motherduck_token", "MD_SA_TOKEN"):
        if os.environ.get(k) and len(os.environ[k]) < 460:
            os.environ.pop(k, None)

    token = get_token()
    con = duckdb.connect(f"md:{DB}?motherduck_token={token}")
    print(f"Connected to {DB}")

    s0 = step0_preflight(con)
    step1_backup(con)
    check_invariants(con, "post-backup")

    s2 = step2_calcium_audit(con)
    s3 = step3_calcium_corrections(con)
    check_invariants(con, "post-phase1-corrections")

    s4 = step4_rederive_canonical_calcium(con)
    check_invariants(con, "post-phase1-rederive")

    s5 = step5_rederive_flags(con)
    check_invariants(con, "post-phase2")

    s6 = step6_nsqip_recovery(con)
    check_invariants(con, "post-phase3")

    s7 = step7_hypopara_assessment(con)
    check_invariants(con, "post-phase4")

    s8 = step8_rebuild_phenotype(con)
    s9 = step9_rebuild_patient_summary(con)
    check_invariants(con, "post-phase5")

    s10 = step10_registry_and_readme(con)
    s11 = step11_data_dictionary(con)
    s12 = step12_final_validation(con)

    step13_cleanup(con)

    step14_report(
        con, s0, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12
    )

    banner("SCRIPT 235 COMPLETE")


if __name__ == "__main__":
    main()
