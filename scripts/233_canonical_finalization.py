#!/usr/bin/env python3
"""
Script 233 — Canonical Publication DB Finalization.

Target DB (read+write): thyroid_canonical_publication_v1_0
Legacy/reference DB (reads only; archive writes only): "Thyroid 2026 UPdated"

Run phases individually or all together:
    .venv/bin/python scripts/233_canonical_finalization.py --phase preflight
    .venv/bin/python scripts/233_canonical_finalization.py --phase 1a
    .venv/bin/python scripts/233_canonical_finalization.py --phase 1b
    .venv/bin/python scripts/233_canonical_finalization.py --phase 1c
    .venv/bin/python scripts/233_canonical_finalization.py --phase 1d
    .venv/bin/python scripts/233_canonical_finalization.py --phase 1e
    .venv/bin/python scripts/233_canonical_finalization.py --phase 2
    .venv/bin/python scripts/233_canonical_finalization.py --phase 3
    .venv/bin/python scripts/233_canonical_finalization.py --phase 4 --plan-only
    .venv/bin/python scripts/233_canonical_finalization.py --phase 4 --apply-evictions
    .venv/bin/python scripts/233_canonical_finalization.py --phase 5

Invariants enforced at start + end of every phase:
    canonical_patient_master: 10,871 / 10,871 distinct rid / 0 NULL rid / 0 NULL fna_path_outcome.

Every DROP is preceded by a timestamped copy into "Thyroid 2026 UPdated".archive_pub_v1_0.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

import duckdb  # noqa: E402

from motherduck_client import get_token  # noqa: E402

PUB = "thyroid_canonical_publication_v1_0"
REF = "Thyroid 2026 UPdated"
ARCH_SCHEMA = "archive_pub_v1_0"
TS = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
OUT = REPO / "scripts" / "output" / "233"
OUT.mkdir(parents=True, exist_ok=True)
REPORT: dict[str, Any] = {"phases": {}, "invariants": {}, "started_at": TS}


# ======================================================================
# Connection + invariants
# ======================================================================

def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(f"md:?motherduck_token={get_token()}")
    con.execute(f'USE "{PUB}"')
    con.execute(f'USE "{PUB}".main')
    dbs = {r[0] for r in con.execute("SELECT database_name FROM duckdb_databases()").fetchall()}
    if PUB not in dbs:
        raise SystemExit(f"Publication DB '{PUB}' not attached.")
    return con


def check_invariants(con: duckdb.DuckDBPyConnection, label: str) -> dict[str, int]:
    row = con.execute(f"""
        SELECT COUNT(*) r, COUNT(DISTINCT research_id) d,
               COUNT(*) FILTER (WHERE research_id IS NULL) nr,
               COUNT(*) FILTER (WHERE fna_path_outcome IS NULL) nf
          FROM "{PUB}".main.canonical_patient_master
    """).fetchone()
    inv = dict(zip(["rows", "distinct_rid", "null_rid", "null_fna"], row))
    print(f"[INV {label}] {inv}")
    REPORT["invariants"][label] = inv
    if inv["rows"] != 10871 or inv["distinct_rid"] != 10871 or inv["null_rid"] != 0 or inv["null_fna"] != 0:
        raise SystemExit(f"Invariant broken at {label}: {inv}")
    return inv


def column_exists(con, schema: str, table: str, column: str, catalog: str = PUB) -> bool:
    return bool(con.execute(f"""
        SELECT 1 FROM information_schema.columns
        WHERE table_catalog = '{catalog}' AND table_schema = '{schema}'
          AND table_name = '{table}' AND column_name = '{column}'
        LIMIT 1
    """).fetchone())


def safe_add_column(con, table: str, column: str, dtype: str) -> bool:
    """Add column to main.<table> if missing. Returns True if added."""
    if column_exists(con, "main", table, column):
        print(f"  [schema] {table}.{column} already present")
        return False
    con.execute(f'ALTER TABLE "{PUB}".main."{table}" ADD COLUMN "{column}" {dtype}')
    print(f"  [schema] ADDED {table}.{column} {dtype}")
    return True


def ensure_archive_schema(con) -> None:
    con.execute(f'CREATE SCHEMA IF NOT EXISTS "{REF}"."{ARCH_SCHEMA}"')


def archive_table(con, src_schema: str, src_name: str, tag: str = TS) -> str:
    """Copy a pub-DB table to the reference archive schema. Returns full target name."""
    ensure_archive_schema(con)
    dst = f'"{REF}"."{ARCH_SCHEMA}"."{src_name}_{tag}"'
    print(f"  [archive] {PUB}.{src_schema}.{src_name} -> {dst}")
    con.execute(
        f'CREATE OR REPLACE TABLE {dst} AS '
        f'SELECT * FROM "{PUB}"."{src_schema}"."{src_name}"'
    )
    n = con.execute(f"SELECT COUNT(*) FROM {dst}").fetchone()[0]
    print(f"  [archive]   {n:,} rows copied")
    return dst


def write_phase_report(phase: str, payload: dict) -> None:
    REPORT["phases"][phase] = payload
    (OUT / f"phase_{phase}_{TS}.json").write_text(json.dumps(payload, default=str, indent=2))


# ======================================================================
# PHASE 1A — any_recurrence_flag derivation fix (Issue #2)
# ======================================================================
# Current logic leaves `any_recurrence_flag = TRUE` for 1,521 patients whose
# recurrence_definition = 'no_recurrence_evidence'. Correct derivation:
#
#     any_recurrence_flag := (
#         COALESCE(recurrence_flag_v2, FALSE) = TRUE
#         OR COALESCE(recurrence_flag_scoring, FALSE) = TRUE
#         OR COALESCE(structural_recurrence_flag, FALSE) = TRUE
#     ) AND COALESCE(recurrence_definition, '') <> 'no_recurrence_evidence'

def phase_1a(con) -> dict:
    print("\n[1A] Recurrence flag reconciliation (Issue #2)")
    check_invariants(con, "1A_start")

    pre = con.execute(f"""
        SELECT COUNT(*) FILTER (WHERE any_recurrence_flag=TRUE) n_any,
               COUNT(*) FILTER (WHERE any_recurrence_flag=TRUE
                                AND recurrence_definition='no_recurrence_evidence') n_phantom
          FROM "{PUB}".main.canonical_patient_master
    """).fetchone()

    safe_add_column(con, "canonical_patient_master", "any_recurrence_flag_prev_233", "BOOLEAN")
    # Snapshot only if not previously snapshotted (idempotent)
    already = con.execute(f"""
        SELECT COUNT(*) FROM "{PUB}".main.canonical_patient_master
         WHERE any_recurrence_flag_prev_233 IS NOT NULL
    """).fetchone()[0]
    if already == 0:
        con.execute(f"""
            UPDATE "{PUB}".main.canonical_patient_master
               SET any_recurrence_flag_prev_233 = any_recurrence_flag
        """)
        print("  [1A] snapshot filled")
    else:
        print(f"  [1A] snapshot already present ({already:,} rows) — skipping overwrite")

    con.execute(f"""
        UPDATE "{PUB}".main.canonical_patient_master
           SET any_recurrence_flag = (
               (COALESCE(recurrence_flag_v2, FALSE) = TRUE
                OR COALESCE(recurrence_flag_scoring, FALSE) = TRUE
                OR COALESCE(structural_recurrence_flag, FALSE) = TRUE)
               AND COALESCE(recurrence_definition, '') <> 'no_recurrence_evidence'
           )
    """)

    post = con.execute(f"""
        SELECT COUNT(*) FILTER (WHERE any_recurrence_flag=TRUE) n_any,
               COUNT(*) FILTER (WHERE any_recurrence_flag=TRUE
                                AND recurrence_definition='no_recurrence_evidence') n_phantom
          FROM "{PUB}".main.canonical_patient_master
    """).fetchone()
    print(f"  [1A] before: n_any={pre[0]:,} phantom={pre[1]:,}")
    print(f"  [1A]  after: n_any={post[0]:,} phantom={post[1]:,}")
    if post[1] != 0:
        raise SystemExit(f"[1A] still {post[1]} phantom recurrences — aborting")

    # Attach a column comment documenting the new rule
    con.execute(f"""
        COMMENT ON COLUMN "{PUB}".main.canonical_patient_master.any_recurrence_flag IS
        'TRUE only when (recurrence_flag_v2 OR recurrence_flag_scoring OR structural_recurrence_flag) AND recurrence_definition <> ''no_recurrence_evidence''. Derived by Script 233 (Issue #2 fix); prior value preserved in any_recurrence_flag_prev_233.'
    """)

    check_invariants(con, "1A_end")
    return {"before": dict(zip(["n_any", "n_phantom"], pre)),
            "after":  dict(zip(["n_any", "n_phantom"], post))}


# ======================================================================
# PHASE 1B — time_to_recurrence_days backfill + negative rds quarantine
# ======================================================================
# 1. Backfill time_to_recurrence_days from recurrence_days_from_surg where
#    rds >= 0 and current ttr is NULL (Tier 4/5 from 203).
# 2. Quarantine negative recurrence_days_from_surg into a new column; NULL original.
# 3. Guard: no negatives may remain in either field.

def phase_1b(con) -> dict:
    print("\n[1B] t2r backfill + negative rds quarantine (Issue #3)")
    check_invariants(con, "1B_start")

    pre = con.execute(f"""
        SELECT COUNT(*) FILTER (WHERE time_to_recurrence_days IS NOT NULL) ttr_nn,
               COUNT(*) FILTER (WHERE time_to_recurrence_days < 0) ttr_neg,
               COUNT(*) FILTER (WHERE recurrence_days_from_surg IS NOT NULL) rds_nn,
               COUNT(*) FILTER (WHERE recurrence_days_from_surg < 0) rds_neg,
               COUNT(*) FILTER (WHERE time_to_recurrence_days IS NULL
                                AND recurrence_days_from_surg IS NOT NULL
                                AND recurrence_days_from_surg >= 0) backfill_candidates
          FROM "{PUB}".main.canonical_patient_master
    """).fetchone()
    print(f"  [1B] pre: ttr_nn={pre[0]:,} ttr_neg={pre[1]:,} rds_nn={pre[2]:,} rds_neg={pre[3]:,} backfill_candidates={pre[4]:,}")

    # (1) Backfill ttr from rds where safe
    con.execute(f"""
        UPDATE "{PUB}".main.canonical_patient_master
           SET time_to_recurrence_days = recurrence_days_from_surg
         WHERE time_to_recurrence_days IS NULL
           AND recurrence_days_from_surg IS NOT NULL
           AND recurrence_days_from_surg >= 0
    """)

    # (2) Quarantine negative rds
    safe_add_column(con, "canonical_patient_master",
                    "recurrence_days_from_surg_quarantined", "INTEGER")
    # Idempotent: only quarantine rows still negative
    con.execute(f"""
        UPDATE "{PUB}".main.canonical_patient_master
           SET recurrence_days_from_surg_quarantined = recurrence_days_from_surg,
               recurrence_days_from_surg = NULL
         WHERE recurrence_days_from_surg < 0
    """)

    # (3) Also guard ttr negatives (should already be 0)
    con.execute(f"""
        UPDATE "{PUB}".main.canonical_patient_master
           SET time_to_recurrence_days = NULL
         WHERE time_to_recurrence_days < 0
    """)

    post = con.execute(f"""
        SELECT COUNT(*) FILTER (WHERE time_to_recurrence_days IS NOT NULL) ttr_nn,
               COUNT(*) FILTER (WHERE time_to_recurrence_days < 0) ttr_neg,
               COUNT(*) FILTER (WHERE recurrence_days_from_surg IS NOT NULL) rds_nn,
               COUNT(*) FILTER (WHERE recurrence_days_from_surg < 0) rds_neg,
               COUNT(*) FILTER (WHERE recurrence_days_from_surg_quarantined IS NOT NULL) rds_q
          FROM "{PUB}".main.canonical_patient_master
    """).fetchone()
    print(f"  [1B] post: ttr_nn={post[0]:,} ttr_neg={post[1]:,} rds_nn={post[2]:,} rds_neg={post[3]:,} rds_quarantined={post[4]:,}")
    if post[1] != 0 or post[3] != 0:
        raise SystemExit(f"[1B] negatives remain: {post}")

    con.execute(f"""
        COMMENT ON COLUMN "{PUB}".main.canonical_patient_master.recurrence_days_from_surg_quarantined IS
        'Quarantined negative values of recurrence_days_from_surg (Issue #3 fix, Script 233). Original column now NULL for affected rows because negative day counts indicate upstream date-ordering errors.'
    """)
    con.execute(f"""
        COMMENT ON COLUMN "{PUB}".main.canonical_patient_master.time_to_recurrence_days IS
        'Days from first_surgery_date to recurrence_date. Script 233 backfilled Tier 4/5 cases from recurrence_days_from_surg (>= 0 only). Negative values excluded by construction.'
    """)

    check_invariants(con, "1B_end")
    return {"before": dict(zip(["ttr_nn", "ttr_neg", "rds_nn", "rds_neg", "backfill_candidates"], pre)),
            "after":  dict(zip(["ttr_nn", "ttr_neg", "rds_nn", "rds_neg", "rds_quarantined"], post))}


# ======================================================================
# PHASE 1C — Follow-up recovery for zero-followup patients (Issue #1)
# ======================================================================
# Strategy (all sources live in PUB; no writes elsewhere):
#   (a) If first_surgery_date IS NULL: recover from operative_episode_detail_v2,
#       nsqip_enrichment.nsqip_operation_date, note_entities_llm_past_surgical_hx.
#   (b) Build a superset last-contact date per research_id from:
#         followup_or_death_date, death_date, last_tg_date, last_contact_date,
#         tg_postop_surveillance_windows_v1.window_last_date,
#         rai_treatment_episode_v2.resolved_rai_date,
#         note_entities_llm_{survival_followup,recurrence}.note_date,
#         ultrasound_reports.ultrasound_date,
#         ct_imaging.date_of_exam, mri_imaging.date_of_exam,
#         nuclear_med.scandate.
#   (c) Recompute followup_days/followup_years = (last_contact - first_surgery).days.
#   (d) Preserve prior columns to _prev_233 before overwrite.

def phase_1c(con) -> dict:
    print("\n[1C] Follow-up recovery (Issue #1)")
    check_invariants(con, "1C_start")

    # Baseline
    pre = con.execute(f"""
        SELECT COUNT(*) FILTER (WHERE COALESCE(followup_days, 0) = 0) n_zero_fu,
               COUNT(*) FILTER (WHERE first_surgery_date IS NULL) n_null_surg,
               AVG(COALESCE(followup_days, 0)) avg_fu,
               COUNT(*) FILTER (WHERE followup_days > 0) n_positive_fu
          FROM "{PUB}".main.canonical_patient_master
    """).fetchone()
    print(f"  [1C] pre: zero_fu={pre[0]:,} null_surg={pre[1]:,} avg_fu={pre[2]:.1f} positive_fu={pre[3]:,}")

    # Snapshot columns
    for col in ("followup_days", "followup_years", "last_contact_date",
                "last_contact_source", "first_surgery_date"):
        dtype = con.execute(f"""
            SELECT data_type FROM information_schema.columns
            WHERE table_catalog='{PUB}' AND table_schema='main'
              AND table_name='canonical_patient_master' AND column_name='{col}'
        """).fetchone()[0]
        safe_add_column(con, "canonical_patient_master", f"{col}_prev_233", dtype)
    # Snapshot once
    already = con.execute(f"""
        SELECT COUNT(*) FROM "{PUB}".main.canonical_patient_master
         WHERE followup_days_prev_233 IS NOT NULL
            OR last_contact_date_prev_233 IS NOT NULL
            OR first_surgery_date_prev_233 IS NOT NULL
    """).fetchone()[0]
    if already == 0:
        con.execute(f"""
            UPDATE "{PUB}".main.canonical_patient_master
               SET followup_days_prev_233 = followup_days,
                   followup_years_prev_233 = followup_years,
                   last_contact_date_prev_233 = last_contact_date,
                   last_contact_source_prev_233 = last_contact_source,
                   first_surgery_date_prev_233 = first_surgery_date
        """)
        print("  [1C] snapshot filled")
    else:
        print(f"  [1C] snapshot already filled ({already:,} rows) — skipping overwrite")

    # Build superset last-contact date + new first_surgery_date candidates
    con.execute(f"""
        CREATE OR REPLACE TABLE "{PUB}".main._followup_all_dates_233 AS
        WITH u AS (
            -- Canonical master's own columns
            SELECT CAST(research_id AS VARCHAR) rid,
                   TRY_CAST(last_contact_date AS DATE) d, 'cpm_last_contact' src
              FROM "{PUB}".main.canonical_patient_master
             WHERE last_contact_date IS NOT NULL
            UNION ALL
            SELECT CAST(research_id AS VARCHAR),
                   TRY_CAST(followup_or_death_date AS DATE), 'cpm_followup_or_death'
              FROM "{PUB}".main.canonical_patient_master
             WHERE followup_or_death_date IS NOT NULL
            UNION ALL
            SELECT CAST(research_id AS VARCHAR),
                   TRY_CAST(death_date AS DATE), 'cpm_death'
              FROM "{PUB}".main.canonical_patient_master
             WHERE death_date IS NOT NULL
            UNION ALL
            SELECT CAST(research_id AS VARCHAR),
                   TRY_CAST(last_tg_date AS DATE), 'cpm_last_tg'
              FROM "{PUB}".main.canonical_patient_master
             WHERE last_tg_date IS NOT NULL
            -- Tg surveillance windows
            UNION ALL
            SELECT CAST(research_id AS VARCHAR),
                   TRY_CAST(window_last_date AS DATE), 'tg_surveillance_window'
              FROM "{PUB}".main.tg_postop_surveillance_windows_v1
             WHERE window_last_date IS NOT NULL
            -- RAI
            UNION ALL
            SELECT CAST(research_id AS VARCHAR),
                   TRY_CAST(resolved_rai_date AS DATE), 'rai_treatment_episode_v2'
              FROM "{PUB}".main.rai_treatment_episode_v2
             WHERE resolved_rai_date IS NOT NULL
            -- Note entity dates
            UNION ALL
            SELECT CAST(research_id AS VARCHAR),
                   TRY_CAST(note_date AS DATE), 'nlp_survival_followup'
              FROM "{PUB}".main.note_entities_llm_survival_followup
             WHERE note_date IS NOT NULL
            UNION ALL
            SELECT CAST(research_id AS VARCHAR),
                   TRY_CAST(note_date AS DATE), 'nlp_recurrence'
              FROM "{PUB}".main.note_entities_llm_recurrence
             WHERE note_date IS NOT NULL
            -- Imaging
            UNION ALL
            SELECT CAST(research_id AS VARCHAR),
                   TRY_CAST(ultrasound_date AS DATE), 'ultrasound_reports'
              FROM "{PUB}".raw.ultrasound_reports
             WHERE ultrasound_date IS NOT NULL
            UNION ALL
            SELECT CAST(research_id AS VARCHAR),
                   TRY_CAST(date_of_exam AS DATE), 'ct_imaging'
              FROM "{PUB}".main.ct_imaging
             WHERE date_of_exam IS NOT NULL
            UNION ALL
            SELECT CAST(research_id AS VARCHAR),
                   TRY_CAST(date_of_exam AS DATE), 'mri_imaging'
              FROM "{PUB}".main.mri_imaging
             WHERE date_of_exam IS NOT NULL
            UNION ALL
            SELECT CAST(research_id AS VARCHAR),
                   TRY_CAST(scandate AS DATE), 'nuclear_med'
              FROM "{PUB}".main.nuclear_med
             WHERE scandate IS NOT NULL
        )
        SELECT rid, d, src
          FROM u
         WHERE d IS NOT NULL
           AND d > DATE '1990-01-01' AND d <= CURRENT_DATE
    """)

    # Roll up to one row per patient with max date and source
    con.execute(f"""
        CREATE OR REPLACE TABLE "{PUB}".main._followup_patient_max_233 AS
        WITH r AS (
            SELECT rid, d, src,
                   ROW_NUMBER() OVER (PARTITION BY rid ORDER BY d DESC, src) rn
              FROM "{PUB}".main._followup_all_dates_233
        )
        SELECT rid AS research_id, d AS last_contact_date_new, src AS last_contact_source_new
          FROM r WHERE rn = 1
    """)

    # Surgery-date recovery for NULL first_surgery_date cases
    con.execute(f"""
        CREATE OR REPLACE TABLE "{PUB}".main._first_surg_recovery_233 AS
        WITH u AS (
            SELECT CAST(research_id AS VARCHAR) rid,
                   MIN(COALESCE(TRY_CAST(surgery_date_native AS DATE),
                                TRY_CAST(resolved_surgery_date AS DATE))) d,
                   'operative_episode_detail_v2' src
              FROM "{PUB}".main.operative_episode_detail_v2
             WHERE COALESCE(TRY_CAST(surgery_date_native AS DATE),
                            TRY_CAST(resolved_surgery_date AS DATE)) IS NOT NULL
             GROUP BY 1
            UNION ALL
            SELECT CAST(research_id AS VARCHAR),
                   MIN(TRY_CAST(nsqip_operation_date AS DATE)),
                   'nsqip_enrichment'
              FROM "{PUB}".main.nsqip_enrichment
             WHERE nsqip_operation_date IS NOT NULL
             GROUP BY 1
            UNION ALL
            SELECT CAST(research_id AS VARCHAR),
                   MIN(TRY_CAST(surg_date AS DATE)),
                   'path_synoptics'
              FROM "{PUB}".main.path_synoptics
             WHERE TRY_CAST(surg_date AS DATE) IS NOT NULL
             GROUP BY 1
        )
        SELECT rid AS research_id, MIN(d) AS first_surgery_date_new,
               STRING_AGG(DISTINCT src, ';') AS first_surgery_sources_new
          FROM u WHERE d IS NOT NULL
             AND d > DATE '1990-01-01' AND d <= CURRENT_DATE
         GROUP BY rid
    """)

    # Apply surgery recovery ONLY where currently NULL
    con.execute(f"""
        UPDATE "{PUB}".main.canonical_patient_master AS m
           SET first_surgery_date = r.first_surgery_date_new
          FROM "{PUB}".main._first_surg_recovery_233 AS r
         WHERE CAST(m.research_id AS VARCHAR) = CAST(r.research_id AS VARCHAR)
           AND m.first_surgery_date IS NULL
           AND r.first_surgery_date_new IS NOT NULL
    """)

    # Apply last_contact upgrade where new > old or old is NULL; track source
    con.execute(f"""
        UPDATE "{PUB}".main.canonical_patient_master AS m
           SET last_contact_date = r.last_contact_date_new,
               last_contact_source = CASE
                   WHEN m.last_contact_source IS NULL THEN r.last_contact_source_new
                   ELSE m.last_contact_source || '+' || r.last_contact_source_new
               END
          FROM "{PUB}".main._followup_patient_max_233 AS r
         WHERE CAST(m.research_id AS VARCHAR) = CAST(r.research_id AS VARCHAR)
           AND r.last_contact_date_new IS NOT NULL
           AND (m.last_contact_date IS NULL
                OR r.last_contact_date_new > TRY_CAST(m.last_contact_date AS DATE))
    """)

    # Recompute followup_days / followup_years
    con.execute(f"""
        UPDATE "{PUB}".main.canonical_patient_master
           SET followup_days = CASE
                   WHEN first_surgery_date IS NOT NULL
                    AND last_contact_date IS NOT NULL
                    AND TRY_CAST(last_contact_date AS DATE) >= TRY_CAST(first_surgery_date AS DATE)
                   THEN DATE_DIFF('day',
                                  TRY_CAST(first_surgery_date AS DATE),
                                  TRY_CAST(last_contact_date AS DATE))
                   ELSE NULL
               END,
               followup_years = CASE
                   WHEN first_surgery_date IS NOT NULL
                    AND last_contact_date IS NOT NULL
                    AND TRY_CAST(last_contact_date AS DATE) >= TRY_CAST(first_surgery_date AS DATE)
                   THEN DATE_DIFF('day',
                                  TRY_CAST(first_surgery_date AS DATE),
                                  TRY_CAST(last_contact_date AS DATE)) / 365.25
                   ELSE NULL
               END
    """)

    # Report after
    post = con.execute(f"""
        SELECT COUNT(*) FILTER (WHERE COALESCE(followup_days, 0) = 0) n_zero_fu,
               COUNT(*) FILTER (WHERE first_surgery_date IS NULL) n_null_surg,
               AVG(COALESCE(followup_days, 0)) avg_fu,
               COUNT(*) FILTER (WHERE followup_days > 0) n_positive_fu
          FROM "{PUB}".main.canonical_patient_master
    """).fetchone()
    print(f"  [1C] post: zero_fu={post[0]:,} null_surg={post[1]:,} avg_fu={post[2]:.1f} positive_fu={post[3]:,}")

    # Drop working tables
    for t in ("_followup_all_dates_233", "_followup_patient_max_233", "_first_surg_recovery_233"):
        con.execute(f'DROP TABLE IF EXISTS "{PUB}".main."{t}"')

    con.execute(f"""
        COMMENT ON COLUMN "{PUB}".main.canonical_patient_master.followup_days IS
        'Days from first_surgery_date to last_contact_date. Script 233 extended date-union sources (FOD, Tg, RAI, imaging, surveillance windows, NLP dates) and recovered surgery dates for previously-NULL patients. Prior values preserved in followup_days_prev_233.'
    """)

    check_invariants(con, "1C_end")
    return {
        "before": dict(zip(["zero_fu", "null_surg", "avg_fu", "positive_fu"], pre)),
        "after":  dict(zip(["zero_fu", "null_surg", "avg_fu", "positive_fu"], post)),
        "zero_fu_recovered": pre[0] - post[0],
        "null_surg_recovered": pre[1] - post[1],
        "positive_fu_delta": post[3] - pre[3],
    }


# ======================================================================
# PHASE 1D — Verify Issue #4 (recurrence_site) is closed (no action)
# ======================================================================

def phase_1d(con) -> dict:
    print("\n[1D] Verify recurrence_site closure (Issue #4)")
    check_invariants(con, "1D_start")
    res = con.execute(f"""
        SELECT COUNT(*) FILTER (WHERE recurrence_site IS NULL
                                AND recurrence_site_text IS NOT NULL) AS residual
          FROM "{PUB}".main.canonical_patient_master
    """).fetchone()[0]
    print(f"  [1D] residual (should be 0): {res}")
    if res != 0:
        raise SystemExit(f"[1D] Issue #4 is NOT closed in live data ({res} residual).")
    return {"residual": 0}


# ======================================================================
# PHASE 1E — Add mortality_type column (Issue #5)
# ======================================================================

def phase_1e(con) -> dict:
    print("\n[1E] Add mortality_type (Issue #5)")
    check_invariants(con, "1E_start")

    safe_add_column(con, "canonical_patient_master", "mortality_type", "VARCHAR")
    con.execute(f"""
        UPDATE "{PUB}".main.canonical_patient_master
           SET mortality_type = CASE
               WHEN death_occurred = TRUE AND fna_path_outcome = 'malignant'
                   THEN 'cancer_cohort_death'
               WHEN death_occurred = TRUE AND fna_path_outcome IN ('benign','borderline','other')
                   THEN 'all_cause_non_cancer_death'
               WHEN death_occurred = TRUE
                   THEN 'unknown_cohort_death'
               ELSE NULL
           END
    """)
    dist = con.execute(f"""
        SELECT mortality_type, COUNT(*) n
          FROM "{PUB}".main.canonical_patient_master
         GROUP BY 1 ORDER BY 2 DESC
    """).fetchall()
    print(f"  [1E] mortality_type distribution: {dist}")

    con.execute(f"""
        COMMENT ON COLUMN "{PUB}".main.canonical_patient_master.mortality_type IS
        'Distinguishes cancer-specific from all-cause mortality. Added by Script 233 per coworker audit recommendation. Use in Kaplan-Meier analyses to avoid inflating disease-specific mortality.'
    """)
    check_invariants(con, "1E_end")
    return {"distribution": [{"mortality_type": r[0], "n": r[1]} for r in dist]}


# ======================================================================
# PHASE 2 — __readme rebuild
# ======================================================================
# Per user clarification: pub DB contains ONLY clean/finalized artifacts.
# Stale pointers (data_dictionary_v221/_v2/_parquet, md_* shadows, thyroid_scoring_py_v1)
# are left in the reference DB — we drop them from __readme (not copy them in).
# The new __readme is built by enumerating main-schema BASE TABLEs.

_README_DESCRIPTIONS: dict[str, str] = {
    "canonical_patient_master": "PRIMARY patient-level master (10,871 rows). Authoritative denormalised feature set for the publication cohort.",
    "canonical_recurrence_v1": "Per-patient recurrence classification with Tier 1-5 evidence sources.",
    "canonical_survival_followup_v1": "Longitudinal survival/follow-up timeline (multi-row per patient).",
    "canonical_diagnosis_unified_v1": "Unified diagnosis (benign + malignant + molecular) per patient-episode.",
    "canonical_benign_diagnosis_v1": "Benign diagnosis slice of canonical_diagnosis_unified_v1.",
    "canonical_malignant_diagnosis_v1": "Malignant diagnosis slice of canonical_diagnosis_unified_v1.",
    "canonical_molecular_tested_v1": "Patients with any molecular testing event.",
    "ete_adjudication_v1": "45 clinician-adjudicated ETE cases (Script 232).",
    "patient_tumor_rollup_v1": "Per-patient worst-tumor rollup feeding margin/LVI/multifocal canonical columns (Script 230).",
    "_molecular_patient_rollup_v227": "Per-patient molecular rollup (RET fusion/any-fusion) — supersedes v225.",
    "ret_patient_adjudicated_v226": "Manually adjudicated RET-fusion evidence per patient.",
    "ret_note_entity_adjudication_v226": "Entity-level RET-fusion adjudication underlying ret_patient_adjudicated_v226.",
    "path_outcome_classification_v1": "Per-patient pathology-outcome classification feeding fna_path_outcome.",
    "path_synoptics": "Raw pathology synoptic lines.",
    "synoptic_tumor_long_v1": "Tumor-long narrow form of path synoptics.",
    "specimen_master_v1": "Per-specimen canonical record.",
    "specimen_tumor_focus_v1": "Tumor foci per specimen (multifocal evidence).",
    "specimen_source_xref_v1": "Specimen ↔ pathology/imaging linkage.",
    "specimen_genomic_assay_v1": "Specimen ↔ molecular assay linkage.",
    "operative_episode_detail_v2": "Per-surgery operative episode detail.",
    "patient_completion_oed_path_linkage_v1": "Operative-episode ↔ pathology completion linkage per patient.",
    "patient_analysis_resolved_v1": "Per-patient resolved analysis state (feeds cohort views).",
    "patient_cross_domain_timeline_v2": "Cross-domain event timeline per patient.",
    "episode_analysis_resolved_v1_dedup": "Per-episode resolved analysis (de-duplicated).",
    "lesion_analysis_resolved_v1": "Lesion-grain resolved analysis.",
    "analysis_molecular_subset_v1": "Molecular-cohort analytical subset.",
    "manuscript_cohort_v1": "Primary manuscript cohort anchor.",
    "fna_history": "Per-patient FNA history (multi-row).",
    "fna_cytology": "Per-FNA cytology detail.",
    "fna_episode_master_v2": "FNA episode master (v2).",
    "extracted_fna_bethesda_v1": "LLM-extracted Bethesda category per FNA.",
    "tirads_llm_extracted_v2": "LLM-extracted TIRADS components.",
    "tirads_llm_validation_v2": "TIRADS validation audit.",
    "extracted_tirads_validated_v1": "Validated TIRADS per nodule.",
    "us_nodules_tirads": "Ultrasound nodules with TIRADS scores.",
    "imaging_nodule_master_v1": "Per-nodule imaging master.",
    "imaging_nodule_long_v2": "Nodule-long narrow imaging form.",
    "imaging_exam_master_v1": "Per-exam imaging master.",
    "imaging_fna_linkage_v3": "Imaging ↔ FNA linkage v3.",
    "imaging_patient_summary_v1": "Per-patient imaging summary.",
    "ct_imaging": "CT imaging exam records.",
    "mri_imaging": "MRI imaging exam records.",
    "ultrasound_reports": "Ultrasound report records.",
    "nuclear_med": "Nuclear medicine scan records.",
    "serial_imaging_us": "Serial US imaging observations.",
    "extracted_ete_subgraded_v1": "LLM-extracted ETE sub-grade per tumor.",
    "extracted_braf_recovery_v1": "LLM-extracted BRAF status recovery.",
    "extracted_ras_patient_summary_v1": "LLM-extracted RAS patient summary.",
    "extracted_complications_refined_v5": "Refined LLM-extracted complications (v5).",
    "extracted_rln_injury_refined_v2": "Refined RLN-injury extraction (v2).",
    "extracted_postop_labs_expanded_v1": "Expanded post-op lab extractions.",
    "complication_patient_summary_v1": "Per-patient complications rollup.",
    "complication_phenotype_v1": "Complication phenotype classification.",
    "rai_treatment_episode_v2": "Per-RAI-episode detail.",
    "recurrence_event_clean_v1": "Legacy cleaned recurrence events (inputs to canonical_recurrence_v1).",
    "longitudinal_lab_canonical_v1": "Per-lab longitudinal canonical table.",
    "thyroglobulin_lab_canonical_v1": "Per-Tg-result canonical lab table.",
    "tg_timeline_patient_summary_v1": "Per-patient Tg timeline summary.",
    "tg_postop_surveillance_windows_v1": "Post-op Tg surveillance windows.",
    "lab_cross_wave_dedup_map_v1": "Lab de-duplication mapping across waves.",
    "thyroid_sizes": "Gland size measurements (ingested 2026-04-16).",
    "thyroid_weights": "Gland weight measurements.",
    "nsqip_enrichment": "NSQIP perioperative enrichment.",
    "nsqip_patient_summary": "Per-patient NSQIP summary.",
    "survival_cohort_enriched": "Enriched survival cohort (longitudinal).",
    "tumor_pathology": "Per-tumor pathology records.",
    "tumor_episode_master_v2": "Tumor-episode master (v2).",
    "ln_master_rollup_v1": "Per-patient lymph-node rollup.",
    "ln_crossval_v1": "Lymph-node cross-validation QA.",
    "clinical_notes_long": "Long clinical-note source (dates only retained).",
    "clinical_note_ln_extracted_v1": "Clinical-note LN extractions.",
    "note_entities_problem_list": "Problem-list entities.",
    "note_entities_procedures": "Procedure entities.",
    "note_entities_staging": "Staging entities.",
    "note_entities_complications": "Complication entities.",
    "note_entities_genetics": "Genetics entities.",
    "note_entities_medications": "Medication entities.",
    "note_entities_operative_detail": "Operative-detail entities.",
    "note_entities_llm_airway_invasion": "LLM airway-invasion entities.",
    "note_entities_llm_cervical_ln_detail": "LLM cervical-LN entities.",
    "note_entities_llm_dynamic_risk_response": "LLM ATA dynamic-risk entities.",
    "note_entities_llm_frozen_section_detail": "LLM frozen-section entities.",
    "note_entities_llm_functional_outcomes": "LLM functional-outcome entities.",
    "note_entities_llm_imaging": "LLM imaging entities.",
    "note_entities_llm_labs": "LLM lab entities.",
    "note_entities_llm_parathyroid_detail": "LLM parathyroid entities.",
    "note_entities_llm_past_medical_hx": "LLM PMH entities.",
    "note_entities_llm_past_surgical_hx": "LLM past-surgical entities.",
    "note_entities_llm_pathology": "LLM pathology entities.",
    "note_entities_llm_patient_decision_adherence": "LLM decision-adherence entities.",
    "note_entities_llm_physical_exam": "LLM physical-exam entities.",
    "note_entities_llm_presenting_symptoms": "LLM presenting-symptom entities.",
    "note_entities_llm_rad_treatment": "LLM radiation-treatment entities.",
    "note_entities_llm_rai_detailed": "LLM RAI-detailed entities.",
    "note_entities_llm_recurrence": "LLM recurrence entities.",
    "note_entities_llm_survival_followup": "LLM survival/follow-up entities.",
    "note_entities_llm_synoptic_pathology_enrichment": "LLM synoptic pathology enrichment.",
    "note_entities_llm_tg_kinetics": "LLM Tg-kinetics entities.",
    "note_entities_llm_tirads_granular": "LLM granular TIRADS entities.",
    "note_entities_llm_us_nodule_dynamics": "LLM US-nodule dynamics entities.",
    "note_entities_llm_vascular_invasion": "LLM vascular-invasion entities.",
    "molecular_testing": "Per-test molecular record.",
    "molecular_results": "Per-result molecular record.",
    "molecular_test_episode_v2": "Molecular test episode (v2).",
    "molecular_variant_long": "Molecular variant long form.",
    "molecular_assay_dictionary": "Molecular assay dictionary.",
    "molecular_code_crosswalk": "Molecular code crosswalk.",
    "molecular_ingestion_runs": "Molecular ingestion run log.",
    "thyroseq_molecular_enrichment": "ThyroSeq molecular enrichment.",
    "qa_fusion_parse_triage_v1": "QA triage for fusion-parse pipeline (not a publication artifact; scheduled for eviction in Phase 4).",
}


def phase_2(con) -> dict:
    print("\n[2] __readme rebuild")
    check_invariants(con, "2_start")

    # Archive current __readme before overwriting
    archive_table(con, "main", "__readme")

    # Enumerate current main-schema base tables
    tables = [r[0] for r in con.execute(f"""
        SELECT table_name FROM information_schema.tables
         WHERE table_catalog = '{PUB}' AND table_schema = 'main'
           AND table_type = 'BASE TABLE'
         ORDER BY table_name
    """).fetchall()]
    # Exclude only the __readme itself (single-underscore tables like
    # _molecular_patient_rollup_v227 ARE publication artifacts).
    visible = [t for t in tables if not t.startswith("__")]
    rows = []
    for t in visible:
        n = con.execute(f'SELECT COUNT(*) FROM "{PUB}".main."{t}"').fetchone()[0]
        desc = _README_DESCRIPTIONS.get(
            t,
            "Publication table. See manuscript_workspace.detail_table_registry_v1 for drill-down semantics.",
        )
        rows.append((t, n, desc))

    # Materialize via pandas parameter binding
    import pandas as pd  # noqa: WPS433
    df = pd.DataFrame(rows, columns=["table_name", "rows", "description"]).sort_values(
        "table_name"
    )
    con.register("_readme_new_233", df)
    con.execute(f"""
        CREATE OR REPLACE TABLE "{PUB}".main."__readme" AS
        SELECT table_name, CAST(rows AS BIGINT) AS rows, description
          FROM _readme_new_233
         ORDER BY table_name
    """)
    con.unregister("_readme_new_233")

    out_n = con.execute(f'SELECT COUNT(*) FROM "{PUB}".main."__readme"').fetchone()[0]
    stale = con.execute(f"""
        SELECT table_name FROM "{PUB}".main."__readme" r
         WHERE NOT EXISTS (
             SELECT 1 FROM information_schema.tables t
              WHERE t.table_catalog='{PUB}' AND t.table_schema='main'
                AND t.table_name=r.table_name
         )
    """).fetchall()
    print(f"  [2] __readme rebuilt with {out_n:,} rows; stale pointers: {len(stale)}")
    con.execute(f"""
        COMMENT ON TABLE "{PUB}".main."__readme" IS
        'Canonical table catalog for thyroid_canonical_publication_v1_0. Regenerated by Script 233 from information_schema. Every row corresponds to an existing main-schema BASE TABLE.'
    """)
    check_invariants(con, "2_end")
    return {"n_rows": out_n, "stale_pointers_after": len(stale)}


# ======================================================================
# PHASE 3 — detail_table_registry_v1 refresh + canonical_detail_pointer_v1 view
# ======================================================================
# Existing registry has 110 rows with populated feeds_master_columns.
# Our job: refresh total_rows/total_patients, upsert canonical_version, and ensure
# every user-requested table is present. Also refresh canonical_detail_pointer_v1.

_REGISTRY_UPSERT: list[dict[str, str]] = [
    # Only entries known to map cleanly to a single patient-grained table go here.
    # Many of the user's requested tables are already present in the registry.
    {"detail_table_name": "patient_tumor_rollup_v1", "schema_name": "main",
     "join_key": "research_id", "grain": "patient",
     "domain": "Pathology",
     "feeds_master_columns": "r_class_true;margin_status_true;lvi_ordinal_worst;lvi_any_present_path;multifocal_flag_path;n_tumors_path",
     "description": "Per-patient worst-tumor pathology rollup (Script 230). Feeds margin/LVI/multifocal canonical columns.",
     "canonical_version": "v1"},
    {"detail_table_name": "ete_adjudication_v1", "schema_name": "main",
     "join_key": "research_id", "grain": "patient",
     "domain": "Pathology",
     "feeds_master_columns": "ete_grade_final_v2;ete_adjudicated_flag",
     "description": "45 clinician-adjudicated ETE cases (Script 232). Feeds ete_grade_final_v2 and related.",
     "canonical_version": "v1"},
    {"detail_table_name": "_molecular_patient_rollup_v227", "schema_name": "main",
     "join_key": "research_id", "grain": "patient",
     "domain": "Molecular",
     "feeds_master_columns": "ret_positive_v7;any_fusion_positive;molecular_rollup_version",
     "description": "Per-patient molecular rollup v227 (supersedes v225). Feeds ret_positive_v7 / any_fusion_positive.",
     "canonical_version": "v227"},
    {"detail_table_name": "ret_patient_adjudicated_v226", "schema_name": "main",
     "join_key": "research_id", "grain": "patient",
     "domain": "Molecular/NLP",
     "feeds_master_columns": "ret_adjudicated_flag;ret_evidence_source",
     "description": "Manually adjudicated RET fusion evidence per patient.",
     "canonical_version": "v226"},
    {"detail_table_name": "ret_note_entity_adjudication_v226", "schema_name": "main",
     "join_key": "research_id", "grain": "note_entity",
     "domain": "Molecular/NLP",
     "feeds_master_columns": "(supports ret_patient_adjudicated_v226)",
     "description": "Entity-level RET-fusion adjudication (underlies ret_patient_adjudicated_v226).",
     "canonical_version": "v226"},
    {"detail_table_name": "qa_fusion_parse_triage_v1", "schema_name": "main",
     "join_key": "research_id", "grain": "variant",
     "domain": "QA/Molecular",
     "feeds_master_columns": "(QA artifact; scheduled for eviction Phase 4)",
     "description": "QA triage for fusion-parse pipeline. NOT a publication artifact — scheduled for eviction to reference DB.",
     "canonical_version": "v1"},
]


def phase_3(con) -> dict:
    print("\n[3] detail_table_registry refresh + pointer view")
    check_invariants(con, "3_start")

    registry_fq = f'"{PUB}".manuscript_workspace.detail_table_registry_v1'

    # Archive the live registry before mutating
    con.execute(f'CREATE SCHEMA IF NOT EXISTS "{REF}"."{ARCH_SCHEMA}"')
    con.execute(f"""
        CREATE OR REPLACE TABLE "{REF}"."{ARCH_SCHEMA}"."detail_table_registry_v1_{TS}" AS
        SELECT * FROM {registry_fq}
    """)

    # Upsert rows from the spec list. Delete any matching row first, then insert.
    inserted = []
    for row in _REGISTRY_UPSERT:
        t = row["detail_table_name"]
        # Check that the referenced table actually exists in pub
        exists = con.execute(f"""
            SELECT 1 FROM information_schema.tables
             WHERE table_catalog='{PUB}' AND table_schema='main' AND table_name='{t}'
        """).fetchone()
        if not exists:
            print(f"  [3] skip {t}: not present in pub DB")
            continue
        n_rows = con.execute(f'SELECT COUNT(*) FROM "{PUB}".main."{t}"').fetchone()[0]
        # total_patients via distinct research_id if column exists
        has_rid = column_exists(con, "main", t, "research_id")
        n_pts = None
        if has_rid:
            n_pts = con.execute(
                f'SELECT COUNT(DISTINCT research_id) FROM "{PUB}".main."{t}"'
            ).fetchone()[0]
        con.execute(
            f"DELETE FROM {registry_fq} WHERE detail_table_name = ?", [t]
        )
        con.execute(
            f"""INSERT INTO {registry_fq}
                (detail_table_name, schema_name, join_key, grain, total_rows,
                 total_patients, domain, feeds_master_columns, description, canonical_version)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [t, row["schema_name"], row["join_key"], row["grain"],
             int(n_rows), (int(n_pts) if n_pts is not None else None),
             row["domain"], row["feeds_master_columns"], row["description"],
             row["canonical_version"]],
        )
        inserted.append({"table": t, "rows": n_rows, "patients": n_pts})

    # Refresh total_rows/total_patients for ALL rows whose table exists in pub
    refreshed = 0
    rows_to_refresh = con.execute(f"""
        SELECT detail_table_name, schema_name FROM {registry_fq}
    """).fetchall()
    for t, sch in rows_to_refresh:
        exists = con.execute(f"""
            SELECT 1 FROM information_schema.tables
             WHERE table_catalog='{PUB}' AND table_schema='{sch}' AND table_name='{t}'
        """).fetchone()
        if not exists:
            continue
        n_rows = con.execute(f'SELECT COUNT(*) FROM "{PUB}"."{sch}"."{t}"').fetchone()[0]
        has_rid = column_exists(con, sch, t, "research_id")
        n_pts = None
        if has_rid:
            n_pts = con.execute(
                f'SELECT COUNT(DISTINCT research_id) FROM "{PUB}"."{sch}"."{t}"'
            ).fetchone()[0]
        con.execute(
            f"""UPDATE {registry_fq}
                   SET total_rows = ?, total_patients = ?
                 WHERE detail_table_name = ? AND schema_name = ?""",
            [int(n_rows), (int(n_pts) if n_pts is not None else None), t, sch],
        )
        refreshed += 1

    n_reg = con.execute(f"SELECT COUNT(*) FROM {registry_fq}").fetchone()[0]
    print(f"  [3] registry rows: {n_reg:,}; upserted: {len(inserted)}; refreshed counts: {refreshed}")

    # Refresh canonical_detail_pointer_v1 view — per-column pointer
    con.execute(f"""
        CREATE OR REPLACE VIEW "{PUB}".manuscript_workspace.canonical_detail_pointer_v1 AS
        WITH cpm AS (
            SELECT column_name AS master_column,
                   data_type AS master_column_type,
                   ordinal_position
              FROM information_schema.columns
             WHERE table_catalog = '{PUB}'
               AND table_schema = 'main'
               AND table_name = 'canonical_patient_master'
        ),
        reg AS (
            SELECT detail_table_name, schema_name, domain,
                   feeds_master_columns, total_rows, total_patients
              FROM "{PUB}".manuscript_workspace.detail_table_registry_v1
        ),
        reg_exp AS (
            SELECT detail_table_name, schema_name, domain, total_rows, total_patients,
                   TRIM(fm) AS feeds_column
              FROM reg,
                   UNNEST(STRING_SPLIT(feeds_master_columns, ';')) AS t(fm)
             WHERE feeds_master_columns IS NOT NULL
               AND feeds_master_columns <> ''
        )
        SELECT c.master_column,
               c.master_column_type,
               c.ordinal_position,
               e.detail_table_name,
               e.schema_name,
               e.domain,
               e.total_rows,
               e.total_patients
          FROM cpm c
          LEFT JOIN reg_exp e
                 ON LOWER(TRIM(c.master_column)) = LOWER(TRIM(e.feeds_column))
         ORDER BY c.ordinal_position
    """)
    n_pointer = con.execute(
        f'SELECT COUNT(*) FROM "{PUB}".manuscript_workspace.canonical_detail_pointer_v1'
    ).fetchone()[0]
    n_mapped = con.execute(
        f"""SELECT COUNT(*) FROM "{PUB}".manuscript_workspace.canonical_detail_pointer_v1
            WHERE detail_table_name IS NOT NULL"""
    ).fetchone()[0]
    print(f"  [3] canonical_detail_pointer_v1 rows={n_pointer:,}, mapped_to_detail={n_mapped:,}")

    check_invariants(con, "3_end")
    return {"registry_rows": n_reg, "upserted": len(inserted),
            "counts_refreshed": refreshed,
            "pointer_rows": n_pointer, "pointer_mapped": n_mapped}


# ======================================================================
# PHASE 4 — evict non-publication artifacts
# ======================================================================
# Conservative eviction list (user approves before mutation):
#   - qa_fusion_parse_triage_v1 (QA artifact, not referenced by any view)
# Default is --plan-only. Pass --apply-evictions to actually move+drop.

EVICTION_CANDIDATES: list[dict[str, str]] = [
    {"schema": "main", "name": "qa_fusion_parse_triage_v1",
     "reason": "QA triage (fusion-parse) is not a publication artifact and is not referenced by any cohort view; belongs in reference DB."},
]


def phase_4(con, apply_evictions: bool) -> dict:
    print(f"\n[4] Eviction plan (apply_evictions={apply_evictions})")
    check_invariants(con, "4_start")

    plan = []
    for c in EVICTION_CANDIDATES:
        sch, name = c["schema"], c["name"]
        exists = con.execute(f"""
            SELECT 1 FROM information_schema.tables
             WHERE table_catalog='{PUB}' AND table_schema='{sch}' AND table_name='{name}'
        """).fetchone()
        if not exists:
            print(f"  [4] {sch}.{name} not present — skipping")
            continue
        n = con.execute(f'SELECT COUNT(*) FROM "{PUB}"."{sch}"."{name}"').fetchone()[0]
        # Reference check: does any view reference it?
        refs = con.execute(f"""
            SELECT table_schema || '.' || table_name
              FROM information_schema.views
             WHERE table_catalog='{PUB}'
               AND view_definition LIKE '%{name}%'
        """).fetchall()
        plan.append({**c, "rows": n, "referencing_views": [r[0] for r in refs]})
        print(f"  [4] candidate {sch}.{name}: {n:,} rows; referenced by {len(refs)} views")
        if refs:
            print(f"      Referencing views: {[r[0] for r in refs]}")

    if not apply_evictions:
        print("  [4] --plan-only: no changes made. Re-run with --apply-evictions to execute.")
        return {"applied": False, "plan": plan}

    executed = []
    for p in plan:
        sch, name = p["schema"], p["name"]
        if p["referencing_views"]:
            print(f"  [4] REFUSE to evict {sch}.{name}: still referenced by {p['referencing_views']}")
            continue
        dst = archive_table(con, sch, name)
        con.execute(f'DROP TABLE "{PUB}"."{sch}"."{name}"')
        print(f"  [4] dropped {PUB}.{sch}.{name} (archived to {dst})")
        executed.append({"table": f"{sch}.{name}", "archive": dst, "rows": p["rows"]})

    # Remove evicted names from __readme if present
    for e in executed:
        name = e["table"].split(".", 1)[1]
        con.execute(
            f'DELETE FROM "{PUB}".main."__readme" WHERE table_name = ?', [name]
        )
    check_invariants(con, "4_end")
    return {"applied": True, "plan": plan, "executed": executed}


# ======================================================================
# PHASE 5 — data_dictionary refresh + view validation + final report
# ======================================================================

def phase_5(con) -> dict:
    print("\n[5] data_dictionary rebuild + view validation")
    check_invariants(con, "5_start")

    # Rebuild data_dictionary_v221 locally in pub DB (clean metadata snapshot)
    con.execute(f"""
        CREATE OR REPLACE TABLE "{PUB}".main.data_dictionary_v221 AS
        WITH cols AS (
            SELECT column_name, data_type, is_nullable, ordinal_position
              FROM information_schema.columns
             WHERE table_catalog = '{PUB}'
               AND table_schema = 'main'
               AND table_name = 'canonical_patient_master'
        )
        SELECT c.column_name,
               c.data_type,
               c.is_nullable,
               c.ordinal_position,
               NULL::BIGINT AS non_null_count,
               NULL::DOUBLE AS coverage_pct,
               p.detail_table_name AS inferred_source,
               NULL::VARCHAR AS description
          FROM cols c
          LEFT JOIN (
               SELECT DISTINCT master_column, detail_table_name
                 FROM "{PUB}".manuscript_workspace.canonical_detail_pointer_v1
                WHERE detail_table_name IS NOT NULL
          ) p
                 ON LOWER(c.column_name) = LOWER(p.master_column)
         ORDER BY c.ordinal_position
    """)

    # Populate coverage stats via iteration (can't do this in one SQL with wide table)
    cols = con.execute(f"""
        SELECT column_name FROM "{PUB}".main.data_dictionary_v221
        ORDER BY ordinal_position
    """).fetchall()
    total = con.execute(f'SELECT COUNT(*) FROM "{PUB}".main.canonical_patient_master').fetchone()[0]
    for (c,) in cols:
        cn = c.replace('"', '""')
        n = con.execute(
            f'SELECT COUNT(*) FILTER (WHERE "{cn}" IS NOT NULL) '
            f'FROM "{PUB}".main.canonical_patient_master'
        ).fetchone()[0]
        pct = (100.0 * n / total) if total else 0.0
        con.execute(
            f"""UPDATE "{PUB}".main.data_dictionary_v221
                   SET non_null_count = ?, coverage_pct = ?
                 WHERE column_name = ?""",
            [int(n), float(pct), c],
        )

    n_dict = con.execute(f'SELECT COUNT(*) FROM "{PUB}".main.data_dictionary_v221').fetchone()[0]
    n_sourced = con.execute(f"""
        SELECT COUNT(*) FROM "{PUB}".main.data_dictionary_v221 WHERE inferred_source IS NOT NULL
    """).fetchone()[0]
    print(f"  [5] data_dictionary_v221 rebuilt: {n_dict:,} rows, {n_sourced:,} with detail-source mapping")
    con.execute(f"""
        COMMENT ON TABLE "{PUB}".main.data_dictionary_v221 IS
        'Column-level data dictionary for canonical_patient_master. Regenerated by Script 233 from information_schema + detail_table_registry_v1.'
    """)

    # View validation
    views = con.execute(f"""
        SELECT table_schema, table_name FROM information_schema.views
         WHERE table_catalog='{PUB}'
         ORDER BY 1, 2
    """).fetchall()
    broken = []
    ok = 0
    for sch, name in views:
        try:
            _ = con.execute(f'SELECT COUNT(*) FROM "{PUB}"."{sch}"."{name}"').fetchone()[0]
            ok += 1
        except Exception as e:  # noqa: BLE001
            broken.append({"view": f"{sch}.{name}", "error": str(e)[:300]})
    print(f"  [5] views validated: {ok:,} ok, {len(broken)} broken")
    for b in broken:
        print(f"      BROKEN: {b}")

    check_invariants(con, "5_end")
    return {"dict_rows": n_dict, "dict_sourced": n_sourced, "views_ok": ok, "views_broken": broken}


# ======================================================================
# Driver
# ======================================================================

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--phase",
        default="preflight",
        choices=["preflight", "1a", "1b", "1c", "1d", "1e", "2", "3", "4", "5", "all"],
    )
    ap.add_argument(
        "--apply-evictions", action="store_true",
        help="Phase 4: actually move+drop (default is plan-only).",
    )
    args = ap.parse_args()

    con = connect()
    try:
        if args.phase in ("preflight",):
            check_invariants(con, "preflight")
            print("[preflight] invariants ok")
        if args.phase in ("1a", "all"):
            write_phase_report("1a", phase_1a(con))
        if args.phase in ("1b", "all"):
            write_phase_report("1b", phase_1b(con))
        if args.phase in ("1c", "all"):
            write_phase_report("1c", phase_1c(con))
        if args.phase in ("1d", "all"):
            write_phase_report("1d", phase_1d(con))
        if args.phase in ("1e", "all"):
            write_phase_report("1e", phase_1e(con))
        if args.phase in ("2", "all"):
            write_phase_report("2", phase_2(con))
        if args.phase in ("3", "all"):
            write_phase_report("3", phase_3(con))
        if args.phase in ("4", "all"):
            write_phase_report("4", phase_4(con, apply_evictions=args.apply_evictions))
        if args.phase in ("5", "all"):
            write_phase_report("5", phase_5(con))
    finally:
        REPORT["ended_at"] = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        (OUT / f"233_report_{TS}.json").write_text(json.dumps(REPORT, default=str, indent=2))
        con.close()


if __name__ == "__main__":
    main()
