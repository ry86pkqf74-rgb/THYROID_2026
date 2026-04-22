#!/usr/bin/env python3
"""Script 364B — Survival follow-up consolidation.

Sister script to 364. Promotes long-term survival/follow-up signal out of
the complications canonical (where it doesn't belong — peri-op mortality is
a complication, but long-term mortality is survival) into a dedicated
patient-grain canonical.

Output (single table):
    main.canonical_survival_followup_v1
        research_id                              VARCHAR  PK
        vital_status_current                     VARCHAR  ('alive'|'deceased'|'unknown')
        death_date                               DATE     -- NULL unless deceased
        death_date_source                        VARCHAR
        last_known_alive_date                    DATE
        last_followup_source                     VARCHAR
        days_from_first_surgery_to_last_contact  INTEGER
        followup_complete_at_5yr                 BOOLEAN
        followup_complete_at_10yr                BOOLEAN
        first_surgery_date                       DATE     -- denormalized for convenience
        build_ts                                 TIMESTAMP
        build_script                             VARCHAR
        extraction_run_id                        VARCHAR

Sources:
    main.note_entities_llm_survival_followup
        * vital_status entities (entity_value IN deceased/dead/died/expired
                                 → vital_status_current='deceased')
        * last_followup_date entities (entity_value as date)
    main.canonical_operative_events_v1            (first_surgery_date)
    main.canonical_labs_calcium_v1                (latest lab contact date)
    main.canonical_labs_pth_v1                    (latest lab contact date)
    main.canonical_labs_thyroglobulin_v1          (latest lab contact date)
    main.canonical_labs_tsh_v1                    (latest lab contact date)
    main.canonical_labs_vitamin_d_v1              (latest lab contact date)
    main.canonical_patient_master                 (anchor — one row per patient)

3-commit cascade (lives alongside 364's cascade):
    A.  scripts/364B_survival_followup_consolidation.py --commit
        (build canonical_survival_followup_v1; required input for 364's
         peri-op mortality merge)
    B–E.  Run the 364 cascade as documented in 364_complications_consolidation.py.

CLI::

    python scripts/364B_survival_followup_consolidation.py --dry-run
    python scripts/364B_survival_followup_consolidation.py --commit

PHI rule: research_id only.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from motherduck_client import get_token, token_mode  # noqa: E402

SCRIPT_ID = "364B"
SCRIPT_TAG = f"Script {SCRIPT_ID}"
BUILD_TS = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
RUN_TS_COMPACT = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
RUN_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")

CANONICAL_DB = "thyroid_canonical_publication_v1_0"
ARCHIVE_DB = "Thyroid 2026 UPdated"
ARCHIVE_SCHEMA = "archive_pub_v1_0"
ARCHIVE_FQ = f'"{ARCHIVE_DB}"."{ARCHIVE_SCHEMA}"'
WS_SCHEMA = "manuscript_workspace"
REGISTRY_TABLE = "detail_table_registry_v1"
VIEW_SCHEMA = "views_readable"

OUTPUT_DIR = REPO_ROOT / "scripts" / "output"
QA_DIR = REPO_ROOT / "qa"
LOG_PATH = OUTPUT_DIR / f"{SCRIPT_ID}_run_{RUN_TS_COMPACT}.log"
QA_PATH = QA_DIR / f"qa_script_{SCRIPT_ID}_survival_followup.json"

NEW_TABLE = "canonical_survival_followup_v1"
NEW_VIEW = "survival_followup_VIEW_v1"

# Five days of slack on the 5/10-year follow-up cutoffs to absorb
# scheduling slop (real follow-up exams are scheduled around the
# anniversary, not on the exact date).
COMPLETENESS_SLACK_DAYS = 5
YEAR_5_DAYS = 365 * 5 - COMPLETENESS_SLACK_DAYS
YEAR_10_DAYS = 365 * 10 - COMPLETENESS_SLACK_DAYS

_LOG_LINES: list[str] = []


def log(msg: str, level: str = "INFO") -> None:
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3] + "Z"
    line = f"[{level}] [{ts}] {msg}"
    print(line, flush=True)
    _LOG_LINES.append(line)


def log_warn(msg: str) -> None:
    log(msg, "WARN")


def log_error(msg: str) -> None:
    log(msg, "ERROR")


def flush_log() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("w", encoding="utf-8") as fh:
        fh.write("\n".join(_LOG_LINES) + "\n")


def fq(schema: str, name: str) -> str:
    return f'"{CANONICAL_DB}"."{schema}"."{name}"'


def connect() -> duckdb.DuckDBPyConnection:
    tok = get_token()
    if not tok:
        raise SystemExit(
            f"No MotherDuck RW token (token_mode={token_mode()}). "
            "Set MD_SA_TOKEN / MOTHERDUCK_TOKEN or motherduck.local.toml."
        )
    log(f"Connecting md:{CANONICAL_DB} (token_mode={token_mode()})")
    con = duckdb.connect(f"md:{CANONICAL_DB}?motherduck_token={tok}")
    con.execute(f'USE "{CANONICAL_DB}"')
    con.execute(f'USE "{CANONICAL_DB}".main')
    return con


def table_exists(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_catalog = ? AND table_schema = ? AND table_name = ?",
        [CANONICAL_DB, schema, table],
    ).fetchone()
    return row is not None


def row_count(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> int:
    return int(con.execute(f"SELECT COUNT(*) FROM {fq(schema, table)}").fetchone()[0])


def list_columns(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> list[str]:
    rows = con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_catalog = ? AND table_schema = ? AND table_name = ? "
        "ORDER BY ordinal_position",
        [CANONICAL_DB, schema, table],
    ).fetchall()
    return [r[0] for r in rows]


# ---------------------------------------------------------------------------
# Build SQL
# ---------------------------------------------------------------------------

def _build_sql() -> str:
    target = fq("main", NEW_TABLE)

    # Probe ALL labs canonicals dynamically — but we hardcode the 5 known
    # per Script 347 here. If the labs schema changes, this will need
    # updating; we deliberately do not auto-discover canonical_labs_* to
    # avoid pulling in unrelated tables.
    lab_dates_union = " UNION ALL ".join([
        f"SELECT CAST(research_id AS VARCHAR) AS research_id, "
        f"CAST(lab_datetime AS DATE) AS contact_date, "
        f"'{tbl}' AS source "
        f"FROM main.\"{tbl}\" "
        f"WHERE lab_datetime IS NOT NULL"
        for tbl in (
            "canonical_labs_calcium_v1",
            "canonical_labs_pth_v1",
            "canonical_labs_thyroglobulin_v1",
            "canonical_labs_tsh_v1",
            "canonical_labs_vitamin_d_v1",
        )
    ])

    return f"""
CREATE OR REPLACE TABLE {target} AS
WITH
-- Anchor on canonical_patient_master so we have one row per patient.
all_patients AS (
    SELECT CAST(research_id AS VARCHAR) AS research_id
    FROM main.canonical_patient_master
),
-- First surgery date per patient (denormalized for convenience).
first_surgery AS (
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        MIN(CAST(surgery_date_native AS DATE)) AS first_surgery_date
    FROM main.canonical_operative_events_v1
    WHERE surgery_date_native IS NOT NULL
    GROUP BY research_id
),
-- LLM-derived deceased entities: pull (research_id, death_date).
llm_deceased AS (
    SELECT
        CAST(p.research_id AS VARCHAR) AS research_id,
        TRY_CAST(json_extract_string(e_json, '$.entity_date') AS DATE)
            AS death_date,
        json_extract_string(e_json, '$.entity_value') AS raw_value,
        ROW_NUMBER() OVER (
            PARTITION BY CAST(p.research_id AS VARCHAR)
            ORDER BY
              TRY_CAST(json_extract_string(e_json, '$.entity_date') AS DATE)
                DESC NULLS LAST,
              p.extracted_at DESC
        ) AS rn
    FROM main.note_entities_llm_survival_followup p,
         UNNEST(json_extract(p.result_json, '$.entities')::JSON[]) t(e_json)
    WHERE p.result_json LIKE '{{"entities":%'
      AND p.result_json NOT LIKE '{{"entities": []}}'
      AND json_extract_string(e_json, '$.entity_type') = 'vital_status'
      AND LOWER(json_extract_string(e_json, '$.entity_value')) IN
          ('deceased', 'dead', 'died', 'expired')
),
llm_deceased_one AS (
    SELECT research_id, death_date, raw_value
    FROM llm_deceased
    WHERE rn = 1
),
-- LLM-derived last_followup_date entities.
llm_followup_dates AS (
    SELECT
        CAST(p.research_id AS VARCHAR) AS research_id,
        MAX(TRY_CAST(json_extract_string(e_json, '$.entity_value') AS DATE))
            AS llm_last_followup_date
    FROM main.note_entities_llm_survival_followup p,
         UNNEST(json_extract(p.result_json, '$.entities')::JSON[]) t(e_json)
    WHERE p.result_json LIKE '{{"entities":%'
      AND p.result_json NOT LIKE '{{"entities": []}}'
      AND json_extract_string(e_json, '$.entity_type') = 'last_followup_date'
    GROUP BY 1
),
-- Latest lab contact date per patient (across the 5 known per-analyte
-- canonicals — Script 347 baseline). Labs are the most reliable
-- evidence-of-life signal because they're date-stamped and verifiable.
latest_lab_date AS (
    SELECT
        research_id,
        MAX(contact_date) AS lab_last_date,
        ANY_VALUE(source) AS lab_source_any  -- representative; ARG_MAX nicer but optional
    FROM (
        {lab_dates_union}
    )
    GROUP BY research_id
),
-- Latest operative-event date per patient (an op note implies the
-- patient was alive at the time of surgery + early postop period).
latest_op_date AS (
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        MAX(CAST(surgery_date_native AS DATE)) AS op_last_date,
        MAX(CAST(note_date_resolved AS DATE)) AS op_note_last_date
    FROM main.canonical_operative_events_v1
    GROUP BY research_id
),
-- Compose last_known_alive_date as the greatest of the 4 source signals,
-- preferring lab dates as the most reliable. last_followup_source records
-- which signal won the GREATEST().
last_alive AS (
    SELECT
        p.research_id,
        GREATEST(
            COALESCE(lab.lab_last_date, DATE '1900-01-01'),
            COALESCE(op.op_last_date, DATE '1900-01-01'),
            COALESCE(op.op_note_last_date, DATE '1900-01-01'),
            COALESCE(lf.llm_last_followup_date, DATE '1900-01-01')
        ) AS raw_max,
        lab.lab_last_date,
        op.op_last_date, op.op_note_last_date,
        lf.llm_last_followup_date,
        lab.lab_source_any
    FROM all_patients p
    LEFT JOIN latest_lab_date lab ON lab.research_id = p.research_id
    LEFT JOIN latest_op_date  op  ON op.research_id  = p.research_id
    LEFT JOIN llm_followup_dates lf ON lf.research_id = p.research_id
),
last_alive_resolved AS (
    SELECT
        research_id,
        CASE WHEN raw_max = DATE '1900-01-01' THEN NULL ELSE raw_max END
            AS last_known_alive_date,
        CASE
            WHEN raw_max = DATE '1900-01-01' THEN NULL
            WHEN raw_max = lab_last_date          THEN COALESCE(lab_source_any,
                                                                 'lab_canonical')
            WHEN raw_max = op_last_date           THEN 'operative_events_surgery_date'
            WHEN raw_max = op_note_last_date      THEN 'operative_events_note_date'
            WHEN raw_max = llm_last_followup_date THEN 'llm_last_followup_date_entity'
            ELSE 'unknown'
        END AS last_followup_source
    FROM last_alive
)
SELECT
    p.research_id,
    -- vital_status: deceased if LLM emitted a deceased entity; else alive
    -- if we have ANY contact signal; else unknown.
    CASE
        WHEN dec.research_id IS NOT NULL                          THEN 'deceased'
        WHEN la.last_known_alive_date IS NOT NULL                 THEN 'alive'
        ELSE 'unknown'
    END                                                            AS vital_status_current,
    dec.death_date                                                 AS death_date,
    CASE
        WHEN dec.research_id IS NOT NULL
            THEN 'llm_survival_followup_vital_status'
        ELSE NULL
    END                                                            AS death_date_source,
    la.last_known_alive_date                                       AS last_known_alive_date,
    la.last_followup_source                                        AS last_followup_source,
    CASE
        WHEN la.last_known_alive_date IS NOT NULL
             AND fs.first_surgery_date IS NOT NULL
            THEN DATE_DIFF('day', fs.first_surgery_date, la.last_known_alive_date)
        ELSE NULL
    END                                                            AS days_from_first_surgery_to_last_contact,
    CASE
        WHEN la.last_known_alive_date IS NOT NULL
             AND fs.first_surgery_date IS NOT NULL
             AND DATE_DIFF('day', fs.first_surgery_date, la.last_known_alive_date)
                 >= {YEAR_5_DAYS}
            THEN TRUE
        ELSE FALSE
    END                                                            AS followup_complete_at_5yr,
    CASE
        WHEN la.last_known_alive_date IS NOT NULL
             AND fs.first_surgery_date IS NOT NULL
             AND DATE_DIFF('day', fs.first_surgery_date, la.last_known_alive_date)
                 >= {YEAR_10_DAYS}
            THEN TRUE
        ELSE FALSE
    END                                                            AS followup_complete_at_10yr,
    fs.first_surgery_date                                          AS first_surgery_date,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP)                           AS build_ts,
    '{SCRIPT_ID}'                                                  AS build_script,
    'build_{BUILD_TS}'                                             AS extraction_run_id
FROM all_patients p
LEFT JOIN last_alive_resolved la ON la.research_id = p.research_id
LEFT JOIN llm_deceased_one    dec ON dec.research_id = p.research_id
LEFT JOIN first_surgery       fs  ON fs.research_id  = p.research_id
"""


def step_0_preflight(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    log("=" * 78)
    log(f"STEP 0 — Pre-flight (BUILD_TS={BUILD_TS})")
    log("=" * 78)

    required: list[tuple[str, list[str]]] = [
        ("canonical_patient_master", ["research_id"]),
        ("note_entities_llm_survival_followup",
         ["research_id", "result_json", "extracted_at"]),
        ("canonical_operative_events_v1",
         ["research_id", "surgery_date_native"]),
        ("canonical_labs_calcium_v1",
         ["research_id", "lab_datetime"]),
        ("canonical_labs_pth_v1",
         ["research_id", "lab_datetime"]),
        ("canonical_labs_thyroglobulin_v1",
         ["research_id", "lab_datetime"]),
        ("canonical_labs_tsh_v1",
         ["research_id", "lab_datetime"]),
        ("canonical_labs_vitamin_d_v1",
         ["research_id", "lab_datetime"]),
    ]

    misses: dict[str, list[str]] = {}
    counts: dict[str, int] = {}
    for tbl, cols in required:
        if not table_exists(con, "main", tbl):
            misses[tbl] = ["TABLE MISSING"]
            log_error(f"  required source missing: main.{tbl}")
            continue
        n = row_count(con, "main", tbl)
        counts[tbl] = n
        present = set(list_columns(con, "main", tbl))
        missing_cols = [c for c in cols if c not in present]
        if missing_cols:
            misses[tbl] = missing_cols
            log_error(f"  {tbl}: MISSING required cols {missing_cols}")
        else:
            log(f"  {tbl}: rows={n:,} (all {len(cols)} required cols present)")

    if misses:
        raise RuntimeError(f"Pre-flight failed: {misses}")

    return {"counts": counts}


def step_1_build(con: duckdb.DuckDBPyConnection, do_writes: bool) -> dict[str, Any]:
    log("=" * 78)
    log(f"STEP 1 — Build main.{NEW_TABLE}")
    log("=" * 78)

    sql = _build_sql()
    if not do_writes:
        log("  [dry-run] materialising into a TEMP table for inspection")
        con.execute("DROP TABLE IF EXISTS temp_survival_dry_run")
        temp_sql = sql.replace(
            f"CREATE OR REPLACE TABLE {fq('main', NEW_TABLE)}",
            "CREATE TEMP TABLE temp_survival_dry_run",
        )
        con.execute(temp_sql)
        n = int(con.execute(
            "SELECT COUNT(*) FROM temp_survival_dry_run"
        ).fetchone()[0])
        log(f"  [dry-run] would build {n:,} rows")
        # vital_status distribution
        rows = con.execute(
            "SELECT vital_status_current, COUNT(*) FROM temp_survival_dry_run "
            "GROUP BY 1 ORDER BY 2 DESC"
        ).fetchall()
        log("  [dry-run] vital_status distribution:")
        for v, n in rows:
            log(f"    {v!s:15s} {n:,}")
        n_5yr = int(con.execute(
            "SELECT SUM(CASE WHEN followup_complete_at_5yr THEN 1 ELSE 0 END) "
            "FROM temp_survival_dry_run"
        ).fetchone()[0])
        n_10yr = int(con.execute(
            "SELECT SUM(CASE WHEN followup_complete_at_10yr THEN 1 ELSE 0 END) "
            "FROM temp_survival_dry_run"
        ).fetchone()[0])
        log(f"  [dry-run] followup_complete_at_5yr=TRUE: {n_5yr:,}")
        log(f"  [dry-run] followup_complete_at_10yr=TRUE: {n_10yr:,}")
        # last_followup_source distribution
        rows = con.execute(
            "SELECT last_followup_source, COUNT(*) FROM temp_survival_dry_run "
            "GROUP BY 1 ORDER BY 2 DESC"
        ).fetchall()
        log("  [dry-run] last_followup_source distribution:")
        for v, n in rows:
            log(f"    {v!s:40s} {n:,}")
        # death_date_source presence
        n_with_death_date = int(con.execute(
            "SELECT COUNT(*) FROM temp_survival_dry_run WHERE death_date IS NOT NULL"
        ).fetchone()[0])
        log(f"  [dry-run] rows with death_date populated: {n_with_death_date:,}")
        con.execute("DROP TABLE temp_survival_dry_run")
        return {"created": False, "rows": n}

    con.execute(sql)
    n = row_count(con, "main", NEW_TABLE)
    log(f"  built main.{NEW_TABLE}: {n:,} rows")
    try:
        con.execute(
            f"COMMENT ON TABLE {fq('main', NEW_TABLE)} IS "
            f"'[domain=survival_followup; grain=per_patient] — source: "
            f"{SCRIPT_TAG} ({RUN_DATE}). One row per patient in "
            f"canonical_patient_master. vital_status from LLM survival_followup "
            f"vital_status entities; last_known_alive_date is GREATEST across "
            f"5 lab canonicals + canonical_operative_events_v1 + LLM "
            f"last_followup_date entities. days_from_first_surgery_to_last_contact "
            f"is NULL when first_surgery_date or last_known_alive_date is NULL.'"
        )
    except Exception as exc:
        log_warn(f"  COMMENT ON {NEW_TABLE} failed (non-fatal): {exc}")
    return {"created": True, "rows": n}


def step_2_view(con: duckdb.DuckDBPyConnection, do_writes: bool) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 2 — Create / refresh views_readable view")
    log("=" * 78)
    if do_writes:
        if not table_exists(con, "main", NEW_TABLE):
            log_warn(f"  base table missing, skipping view")
            return {"created": False}
        con.execute(
            f'CREATE OR REPLACE VIEW "{CANONICAL_DB}"."{VIEW_SCHEMA}".'
            f'"{NEW_VIEW}" AS SELECT * FROM {fq("main", NEW_TABLE)}'
        )
        log(f"  created {VIEW_SCHEMA}.{NEW_VIEW}")
    else:
        log(f"  [dry-run] would CREATE OR REPLACE VIEW {VIEW_SCHEMA}.{NEW_VIEW}")
    return {"created": do_writes}


def step_3_registry(
    con: duckdb.DuckDBPyConnection, do_writes: bool
) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 3 — Registry sync")
    log("=" * 78)
    if not table_exists(con, WS_SCHEMA, REGISTRY_TABLE):
        log_warn(f"  registry {WS_SCHEMA}.{REGISTRY_TABLE} missing, skipping")
        return {"skipped": True}
    reg_cols = list_columns(con, WS_SCHEMA, REGISTRY_TABLE)

    n_pre = int(con.execute(
        f"SELECT COUNT(*) FROM {fq(WS_SCHEMA, REGISTRY_TABLE)} "
        f"WHERE detail_table_name = ?",
        [NEW_TABLE],
    ).fetchone()[0])
    log(f"  registry rows for {NEW_TABLE} (pre-delete): {n_pre}")

    if not do_writes:
        log(f"  [dry-run] would DELETE {n_pre} stale row(s) and INSERT new row")
        return {"pre_count": n_pre, "skipped_writes": True}

    con.execute(
        f"DELETE FROM {fq(WS_SCHEMA, REGISTRY_TABLE)} "
        f"WHERE detail_table_name = ?",
        [NEW_TABLE],
    )
    if not table_exists(con, "main", NEW_TABLE):
        log_warn(f"  cannot insert: {NEW_TABLE} not yet built")
        return {"pre_count": n_pre, "skipped_no_table": True}
    n = row_count(con, "main", NEW_TABLE)
    p = int(con.execute(
        f"SELECT COUNT(DISTINCT research_id) FROM {fq('main', NEW_TABLE)}"
    ).fetchone()[0])
    rec: dict[str, Any] = {
        "detail_table_name":          NEW_TABLE,
        "schema_name":                "main",
        "join_key":                   "research_id",
        "grain":                      "per_patient",
        "total_rows":                 n,
        "total_patients":             p,
        "domain":                     "survival_followup",
        "feeds_master_columns":
            "vital_status_current, death_date, last_known_alive_date, "
            "followup_complete_at_5yr, followup_complete_at_10yr",
        "description":
            "Per-patient survival/follow-up canonical. vital_status_current "
            "+ death_date from LLM survival_followup entities; "
            "last_known_alive_date as GREATEST across labs / op events / LLM "
            "follow-up entities. Built by " + SCRIPT_TAG + " on " + RUN_DATE + ".",
        "canonical_version":          f"v1_0_script{SCRIPT_ID}",
        "feeds_master_columns_secondary": None,
        "feeds_master_columns_array": [
            "vital_status_current", "death_date",
            "last_known_alive_date", "followup_complete_at_5yr",
            "followup_complete_at_10yr",
        ],
        "needs_manual_review":        False,
    }
    ordered = [(c, rec[c]) for c in reg_cols if c in rec]
    col_csv = ", ".join(c for c, _ in ordered)
    ph_csv = ", ".join("?" for _ in ordered)
    con.execute(
        f"INSERT INTO {fq(WS_SCHEMA, REGISTRY_TABLE)} ({col_csv}) "
        f"VALUES ({ph_csv})",
        [v for _, v in ordered],
    )
    log(f"  registered {NEW_TABLE}: rows={n:,} patients={p:,}")
    return {"deleted": n_pre, "inserted": 1}


def step_4_qa(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 4 — QA")
    log("=" * 78)
    qa: dict[str, Any] = {"checks": [], "passed": True}

    def check(name: str, ok: bool, **details: Any) -> None:
        qa["checks"].append({"name": name, "passed": bool(ok), **details})
        log(f"  QA {'PASS' if ok else 'FAIL'} {name}: {details}")
        if not ok:
            qa["passed"] = False

    if not table_exists(con, "main", NEW_TABLE):
        check("table_exists", False)
        return qa

    n = row_count(con, "main", NEW_TABLE)
    check("table_exists_and_nonempty", n > 0, rows=n)

    # Patient-anchor parity.
    n_cpm = row_count(con, "main", "canonical_patient_master")
    check("patient_anchor_parity", n == n_cpm,
          survival_rows=n, cpm_rows=n_cpm)

    # vital_status domain check.
    bad_vs = con.execute(
        f"SELECT COUNT(*) FROM {fq('main', NEW_TABLE)} "
        f"WHERE vital_status_current NOT IN ('alive', 'deceased', 'unknown')"
    ).fetchone()[0]
    check("vital_status_in_canonical_set", bad_vs == 0, off_domain=bad_vs)

    # Deceased rows must have death_date populated.
    n_deceased_no_date = con.execute(
        f"SELECT COUNT(*) FROM {fq('main', NEW_TABLE)} "
        f"WHERE vital_status_current = 'deceased' "
        f"  AND death_date IS NULL"
    ).fetchone()[0]
    check("deceased_has_death_date", n_deceased_no_date == 0,
          deceased_no_date=n_deceased_no_date)

    # Alive rows should have last_known_alive_date.
    n_alive_no_lka = con.execute(
        f"SELECT COUNT(*) FROM {fq('main', NEW_TABLE)} "
        f"WHERE vital_status_current = 'alive' "
        f"  AND last_known_alive_date IS NULL"
    ).fetchone()[0]
    check("alive_has_last_known_alive_date", n_alive_no_lka == 0,
          alive_no_date=n_alive_no_lka)

    # Distribution informational.
    rows = con.execute(
        f"SELECT vital_status_current, COUNT(*) FROM {fq('main', NEW_TABLE)} "
        f"GROUP BY 1 ORDER BY 2 DESC"
    ).fetchall()
    qa["informational_vital_status"] = {r[0]: int(r[1]) for r in rows}
    log(f"  informational: vital_status={qa['informational_vital_status']}")

    n_5yr = int(con.execute(
        f"SELECT SUM(CASE WHEN followup_complete_at_5yr THEN 1 ELSE 0 END) "
        f"FROM {fq('main', NEW_TABLE)}"
    ).fetchone()[0] or 0)
    n_10yr = int(con.execute(
        f"SELECT SUM(CASE WHEN followup_complete_at_10yr THEN 1 ELSE 0 END) "
        f"FROM {fq('main', NEW_TABLE)}"
    ).fetchone()[0] or 0)
    qa["informational_followup"] = {
        "complete_at_5yr": n_5yr, "complete_at_10yr": n_10yr,
    }
    log(f"  informational: 5yr-complete={n_5yr:,} 10yr-complete={n_10yr:,}")

    QA_DIR.mkdir(parents=True, exist_ok=True)
    QA_PATH.write_text(json.dumps(qa, indent=2, default=str), encoding="utf-8")
    log(f"  QA report -> {QA_PATH.relative_to(REPO_ROOT)}")
    return qa


def main() -> int:
    parser = argparse.ArgumentParser(description="Survival follow-up consolidation (Script 364B)")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--commit", action="store_true")
    mode.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    do_writes = bool(args.commit)
    log(f"Run config: do_writes={do_writes} BUILD_TS={BUILD_TS}")

    try:
        con = connect()
        results: dict[str, Any] = {"build_ts": BUILD_TS, "do_writes": do_writes}
        results["step_0"] = step_0_preflight(con)
        results["step_1"] = step_1_build(con, do_writes)
        results["step_2"] = step_2_view(con, do_writes)
        results["step_3"] = step_3_registry(con, do_writes)
        if do_writes:
            results["step_4"] = step_4_qa(con)
            if not results["step_4"]["passed"]:
                log_error("QA failed — see qa file for details")
                _write_decision(results)
                flush_log()
                return 2
        else:
            log("STEP 4 — QA SKIPPED in dry-run")
            results["step_4"] = {"skipped_dry_run": True, "passed": True}

        _write_decision(results)
        log(f"{SCRIPT_TAG} complete.")
        flush_log()
        return 0
    except Exception as exc:
        log_error(f"FATAL: {exc!r}")
        flush_log()
        raise


def _write_decision(results: dict[str, Any]) -> None:
    decision_path = OUTPUT_DIR / f"{SCRIPT_ID}_decision_{RUN_TS_COMPACT}.json"
    decision_path.write_text(json.dumps(results, indent=2, default=str))
    log(f"  decision log: {decision_path.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    sys.exit(main())
