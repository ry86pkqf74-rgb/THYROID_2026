#!/usr/bin/env python3
"""Script 399 — Malignant NULL stage_group close-out: queue `ajcc8_t_stage` DDL + 2 backfills
+ 2 CPM UPDATEs + 6 queue INSERTs + snapshot + __readme.

Phases
------
* --phase 0 (default) — probe + H1–H10; probe markdown + PROBE_REPORT_SHA256
  (hashed region excludes footer after ``---HASH-BOUNDARY---``).
* --apply — idempotency; else ``--i-approve`` must match probe hash; apply transaction;
  Phase 3 verify; optional ``--phase4`` git+tag+push.

Token: motherduck_client.get_token() (never printed).
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(SCRIPTS_DIR))

from _md_script_runner_utils import normalize_i_approve, probe_report_sha256  # noqa: E402
from motherduck_client import get_token, token_mode  # noqa: E402

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

PUB_DB = "thyroid_canonical_publication_v1_0"
ARC_SCHEMA = "archive_pub_v1_0"
MAIN_SCHEMA = "main"
MS_SCHEMA = "manuscript_workspace"
CPM_TABLE = "canonical_patient_master"
README_TABLE = "__readme"
QUEUE_TABLE = "cpm_stage_group_manual_review_v1"

SCRIPT_FILE = "apply_malignant_null_stage_group_closeout.py"
SCRIPT_ID = "script_399"
SCRIPT_TAG = "script_399"
SNAPSHOT_PREFIX = "cpm_pre_malignant_null_stage_group_closeout_"

MALIGNANT_DX: tuple[str, ...] = (
    "PTC",
    "FTC",
    "HCC",
    "DTC_NOS",
    "MTC",
    "ATC",
    "other_malignant",
)
MALIGNANT_SQL = "(" + ",".join(f"'{d}'" for d in MALIGNANT_DX) + ")"

APPLY_RID_111 = "111"
APPLY_RID_106 = "106"
QUEUE_RIDS: tuple[str, ...] = ("4015", "9600", "423", "924", "6275", "6768")
COHORT_395_RIDS: tuple[str, ...] = ("1404", "12198")
SNAPSHOT_RIDS: tuple[str, ...] = (APPLY_RID_111, APPLY_RID_106) + QUEUE_RIDS
ALL_EIGHT: tuple[str, ...] = SNAPSHOT_RIDS
ALL_QUEUE_T_RIDS: tuple[str, ...] = COHORT_395_RIDS + QUEUE_RIDS
QUEUE_T_COL = "ajcc8_t_stage"
# Post-apply expected T values on queue (NULL = explicit SQL NULL)
EXPECTED_QUEUE_T_BY_RID: dict[str, str | None] = {
    "1404": None,
    "12198": None,
    "4015": "T2",
    "9600": "T1b",
    "423": None,
    "924": "T3b",
    "6275": None,
    "6768": "T1a",
}

# (research_id, reason, path_stage_raw, gm_path_stage_raw, ajcc8_t_stage, n, m, age, dx)
QUEUE_ROWS: list[tuple[str, str, str | None, str | None, str | None, str, str, int, str]] = [
    (
        "4015",
        "mtc_t2_n1a_m0_rule_yields_iii_no_builder_or_path_corroboration",
        None,
        None,
        "T2",
        "N1a",
        "M0",
        72,
        "MTC",
    ),
    (
        "9600",
        "mtc_m1_ajcc8_rule_yields_ivc_but_builder_and_path_both_say_ivb_edition_adjudication_needed",
        "IVB",
        None,
        "T1b",
        "N0",
        "M1",
        63,
        "MTC",
    ),
    (
        "423",
        "mtc_t_null_cannot_derive_plus_builder_corrected_i_is_dtc_rule_misapplied_to_mtc_n1a_m0_row",
        None,
        None,
        None,
        "N1a",
        "M0",
        47,
        "MTC",
    ),
    (
        "924",
        "mtc_multi_axis_primary_v2_disagreement_t3b_vs_t1a_n1a_vs_n1b_builder_and_path_both_i_no_combination_reconciles",
        "I",
        None,
        "T3b",
        "N1a",
        "M0",
        33,
        "MTC",
    ),
    (
        "6275",
        "other_malignant_staging_rules_undefined_t_null_n_disagreement_n0_vs_n1a",
        None,
        None,
        None,
        "N0",
        "M0",
        38,
        "other_malignant",
    ),
    (
        "6768",
        "other_malignant_staging_rules_undefined_n_disagreement_n1a_vs_n0_path_ii",
        "II",
        None,
        "T1a",
        "N1a",
        "M0",
        62,
        "other_malignant",
    ),
]
BACKFILL_SOURCE_395 = "395"

CPM_EXPECTED = 10_871
EXPECT_MALIGNANT_NULL_PRE = 10
# Post-apply: 2 CF-395-1 rows + 6 Script-399 queue-only rows still NULL in CPM (not UPDATEd)
EXPECT_MALIGNANT_NULL_POST = 8
MALIGNANT_NULL_POST_RIDS: frozenset[str] = frozenset(COHORT_395_RIDS + QUEUE_RIDS)
SOURCE_SCRIPT_QUEUE = "399"

HASH_BOUNDARY = "---HASH-BOUNDARY---"

OUTPUT_DIR = REPO_ROOT / "scripts" / "output"
PROBE_PATH = OUTPUT_DIR / "apply_malignant_null_stage_group_closeout_probe.md"
RUN_LOG_PATH = OUTPUT_DIR / "apply_malignant_null_stage_group_closeout_run.log"
CLOSE_OUT_PATH = REPO_ROOT / "cursor_prompts" / "CLOSE_OUT_399.md"
PROMPT_PATH = REPO_ROOT / "cursor_prompts" / (
    "CURSOR_PROMPT_MALIGNANT_NULL_STAGE_GROUP_CLOSEOUT_20260423_SCRIPT_399.md"
)
TAG_PREFIX = "v1_0-malignant-null-stage-group-closeout-"

FORCE_ADD_PATTERNS: list[str] = [r"scripts/output/.*_run\.log$"]


def cpm_fq() -> str:
    return f'"{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"'


def readme_fq() -> str:
    return f'"{PUB_DB}"."{MAIN_SCHEMA}"."{README_TABLE}"'


def queue_fq() -> str:
    return f'"{PUB_DB}"."{MS_SCHEMA}"."{QUEUE_TABLE}"'


def fq_snap(snap_name: str) -> str:
    return f'"{PUB_DB}"."{ARC_SCHEMA}"."{snap_name}"'


def _should_force_add(path: Path) -> bool:
    try:
        s = str(path.relative_to(REPO_ROOT))
    except ValueError:
        s = str(path)
    return any(re.search(p, s) for p in FORCE_ADD_PATTERNS)


# --------------------------------------------------------------------------- #
# Apply SQL (CPM)
# --------------------------------------------------------------------------- #

# H8: only ajcc8_stage_group in SET — validated by cpm_set_clause_safe
UPDATE_A1_SQL = f"""
UPDATE {cpm_fq()}
SET ajcc8_stage_group = 'I'
WHERE CAST(research_id AS VARCHAR) = '111' AND diagnosis_primary = 'DTC_NOS' AND age_at_surgery = 28
  AND ajcc8_t_stage = 'T1b' AND ajcc8_n_stage = 'N1a' AND ajcc8_m_stage = 'M0'
  AND ajcc8_stage_group IS NULL
  AND ajcc8_stage_group_corrected = 'I'
  AND path_stage_raw = 'I'
RETURNING research_id
""".strip()

UPDATE_A2_SQL = f"""
UPDATE {cpm_fq()}
SET ajcc8_stage_group = 'I'
WHERE CAST(research_id AS VARCHAR) = '106' AND diagnosis_primary = 'MTC' AND age_at_surgery = 60
  AND ajcc8_t_stage = 'T1b' AND ajcc8_n_stage = 'N0' AND ajcc8_m_stage = 'M0'
  AND ajcc8_stage_group IS NULL
  AND path_stage_raw = 'I'
RETURNING research_id
""".strip()


def cpm_set_clause_safe(sql: str) -> bool:
    m = re.search(r"\bSET\s+(.+?)\s+WHERE", sql, re.I | re.S)
    if not m:
        return False
    clause = m.group(1).strip()
    return re.fullmatch(r"ajcc8_stage_group\s*=\s*'I'", clause) is not None


def cpm_update_sql_audit_ok() -> bool:
    for s in (UPDATE_A1_SQL, UPDATE_A2_SQL):
        if not cpm_set_clause_safe(s):
            return False
    return True


def build_queue_insert_sql() -> str:
    """Single INSERT for 6 queue rows; source_script from SOURCE_SCRIPT_QUEUE; includes T column."""
    vals: list[str] = []
    for row in QUEUE_ROWS:
        rid, reason, praw, gmr, t_st, nst, mst, age, dx = row
        pr = "NULL" if praw is None else f"'{praw.replace(chr(39), chr(39)+chr(39))}'"
        gm = "NULL" if gmr is None else f"'{gmr}'"
        t_sql = "NULL" if t_st is None else f"'{t_st}'"
        rv = reason.replace("'", "''")
        vals.append(
            f"  ('{rid}',\n   '{rv}',\n   {pr}, {gm}, {t_sql}, '{nst}', '{mst}', {age}, '{dx}', "
            f"'{SOURCE_SCRIPT_QUEUE}', CAST(CURRENT_TIMESTAMP AS TIMESTAMP))"
        )
    body = ",\n".join(vals)
    return f"""
INSERT INTO {queue_fq()}
  (research_id, reason, path_stage_raw, gm_path_stage_raw, ajcc8_t_stage, ajcc8_n_stage, ajcc8_m_stage,
   age_at_surgery, diagnosis_primary, source_script, inserted_at)
VALUES
{body}
""".strip()


INSERT_QUEUE_SQL = None  # set at module init below — needs build_queue_insert_sql


def _init_queue_sql() -> str:
    global INSERT_QUEUE_SQL
    INSERT_QUEUE_SQL = build_queue_insert_sql()
    return INSERT_QUEUE_SQL


_init_queue_sql()


# --------------------------------------------------------------------------- #
# Logging / connection
# --------------------------------------------------------------------------- #


def log(msg: str, *, also_print: bool = True) -> None:
    line = f"{datetime.now(timezone.utc).isoformat()} {msg}"
    if also_print:
        print(line, flush=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(RUN_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def fresh_log() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    REPO_ROOT.joinpath("cursor_prompts").mkdir(parents=True, exist_ok=True)
    RUN_LOG_PATH.write_text("", encoding="utf-8")


def _connect_with_token(*, log_connect: bool) -> duckdb.DuckDBPyConnection:
    tok = get_token()
    if not tok:
        raise SystemExit(
            f"No MotherDuck RW token (token_mode={token_mode()}).  "
            "Set MD_SA_TOKEN / MOTHERDUCK_TOKEN or motherduck.local.toml."
        )
    if log_connect:
        log(f"Connecting md:{PUB_DB} (token_mode={token_mode()})")
    con = duckdb.connect(f"md:{PUB_DB}?motherduck_token={tok}")
    con.execute(f'USE "{PUB_DB}"')
    con.execute(f'USE "{PUB_DB}".main')
    dbs = {r[0] for r in con.execute("SELECT database_name FROM duckdb_databases()").fetchall()}
    if PUB_DB not in dbs:
        raise SystemExit(f"PUB DB '{PUB_DB}' not attached")
    return con


def connect() -> duckdb.DuckDBPyConnection:
    return _connect_with_token(log_connect=True)


def connect_quiet() -> duckdb.DuckDBPyConnection:
    return _connect_with_token(log_connect=False)


def table_exists(con: duckdb.DuckDBPyConnection, schema: str, name: str) -> bool:
    row = con.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
        """,
        [PUB_DB, schema, name],
    ).fetchone()
    return row is not None


def readme_columns(con: duckdb.DuckDBPyConnection) -> set[str]:
    if not table_exists(con, MAIN_SCHEMA, README_TABLE):
        return set()
    rows = con.execute(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
        """,
        [PUB_DB, MAIN_SCHEMA, README_TABLE],
    ).fetchall()
    return {r[0] for r in rows}


def queue_ajcc8_t_column_state(con: duckdb.DuckDBPyConnection) -> str:
    """Return missing_table | absent | varchar | invalid:<data_type>."""
    if not table_exists(con, MS_SCHEMA, QUEUE_TABLE):
        return "missing_table"
    row = con.execute(
        """
        SELECT data_type FROM information_schema.columns
        WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
          AND LOWER(column_name) = 'ajcc8_t_stage'
        """,
        [PUB_DB, MS_SCHEMA, QUEUE_TABLE],
    ).fetchone()
    if not row:
        return "absent"
    dt = (row[0] or "").upper()
    if "VARCHAR" in dt or "CHAR" in dt or "TEXT" in dt:
        return "varchar"
    return f"invalid:{row[0]}"


def h10_dependents_check() -> tuple[bool, str]:
    """Re-grep repo; only 395, cursor_prompts, 399, 396 apply script (docs) allowed."""
    try:
        r = subprocess.run(
            ["rg", "-l", "cpm_stage_group_manual_review_v1", str(REPO_ROOT)],
            capture_output=True,
            text=True,
            timeout=120,
        )
    except FileNotFoundError:
        r = subprocess.run(
            ["git", "-C", str(REPO_ROOT), "grep", "-l", "cpm_stage_group_manual_review_v1", "--", "."],
            capture_output=True,
            text=True,
            timeout=120,
        )
    if r.returncode not in (0, 1):
        return False, f"search failed (exit {r.returncode}): {r.stderr or r.stdout}"
    paths = [ln.strip() for ln in (r.stdout or "").splitlines() if ln.strip()]

    def _allowed(rel: str) -> bool:
        rpath = rel.replace("\\", "/")
        if rpath.startswith("cursor_prompts/") or "/cursor_prompts/" in rpath:
            return True
        if "395_dtc_t_sync_stage_group_fill" in rpath:
            return True
        if "apply_malignant_null_stage_group_closeout" in rpath:
            return True
        if "apply_dtc_null_n_stage_group_fill" in rpath:
            return True
        return False

    bad: list[str] = []
    for p in paths:
        try:
            rel = str(Path(p).resolve().relative_to(REPO_ROOT))
        except ValueError:
            rel = p
        if not _allowed(rel):
            bad.append(rel)
    if bad:
        return False, f"unexpected file references: {bad}"
    return True, "ok"


def ensure_queue_ajcc8_t_column(con: duckdb.DuckDBPyConnection) -> None:
    st = queue_ajcc8_t_column_state(con)
    if st == "varchar":
        return
    if st == "missing_table":
        raise SystemExit("apply: queue table missing")
    if st.startswith("invalid"):
        raise SystemExit(f"apply: bad ajcc8_t_stage type ({st})")
    if st != "absent":
        raise SystemExit(f"apply: unexpected column state: {st!r}")
    qf = queue_fq()
    try:
        con.execute(
            f"ALTER TABLE {qf} ADD COLUMN IF NOT EXISTS {QUEUE_T_COL} VARCHAR"
        )
    except Exception as e1:
        log(
            f"ADD COLUMN IF NOT EXISTS failed ({e1!r}) — information_schema guard",
            also_print=True,
        )
        ex = con.execute(
            """
            SELECT 1 FROM information_schema.columns
            WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
              AND LOWER(column_name) = 'ajcc8_t_stage'
            """,
            [PUB_DB, MS_SCHEMA, QUEUE_TABLE],
        ).fetchone()
        if ex and queue_ajcc8_t_column_state(con) == "varchar":
            return
        con.execute(f"ALTER TABLE {qf} ADD COLUMN {QUEUE_T_COL} VARCHAR")
    if queue_ajcc8_t_column_state(con) != "varchar":
        raise SystemExit("apply: after ALTER, ajcc8_t_stage must be VARCHAR")


def run_queue_395_t_backfill(con: duckdb.DuckDBPyConnection) -> None:
    cpm = cpm_fq()
    qf = queue_fq()
    for rid in COHORT_395_RIDS:
        con.execute(
            f"""
            UPDATE {qf}
            SET {QUEUE_T_COL} = (
              SELECT c.{QUEUE_T_COL} FROM {cpm} c
              WHERE CAST(c.research_id AS VARCHAR) = ?
            )
            WHERE CAST(research_id AS VARCHAR) = ? AND source_script = ?
            """,
            [rid, rid, BACKFILL_SOURCE_395],
        )
        nq = con.execute(
            f"SELECT COUNT(*) FROM {qf} WHERE CAST(research_id AS VARCHAR) = ?",
            [rid],
        ).fetchone()[0]
        if nq != 1:
            raise SystemExit(f"backfill: expected 1 queue row for {rid}, have {nq}")


def queue_t_stages_mismatch_list(con: duckdb.DuckDBPyConnection) -> list[str]:
    """Differs from EXPECTED_QUEUE_T_BY_RID (NULLs explicit)."""
    errs: list[str] = []
    qf = queue_fq()
    for rid, exp in EXPECTED_QUEUE_T_BY_RID.items():
        rows = con.execute(
            f"SELECT {QUEUE_T_COL} FROM {qf} WHERE CAST(research_id AS VARCHAR) = ?",
            [rid],
        ).fetchall()
        if len(rows) != 1:
            errs.append(f"queue T: {rid} expected 1 row, got {len(rows)}")
            continue
        val = rows[0][0]
        if exp is None:
            if val is not None:
                errs.append(f"queue T: {rid} expected NULL, got {val!r}")
        else:
            if val != exp:
                errs.append(f"queue T: {rid} expected {exp!r}, got {val!r}")
    return errs


def count_snapshots_399(con: duckdb.DuckDBPyConnection) -> int:
    return con.execute(
        f"""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_catalog = ? AND table_schema = ?
          AND table_name LIKE '{SNAPSHOT_PREFIX}%'
        """,
        [PUB_DB, ARC_SCHEMA],
    ).fetchone()[0]


def find_snapshot_399(con: duckdb.DuckDBPyConnection) -> str | None:
    rows = con.execute(
        f"""
        SELECT table_name FROM information_schema.tables
        WHERE table_catalog = ? AND table_schema = ?
          AND table_name LIKE '{SNAPSHOT_PREFIX}%'
        ORDER BY table_name DESC
        LIMIT 1
        """,
        [PUB_DB, ARC_SCHEMA],
    ).fetchall()
    return rows[0][0] if rows else None


def snapshot_run_stamp(snap_table: str) -> str:
    if not snap_table.startswith(SNAPSHOT_PREFIX):
        raise ValueError(f"unexpected snapshot table name: {snap_table!r}")
    return snap_table[len(SNAPSHOT_PREFIX) :]


def readme_399_count(con: duckdb.DuckDBPyConnection) -> int:
    cols = readme_columns(con)
    if not cols:
        return 0
    if "script" in cols:
        return con.execute(
            f"SELECT COUNT(*) FROM {readme_fq()} WHERE script = ?",
            [SCRIPT_ID],
        ).fetchone()[0]
    if "script_tag" in cols:
        return con.execute(
            f'SELECT COUNT(*) FROM {readme_fq()} WHERE script_tag = ?',
            [SCRIPT_TAG],
        ).fetchone()[0]
    return con.execute(
        f"""
        SELECT COUNT(*) FROM {readme_fq()}
        WHERE content LIKE ?
        """,
        [f"%{SCRIPT_ID}%"],
    ).fetchone()[0]


def apply_rows_staged(con: duckdb.DuckDBPyConnection) -> bool:
    cpm = cpm_fq()
    a = con.execute(
        f"""
        SELECT ajcc8_stage_group FROM {cpm}
        WHERE CAST(research_id AS VARCHAR) = ? AND diagnosis_primary = 'DTC_NOS'
        """,
        [APPLY_RID_111],
    ).fetchone()
    b = con.execute(
        f"""
        SELECT ajcc8_stage_group FROM {cpm}
        WHERE CAST(research_id AS VARCHAR) = ? AND diagnosis_primary = 'MTC'
        """,
        [APPLY_RID_106],
    ).fetchone()
    return bool(a and a[0] == "I" and b and b[0] == "I")


def queue_399_complete(con: duckdb.DuckDBPyConnection) -> bool:
    if not table_exists(con, MS_SCHEMA, QUEUE_TABLE):
        return False
    qf = queue_fq()
    for rid in QUEUE_RIDS:
        n = con.execute(
            f"""
            SELECT COUNT(*) FROM {qf}
            WHERE CAST(research_id AS VARCHAR) = ? AND source_script = ?
            """,
            [rid, SOURCE_SCRIPT_QUEUE],
        ).fetchone()[0]
        if n != 1:
            return False
    return True


def idempotency_complete(con: duckdb.DuckDBPyConnection) -> bool:
    if queue_ajcc8_t_column_state(con) != "varchar":
        return False
    if not find_snapshot_399(con):
        return False
    if readme_399_count(con) != 1:
        return False
    if not apply_rows_staged(con):
        return False
    if not queue_399_complete(con):
        return False
    if queue_t_stages_mismatch_list(con):
        return False
    return True


def idempotency_partial(con: duckdb.DuckDBPyConnection) -> tuple[bool, str | None]:
    snap = find_snapshot_399(con)
    n_readme = readme_399_count(con)
    st = apply_rows_staged(con)
    qok = queue_399_complete(con)
    if snap and n_readme == 0 and not st:
        return True, "snapshot without __readme and CPM not staged"
    if n_readme and not snap:
        return True, "__readme without snapshot"
    if snap and st and not qok:
        return True, "snapshot + staged CPM but queue incomplete"
    if qok and st and not snap:
        return True, "queue + CPM without snapshot"
    return False, None


# --------------------------------------------------------------------------- #
# Probe
# --------------------------------------------------------------------------- #


H2_A1_CHECK = f"""
SELECT COUNT(*) FROM {cpm_fq()}
WHERE CAST(research_id AS VARCHAR) = '111' AND diagnosis_primary = 'DTC_NOS' AND age_at_surgery = 28
  AND ajcc8_t_stage = 'T1b' AND ajcc8_n_stage = 'N1a' AND ajcc8_m_stage = 'M0'
  AND ajcc8_stage_group IS NULL
  AND ajcc8_stage_group_corrected = 'I'
  AND path_stage_raw = 'I'
""".strip()

H2_A2_CHECK = f"""
SELECT COUNT(*) FROM {cpm_fq()}
WHERE CAST(research_id AS VARCHAR) = '106' AND diagnosis_primary = 'MTC' AND age_at_surgery = 60
  AND ajcc8_t_stage = 'T1b' AND ajcc8_n_stage = 'N0' AND ajcc8_m_stage = 'M0'
  AND ajcc8_stage_group IS NULL
  AND path_stage_raw = 'I'
""".strip()


def run_probe(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    cpm = cpm_fq()
    h1 = con.execute(
        f"""
        SELECT COUNT(*) FROM {cpm}
        WHERE diagnosis_primary IN {MALIGNANT_SQL}
          AND ajcc8_stage_group IS NULL
        """
    ).fetchone()[0]

    h2a = con.execute(H2_A1_CHECK).fetchone()[0]
    h2b = con.execute(H2_A2_CHECK).fetchone()[0]

    h3_queue_absent: dict[str, int] = {}
    h3_cpm_null: dict[str, bool] = {}
    if table_exists(con, MS_SCHEMA, QUEUE_TABLE):
        for rid in QUEUE_RIDS:
            h3_queue_absent[rid] = con.execute(
                f"SELECT COUNT(*) FROM {queue_fq()} WHERE CAST(research_id AS VARCHAR) = ?",
                [rid],
            ).fetchone()[0]
    else:
        for rid in QUEUE_RIDS:
            h3_queue_absent[rid] = -1

    h3_395: dict[str, int] = {}
    if table_exists(con, MS_SCHEMA, QUEUE_TABLE):
        for rid in COHORT_395_RIDS:
            h3_395[rid] = con.execute(
                f"SELECT COUNT(*) FROM {queue_fq()} WHERE CAST(research_id AS VARCHAR) = ?",
                [rid],
            ).fetchone()[0]

    for rid in QUEUE_RIDS + (APPLY_RID_111, APPLY_RID_106):
        sg = con.execute(
            f"SELECT ajcc8_stage_group IS NULL FROM {cpm} WHERE CAST(research_id AS VARCHAR) = ?",
            [rid],
        ).fetchone()
        h3_cpm_null[rid] = bool(sg and sg[0])

    h4 = con.execute(f"SELECT COUNT(*) FROM {cpm}").fetchone()[0]
    h7 = count_snapshots_399(con)

    row_111 = con.execute(
        f"""
        SELECT age_at_surgery, diagnosis_primary, ajcc8_m_stage, ajcc8_stage_group_corrected, path_stage_raw
        FROM {cpm} WHERE CAST(research_id AS VARCHAR) = '111'
        """
    ).fetchone()
    row_106 = con.execute(
        f"""
        SELECT diagnosis_primary, ajcc8_t_stage, ajcc8_n_stage, ajcc8_m_stage, ajcc8_stage_group_corrected, path_stage_raw
        FROM {cpm} WHERE CAST(research_id AS VARCHAR) = '106'
        """
    ).fetchone()

    reasons = [r[1] for r in QUEUE_ROWS]
    h6_unique = len(set(reasons)) == len(reasons) and all(len(x) > 0 for x in reasons)

    per_eight = con.execute(
        f"""
        SELECT
          CAST(research_id AS VARCHAR), diagnosis_primary, age_at_surgery,
          ajcc8_t_stage, ajcc8_n_stage, ajcc8_m_stage, ajcc8_stage_group,
          ajcc8_stage_group_corrected, path_stage_raw
        FROM {cpm}
        WHERE CAST(research_id AS VARCHAR) IN ({",".join("'" + r + "'" for r in ALL_EIGHT)})
        ORDER BY research_id
        """
    ).fetchall()

    h9_state = queue_ajcc8_t_column_state(con)
    h10_ok, h10_detail = h10_dependents_check()

    return {
        "h1": h1,
        "h2a": h2a,
        "h2b": h2b,
        "h3_queue_absent": h3_queue_absent,
        "h3_395": h3_395,
        "h3_cpm_null": h3_cpm_null,
        "h4": h4,
        "h6_unique": h6_unique,
        "h7": h7,
        "h8_sql": cpm_update_sql_audit_ok(),
        "h9_state": h9_state,
        "h10_ok": h10_ok,
        "h10_detail": h10_detail,
        "row_111": row_111,
        "row_106": row_106,
        "per_eight": per_eight,
    }


def check_h5(p: dict[str, Any]) -> list[str]:
    errs: list[str] = []
    r111 = p.get("row_111")
    r106 = p.get("row_106")
    if not r111:
        errs.append("H5: missing row 111")
    else:
        age, dx, m, _corr, pr = r111
        if m != "M0":
            errs.append(f"H5: rid 111 expected M0, got {m!r}")
        if age is None or int(age) >= 55:
            errs.append(f"H5: rid 111 expected age<55, got {age!r}")
        if dx != "DTC_NOS":
            errs.append(f"H5: rid 111 expected DTC_NOS, got {dx!r}")
    if not r106:
        errs.append("H5: missing row 106")
    else:
        dx, t, n, m, _corr, pr = r106
        if (t, n, m) != ("T1b", "N0", "M0"):
            errs.append(f"H5: rid 106 T/N/M expected T1b,N0,M0, got {t!r},{n!r},{m!r}")
        if dx != "MTC":
            errs.append(f"H5: rid 106 expected MTC, got {dx!r}")
        if pr != "I":
            errs.append(f"H5: rid 106 path_stage_raw must corroborate I, got {pr!r}")
    return errs


def halt_gate(p: dict[str, Any]) -> tuple[bool, list[str]]:
    errs: list[str] = []
    if p["h1"] != EXPECT_MALIGNANT_NULL_PRE:
        errs.append(f"H1: malignant NULL stage_group count={p['h1']}, expected {EXPECT_MALIGNANT_NULL_PRE}")
    if p["h2a"] != 1:
        errs.append(f"H2: Write A-1 predicate count={p['h2a']}, expected 1")
    if p["h2b"] != 1:
        errs.append(f"H2: Write A-2 predicate count={p['h2b']}, expected 1")

    for rid, c in p["h3_queue_absent"].items():
        if c == -1:
            errs.append("H3: queue table missing")
            break
        if c != 0:
            errs.append(f"H3: research_id {rid} already in queue (count={c}), expected 0")
    for rid in QUEUE_RIDS:
        if not p["h3_cpm_null"].get(rid, False):
            errs.append(f"H3: {rid} must have NULL ajcc8_stage_group in CPM pre-apply")
    for rid in (APPLY_RID_111, APPLY_RID_106):
        if not p["h3_cpm_null"].get(rid, False):
            errs.append(f"H3: {rid} must have NULL ajcc8_stage_group in CPM pre-apply")

    for rid in COHORT_395_RIDS:
        nq = p.get("h3_395", {}).get(rid, 0)
        if nq < 1:
            errs.append(f"H3: sanity — {rid} should be in manual review queue (CF-395-1)")

    if p["h4"] != CPM_EXPECTED:
        errs.append(f"H4: CPM count={p['h4']}, expected {CPM_EXPECTED}")
    if not p["h6_unique"]:
        errs.append("H6: queue reason strings must be non-empty and unique")
    h7c = p["h7"]
    if h7c > 1:
        errs.append(f"H7: snapshot prefix count={h7c}, expected <=1")
    if not p["h8_sql"]:
        errs.append("H8: CPM UPDATE SQL must only SET ajcc8_stage_group")

    h9s = p.get("h9_state")
    if h9s == "missing_table":
        errs.append("H9: queue table missing")
    elif h9s not in ("absent", "varchar"):
        errs.append(
            f"H9: ajcc8_t_stage pre-state {h9s!r} — want missing column (ALTER path) or VARCHAR"
        )

    if not p.get("h10_ok"):
        errs.append(f"H10: dependents check failed — {p.get('h10_detail', 'unknown')}")

    errs.extend(check_h5(p))
    return (len(errs) == 0, errs)


def format_probe_hashed_body(
    p: dict[str, Any], gate_ok: bool, gate_errs: list[str]
) -> str:
    lines: list[str] = [
        "# Script 399 — Phase 0 probe (malignant NULL stage_group close-out)",
        "",
        "## Halt gates (H1–H10)",
        "",
        f"| all_pass | {gate_ok} |",
        "",
        f"- **H1 (malignant NULL stage_group):** {p['h1']} (expected {EXPECT_MALIGNANT_NULL_PRE})",
        f"- **H2a (A-1 WHERE rows):** {p['h2a']}",
        f"- **H2b (A-2 WHERE rows):** {p['h2b']}",
        f"- **H3 queue — 6 targets absent from queue (counts):** {p['h3_queue_absent']}",
        f"- **H3 395 sanity (1404, 12198 in queue):** {p.get('h3_395', {})}",
        f"- **H4 CPM total:** {p['h4']}",
        f"- **H6 reason uniqueness / non-empty:** {p['h6_unique']}",
        f"- **H7 snapshot prefix tables:** {p['h7']}",
        f"- **H8 CPM SET audit (stage_group only):** {p['h8_sql']}",
        f"- **H9 `ajcc8_t_stage` column (queue):** {p.get('h9_state')} (want absent or VARCHAR)",
        f"- **H10 repo dependents:** ok={p.get('h10_ok')} ({p.get('h10_detail')})",
        "",
    ]
    if gate_errs:
        lines.extend(["### Halts", ""] + [f"- {e}" for e in gate_errs] + [""])

    lines.extend(
        [
            "## H5 — Apply-row signals",
            "",
        ]
    )
    if p.get("row_111"):
        lines.append(f"- **111:** {p['row_111']}")
    if p.get("row_106"):
        lines.append(f"- **106:** {p['row_106']}")
    lines.append("")
    lines.append("## Target cohort (8 rows) — CPM current")
    lines.append("")
    lines.append(
        "| rid | dx | age | T | N | M | stage | corrected | path_raw |"
    )
    lines.append("|---:|---|---:|---|---|---|---:|---|---|")
    for r in p.get("per_eight", []):
        rid, dx, age, t, n, m, sg, sc, pr = r
        def s(x: Any) -> str:
            return "NULL" if x is None else str(x)
        lines.append(
            f"| {rid} | {s(dx)} | {s(age)} | {s(t)} | {s(n)} | {s(m)} | {s(sg)} | {s(sc)} | {s(pr)} |"
        )
    lines.append("")
    lines.append("## Planned writes (revised)")
    lines.append("")
    lines.append(
        "- **S-1:** `ALTER TABLE ... ADD COLUMN` `ajcc8_t_stage` VARCHAR (idempotent) if absent."
    )
    lines.append(
        "- **S-2, S-3:** Backfill 1404, 12198 queue rows from CPM `ajcc8_t_stage` (source_script=395)."
    )
    lines.append("- **A-1, A-2 (CPM):** 111 → I; 106 → I (stage_group only).")
    lines.append(
        "- **B-1..6 (queue):** 4015..6768 with structured `ajcc8_t_stage` (source_script=399)."
    )
    lines.append(
        f"- **Snapshot:** {PUB_DB}.{ARC_SCHEMA}.{SNAPSHOT_PREFIX}<ts> (8 CPM rows)."
    )
    lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def write_probe_file(hashed_body: str) -> str:
    footer_lines = [
        "",
        HASH_BOUNDARY,
        "",
        "## Generation footer (excluded from PROBE_REPORT_SHA256)",
        "",
        f"Written UTC: {datetime.now(timezone.utc).isoformat()}",
        "",
    ]
    PROBE_PATH.parent.mkdir(parents=True, exist_ok=True)
    PROBE_PATH.write_text(hashed_body + "\n".join(footer_lines), encoding="utf-8")
    return probe_report_sha256(hashed_body)


def read_probe_hashed_body_digest() -> str:
    """SHA256 of probe file region above ---HASH-BOUNDARY--- (matches --i-approve)."""
    t = PROBE_PATH.read_text(encoding="utf-8")
    if HASH_BOUNDARY in t:
        body = t.split(HASH_BOUNDARY)[0].rstrip() + "\n"
    else:
        body = t
    return probe_report_sha256(body)


# --------------------------------------------------------------------------- #
# Apply
# --------------------------------------------------------------------------- #


def insert_readme_399(
    con: duckdb.DuckDBPyConnection,
    *,
    snap_fqn: str,
    probe_sha: str,
    run_stamp: str,
) -> None:
    cols = readme_columns(con)
    body = (
        f"Script 399: ADD {QUEUE_T_COL} to queue (if needed); backfill 1404,12198 from CPM; "
        f"2 CPM stage_group updates (111→I, 106→I); 6 queue rows (399) with T column; "
        f"snapshot {snap_fqn}. Malignant NULL 10→8 in CPM (2 CF-395-1 + 6 queue-only 399; 111/106→I). "
        f"Probe SHA256: {probe_sha}. run_stamp={run_stamp}."
    )
    if {"script_name", "script_tag", "run_timestamp", "summary"}.issubset(cols):
        con.execute(
            f"""
            INSERT INTO {readme_fq()} (script_name, script_tag, run_timestamp, summary)
            VALUES (?, ?, CAST(CURRENT_TIMESTAMP AS TIMESTAMP), ?)
            """,
            [SCRIPT_FILE, SCRIPT_TAG, body],
        )
        return
    if {"script", "script_name", "run_timestamp", "content"}.issubset(cols):
        con.execute(
            f"""
            INSERT INTO {readme_fq()}
              (script, script_name, run_timestamp, content)
            VALUES (?, ?, CAST(CURRENT_TIMESTAMP AS TIMESTAMP), ?)
            """,
            [SCRIPT_ID, SCRIPT_FILE, body],
        )
        return
    insert_cols: list[str] = ["content", "updated_at"]
    value_exprs = ["?", "CAST(CURRENT_TIMESTAMP AS TIMESTAMP)"]
    params: list[Any] = [f"Script 399: {SCRIPT_ID} {body}"]
    if "script" in cols:
        insert_cols.append("script")
        value_exprs.append("?")
        params.append(SCRIPT_ID)
    con.execute(
        f"INSERT INTO {readme_fq()} ({', '.join(insert_cols)}) VALUES ({', '.join(value_exprs)})",
        params,
    )


def apply_transaction(
    con: duckdb.DuckDBPyConnection, probe_sha: str, run_stamp: str
) -> str:
    if count_snapshots_399(con) != 0:
        raise SystemExit("Apply: existing snapshot(s) for this script — use NO-OP or clear archive")

    cpm = cpm_fq()
    snap_name = f"{SNAPSHOT_PREFIX}{run_stamp}"
    ssql = fq_snap(snap_name)
    in_list = ",".join("'" + r + "'" for r in ALL_EIGHT)

    con.execute("BEGIN TRANSACTION")
    try:
        log("S-1: ensure ajcc8_t_stage on queue (ALTER if needed)", also_print=True)
        ensure_queue_ajcc8_t_column(con)
        log("S-2, S-3: backfill queue T for 1404, 12198 from CPM", also_print=True)
        run_queue_395_t_backfill(con)

        con.execute(
            f"""
            CREATE TABLE {ssql} AS
            SELECT * FROM {cpm}
            WHERE CAST(research_id AS VARCHAR) IN ({in_list})
            """
        )
        n_snap = con.execute(f"SELECT COUNT(*) FROM {ssql}").fetchone()[0]
        if n_snap != 8:
            raise SystemExit(f"Snapshot rows={n_snap}, expected 8")

        u1 = con.execute(UPDATE_A1_SQL).fetchall()
        u2 = con.execute(UPDATE_A2_SQL).fetchall()
        if len(u1) != 1 or len(u2) != 1:
            raise SystemExit(f"UPDATEs expected 1+1 rows, got {len(u1)}+{len(u2)}")

        con.execute(INSERT_QUEUE_SQL)  # type: ignore[arg-type]

        for rid in QUEUE_RIDS:
            nq = con.execute(
                f"""
                SELECT COUNT(*) FROM {queue_fq()}
                WHERE CAST(research_id AS VARCHAR) = ? AND source_script = ?
                """,
                [rid, SOURCE_SCRIPT_QUEUE],
            ).fetchone()[0]
            if nq != 1:
                raise SystemExit(f"Queue row for {rid} not unique after insert (n={nq})")

        snap_fqn = f"{PUB_DB}.{ARC_SCHEMA}.{snap_name}"
        insert_readme_399(con, snap_fqn=snap_fqn, probe_sha=probe_sha, run_stamp=run_stamp)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    return snap_name


# --------------------------------------------------------------------------- #
# Verify
# --------------------------------------------------------------------------- #


def run_verify(
    con: duckdb.DuckDBPyConnection, snap_name: str
) -> tuple[bool, list[str], dict[str, Any]]:
    cpm = cpm_fq()
    snap = fq_snap(snap_name)
    errs: list[str] = []
    info: dict[str, Any] = {}

    p1 = con.execute(f"SELECT COUNT(*) FROM {cpm}").fetchone()[0]
    if p1 != CPM_EXPECTED:
        errs.append(f"P1: CPM={p1}, expected {CPM_EXPECTED}")

    for rid, exp in ((APPLY_RID_111, "I"), (APPLY_RID_106, "I")):
        g = con.execute(
            f"SELECT ajcc8_stage_group FROM {cpm} WHERE CAST(research_id AS VARCHAR) = ?",
            [rid],
        ).fetchone()
        if not g or g[0] != exp:
            errs.append(f"P2: {rid} stage_group={g}, expected {exp}")

    p3 = con.execute(
        f"""
        SELECT COUNT(*) FROM {cpm}
        WHERE diagnosis_primary IN {MALIGNANT_SQL} AND ajcc8_stage_group IS NULL
        """
    ).fetchone()[0]
    if p3 != EXPECT_MALIGNANT_NULL_POST:
        errs.append(f"P3: malignant NULL stage_group={p3}, expected {EXPECT_MALIGNANT_NULL_POST}")

    nulls = con.execute(
        f"""
        SELECT CAST(research_id AS VARCHAR) FROM {cpm}
        WHERE diagnosis_primary IN {MALIGNANT_SQL} AND ajcc8_stage_group IS NULL
        ORDER BY 1
        """
    ).fetchall()
    nset = {r[0] for r in nulls}
    if nset != MALIGNANT_NULL_POST_RIDS:
        errs.append(
            f"P3: NULL malignant rids={sorted(nset)}, expected {sorted(MALIGNANT_NULL_POST_RIDS)}"
        )

    p4 = readme_399_count(con)
    if p4 != 1:
        errs.append(f"P4: __readme {SCRIPT_ID} rows={p4}, expected 1")

    p5 = con.execute(f"SELECT COUNT(*) FROM {snap}").fetchone()[0]
    if p5 != 8:
        errs.append(f"P5: snapshot rows={p5}, expected 8")

    # P6: eight rids have queue coverage; 6× source 399
    qf = queue_fq()
    for r8 in COHORT_395_RIDS + QUEUE_RIDS:
        n8 = con.execute(
            f"SELECT COUNT(*) FROM {qf} WHERE CAST(research_id AS VARCHAR) = ?",
            [r8],
        ).fetchone()[0]
        if n8 < 1:
            errs.append(f"P6: research_id {r8} missing from queue")
    n399 = con.execute(
        f"""
        SELECT COUNT(*) FROM {qf} WHERE source_script = ?
        """,
        [SOURCE_SCRIPT_QUEUE],
    ).fetchone()[0]
    if n399 != 6:
        errs.append(f"P6: rows with source_script=399: {n399}, expected 6")

    p9s = queue_ajcc8_t_column_state(con)
    if p9s != "varchar":
        errs.append(f"P9: queue {QUEUE_T_COL} not VARCHAR (state={p9s!r})")

    for tmsg in queue_t_stages_mismatch_list(con):
        errs.append(f"P10: {tmsg}")

    # P7 / P8 — snapshot (pre) vs CPM: 2× NULL→I only; 6× no change
    snap_cols = [
        r[0]
        for r in con.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
            ORDER BY ordinal_position
            """,
            [PUB_DB, ARC_SCHEMA, snap_name],
        ).fetchall()
    ]
    non_stage = [c for c in snap_cols if c.lower() != "ajcc8_stage_group"]
    if not non_stage:
        errs.append("P7/P8: snapshot has no columns")
    else:
        ns_match = " AND ".join(
            f's."{c}" IS NOT DISTINCT FROM c."{c}"' for c in non_stage
        )
        for rid in (APPLY_RID_111, APPLY_RID_106):
            n_ok = con.execute(
                f"""
                SELECT COUNT(*) FROM {snap} s
                INNER JOIN {cpm} c
                  ON CAST(s.research_id AS VARCHAR) = CAST(c.research_id AS VARCHAR)
                WHERE CAST(s.research_id AS VARCHAR) = ?
                  AND s.ajcc8_stage_group IS NULL
                  AND c.ajcc8_stage_group = 'I'
                  AND {ns_match}
                """,
                [rid],
            ).fetchone()[0]
            if n_ok != 1:
                errs.append(
                    f"P7/P8: apply rid {rid} — want snapshot vs CPM identical except NULL→'I' on stage_group"
                )

        full_match = " AND ".join(
            f's."{c}" IS NOT DISTINCT FROM c."{c}"' for c in snap_cols
        )
        for rid in QUEUE_RIDS:
            n_eq = con.execute(
                f"""
                SELECT COUNT(*) FROM {snap} s
                INNER JOIN {cpm} c
                  ON CAST(s.research_id AS VARCHAR) = CAST(c.research_id AS VARCHAR)
                WHERE CAST(s.research_id AS VARCHAR) = ?
                  AND ({full_match})
                """,
                [rid],
            ).fetchone()[0]
            if n_eq != 1:
                errs.append(
                    f"P7/P8: queue rid {rid} — CPM must match snapshot (no CPM write on this row)"
                )

    return (len(errs) == 0, errs, info)


def format_close_out(
    *,
    commit_sha: str,
    tag_name: str,
    utc_ts: str,
    probe_sha: str,
    snap_fqn: str,
    gate_table: str,
    verify_ok: bool,
    verify_errs: list[str],
) -> str:
    t_lines = [
        f"- **{k}:** `{'NULL' if v is None else v}`"
        for k, v in EXPECTED_QUEUE_T_BY_RID.items()
    ]
    reasons_md = "\n".join(
        f"- **{r[0]}:** `{r[1]}`" for r in QUEUE_ROWS
    )
    return "\n".join(
        [
            "# Script 399 — Close-out (malignant NULL stage_group)",
            "",
            f"- **Git commit / SHA:** `{commit_sha}`",
            f"- **Tag:** `{tag_name}`",
            f"- **UTC timestamp:** {utc_ts}",
            f"- **Probe SHA256 (consumed):** `{probe_sha}`",
            f"- **Snapshot FQN (8-row audit):** `{snap_fqn}`",
            "",
            "## Schema (queue table)",
            "",
            f"- `manuscript_workspace.{QUEUE_TABLE}`: additive `{QUEUE_T_COL} VARCHAR` (S-1).",
            "- Backfill S-2/S-3: 1404, 12198 T from CPM (both NULL at apply time).",
            "",
            "## Halt-gate verdicts (Phase 0) — H1–H10",
            "",
            gate_table,
            "",
            "## Apply summary (CPM — stage_group only)",
            "",
            "- **111 / DTC_NOS / T1b N1a M0 / age 28 → Stage I** (DTC age<55 M0; corrected+path I).",
            "- **106 / MTC / T1b N0 M0 / age 60 → Stage I** (MTC AJCC8; path I).",
            "",
            "## Queue `ajcc8_t_stage` (8 rows, P10)",
            "",
            "\n".join(t_lines),
            "",
            "## Queue INSERT reasons (`source_script='399'`, 6 rows)",
            "",
            reasons_md,
            "",
            "## Malignant allowlist status",
            "",
            "- Malignant NULL `ajcc8_stage_group` in CPM: **10 → 8** (1404, 12198 + six 399 queue-only rids; 111/106 now staged).",
            "- Full malignant cohort: every row is staged or queued; no orphans.",
            "",
            "## CF-399 follow-ups",
            "",
            "- **CF-399-1:** MTC vs DTC staging in builder — e.g. rid 423 `_corrected` vs MTC N1a M0; audit MTC row staging rules (potential Script 400).",
            "- **CF-399-2:** MTC M1 stage authority — rid 9600 (IVB vs IVC) — registry / edition adjudication.",
            "- **CF-399-3:** `other_malignant` staging framework (6275, 6768).",
            "- **CF-399-4:** CF-395-1 — chart review for 1404 and 12198 (unchanged).",
            "- **CF-399-5:** Optional v2/dominant columns on queue; v2 for 924/6768 still in `reason` only.",
            "",
            "## Phase 3 verification",
            "",
            f"- **all_pass:** {verify_ok}",
        ]
        + (["", "### Failures", ""] + [f"- {e}" for e in verify_errs] if verify_errs else [""])
    )


def git_commit_and_tag(run_stamp: str) -> tuple[str, str]:
    tag_name = f"{TAG_PREFIX}{run_stamp}"
    paths = [
        REPO_ROOT / "scripts" / SCRIPT_FILE,
        PROBE_PATH,
        RUN_LOG_PATH,
        PROMPT_PATH,
        CLOSE_OUT_PATH,
    ]
    for p in paths:
        if not p.exists():
            raise SystemExit(f"Phase 4: missing file {p}")
    msg = (
        "Script 399: malignant NULL stage_group close-out (2 apply: 111→I, 106→I; 6 queue)"
    )
    root = str(REPO_ROOT)
    for p in paths:
        add_cmd = ["git", "-C", root, "add", "--", str(p)]
        if _should_force_add(p):
            add_cmd = ["git", "-C", root, "add", "-f", "--", str(p)]
        subprocess.run(add_cmd, check=True)
    diff = subprocess.run(["git", "-C", root, "diff", "--staged", "--quiet"])
    if diff.returncode != 0:
        subprocess.run(["git", "-C", root, "commit", "-m", msg], check=True)
    else:
        log("Phase 4: no staged changes — skipping commit (tree already matches index)")
    r = subprocess.run(
        ["git", "-C", root, "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    )
    commit_sha = r.stdout.strip()
    subprocess.run(["git", "-C", root, "tag", "-f", tag_name, commit_sha], check=True)
    out_head = subprocess.run(
        ["git", "-C", root, "push", "origin", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    )
    log(
        f"Phase 4: git push origin HEAD — {out_head.stdout.strip() or out_head.stderr.strip() or 'ok'}"
    )
    out_tag = subprocess.run(
        ["git", "-C", root, "push", "origin", tag_name],
        check=True,
        capture_output=True,
        text=True,
    )
    log(
        f"Phase 4: git push origin {tag_name} — {out_tag.stdout.strip() or out_tag.stderr.strip() or 'ok'}"
    )
    return commit_sha, tag_name


def gate_table_from_probe(ok: bool, errs: list[str]) -> str:
    g = "PASS" if ok else "FAIL"
    rows = [
        "| gate | result |",
        "|---|---|",
        f"| H1 | {g} |",
        f"| H2 | {g} |",
        f"| H3 | {g} (pre-apply) |",
        f"| H4 | {g} |",
        f"| H5 | {g} |",
        f"| H6 | {g} |",
        f"| H7 | {g} |",
        f"| H8 | {g} |",
        f"| H9 | {g} (queue T column) |",
        f"| H10 | {g} (repo grep) |",
    ]
    if errs:
        rows.append(f"| (halts) | {len(errs)} message(s) |")
    return "\n".join(rows)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--phase", type=int, default=0, help="0=probe (default)")
    ap.add_argument("--apply", action="store_true", help="Run apply after gates + approval")
    ap.add_argument(
        "--i-approve",
        default="",
        metavar="SHA256",
        help="SHA256 of hashed probe body (required for first apply)",
    )
    ap.add_argument(
        "--force-stamp",
        default="",
        help="Override RUN_STAMP (UTC YYYYMMDD_HHMMSS) for snapshot/tag",
    )
    ap.add_argument(
        "--phase4",
        action="store_true",
        help="After apply+verify (or idempotent NO-OP verify), git add/commit/tag/push",
    )
    args = ap.parse_args()

    if args.phase != 0:
        print("Only --phase 0 is implemented.", flush=True)
        return 1

    if args.apply:
        c0 = connect_quiet()
        try:
            partial, pr = idempotency_partial(c0)
            if partial and pr and not idempotency_complete(c0):
                print(f"HALT: partial apply — {pr}", flush=True)
                return 2
            if idempotency_complete(c0):
                line = (
                    "NO-OP: snapshot + script_399 __readme + queue ajcc8_t_stage VARCHAR + "
                    "111/106=I + 6×399 queue rows + 8/8 T-stage expectations — "
                    "Phase 3 verify only. Close-out: created if missing, else not overwritten."
                )
                print(line, flush=True)
                OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
                with open(RUN_LOG_PATH, "a", encoding="utf-8") as f:
                    f.write(f"{datetime.now(timezone.utc).isoformat()} {line}\n")
                snap = find_snapshot_399(c0)
                if not snap:
                    print("HALT: idempotency inconsistency — missing snapshot", flush=True)
                    return 2
                ok, errs, _ = run_verify(c0, snap)
                if not ok:
                    print("\n--- PHASE 3 VERIFY FAILED ---\n" + "\n".join(errs), flush=True)
                    return 2
                print("Phase 3 verify OK (NO-OP).", flush=True)
                if args.phase4:
                    rs = snapshot_run_stamp(snap)
                    log(f"Phase 4: NO-OP path — run_stamp {rs}")
                    utc_co = datetime.now(timezone.utc).isoformat()
                    snap_fqn = f"{PUB_DB}.{ARC_SCHEMA}.{snap}"
                    d_probe = read_probe_hashed_body_digest()
                    note_co = (
                        "\n## Note (NO-OP materialization)\n\n"
                        "Database was committed in a prior run; P3 in this script was "
                        "subsequently corrected to expect **8** malignant CPM NULLs "
                        "(2×CF-395-1 + 6×Script-399 queue-only rows).\n"
                    )
                    if not CLOSE_OUT_PATH.exists():
                        pre = format_close_out(
                            commit_sha="(pending — Phase 4 commit)",
                            tag_name=f"{TAG_PREFIX}{rs}",
                            utc_ts=utc_co,
                            probe_sha=d_probe,
                            snap_fqn=snap_fqn,
                            gate_table=gate_table_from_probe(True, []),
                            verify_ok=True,
                            verify_errs=[],
                        )
                        CLOSE_OUT_PATH.write_text(pre + note_co, encoding="utf-8")
                        log(f"Wrote {CLOSE_OUT_PATH} (was missing)")

                    csha, tag_n = git_commit_and_tag(rs)
                    final_co = format_close_out(
                        commit_sha=csha,
                        tag_name=tag_n,
                        utc_ts=utc_co,
                        probe_sha=d_probe,
                        snap_fqn=snap_fqn,
                        gate_table=gate_table_from_probe(True, []),
                        verify_ok=True,
                        verify_errs=[],
                    ) + note_co
                    CLOSE_OUT_PATH.write_text(final_co, encoding="utf-8")
                    log(f"Phase 4: commit_sha={csha} tag={tag_n}; refreshed {CLOSE_OUT_PATH}")
                    print(f"commit_sha={csha} tag={tag_n}", flush=True)
                return 0
        finally:
            c0.close()

    fresh_log()
    con = connect()
    try:
        probe = run_probe(con)
        ok, errs = halt_gate(probe)
        hashed = format_probe_hashed_body(probe, ok, errs)
        digest = write_probe_file(hashed)
        log(f"Wrote {PROBE_PATH}")
        print(f"PROBE_REPORT_SHA256={digest}", flush=True)

        if not ok:
            for e in errs:
                log(f"HALT: {e}")
            print("\n--- HALT GATE FAILED ---\n" + "\n".join(errs), flush=True)
            return 1

        if not args.apply:
            log("Probe only — re-run with --apply --i-approve after approval.")
            return 0

        approved = normalize_i_approve(args.i_approve)
        if not approved:
            log("HALT: --apply requires --i-approve=<sha256>")
            print(
                f"\nRun after approval:\n  python3 scripts/{SCRIPT_FILE} --apply"
                f" --i-approve={digest} --phase4\n",
                flush=True,
            )
            return 5
        if approved != digest:
            log(f"HALT: --i-approve mismatch (expected {digest}, got {approved})")
            print(f"Use: --i-approve={digest}", flush=True)
            return 3

        if idempotency_complete(con):
            log("Idempotency complete — use NO-OP --apply re-run")
            return 0

        n_snap = count_snapshots_399(con)
        if n_snap > 0:
            log("HALT: snapshot table exists but idempotency not complete")
            return 2

        run_stamp = args.force_stamp.strip() or datetime.now(timezone.utc).strftime(
            "%Y%m%d_%H%M%S"
        )
        if not cpm_update_sql_audit_ok():
            log("H8: abort")
            return 1

        snap_name = apply_transaction(con, digest, run_stamp)
        ok3, verr, _ = run_verify(con, snap_name)
        if not ok3:
            for e in verr:
                log(f"VERIFY: {e}")
            print("\n--- PHASE 3 VERIFY FAILED ---\n" + "\n".join(verr), flush=True)
            return 2

        utc = datetime.now(timezone.utc).isoformat()
        snap_fqn = f"{PUB_DB}.{ARC_SCHEMA}.{snap_name}"
        gtab = gate_table_from_probe(True, [])

        close_body = format_close_out(
            commit_sha="(pending; run with Phase 4)",
            tag_name=f"{TAG_PREFIX}{run_stamp}",
            utc_ts=utc,
            probe_sha=digest,
            snap_fqn=snap_fqn,
            gate_table=gtab,
            verify_ok=True,
            verify_errs=[],
        )
        PROMPT_PATH.parent.mkdir(parents=True, exist_ok=True)
        if not PROMPT_PATH.exists():
            PROMPT_PATH.write_text(
                "# Script 399 — Malignant NULL stage_group close-out\n\n"
                "See `scripts/apply_malignant_null_stage_group_closeout.py` and project history.\n",
                encoding="utf-8",
            )
        CLOSE_OUT_PATH.write_text(close_body, encoding="utf-8")
        log(f"Wrote {CLOSE_OUT_PATH}")

        csha = "(not committed)"
        tag_n = f"{TAG_PREFIX}{run_stamp}"
        if args.phase4:
            csha, tag_n = git_commit_and_tag(run_stamp)
            close_body2 = format_close_out(
                commit_sha=csha,
                tag_name=tag_n,
                utc_ts=utc,
                probe_sha=digest,
                snap_fqn=snap_fqn,
                gate_table=gtab,
                verify_ok=True,
                verify_errs=[],
            )
            CLOSE_OUT_PATH.write_text(close_body2, encoding="utf-8")

        log("Script 399 apply + verify complete.")
        print(f"commit_sha={csha} tag={tag_n}", flush=True)
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
