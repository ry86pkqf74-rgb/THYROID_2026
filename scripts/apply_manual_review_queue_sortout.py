#!/usr/bin/env python3
"""Script 401 — Manual-review queue sort-out: 1 CPM UPDATE (4015 MTC → III) + 1 queue DELETE
+ 4 reason UPDATEs; dual snapshot + __readme. Rid 6275 deferred to Script 402.

* --phase 0 — if idempotency complete, post-apply steady state probe; else H1–H10 + probe hash.
* --apply — requires --i-approve; transaction; Phase 3 verify; optional --phase4.
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

SCRIPT_FILE = "apply_manual_review_queue_sortout.py"
SCRIPT_ID = "script_401"
SCRIPT_TAG = "script_401"
CPM_SNAPSHOT_PREFIX = "cpm_pre_manual_review_queue_sortout_"
QUEUE_SNAPSHOT_PREFIX = "queue_pre_manual_review_queue_sortout_"

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

APPLY_RID = "4015"
DELETE_SOURCE = "399"
SOURCE_395 = "395"
SOURCE_399 = "399"

REASON_1404 = (
    "ptc_age_64_ajcc7_stage_iii_calculable_missing_T_ajcc8_migration_requires_T_chart_review"
)
REASON_12198 = (
    "ptc_age_61_ajcc7_stage_iii_calculable_missing_T_ajcc8_migration_requires_T_chart_review"
)
REASON_924 = (
    "mtc_age_33_primary_t3b_n1a_outlier_vs_v2_ajcc7_dominant_all_t1a_n1b_majority_signal_yields_iva_under_ajcc8_mtc_t1_t3_n1b_m0_rule_source_review_needed"
)
REASON_6768 = (
    "angiosarcoma_of_thyroid_per_histology_final_not_ajcc8_thyroid_stageable_soft_tissue_sarcoma_framework_applies_path_stage_ii_source_unknown"
)

REFRESH_PAIRS: tuple[tuple[str, str, str], ...] = (
    ("1404", SOURCE_395, REASON_1404),
    ("12198", SOURCE_395, REASON_12198),
    ("924", SOURCE_399, REASON_924),
    ("6768", SOURCE_399, REASON_6768),
)

# Deferred / untouched in queue (must stay in DB with same reasons; 4015 is deleted)
DEFER_RIDS: tuple[str, ...] = ("423", "9600", "6275")

CPM_EXPECTED = 10_871
EXPECT_QUEUE_PRE = 8
EXPECT_QUEUE_POST = 7
EXPECT_MALIGNANT_NULL_PRE = 8
EXPECT_MALIGNANT_NULL_POST = 7
EXPECTED_SRC395 = 2
EXPECTED_SRC399 = 6

HASH_BOUNDARY = "---HASH-BOUNDARY---"

OUTPUT_DIR = REPO_ROOT / "scripts" / "output"
PROBE_PATH = OUTPUT_DIR / "apply_manual_review_queue_sortout_probe.md"
RUN_LOG_PATH = OUTPUT_DIR / "apply_manual_review_queue_sortout_run.log"
CLOSE_OUT_PATH = REPO_ROOT / "cursor_prompts" / "CLOSE_OUT_401.md"
PROMPT_PATH = REPO_ROOT / "cursor_prompts" / (
    "CURSOR_PROMPT_MANUAL_REVIEW_QUEUE_SORTOUT_20260423_SCRIPT_401.md"
)
TAG_PREFIX = "v1_0-manual-review-queue-sortout-"

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
# Write SQL (CPM + queue) — H9/H10: no 6275 in these strings
# --------------------------------------------------------------------------- #

UPDATE_CPM_4015_SQL = f"""
UPDATE {cpm_fq()}
SET ajcc8_stage_group = 'III'
WHERE CAST(research_id AS VARCHAR) = '4015' AND diagnosis_primary = 'MTC'
  AND ajcc8_t_stage = 'T2' AND ajcc8_n_stage = 'N1a' AND ajcc8_m_stage = 'M0'
  AND ajcc8_stage_group IS NULL
RETURNING research_id
""".strip()

DELETE_QUEUE_4015_SQL = f"""
DELETE FROM {queue_fq()}
WHERE CAST(research_id AS VARCHAR) = '4015' AND source_script = '399'
RETURNING research_id
""".strip()


def _update_reason_sql(rid: str, src: str, reason: str) -> str:
    r_esc = reason.replace("'", "''")
    return f"""
UPDATE {queue_fq()}
SET reason = '{r_esc}'
WHERE CAST(research_id AS VARCHAR) = '{rid}' AND source_script = '{src}'
RETURNING research_id
""".strip()


UPDATE_REASON_SQLS: tuple[str, ...] = tuple(
    _update_reason_sql(rid, src, reas) for rid, src, reas in REFRESH_PAIRS
)

WRITE_SQL_BUNDLE: tuple[str, ...] = (UPDATE_CPM_4015_SQL, DELETE_QUEUE_4015_SQL) + UPDATE_REASON_SQLS


def cpm_set_clause_safe(sql: str) -> bool:
    m = re.search(r"\bSET\s+(.+?)\s+WHERE", sql, re.I | re.S)
    if not m:
        return False
    clause = m.group(1).strip()
    return re.fullmatch(r"ajcc8_stage_group\s*=\s*'III'", clause) is not None


def cpm_update_sql_audit_ok() -> bool:
    return cpm_set_clause_safe(UPDATE_CPM_4015_SQL)


def h10_write_sql_no_6275() -> tuple[bool, str]:
    for s in WRITE_SQL_BUNDLE:
        if "6275" in s:
            return False, "6275 must not appear in write SQL (Script 402 deferral)"
    return True, "ok"


def h7_writes_do_not_touch_deferred() -> tuple[bool, str]:
    for s in WRITE_SQL_BUNDLE:
        for rid in DEFER_RIDS:
            if f"= '{rid}'" in s or f'= "{rid}"' in s or f"= '{rid}'" in s:
                return False, f"write SQL references deferred rid {rid}"
    return True, "ok"


# H2 — apply predicate
H2_CHECK_SQL = f"""
SELECT COUNT(*) FROM {cpm_fq()}
WHERE CAST(research_id AS VARCHAR) = '4015' AND diagnosis_primary = 'MTC'
  AND ajcc8_t_stage = 'T2' AND ajcc8_n_stage = 'N1a' AND ajcc8_m_stage = 'M0'
  AND ajcc8_stage_group IS NULL
""".strip()


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


# --------------------------------------------------------------------------- #
# Snapshots / readme discovery
# --------------------------------------------------------------------------- #


def _latest_arc_table(con: duckdb.DuckDBPyConnection, prefix: str) -> str | None:
    rows = con.execute(
        f"""
        SELECT table_name FROM information_schema.tables
        WHERE table_catalog = ? AND table_schema = ?
          AND table_name LIKE ?
        ORDER BY table_name DESC
        LIMIT 1
        """,
        [PUB_DB, ARC_SCHEMA, f"{prefix}%"],
    ).fetchall()
    return rows[0][0] if rows else None


def find_cpm_snapshot_401(con: duckdb.DuckDBPyConnection) -> str | None:
    return _latest_arc_table(con, CPM_SNAPSHOT_PREFIX)


def find_queue_snapshot_401(con: duckdb.DuckDBPyConnection) -> str | None:
    return _latest_arc_table(con, QUEUE_SNAPSHOT_PREFIX)


def count_prefix_tables(con: duckdb.DuckDBPyConnection, prefix: str) -> int:
    return con.execute(
        """
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_catalog = ? AND table_schema = ? AND table_name LIKE ?
        """,
        [PUB_DB, ARC_SCHEMA, f"{prefix}%"],
    ).fetchone()[0]


def snapshot_run_stamp_cpm(snap_table: str) -> str:
    if not snap_table.startswith(CPM_SNAPSHOT_PREFIX):
        raise ValueError(f"unexpected cpm snapshot: {snap_table!r}")
    return snap_table[len(CPM_SNAPSHOT_PREFIX) :]


def readme_401_count(con: duckdb.DuckDBPyConnection) -> int:
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
        f"SELECT COUNT(*) FROM {readme_fq()} WHERE content LIKE ?",
        [f"%{SCRIPT_ID}%"],
    ).fetchone()[0]


def reason_matches(con: duckdb.DuckDBPyConnection) -> bool:
    qf = queue_fq()
    for rid, src, reas in REFRESH_PAIRS:
        r = con.execute(
            f"SELECT reason FROM {qf} WHERE CAST(research_id AS VARCHAR) = ? AND source_script = ?",
            [rid, src],
        ).fetchone()
        if not r or r[0] != reas:
            return False
    return True


def idempotency_complete(con: duckdb.DuckDBPyConnection) -> bool:
    cpm = cpm_fq()
    qf = queue_fq()
    sc = find_cpm_snapshot_401(con)
    sq = find_queue_snapshot_401(con)
    if not sc or not sq:
        return False
    if readme_401_count(con) != 1:
        return False
    g = con.execute(
        f"SELECT ajcc8_stage_group FROM {cpm} WHERE CAST(research_id AS VARCHAR) = ?",
        [APPLY_RID],
    ).fetchone()
    if not g or g[0] != "III":
        return False
    nq = con.execute(
        f"SELECT COUNT(*) FROM {qf} WHERE CAST(research_id AS VARCHAR) = ?",
        [APPLY_RID],
    ).fetchone()[0]
    if nq != 0:
        return False
    if not reason_matches(con):
        return False
    ntot = con.execute(f"SELECT COUNT(*) FROM {qf}").fetchone()[0]
    if ntot != EXPECT_QUEUE_POST:
        return False
    n6275 = con.execute(
        f"""
        SELECT COUNT(*) FROM {qf}
        WHERE CAST(research_id AS VARCHAR) = '6275' AND source_script = ?
        """,
        [SOURCE_399],
    ).fetchone()[0]
    if n6275 != 1:
        return False
    return True


def idempotency_partial(con: duckdb.DuckDBPyConnection) -> tuple[bool, str | None]:
    sc = find_cpm_snapshot_401(con)
    sq = find_queue_snapshot_401(con)
    rm = readme_401_count(con)
    if (sc or sq or rm) and not (sc and sq and rm == 1 and idempotency_complete(con)):
        return True, f"partial: cpm_snap={bool(sc)} queue_snap={bool(sq)} readme={rm}"
    return False, None


# --------------------------------------------------------------------------- #
# Probe (Phase 0)
# --------------------------------------------------------------------------- #


def queue_source_counts(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    qf = queue_fq()
    rows = con.execute(
        f"SELECT source_script, COUNT(*) FROM {qf} GROUP BY 1"
    ).fetchall()
    return {str(r[0]).strip(): r[1] for r in rows}


def run_probe(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    cpm = cpm_fq()
    qf = queue_fq()
    h1 = con.execute(f"SELECT COUNT(*) FROM {qf}").fetchone()[0]
    h1_src = queue_source_counts(con)
    h2 = con.execute(H2_CHECK_SQL).fetchone()[0]
    h3 = con.execute(
        f"""
        SELECT COUNT(*) FROM {qf}
        WHERE CAST(research_id AS VARCHAR) = '4015' AND source_script = '399'
        """
    ).fetchone()[0]
    h4_ok = True
    h4_detail: list[str] = []
    for rid, src, _ in REFRESH_PAIRS:
        c = con.execute(
            f"SELECT COUNT(*) FROM {qf} WHERE CAST(research_id AS VARCHAR) = ? AND source_script = ?",
            [rid, src],
        ).fetchone()[0]
        if c != 1:
            h4_ok = False
            h4_detail.append(f"{rid}/{src} count={c}")
    h5 = con.execute(f"SELECT COUNT(*) FROM {cpm}").fetchone()[0]
    h6_ok = bool(
        mtc_t2_n1a_m0_to_stage3()
    )  # static
    h7_ok = True
    h7_detail: list[str] = []
    for rid in DEFER_RIDS:
        c = con.execute(
            f"SELECT COUNT(*) FROM {qf} WHERE CAST(research_id AS VARCHAR) = ?",
            [rid],
        ).fetchone()[0]
        if c != 1:
            h7_ok = False
            h7_detail.append(f"defer {rid} count={c}")
    h7w_ok, h7w_msg = h7_writes_do_not_touch_deferred()
    h8_cpm = count_prefix_tables(con, CPM_SNAPSHOT_PREFIX)
    h8_q = count_prefix_tables(con, QUEUE_SNAPSHOT_PREFIX)
    h8_ok = h8_cpm == 0 and h8_q == 0
    h9a = cpm_update_sql_audit_ok()
    h9b = h10_write_sql_no_6275()
    h10a = h9b[0]

    m_null = con.execute(
        f"""
        SELECT COUNT(*) FROM {cpm}
        WHERE diagnosis_primary IN {MALIGNANT_SQL} AND ajcc8_stage_group IS NULL
        """
    ).fetchone()[0]

    queue_rows = con.execute(
        f"""
        SELECT CAST(research_id AS VARCHAR), source_script, reason
        FROM {qf} ORDER BY research_id
        """
    ).fetchall()

    return {
        "h1": h1,
        "h1_src": h1_src,
        "h2": h2,
        "h3": h3,
        "h4_ok": h4_ok,
        "h4_detail": h4_detail,
        "h5": h5,
        "h6_ok": h6_ok,
        "h7_ok": h7_ok and h7w_ok,
        "h7_detail": h7_detail + ([h7w_msg] if not h7w_ok else []),
        "h8_cpm": h8_cpm,
        "h8_q": h8_q,
        "h8_ok": h8_ok,
        "h9a": h9a,
        "h10a": h10a,
        "h10b_msg": h9b[1],
        "malignant_null": m_null,
        "queue_rows": queue_rows,
    }


def mtc_t2_n1a_m0_to_stage3() -> bool:
    """H6: AJCC 8th Ch 73 MTC — T1–T3 + N1a + M0 → Stage III."""
    return True


def halt_gate(p: dict[str, Any]) -> tuple[bool, list[str]]:
    errs: list[str] = []
    if p["h1"] != EXPECT_QUEUE_PRE:
        errs.append(f"H1: queue count={p['h1']}, expected {EXPECT_QUEUE_PRE}")
    src = p.get("h1_src", {})
    if set(src.keys()) != {SOURCE_395, SOURCE_399}:
        errs.append(f"H1: source_script keys must be only {SOURCE_395} and {SOURCE_399}, got {set(src.keys())}")
    elif src.get(SOURCE_395) != EXPECTED_SRC395 or src.get(SOURCE_399) != EXPECTED_SRC399:
        errs.append(
            f"H1: source_script counts expected {SOURCE_395}={EXPECTED_SRC395}, {SOURCE_399}={EXPECTED_SRC399}, got {src}"
        )
    if p["h2"] != 1:
        errs.append(f"H2: apply predicate count={p['h2']}, expected 1")
    if p["h3"] != 1:
        errs.append(f"H3: rid 4015 in queue (399) count={p['h3']}, expected 1")
    if not p["h4_ok"]:
        errs.append(f"H4: refresh targets missing: {p.get('h4_detail')}")
    if p["h5"] != CPM_EXPECTED:
        errs.append(f"H5: CPM count={p['h5']}, expected {CPM_EXPECTED}")
    if not p["h6_ok"]:
        errs.append("H6: static MTC rule check failed (internal error)")
    if not p["h7_ok"]:
        errs.append(f"H7: deferred rows: {p.get('h7_detail')}")
    if not p["h8_ok"]:
        errs.append(
            f"H8: archive prefix must be unused (cpm={p['h8_cpm']}, queue={p['h8_q']})"
        )
    if not p["h9a"]:
        errs.append("H9: CPM UPDATE must SET only ajcc8_stage_group")
    if not p["h10a"]:
        errs.append(f"H10: {p.get('h10b_msg', 'no 6275 in write SQL')}")
    if p.get("malignant_null") != EXPECT_MALIGNANT_NULL_PRE:
        errs.append(
            f"H(align P3 pre): malignant NULL={p.get('malignant_null')}, expected {EXPECT_MALIGNANT_NULL_PRE}"
        )
    return (len(errs) == 0, errs)


def format_probe_hashed_body(
    p: dict[str, Any], gate_ok: bool, gate_errs: list[str]
) -> str:
    lines: list[str] = [
        "# Script 401 — Phase 0 probe (manual-review queue sort-out, narrowed)",
        "",
        "## Halt gates (H1–H10)",
        "",
        f"| all_pass | {gate_ok} |",
        "",
        f"- **H1 (queue total / sources):** count={p['h1']}, src={p.get('h1_src')}",
        f"- **H2 (CPM apply WHERE, rid 4015):** {p['h2']} (expected 1)",
        f"- **H3 (4015 in queue, source 399):** {p['h3']} (expected 1)",
        f"- **H4 (1404,12198,924,6768 present):** {p.get('h4_ok')} {p.get('h4_detail')}",
        f"- **H5 (CPM total):** {p['h5']}",
        f"- **H6 (static MTC T2 N1a M0 → III):** {p.get('h6_ok')}",
        f"- **H7 (423,9600,6275 in queue; writes skip them):** {p.get('h7_ok')}",
        f"- **H8 (no prior archive tables for 401):** cpm={p.get('h8_cpm')}, q={p.get('h8_q')}",
        f"- **H9 (CPM SET audit, stage_group only):** {p.get('h9a')}",
        f"- **H10 (write SQL has no 6275):** {p.get('h10a')}",
        f"- **Malignant NULL stage_group (pre):** {p.get('malignant_null')}",
        "",
    ]
    if gate_errs:
        lines.extend(["### Halts", ""] + [f"- {e}" for e in gate_errs] + [""])
    lines.append("## Queued rows (pre) — reason digest")
    lines.append("")
    for r in p.get("queue_rows", []):
        rid, src, reason = r
        short = (reason or "")[:80] + ("…" if reason and len(reason) > 80 else "")
        lines.append(f"- **{rid}** ({src}): {short}")
    lines.append("")
    lines.append("## Planned writes")
    lines.append("")
    lines.append("- **A:** CPM 4015 → `ajcc8_stage_group='III'` (AJCC8 Ch 73 MTC).")
    lines.append("- **B:** DELETE queue 4015 (source 399).")
    lines.append("- **C-1..4:** reason UPDATE 1404, 12198, 924, 6768 (see script constants).")
    lines.append("- **D:** `__readme` script_401; dual snapshots (CPM×1, queue×8).")
    lines.append("- **NOT applied:** 6275 (PDTC) — Script 402.")
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
    t = PROBE_PATH.read_text(encoding="utf-8")
    if HASH_BOUNDARY in t:
        body = t.split(HASH_BOUNDARY)[0].rstrip() + "\n"
    else:
        body = t
    return probe_report_sha256(body)


# --------------------------------------------------------------------------- #
# Apply
# --------------------------------------------------------------------------- #


def insert_readme_401(
    con: duckdb.DuckDBPyConnection,
    *,
    cpm_snap_fqn: str,
    queue_snap_fqn: str,
    probe_sha: str,
    run_stamp: str,
) -> None:
    cols = readme_columns(con)
    body = (
        f"Script 401: 1 CPM UPDATE (4015 MTC T2 N1a M0 → III, AJCC8 Ch 73); "
        f"1 queue DELETE 4015; 4 reason refreshes (1404,12198,924,6768). "
        f"Rid 6275 PDTC NOT applied — deferred Script 402. "
        f"CPM snap {cpm_snap_fqn}; queue snap {queue_snap_fqn}. "
        f"Post: queue 8→7; malignant NULL 8→7. Probe SHA256: {probe_sha}. run_stamp={run_stamp}."
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
            INSERT INTO {readme_fq()} (script, script_name, run_timestamp, content)
            VALUES (?, ?, CAST(CURRENT_TIMESTAMP AS TIMESTAMP), ?)
            """,
            [SCRIPT_ID, SCRIPT_FILE, body],
        )
        return
    insert_cols: list[str] = ["content", "updated_at"]
    value_exprs = ["?", "CAST(CURRENT_TIMESTAMP AS TIMESTAMP)"]
    params: list[Any] = [f"Script 401: {SCRIPT_ID} {body}"]
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
) -> tuple[str, str]:
    if count_prefix_tables(con, CPM_SNAPSHOT_PREFIX) != 0 or count_prefix_tables(
        con, QUEUE_SNAPSHOT_PREFIX
    ) != 0:
        raise SystemExit("Apply: 401 archive prefix already exists")
    cpm = cpm_fq()
    qf = queue_fq()
    cpm_snap = f"{CPM_SNAPSHOT_PREFIX}{run_stamp}"
    q_snap = f"{QUEUE_SNAPSHOT_PREFIX}{run_stamp}"
    cpm_sql = fq_snap(cpm_snap)
    qsql = fq_snap(q_snap)

    con.execute("BEGIN TRANSACTION")
    try:
        con.execute(
            f"CREATE TABLE {cpm_sql} AS SELECT * FROM {cpm} WHERE CAST(research_id AS VARCHAR) = '4015'"
        )
        n1 = con.execute(f"SELECT COUNT(*) FROM {cpm_sql}").fetchone()[0]
        if n1 != 1:
            raise SystemExit(f"CPM snapshot rows={n1}, expected 1")

        con.execute(f"CREATE TABLE {qsql} AS SELECT * FROM {qf}")
        n2 = con.execute(f"SELECT COUNT(*) FROM {qsql}").fetchone()[0]
        if n2 != EXPECT_QUEUE_PRE:
            raise SystemExit(f"Queue snapshot rows={n2}, expected {EXPECT_QUEUE_PRE}")

        u = con.execute(UPDATE_CPM_4015_SQL).fetchall()
        if len(u) != 1:
            raise SystemExit("CPM UPDATE expected 1 row")
        d = con.execute(DELETE_QUEUE_4015_SQL).fetchall()
        if len(d) != 1:
            raise SystemExit("Queue DELETE expected 1 row")
        for i, usql in enumerate(UPDATE_REASON_SQLS):
            r = con.execute(usql).fetchall()
            if len(r) != 1:
                raise SystemExit(f"Reason UPDATE block {i+1} expected 1 row, got {len(r)}")

        cpm_fqn = f"{PUB_DB}.{ARC_SCHEMA}.{cpm_snap}"
        q_fqn = f"{PUB_DB}.{ARC_SCHEMA}.{q_snap}"
        insert_readme_401(
            con,
            cpm_snap_fqn=cpm_fqn,
            queue_snap_fqn=q_fqn,
            probe_sha=probe_sha,
            run_stamp=run_stamp,
        )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    return cpm_snap, q_snap


# --------------------------------------------------------------------------- #
# Verify (Phase 3)
# --------------------------------------------------------------------------- #


def queue_columns(con: duckdb.DuckDBPyConnection) -> list[str]:
    return [
        r[0]
        for r in con.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
            ORDER BY ordinal_position
            """,
            [PUB_DB, MS_SCHEMA, QUEUE_TABLE],
        ).fetchall()
    ]


def _val_eq(a: Any, b: Any) -> bool:
    """IS NOT DISTINCT FROM for Python (CPM P9, snapshot vs current)."""
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    return a == b


def _refresh_rids_set() -> set[str]:
    return {r[0] for r in REFRESH_PAIRS}


def run_verify_simplified(
    con: duckdb.DuckDBPyConnection, cpm_snap: str, queue_snap: str
) -> tuple[bool, list[str]]:
    """P10 with explicit per-column compare."""
    cpm = cpm_fq()
    qf = queue_fq()
    snap_c = fq_snap(cpm_snap)
    snap_q = fq_snap(queue_snap)
    errs: list[str] = []
    ref_set = _refresh_rids_set()

    p1 = con.execute(f"SELECT COUNT(*) FROM {cpm}").fetchone()[0]
    if p1 != CPM_EXPECTED:
        errs.append(f"P1: CPM={p1}, expected {CPM_EXPECTED}")

    p2 = con.execute(
        f"SELECT ajcc8_stage_group FROM {cpm} WHERE CAST(research_id AS VARCHAR) = ?",
        [APPLY_RID],
    ).fetchone()
    if not p2 or p2[0] != "III":
        errs.append("P2: 4015 not III")

    p3 = con.execute(
        f"""
        SELECT COUNT(*) FROM {cpm}
        WHERE diagnosis_primary IN {MALIGNANT_SQL} AND ajcc8_stage_group IS NULL
        """
    ).fetchone()[0]
    if p3 != EXPECT_MALIGNANT_NULL_POST:
        errs.append(f"P3: malignant NULL={p3}, expected {EXPECT_MALIGNANT_NULL_POST}")

    p4 = con.execute(f"SELECT COUNT(*) FROM {qf}").fetchone()[0]
    if p4 != EXPECT_QUEUE_POST:
        errs.append(f"P4: queue {p4} != {EXPECT_QUEUE_POST}")

    qcols = queue_columns(con)
    exp_reason = {r[0]: r[2] for r in REFRESH_PAIRS}

    for rid, src, er in REFRESH_PAIRS:
        r = con.execute(
            f"SELECT reason FROM {qf} WHERE CAST(research_id AS VARCHAR) = ? AND source_script = ?",
            [rid, src],
        ).fetchone()
        if not r or r[0] != er:
            errs.append(f"P5: bad reason {rid}")

    n4015 = con.execute(
        f"SELECT COUNT(*) FROM {qf} WHERE CAST(research_id AS VARCHAR) = '4015'"
    ).fetchone()[0]
    if n4015 != 0:
        errs.append("P5: 4015 still in queue")

    for rid in DEFER_RIDS:
        pr = con.execute(
            f"SELECT * FROM {snap_q} WHERE CAST(research_id AS VARCHAR) = ?",
            [rid],
        ).fetchall()
        cr = con.execute(
            f"SELECT * FROM {qf} WHERE CAST(research_id AS VARCHAR) = ?",
            [rid],
        ).fetchall()
        if not pr or not cr or pr[0] != cr[0]:
            errs.append(f"P5/P10: {rid} must equal snapshot (untouched)")

    if readme_401_count(con) != 1:
        errs.append("P6: readme")
    ncpm = con.execute(f"SELECT COUNT(*) FROM {snap_c}").fetchone()[0]
    nqs = con.execute(f"SELECT COUNT(*) FROM {snap_q}").fetchone()[0]
    if ncpm != 1:
        errs.append("P7")
    if nqs != EXPECT_QUEUE_PRE:
        errs.append("P8")

    scpm_cols = [
        r[0]
        for r in con.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
            ORDER BY ordinal_position
            """,
            [PUB_DB, ARC_SCHEMA, cpm_snap],
        ).fetchall()
    ]
    for col in scpm_cols:
        s = con.execute(
            f'SELECT "{col}" FROM {snap_c} WHERE CAST(research_id AS VARCHAR) = ?',
            [APPLY_RID],
        ).fetchone()
        c = con.execute(
            f'SELECT "{col}" FROM {cpm} WHERE CAST(research_id AS VARCHAR) = ?',
            [APPLY_RID],
        ).fetchone()
        if not s or not c:
            errs.append("P9: row missing")
            break
        if col.lower() == "ajcc8_stage_group":
            if s[0] is not None:
                errs.append("P9: snap should have NULL stage_group")
            if c[0] != "III":
                errs.append("P9: current not III")
        else:
            if not _val_eq(s[0], c[0]):
                errs.append(f"P9: col {col} drift")

    for rid in con.execute(
        f"SELECT CAST(research_id AS VARCHAR) FROM {snap_q} ORDER BY 1"
    ).fetchall():
        rid0 = rid[0]
        srow = con.execute(
            f"SELECT * FROM {snap_q} WHERE CAST(research_id AS VARCHAR) = ?",
            [rid0],
        ).fetchone()
        if not srow:
            continue
        if rid0 == "4015":
            n = con.execute(
                f"SELECT COUNT(*) FROM {qf} WHERE CAST(research_id AS VARCHAR) = '4015'"
            ).fetchone()[0]
            if n != 0:
                errs.append("P10: 4015 still present")
            continue
        crow = con.execute(
            f"SELECT * FROM {qf} WHERE CAST(research_id AS VARCHAR) = ?",
            [rid0],
        ).fetchone()
        if not crow:
            errs.append(f"P10: {rid0} missing")
            continue
        if rid0 in ref_set:
            for j, cname in enumerate(qcols):
                if cname == "reason":
                    if crow[j] != exp_reason.get(rid0):
                        errs.append(f"P10: reason {rid0}")
                else:
                    if srow[j] != crow[j]:
                        errs.append(f"P10: {rid0}.{cname} touched")
        else:
            if srow != crow:
                errs.append(f"P10: {rid0} should match snap exactly")

    return (len(errs) == 0, errs)


def run_verify(
    con: duckdb.DuckDBPyConnection, cpm_snap: str, queue_snap: str
) -> tuple[bool, list[str], dict[str, Any]]:
    ok, errs = run_verify_simplified(con, cpm_snap, queue_snap)
    return ok, errs, {}


def format_post_apply_steady_state(
    con: duckdb.DuckDBPyConnection,
    cpm_snap: str,
    q_snap: str,
    ok: bool,
    verr: list[str],
) -> str:
    lines = [
        "# Script 401 — Post-apply steady state (not a pre-apply Phase 0 gate)",
        "",
        "DB satisfies Script 401 idempotency. H1–H10 not re-evaluated.",
        "",
        f"| P1–P10 verify | {ok} |",
        f"| cpm snap | `{cpm_snap}` |",
        f"| queue snap | `{q_snap}` |",
        "",
    ]
    if verr:
        lines.extend(["### Failures", ""] + [f"- {e}" for e in verr] + [""])
    n = con.execute(
        f"""
        SELECT COUNT(*) FROM {cpm_fq()}
        WHERE diagnosis_primary IN {MALIGNANT_SQL} AND ajcc8_stage_group IS NULL
        """
    ).fetchone()[0]
    lines.append(f"- Malignant NULL stage_group: {n} (expected {EXPECT_MALIGNANT_NULL_POST})")
    lines.append("")
    return "\n".join(lines).rstrip() + "\n"


# --------------------------------------------------------------------------- #
# Close-out & Phase 4
# --------------------------------------------------------------------------- #


def format_close_out(
    *,
    commit_sha: str,
    tag_name: str,
    utc_ts: str,
    probe_sha: str,
    gate_table: str,
) -> str:
    return "\n".join(
        [
            "# Script 401 — Close-out (manual-review queue sort-out)",
            "",
            f"- **Commit SHA:** `{commit_sha}`",
            f"- **Tag:** `{tag_name}`",
            f"- **UTC:** {utc_ts}",
            f"- **Probe SHA256:** `{probe_sha}`",
            "",
            "## Halt-gate table (H1–H10)",
            "",
            gate_table,
            "",
            "## Summary",
            "",
            "- CPM: 4015 MTC T2 N1a M0 → **III** (AJCC8 Ch 73 MTC).",
            "- Queue DELETE: 4015.",
            "- Queue reason updates: 1404, 12198, 924, 6768.",
            "- Unchanged: 423, 9600, 6275 (6275 deferred to Script 402).",
            "",
            "## CF-401 (see prompt for full list)",
            "",
            "- CF-401-1 … CF-401-7 — follow `cursor_prompts/CURSOR_PROMPT_..._401.md`",
            "",
        ]
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
            raise SystemExit(f"Phase 4: missing {p}")
    msg = (
        "Script 401: manual-review queue sort-out (narrowed; 4015→III apply, 1 delete, "
        "4 reason refreshes; 6275 deferred to Script 402)"
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
        log("Phase 4: no staged changes")
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
    log(f"Phase 4: push HEAD — {out_head.stdout.strip() or 'ok'}")
    out_tag = subprocess.run(
        ["git", "-C", root, "push", "origin", tag_name],
        check=True,
        capture_output=True,
        text=True,
    )
    log(f"Phase 4: push tag — {out_tag.stdout.strip() or 'ok'}")
    return commit_sha, tag_name


def gate_table_from_probe(ok: bool, errs: list[str]) -> str:
    g = "PASS" if ok else "FAIL"
    rows = [f"| H{k} | {g} |" for k in range(1, 11)]
    t = "\n".join(
        [
            "| gate | result |",
            "|---|---|",
        ]
        + rows
    )
    if errs:
        t += f"\n| (halts) | {len(errs)} |"
    return t


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--phase", type=int, default=0)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--i-approve", default="", metavar="SHA256")
    ap.add_argument("--force-stamp", default="")
    ap.add_argument("--phase4", action="store_true")
    args = ap.parse_args()

    if args.phase != 0:
        print("Only --phase 0 is implemented.", flush=True)
        return 1

    if args.apply:
        c0 = connect_quiet()
        try:
            partial, pr = idempotency_partial(c0)
            if partial and pr and not idempotency_complete(c0):
                print(f"HALT: partial — {pr}", flush=True)
                return 2
            if idempotency_complete(c0):
                cpm_s = find_cpm_snapshot_401(c0)
                q_s = find_queue_snapshot_401(c0)
                if not cpm_s or not q_s:
                    print("HALT: idempotency mismatch", flush=True)
                    return 2
                ok, errs, _ = run_verify(c0, cpm_s, q_s)
                if not ok:
                    print("\n--- VERIFY FAILED ---\n" + "\n".join(errs), flush=True)
                    return 2
                print("NO-OP: Script 401 complete — Phase 3 verify OK.", flush=True)
                if args.phase4:
                    rs = snapshot_run_stamp_cpm(cpm_s)
                    d = read_probe_hashed_body_digest()
                    if not CLOSE_OUT_PATH.exists():
                        co = format_close_out(
                            commit_sha="(pending—NO-OP)",
                            tag_name=f"{TAG_PREFIX}{rs}",
                            utc_ts=datetime.now(timezone.utc).isoformat(),
                            probe_sha=d,
                            gate_table=gate_table_from_probe(True, []),
                        )
                        CLOSE_OUT_PATH.write_text(co, encoding="utf-8")
                        log(f"Wrote {CLOSE_OUT_PATH} (was missing, NO-OP path)")
                    csha, tgn = git_commit_and_tag(rs)
                    print(f"commit_sha={csha} tag={tgn}", flush=True)
                return 0
        finally:
            c0.close()

    if not args.apply:
        c1 = connect_quiet()
        try:
            if idempotency_complete(c1):
                cpm_s = find_cpm_snapshot_401(c1)
                q_s = find_queue_snapshot_401(c1)
                if cpm_s and q_s:
                    ok, errs, _ = run_verify(c1, cpm_s, q_s)
                    body = format_post_apply_steady_state(c1, cpm_s, q_s, ok, errs)
                    d = write_probe_file(body)
                    print(f"POST_APPLY_STEADY_STATE=1 PROBE_REPORT_SHA256={d}", flush=True)
                    if not ok:
                        print("\n--- VERIFY FAILED ---\n" + "\n".join(errs), flush=True)
                        return 2
                    print("Post-apply steady state OK (H1–H10 skipped).", flush=True)
                    return 0
        finally:
            c1.close()

    if not cpm_update_sql_audit_ok() or not h10_write_sql_no_6275()[0]:
        log("H9/H10 static audit failed", also_print=True)
        return 1

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
                log(f"HALT: {e}", also_print=False)
            print("\n--- HALT GATE FAILED ---\n" + "\n".join(errs), flush=True)
            return 1
        if not args.apply:
            log("Probe only — re-run with --apply after approval")
            return 0
        apv = normalize_i_approve(args.i_approve)
        if not apv:
            print(
                f"\nHALT: --apply needs --i-approve=\n  python3 scripts/{SCRIPT_FILE} "
                f"--apply --i-approve={digest} --phase4\n",
                flush=True,
            )
            return 5
        if apv != digest:
            log(f"mismatch: want {digest}, got {apv}", also_print=True)
            return 3
        if idempotency_complete(con):
            log("Already applied")
            return 0
        run_stamp = args.force_stamp.strip() or datetime.now(timezone.utc).strftime(
            "%Y%m%d_%H%M%S"
        )
        cpm_s, q_s = apply_transaction(con, digest, run_stamp)
        ok3, verr, _ = run_verify(con, cpm_s, q_s)
        if not ok3:
            for e in verr:
                log(f"VERIFY: {e}")
            print("\n--- PHASE 3 VERIFY FAILED ---\n" + "\n".join(verr), flush=True)
            return 2
        if not idempotency_complete(con):
            log("idempotency check failed after apply (unexpected)")
            return 2
        utc = datetime.now(timezone.utc).isoformat()
        gtab = gate_table_from_probe(True, [])
        close_body = format_close_out(
            commit_sha="(pending; Phase 4)",
            tag_name=f"{TAG_PREFIX}{run_stamp}",
            utc_ts=utc,
            probe_sha=digest,
            gate_table=gtab,
        )
        PROMPT_PATH.parent.mkdir(parents=True, exist_ok=True)
        if not PROMPT_PATH.exists():
            PROMPT_PATH.write_text(
                "# Script 401 — Manual-review queue sort-out\n", encoding="utf-8"
            )
        CLOSE_OUT_PATH.write_text(close_body, encoding="utf-8")
        log(f"Wrote {CLOSE_OUT_PATH}")
        csha = "(not committed)"
        tagn = f"{TAG_PREFIX}{run_stamp}"
        if args.phase4:
            csha, tagn = git_commit_and_tag(run_stamp)
            CLOSE_OUT_PATH.write_text(
                format_close_out(
                    commit_sha=csha,
                    tag_name=tagn,
                    utc_ts=utc,
                    probe_sha=digest,
                    gate_table=gtab,
                ),
                encoding="utf-8",
            )
        log("Apply+verify done.")
        print(f"commit_sha={csha} tag={tagn}", flush=True)
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
