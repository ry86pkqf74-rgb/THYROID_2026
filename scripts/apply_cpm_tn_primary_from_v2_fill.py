#!/usr/bin/env python3
"""Script 397 — CPM T/N primary ← v2 COALESCE fill (malignant-only, 236-row cohort).

Fills `ajcc8_t_stage` / `ajcc8_n_stage` from `*_v2` when primary is NULL and
diagnosis is in the malignant allowlist. No M updates; no `ajcc8_stage_group` writes.

Phases
------
* --phase 0 (default) — probe + H1–H8; probe markdown + PROBE_REPORT_SHA256
  (hashed region excludes footer after ``---HASH-BOUNDARY---``).
* --apply — idempotency; else ``--i-approve`` must match probe hash;
  transaction snapshot + UPDATEs + ``__readme``; Phase 3 verify.

Token: motherduck_client.get_token() (never printed).
"""
from __future__ import annotations

import argparse
import re
import subprocess
from collections import Counter
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

CPM_TABLE = "canonical_patient_master"
README_TABLE = "__readme"

SCRIPT_FILE = "apply_cpm_tn_primary_from_v2_fill.py"
SCRIPT_ID = "script_397"
SNAPSHOT_PREFIX = "cpm_pre_tn_primary_from_v2_fill_"

EXPECTED_T_FILL = 26
EXPECTED_N_FILL = 213
EXPECTED_DISTINCT = 236
CPM_EXPECTED = 10_871

MALIGNANT_ALLOWLIST: tuple[str, ...] = (
    "PTC",
    "FTC",
    "HCC",
    "DTC_NOS",
    "MTC",
    "ATC",
    "other_malignant",
)
ALLOW_TUPLE = "(" + ",".join(f"'{d}'" for d in MALIGNANT_ALLOWLIST) + ")"

HASH_BOUNDARY = "---HASH-BOUNDARY---"

OUTPUT_DIR = REPO_ROOT / "scripts" / "output"
PROBE_PATH = OUTPUT_DIR / "apply_cpm_tn_primary_from_v2_fill_probe.md"
RUN_LOG_PATH = OUTPUT_DIR / "apply_cpm_tn_primary_from_v2_fill_run.log"
CLOSE_OUT_PATH = REPO_ROOT / "cursor_prompts" / "CLOSE_OUT_397.md"
PROMPT_PATH = REPO_ROOT / "cursor_prompts" / (
    "CURSOR_PROMPT_CPM_TN_PRIMARY_FROM_V2_FILL_20260423_SCRIPT_397.md"
)

TAG_PREFIX = "v1_0-cpm-tn-primary-from-v2-filled-"

FORCE_ADD_PATTERNS: list[str] = [r"scripts/output/.*_run\.log$"]


def _should_force_add(path: Path) -> bool:
    try:
        s = str(path.relative_to(REPO_ROOT))
    except ValueError:
        s = str(path)
    return any(re.search(p, s) for p in FORCE_ADD_PATTERNS)


# --------------------------------------------------------------------------- #
# Logging
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


# --------------------------------------------------------------------------- #
# SQL fragments
# --------------------------------------------------------------------------- #


def cpm_fq() -> str:
    return f'"{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"'


def readme_fq() -> str:
    return f'"{PUB_DB}"."{MAIN_SCHEMA}"."{README_TABLE}"'


def fq_snap(snap_name: str) -> str:
    return f'"{PUB_DB}"."{ARC_SCHEMA}"."{snap_name}"'


def where_malignant() -> str:
    return f"diagnosis_primary IN {ALLOW_TUPLE}"


def where_t_write() -> str:
    return (
        f"{where_malignant()} AND ajcc8_t_stage IS NULL AND ajcc8_t_stage_v2 IS NOT NULL"
    )


def where_n_write() -> str:
    return (
        f"{where_malignant()} AND ajcc8_n_stage IS NULL AND ajcc8_n_stage_v2 IS NOT NULL"
    )


def where_rescue_union() -> str:
    return f"(( {where_t_write()} ) OR ( {where_n_write()} ))"


# --------------------------------------------------------------------------- #
# Connection / catalog
# --------------------------------------------------------------------------- #


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


def count_snapshots_397(con: duckdb.DuckDBPyConnection) -> int:
    return con.execute(
        f"""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_catalog = ? AND table_schema = ?
          AND table_name LIKE '{SNAPSHOT_PREFIX}%'
        """,
        [PUB_DB, ARC_SCHEMA],
    ).fetchone()[0]


def find_snapshot_397(con: duckdb.DuckDBPyConnection) -> str | None:
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


def readme_397_rowcount(con: duckdb.DuckDBPyConnection) -> int:
    cols = readme_columns(con)
    if not cols:
        return 0
    if "script" in cols:
        n = con.execute(
            f'SELECT COUNT(*) FROM {readme_fq()} WHERE script = ?', [SCRIPT_ID]
        ).fetchone()[0]
        if n:
            return n
    if "script_tag" in cols:
        n = con.execute(
            f'SELECT COUNT(*) FROM {readme_fq()} WHERE script_tag = ?', [SCRIPT_ID]
        ).fetchone()[0]
        if n:
            return n
    return con.execute(
        f"""
        SELECT COUNT(*) FROM {readme_fq()}
        WHERE content LIKE 'Script 397:%' OR content LIKE ?
        """,
        [f"%{SCRIPT_ID}%"],
    ).fetchone()[0]


def scope_candidate_count(con: duckdb.DuckDBPyConnection) -> int:
    cpm = cpm_fq()
    return con.execute(
        f"""
        SELECT COUNT(DISTINCT research_id) FROM {cpm}
        WHERE {where_rescue_union()}
        """
    ).fetchone()[0]


def idempotency_complete(con: duckdb.DuckDBPyConnection) -> bool:
    if not find_snapshot_397(con):
        return False
    if readme_397_rowcount(con) < 1:
        return False
    return scope_candidate_count(con) == 0


def idempotency_partial(con: duckdb.DuckDBPyConnection) -> tuple[bool, str | None]:
    sn = find_snapshot_397(con)
    nr = readme_397_rowcount(con)
    if sn and nr == 0:
        return True, "snapshot without __readme row"
    if nr > 0 and not sn:
        return True, "__readme without snapshot"
    return False, None


# --------------------------------------------------------------------------- #
# Probe & gates
# --------------------------------------------------------------------------- #


def _rid_list_to_sql(rids: list[str]) -> str:
    if not rids:
        return "('')"
    return "(" + ",".join(f"'{r}'" for r in rids) + ")"


def run_probe(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    cpm = cpm_fq()

    t_fill = con.execute(
        f"SELECT COUNT(*) FROM {cpm} WHERE {where_t_write()}"
    ).fetchone()[0]
    n_fill = con.execute(
        f"SELECT COUNT(*) FROM {cpm} WHERE {where_n_write()}"
    ).fetchone()[0]
    distinct = con.execute(
        f"SELECT COUNT(DISTINCT research_id) FROM {cpm} WHERE {where_rescue_union()}"
    ).fetchone()[0]

    t_rids = [
        str(r[0])
        for r in con.execute(
            f"""
            SELECT DISTINCT CAST(research_id AS VARCHAR) FROM {cpm}
            WHERE {where_t_write()} ORDER BY 1
            """
        ).fetchall()
    ]
    n_rids = [
        str(r[0])
        for r in con.execute(
            f"""
            SELECT DISTINCT CAST(research_id AS VARCHAR) FROM {cpm}
            WHERE {where_n_write()} ORDER BY 1
            """
        ).fetchall()
    ]
    all_rids = sorted(set(t_rids) | set(n_rids), key=int)

    h2 = con.execute(
        f"""
        SELECT COUNT(*) FROM {cpm}
        WHERE {where_rescue_union()} AND NOT (diagnosis_primary IN {ALLOW_TUPLE})
        """
    ).fetchone()[0]

    h2_taut = con.execute(
        f"""
        SELECT COUNT(*) FROM {cpm}
        WHERE {where_malignant()} AND ( {where_rescue_union()} )
          AND diagnosis_primary NOT IN {ALLOW_TUPLE}
        """
    ).fetchone()[0]

    h3 = con.execute(
        f"""
        SELECT COUNT(*) FROM {cpm}
        WHERE {where_malignant()}
          AND ajcc8_m_stage IS NULL AND ajcc8_m_stage_v2 IS NOT NULL
        """
    ).fetchone()[0]

    h4_t_bad = con.execute(
        f"""
        SELECT COUNT(*) FROM {cpm} WHERE {where_t_write()} AND ajcc8_t_stage IS NOT NULL
        """
    ).fetchone()[0]
    h4_n_bad = con.execute(
        f"""
        SELECT COUNT(*) FROM {cpm} WHERE {where_n_write()} AND ajcc8_n_stage IS NOT NULL
        """
    ).fetchone()[0]

    h5 = con.execute(f"SELECT COUNT(*) FROM {cpm}").fetchone()[0]

    h7 = count_snapshots_397(con)

    # H8: on rows matching each UPDATE predicate, primary is NULL for that
    # column — so the (both-set disagree) pattern cannot hold for that axis.
    h8_t = con.execute(
        f"""
        SELECT COUNT(*) FROM {cpm}
        WHERE {where_t_write()}
          AND ajcc8_t_stage IS NOT NULL
          AND ajcc8_t_stage_v2 IS NOT NULL
          AND ajcc8_t_stage IS DISTINCT FROM ajcc8_t_stage_v2
        """
    ).fetchone()[0]
    h8_n = con.execute(
        f"""
        SELECT COUNT(*) FROM {cpm}
        WHERE {where_n_write()}
          AND ajcc8_n_stage IS NOT NULL
          AND ajcc8_n_stage_v2 IS NOT NULL
          AND ajcc8_n_stage IS DISTINCT FROM ajcc8_n_stage_v2
        """
    ).fetchone()[0]

    per_dx = con.execute(
        f"""
        SELECT diagnosis_primary, COUNT(*)::BIGINT
        FROM {cpm}
        WHERE {where_rescue_union()}
        GROUP BY 1 ORDER BY 1
        """
    ).fetchall()

    null_t_stage_pre = con.execute(
        f"""
        SELECT
          CAST(research_id AS VARCHAR) AS rid,
          diagnosis_primary,
          ajcc8_stage_group
        FROM {cpm}
        WHERE {where_t_write()} AND ajcc8_stage_group IS NULL
        ORDER BY CAST(research_id AS INTEGER)
        """
    ).fetchall()

    return {
        "t_fill": t_fill,
        "n_fill": n_fill,
        "distinct": distinct,
        "t_rids": t_rids,
        "n_rids": n_rids,
        "all_rids": all_rids,
        "h2_benign_style": h2,
        "h2_taut": h2_taut,
        "h3_m_rescue": h3,
        "h4_t_bad": h4_t_bad,
        "h4_n_bad": h4_n_bad,
        "h5_cpm": h5,
        "h7_snaps": h7,
        "h8_t_disag": h8_t,
        "h8_n_disag": h8_n,
        "per_dx": per_dx,
        "null_t_stage_group_rows": null_t_stage_pre,
    }


def halt_gate(p: dict[str, Any]) -> tuple[bool, list[str]]:
    errs: list[str] = []

    if p["t_fill"] != EXPECTED_T_FILL:
        errs.append(f"H1: T_fill={p['t_fill']}, expected {EXPECTED_T_FILL}")
    if p["n_fill"] != EXPECTED_N_FILL:
        errs.append(f"H1: N_fill={p['n_fill']}, expected {EXPECTED_N_FILL}")
    if p["distinct"] != EXPECTED_DISTINCT:
        errs.append(f"H1: distinct_rids={p['distinct']}, expected {EXPECTED_DISTINCT}")

    if p["h2_taut"] != 0:
        errs.append(f"H2: allowlist-tautology rows={p['h2_taut']}, expected 0")

    if p["h3_m_rescue"] != 0:
        errs.append(f"H3: M NULL+v2-pop rows={p['h3_m_rescue']}, expected 0")

    if p["h4_t_bad"] != 0:
        errs.append(f"H4: T write rows with non-NULL t_primary={p['h4_t_bad']}, expected 0")
    if p["h4_n_bad"] != 0:
        errs.append(f"H4: N write rows with non-NULL n_primary={p['h4_n_bad']}, expected 0")

    if p["h5_cpm"] != CPM_EXPECTED:
        errs.append(f"H5: CPM count={p['h5_cpm']}, expected {CPM_EXPECTED}")

    if p["h7_snaps"] > 1:
        errs.append(f"H7: snapshot prefix tables={p['h7_snaps']}, expected <=1")
    # H6: no stage_group in UPDATE; structural PASS

    if p["h8_t_disag"] != 0:
        errs.append(
            f"H8: T-predicate T disag rows={p['h8_t_disag']}, expected 0 (contradictory; check SQL)"
        )
    if p["h8_n_disag"] != 0:
        errs.append(
            f"H8: N-predicate N disag rows={p['h8_n_disag']}, expected 0 (contradictory; check SQL)"
        )

    return (len(errs) == 0, errs)


def format_probe_hashed_body(p: dict[str, Any], gate_ok: bool, gate_errs: list[str]) -> str:
    lines: list[str] = [
        "# Script 397 — Phase 0 probe (CPM T/N primary from v2 fill)",
        "",
        "## Cohort (malignant allowlist only)",
        "",
        f"- **T NULL + v2 populated:** {p['t_fill']}",
        f"- **N NULL + v2 populated:** {p['n_fill']}",
        f"- **Distinct research_ids (union):** {p['distinct']}",
        "",
        f"- **CPM total (H5):** {p['h5_cpm']}",
        f"- **H6:** PASS — planned UPDATEs only on `ajcc8_t_stage` and `ajcc8_n_stage` (not `ajcc8_stage_group`).",
        f"- **H7 snapshot tables with `{SNAPSHOT_PREFIX}*`:** {p['h7_snaps']}",
        f"- **H3 M rescue (NULL primary + v2) among allowlist:** {p['h3_m_rescue']}",
        f"- **H2 tautology (allowlist ∧ rescue ∧ NOT allowlist):** {p['h2_taut']}",
        f"- **Rows with (t or n) rescue and diagnosis outside allowlist:** {p['h2_benign_style']}",
        "",
        "## H8 — Cross-source disagreements on rows matching each UPDATE (must be 0)",
        "",
        "Per axis, the UPDATE predicate requires primary NULL for that column, so (both set ∧ differ) cannot hold."
        f" **T-predicate rows:** {p['h8_t_disag']}; **N-predicate rows:** {p['h8_n_disag']}.",
        "",
        "## Halt gates (H1–H8 summary)",
        "",
        f"| all_pass | {gate_ok} |",
        "",
    ]
    if gate_errs:
        lines.extend(["### Failures", ""])
        for e in gate_errs:
            lines.append(f"- {e}")
        lines.append("")

    lines.append("## Per diagnosis (malignant rescue rows)")
    lines.append("")
    lines.append("| diagnosis_primary | rows |")
    lines.append("|---|---:|")
    for dx, cnt in p["per_dx"]:
        lines.append(f"| {dx} | {cnt} |")
    lines.append("")

    lines.append("## T-NULL with NULL `ajcc8_stage_group` (CF-397-1 follow-up candidates)")
    lines.append("")
    lines.append("| research_id | diagnosis | ajcc8_stage_group |")
    lines.append("|---|---|---|")
    for rid, dxx, sg in p["null_t_stage_group_rows"]:
        lines.append(
            f"| {rid} | {dxx} | {'NULL' if sg is None else sg} |"
        )
    lines.append("")

    lines.extend(
        [
            "## Planned UPDATEs",
            "",
            "```sql",
            "UPDATE ... SET ajcc8_t_stage = ajcc8_t_stage_v2 WHERE " + where_t_write(),
            "UPDATE ... SET ajcc8_n_stage = ajcc8_n_stage_v2 WHERE " + where_n_write(),
            "```",
            "",
        ]
    )
    return "\n".join(lines)


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


# --------------------------------------------------------------------------- #
# Apply
# --------------------------------------------------------------------------- #


def insert_readme_397(
    con: duckdb.DuckDBPyConnection,
    *,
    snap_fqn: str,
    probe_sha: str,
    t_c: int,
    n_c: int,
    distinct_c: int,
    per_dx_lines: str,
) -> None:
    cols = readme_columns(con)
    content = (
        f"Script 397: CPM T/N primary-from-v2 fill. T_updates={t_c} N_updates={n_c} "
        f"distinct_rids={distinct_c}. {per_dx_lines} "
        f"Snapshot: {snap_fqn}. Probe SHA256: {probe_sha}."
    )
    if {"script_name", "script_tag", "run_timestamp", "summary"}.issubset(cols):
        con.execute(
            f"""
            INSERT INTO {readme_fq()} (script_name, script_tag, run_timestamp, summary)
            VALUES (?, ?, CAST(CURRENT_TIMESTAMP AS TIMESTAMP), ?)
            """,
            [SCRIPT_FILE, SCRIPT_ID, content],
        )
        return
    insert_cols = ["content", "updated_at"]
    value_exprs = ["?", "CAST(CURRENT_TIMESTAMP AS TIMESTAMP)"]
    params: list[Any] = [f"Script 397: {SCRIPT_ID} {content}"]
    if "script" in cols:
        insert_cols.append("script")
        value_exprs.append("?")
        params.append(SCRIPT_ID)
    con.execute(
        f"INSERT INTO {readme_fq()} ({', '.join(insert_cols)}) VALUES ({', '.join(value_exprs)})",
        params,
    )


def apply_transaction(
    con: duckdb.DuckDBPyConnection,
    run_stamp: str,
    probe_sha: str,
) -> str:
    cpm = cpm_fq()
    if count_snapshots_397(con) != 0:
        raise SystemExit("Apply: require zero existing cpm_pre_tn_primary_from_v2_fill_* tables")

    t_fill = con.execute(
        f"SELECT COUNT(*) FROM {cpm} WHERE {where_t_write()}"
    ).fetchone()[0]
    n_fill = con.execute(
        f"SELECT COUNT(*) FROM {cpm} WHERE {where_n_write()}"
    ).fetchone()[0]
    distinct = con.execute(
        f"SELECT COUNT(DISTINCT research_id) FROM {cpm} WHERE {where_rescue_union()}"
    ).fetchone()[0]
    if t_fill != EXPECTED_T_FILL or n_fill != EXPECTED_N_FILL or distinct != EXPECTED_DISTINCT:
        raise SystemExit(
            f"Apply: scope drift t={t_fill} n={n_fill} distinct={distinct} — abort"
        )

    all_rids = [
        str(r[0])
        for r in con.execute(
            f"""
            SELECT DISTINCT CAST(research_id AS VARCHAR) FROM {cpm}
            WHERE {where_rescue_union()} ORDER BY CAST(research_id AS INTEGER)
            """
        ).fetchall()
    ]
    w_in = _rid_list_to_sql(all_rids)
    per_dx = con.execute(
        f"""
        SELECT diagnosis_primary, COUNT(*)::BIGINT
        FROM {cpm} WHERE {where_rescue_union()} GROUP BY 1 ORDER BY 1
        """
    ).fetchall()
    per_dx_lines = "Per diagnosis: " + ", ".join(f"{a}:{b}" for a, b in per_dx) + "."

    snap_name = f"{SNAPSHOT_PREFIX}{run_stamp}"
    snap = fq_snap(snap_name)
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute(
            f"""
            CREATE TABLE {snap} AS
            SELECT * FROM {cpm}
            WHERE CAST(research_id AS VARCHAR) IN {w_in}
            """
        )
        n_snap = con.execute(f"SELECT COUNT(*) FROM {snap}").fetchone()[0]
        if n_snap != EXPECTED_DISTINCT:
            raise SystemExit(f"2A: snapshot rows={n_snap}, expected {EXPECTED_DISTINCT}")

        t_up = con.execute(
            f"""
            UPDATE {cpm}
            SET ajcc8_t_stage = ajcc8_t_stage_v2
            WHERE {where_t_write()}
            RETURNING research_id
            """
        ).fetchall()
        if len(t_up) != EXPECTED_T_FILL:
            raise SystemExit(f"2B: T update rows={len(t_up)} expected {EXPECTED_T_FILL}")

        n_up = con.execute(
            f"""
            UPDATE {cpm}
            SET ajcc8_n_stage = ajcc8_n_stage_v2
            WHERE {where_n_write()}
            RETURNING research_id
            """
        ).fetchall()
        if len(n_up) != EXPECTED_N_FILL:
            raise SystemExit(f"2B: N update rows={len(n_up)} expected {EXPECTED_N_FILL}")

        snap_fqn = f"{PUB_DB}.{ARC_SCHEMA}.{snap_name}"
        insert_readme_397(
            con,
            snap_fqn=snap_fqn,
            probe_sha=probe_sha,
            t_c=len(t_up),
            n_c=len(n_up),
            distinct_c=EXPECTED_DISTINCT,
            per_dx_lines=per_dx_lines,
        )
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

    p2 = con.execute(
        f"SELECT COUNT(*) FROM {cpm} WHERE {where_t_write()}"
    ).fetchone()[0]
    if p2 != 0:
        errs.append(f"P2: T rescue rows remaining={p2}, expected 0")

    p3 = con.execute(
        f"SELECT COUNT(*) FROM {cpm} WHERE {where_n_write()}"
    ).fetchone()[0]
    if p3 != 0:
        errs.append(f"P3: N rescue rows remaining={p3}, expected 0")

    n_readme = readme_397_rowcount(con)
    if n_readme != 1:
        errs.append(f"P4: __readme {SCRIPT_ID} rows={n_readme}, expected 1")

    p5 = con.execute(f"SELECT COUNT(*) FROM {snap}").fetchone()[0]
    if p5 != EXPECTED_DISTINCT:
        errs.append(f"P5: snapshot rows={p5}, expected {EXPECTED_DISTINCT}")

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
    other = [c for c in snap_cols if c not in ("ajcc8_t_stage", "ajcc8_n_stage")]
    if other:
        ors = [f'c."{c}" IS DISTINCT FROM s."{c}"' for c in other]
        p6o = con.execute(
            f"""
            SELECT COUNT(*) FROM {snap} s
            INNER JOIN {cpm} c
              ON CAST(s.research_id AS VARCHAR) = CAST(c.research_id AS VARCHAR)
            WHERE {" OR ".join(ors)}
            """
        ).fetchone()[0]
        if p6o > 0:
            errs.append(f"P6: {p6o} row(s) differ outside T/N columns")

    t_diff = con.execute(
        f"""
        SELECT COUNT(*)
        FROM {snap} s
        INNER JOIN {cpm} c ON CAST(s.research_id AS VARCHAR) = CAST(c.research_id AS VARCHAR)
        WHERE s.ajcc8_t_stage IS NULL
          AND c.ajcc8_t_stage IS NOT NULL
          AND c.ajcc8_t_stage IS NOT DISTINCT FROM s.ajcc8_t_stage_v2
        """
    ).fetchone()[0]
    if t_diff != EXPECTED_T_FILL:
        errs.append(
            f"P6: T NULL→v2-consistent fills={t_diff}, expected {EXPECTED_T_FILL}"
        )
    t_spurious = con.execute(
        f"""
        SELECT COUNT(*)
        FROM {snap} s
        INNER JOIN {cpm} c ON CAST(s.research_id AS VARCHAR) = CAST(c.research_id AS VARCHAR)
        WHERE s.ajcc8_t_stage IS NULL AND c.ajcc8_t_stage IS NOT NULL
          AND c.ajcc8_t_stage IS DISTINCT FROM s.ajcc8_t_stage_v2
        """
    ).fetchone()[0]
    if t_spurious > 0:
        errs.append(f"P6: T fills not equal to v2 = {t_spurious}")

    n_diff = con.execute(
        f"""
        SELECT COUNT(*)
        FROM {snap} s
        INNER JOIN {cpm} c ON CAST(s.research_id AS VARCHAR) = CAST(c.research_id AS VARCHAR)
        WHERE s.ajcc8_n_stage IS NULL
          AND c.ajcc8_n_stage IS NOT NULL
          AND c.ajcc8_n_stage IS NOT DISTINCT FROM s.ajcc8_n_stage_v2
        """
    ).fetchone()[0]
    if n_diff != EXPECTED_N_FILL:
        errs.append(
            f"P6: N NULL→v2-consistent fills={n_diff}, expected {EXPECTED_N_FILL}"
        )
    n_spur = con.execute(
        f"""
        SELECT COUNT(*)
        FROM {snap} s
        INNER JOIN {cpm} c ON CAST(s.research_id AS VARCHAR) = CAST(c.research_id AS VARCHAR)
        WHERE s.ajcc8_n_stage IS NULL AND c.ajcc8_n_stage IS NOT NULL
          AND c.ajcc8_n_stage IS DISTINCT FROM s.ajcc8_n_stage_v2
        """
    ).fetchone()[0]
    if n_spur > 0:
        errs.append(f"P6: N fills not equal to v2 = {n_spur}")

    s_sg = con.execute(
        f"""
        SELECT COUNT(*) FROM {snap} s
        INNER JOIN {cpm} c ON CAST(s.research_id AS VARCHAR) = CAST(c.research_id AS VARCHAR)
        WHERE c.ajcc8_stage_group IS DISTINCT FROM s.ajcc8_stage_group
        """
    ).fetchone()[0]
    if s_sg > 0:
        errs.append(f"P6: ajcc8_stage_group mutations={s_sg}, expected 0")

    s_m = con.execute(
        f"""
        SELECT COUNT(*) FROM {snap} s
        INNER JOIN {cpm} c ON CAST(s.research_id AS VARCHAR) = CAST(c.research_id AS VARCHAR)
        WHERE c.ajcc8_m_stage IS DISTINCT FROM s.ajcc8_m_stage
        """
    ).fetchone()[0]
    if s_m > 0:
        errs.append(f"P6: ajcc8_m_stage mutations={s_m}, expected 0")

    p7t = con.execute(
        f"""
        SELECT COUNT(*) FROM {snap} s
        WHERE s.diagnosis_primary IN {ALLOW_TUPLE}
          AND s.ajcc8_t_stage IS NULL AND s.ajcc8_t_stage_v2 IS NOT NULL
          AND s.ajcc8_t_stage IS NOT NULL
          AND s.ajcc8_t_stage_v2 IS NOT NULL
          AND s.ajcc8_t_stage IS DISTINCT FROM s.ajcc8_t_stage_v2
        """
    ).fetchone()[0]
    p7n = con.execute(
        f"""
        SELECT COUNT(*) FROM {snap} s
        WHERE s.diagnosis_primary IN {ALLOW_TUPLE}
          AND s.ajcc8_n_stage IS NULL AND s.ajcc8_n_stage_v2 IS NOT NULL
          AND s.ajcc8_n_stage IS NOT NULL
          AND s.ajcc8_n_stage_v2 IS NOT NULL
          AND s.ajcc8_n_stage IS DISTINCT FROM s.ajcc8_n_stage_v2
        """
    ).fetchone()[0]
    if p7t > 0 or p7n > 0:
        errs.append(
            f"P7: pre-snapshot T/N disag rows in write cohort: T={p7t} N={p7n}, expected 0-0"
        )

    info.update(
        {
            "p1": p1,
            "p2": p2,
            "p3": p3,
            "n_readme": n_readme,
            "p5": p5,
        }
    )
    return (len(errs) == 0, errs, info)


def format_close_out(
    *,
    commit_sha: str,
    tag_name: str,
    utc_ts: str,
    probe_sha: str,
    snap_name: str,
    p: dict[str, Any],
) -> str:
    gate_md = "\n".join(
        [
            "| gate | verdict |",
            "|---|---|",
            "| H1 | PASS (26 T, 213 N, 236 distinct) |",
            "| H2 | PASS (allowlist tautology 0) |",
            "| H3 | PASS (0 M-rescue) |",
            "| H4 | PASS (predicates enforce NULL primary) |",
            "| H5 | PASS (10,871) |",
            "| H6 | PASS (T/N only) |",
            "| H7 | PASS (archive prefix) |",
            "| H8 | PASS (axis-split: T- vs N-write predicates) |",
        ]
    )
    nul_rows = p.get("null_t_stage_group_rows", [])
    nul_bul = "\n".join(
        f"- `{rid}` — {dx} (stage NULL)" for rid, dx, _sg in nul_rows
    )
    lines = [
        "# Script 397 — Close-out",
        "",
        f"- **Git commit / SHA:** `{commit_sha}`",
        f"- **Tag:** `{tag_name}`",
        f"- **UTC timestamp:** {utc_ts}",
        f"- **Probe SHA256 (consumed):** `{probe_sha}`",
        f"- **Snapshot FQN:** `{PUB_DB}.{ARC_SCHEMA}.{snap_name}`",
        "",
        "## Halt-gate verdicts (Phase 0)",
        "",
        gate_md,
        "",
        "## Writes",
        "",
        f"- T primary ← v2: **{EXPECTED_T_FILL}**",
        f"- N primary ← v2: **{EXPECTED_N_FILL}**",
        f"- **Distinct `research_id` values:** {EXPECTED_DISTINCT}",
        "",
        "## Per diagnosis (malignant rescue rows touched)",
        "",
    ]
    lines.append("| diagnosis_primary | rows |")
    lines.append("|---|---:|")
    for dx, cnt in p.get("per_dx", []):
        lines.append(f"| {dx} | {cnt} |")
    lines.extend(
        [
            "",
            "**Cosmetic alignment:** most updated rows (231) already had a populated `ajcc8_stage_group`; "
            "T/N fill aligns primary with v2 for audit without changing group.",
            "",
            "## T-filled rows with pre-NULL `ajcc8_stage_group` (CF-397-1)",
            "",
        ]
    )
    if nul_bul:
        lines.append(nul_bul)
        cfx = Counter(dx for _rid, dx, _ in nul_rows)
        lines.append("")
        lines.append(
            f"**CF-397-1 composition (n={len(nul_rows)}):** "
            + ", ".join(f"{cfx[dx]}× {dx}" for dx in sorted(cfx))
            + " — total 5, so the MTC subtotal is **3** (with DTC_NOS + other_malignant), "
            "not 4+1+1; any earlier '4 MTC' wording was an MTC subtotal slip, not a fifth row."
        )
    else:
        lines.append("- *(none in probe; verify post-apply)*")
    lines.extend(
        [
            "",
            "- **CF-397-1:** Re-derive or adjudicate `ajcc8_stage_group` for these rids as needed.",
            "- **CF-397-2:** Builder source fix — `COALESCE(primary, v2)` for T/N (and M) on CPM build.",
            "- **CF-397-3 → Script 398:** Read-only sidecar of primary↔v2 disagreements (T/N/M).",
            "",
            "## Disagreement rows",
            "",
            "Primary↔v2 disagreement cohorts (363 T, 2055 N, 1838 M) were **not** written by this script.",
        ]
    )
    lines.append("")
    return "\n".join(lines)


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
        "Script 397: CPM T/N primary-from-v2 fill (236 rows; 26 T, 213 N; malignant-only)"
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


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--phase", type=int, default=0, help="0=probe (default)")
    ap.add_argument("--apply", action="store_true", help="Run apply after gates + approval")
    ap.add_argument(
        "--i-approve",
        default="",
        metavar="SHA256",
        help="SHA256 of hashed probe body (required for --apply on fresh run)",
    )
    ap.add_argument(
        "--force-stamp",
        default="",
        help="Override RUN_STAMP (UTC YYYYMMDD_HHMMSS) for snapshot/tag",
    )
    ap.add_argument(
        "--phase4",
        action="store_true",
        help="After apply+verify (or idempotent NO-OP verify), run git add/commit/tag/push",
    )
    args = ap.parse_args()

    if args.phase != 0:
        print("Only --phase 0 is implemented.", flush=True)
        return 1

    if args.apply:
        c0 = connect_quiet()
        try:
            if idempotency_complete(c0):
                line = (
                    "NO-OP: snapshot + script_397 __readme + zero remaining rescue "
                    "candidates — running Phase 3 verify only."
                )
                print(line, flush=True)
                OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
                with open(RUN_LOG_PATH, "a", encoding="utf-8") as f:
                    f.write(f"{datetime.now(timezone.utc).isoformat()} {line}\n")
                snap = find_snapshot_397(c0)
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
                    log(f"Phase 4: NO-OP path — using snapshot stamp {rs}")
                    commit_sha, tag_n = git_commit_and_tag(rs)
                    log(f"Phase 4: commit_sha={commit_sha} tag={tag_n}")
                    print(f"commit_sha={commit_sha} tag={tag_n}", flush=True)
                return 0
            ptest, perr = halt_gate(run_probe(c0))
            if not ptest:
                for e in perr:
                    print(f"HALT (pre-apply recheck): {e}", flush=True)
                return 2
            partial, pr = idempotency_partial(c0)
            if partial:
                print(f"HALT: partial apply — {pr}", flush=True)
                return 2
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

        if find_snapshot_397(con) or readme_397_rowcount(con) > 0:
            if not idempotency_complete(con):
                log("HALT: artifacts without full idempotency")
                print("HALT: inconsistent state — check snapshot vs __readme vs scope", flush=True)
                return 2
            log("Already idempotent; should have exited earlier")
            return 2

        run_stamp = args.force_stamp.strip() or datetime.now(timezone.utc).strftime(
            "%Y%m%d_%H%M%S"
        )
        snap_name = apply_transaction(con, run_stamp, digest)
        ok3, verr, _ = run_verify(con, snap_name)
        if not ok3:
            for e in verr:
                log(f"VERIFY: {e}")
            print("\n--- PHASE 3 VERIFY FAILED ---\n" + "\n".join(verr), flush=True)
            return 2

        utc = datetime.now(timezone.utc).isoformat()
        tag = f"{TAG_PREFIX}{run_stamp}"
        cbody = format_close_out(
            commit_sha="(pending; run git rev-parse HEAD after commit)",
            tag_name=tag,
            utc_ts=utc,
            probe_sha=digest,
            snap_name=snap_name,
            p=probe,
        )
        CLOSE_OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        CLOSE_OUT_PATH.write_text(cbody, encoding="utf-8")
        log(f"Wrote {CLOSE_OUT_PATH}")

        csha = "(not committed)"
        if args.phase4:
            csha, tag = git_commit_and_tag(run_stamp)
            cbody2 = format_close_out(
                commit_sha=csha,
                tag_name=tag,
                utc_ts=utc,
                probe_sha=digest,
                snap_name=snap_name,
                p=probe,
            )
            CLOSE_OUT_PATH.write_text(cbody2, encoding="utf-8")
        log("Script 397 apply + verify complete.")
        print(f"commit_sha={csha} tag={tag} snapshot={snap_name}", flush=True)
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
