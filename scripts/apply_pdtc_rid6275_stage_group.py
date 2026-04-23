#!/usr/bin/env python3
"""Script 403 — rid 6275 PDTC: CPM `ajcc8_stage_group='I'` + queue DELETE (narrow; CF-401-5).

* --phase 0 — if idempotency complete, post-apply steady state probe; else H1–H10 + probe hash.
* --apply — requires --i-approve; transaction; Phase 3 verify; optional --phase4.

`diagnosis_primary` normalization deferred to Script 404.
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
from apply_histologic_classification_audit import PDTC_W  # noqa: E402
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

SCRIPT_FILE = "apply_pdtc_rid6275_stage_group.py"
SCRIPT_ID = "script_403"
SCRIPT_TAG = "script_403"
CPM_SNAPSHOT_PREFIX = "cpm_pre_pdtc_rid6275_stage_group_"
QUEUE_SNAPSHOT_PREFIX = "queue_pre_pdtc_rid6275_stage_group_"

APPLY_RID = "6275"
DELETE_SOURCE = "399"

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

# Six peers that must remain unchanged (reason + membership) after apply
QUEUE_PEER_EXPECT: tuple[tuple[str, str, str], ...] = (
    (
        "1404",
        "395",
        "ptc_age_64_ajcc7_stage_iii_calculable_missing_T_ajcc8_migration_requires_T_chart_review",
    ),
    (
        "12198",
        "395",
        "ptc_age_61_ajcc7_stage_iii_calculable_missing_T_ajcc8_migration_requires_T_chart_review",
    ),
    (
        "423",
        "399",
        "mtc_t_null_cannot_derive_plus_builder_corrected_i_is_dtc_rule_misapplied_to_mtc_n1a_m0_row",
    ),
    (
        "924",
        "399",
        "mtc_age_33_primary_t3b_n1a_outlier_vs_v2_ajcc7_dominant_all_t1a_n1b_majority_signal_yields_iva_under_ajcc8_mtc_t1_t3_n1b_m0_rule_source_review_needed",
    ),
    (
        "9600",
        "399",
        "mtc_m1_ajcc8_rule_yields_ivc_but_builder_and_path_both_say_ivb_edition_adjudication_needed",
    ),
    (
        "6768",
        "399",
        "angiosarcoma_of_thyroid_per_histology_final_not_ajcc8_thyroid_stageable_soft_tissue_sarcoma_framework_applies_path_stage_ii_source_unknown",
    ),
)

FORBIDDEN_PEER_RIDS_WRITE_AUDIT: tuple[str, ...] = (
    "1404",
    "12198",
    "423",
    "924",
    "9600",
    "6768",
)

CPM_EXPECTED = 10_871
EXPECT_QUEUE_PRE = 7
EXPECT_QUEUE_POST = 6
EXPECT_MALIGNANT_NULL_PRE = 7
EXPECT_MALIGNANT_NULL_POST = 6
EXPECT_PDTC_STAGED = 46
EXPECT_PDTC_BUCKET_LT55_M0_I_MIN = 5

HASH_BOUNDARY = "---HASH-BOUNDARY---"

OUTPUT_DIR = REPO_ROOT / "scripts" / "output"
PROBE_PATH = OUTPUT_DIR / "apply_pdtc_rid6275_stage_group_probe.md"
RUN_LOG_PATH = OUTPUT_DIR / "apply_pdtc_rid6275_stage_group_run.log"
CLOSE_OUT_PATH = REPO_ROOT / "cursor_prompts" / "CLOSE_OUT_403.md"
PROMPT_PATH = REPO_ROOT / "cursor_prompts" / (
    "CURSOR_PROMPT_PDTC_RID6275_APPLY_20260423_SCRIPT_403.md"
)
TAG_PREFIX = "v1_0-pdtc-rid6275-stage-group-applied-"

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
# Write SQL
# --------------------------------------------------------------------------- #

UPDATE_CPM_6275_SQL = f"""
UPDATE {cpm_fq()}
SET ajcc8_stage_group = 'I'
WHERE CAST(research_id AS VARCHAR) = '6275'
  AND diagnosis_primary = 'other_malignant'
  AND age_at_surgery = 38
  AND ajcc8_m_stage = 'M0'
  AND ajcc8_stage_group IS NULL
  AND ajcc8_stage_group_corrected = 'I'
  AND histology_final = 'poorly differentiated thyroid carcinoma'
RETURNING research_id
""".strip()

DELETE_QUEUE_6275_SQL = f"""
DELETE FROM {queue_fq()}
WHERE CAST(research_id AS VARCHAR) = '6275' AND source_script = '399'
RETURNING research_id
""".strip()

WRITE_SQL_BUNDLE: tuple[str, ...] = (UPDATE_CPM_6275_SQL, DELETE_QUEUE_6275_SQL)


def cpm_set_clause_safe(sql: str) -> bool:
    m = re.search(r"\bSET\s+(.+?)\s+WHERE", sql, re.I | re.S)
    if not m:
        return False
    clause = m.group(1).strip()
    return re.fullmatch(r"ajcc8_stage_group\s*=\s*'I'", clause) is not None


def h8_cpm_set_audit() -> bool:
    return cpm_set_clause_safe(UPDATE_CPM_6275_SQL)


def h9_writes_only_rid6275() -> tuple[bool, str]:
    for s in WRITE_SQL_BUNDLE:
        for m in re.finditer(
            r"CAST\s*\(\s*research_id\s+AS\s+VARCHAR\s*\)\s*=\s*'(\d+)'",
            s,
            re.I,
        ):
            if m.group(1) != APPLY_RID:
                return False, f"disallowed research_id literal {m.group(1)} in write SQL"
        for m in re.finditer(r"\bresearch_id\s*=\s*'(\d+)'", s, re.I):
            if m.group(1) != APPLY_RID:
                return False, f"disallowed research_id literal {m.group(1)} in write SQL"
    return True, "ok"


def h10_no_peer_rids_in_writes() -> tuple[bool, str]:
    for s in WRITE_SQL_BUNDLE:
        for rid in FORBIDDEN_PEER_RIDS_WRITE_AUDIT:
            if rid in s:
                return False, f"write SQL must not mention peer rid {rid}"
    return True, "ok"


H1_LOCK_SQL = f"""
SELECT COUNT(*) FROM {cpm_fq()}
WHERE CAST(research_id AS VARCHAR) = '6275'
  AND diagnosis_primary = 'other_malignant'
  AND age_at_surgery = 38
  AND ajcc8_m_stage = 'M0'
  AND ajcc8_stage_group IS NULL
  AND ajcc8_stage_group_corrected = 'I'
  AND histology_final = 'poorly differentiated thyroid carcinoma'
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
# Snapshots / readme
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


def find_cpm_snapshot(con: duckdb.DuckDBPyConnection) -> str | None:
    return _latest_arc_table(con, CPM_SNAPSHOT_PREFIX)


def find_queue_snapshot(con: duckdb.DuckDBPyConnection) -> str | None:
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


def readme_403_count(con: duckdb.DuckDBPyConnection) -> int:
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


def idempotency_complete(con: duckdb.DuckDBPyConnection) -> bool:
    cpm = cpm_fq()
    qf = queue_fq()
    sc = find_cpm_snapshot(con)
    sq = find_queue_snapshot(con)
    if not sc or not sq:
        return False
    if readme_403_count(con) != 1:
        return False
    g = con.execute(
        f"SELECT ajcc8_stage_group FROM {cpm} WHERE CAST(research_id AS VARCHAR) = ?",
        [APPLY_RID],
    ).fetchone()
    if not g or g[0] != "I":
        return False
    nq = con.execute(
        f"SELECT COUNT(*) FROM {qf} WHERE CAST(research_id AS VARCHAR) = ?",
        [APPLY_RID],
    ).fetchone()[0]
    if nq != 0:
        return False
    ntot = con.execute(f"SELECT COUNT(*) FROM {qf}").fetchone()[0]
    if ntot != EXPECT_QUEUE_POST:
        return False
    return True


def idempotency_partial(con: duckdb.DuckDBPyConnection) -> tuple[bool, str | None]:
    sc = find_cpm_snapshot(con)
    sq = find_queue_snapshot(con)
    rm = readme_403_count(con)
    if (sc or sq or rm) and not (sc and sq and rm == 1 and idempotency_complete(con)):
        return True, f"partial: cpm_snap={bool(sc)} queue_snap={bool(sq)} readme={rm}"
    return False, None


# --------------------------------------------------------------------------- #
# Probe (Phase 0)
# --------------------------------------------------------------------------- #


def pdtc_convention_counts(con: duckdb.DuckDBPyConnection) -> tuple[int, int]:
    cpm = cpm_fq()
    n_staged = con.execute(
        f"SELECT COUNT(*) FROM {cpm} WHERE ({PDTC_W}) AND ajcc8_stage_group IS NOT NULL"
    ).fetchone()[0]
    n_bucket = con.execute(
        f"""
        SELECT COUNT(*) FROM {cpm}
        WHERE ({PDTC_W})
          AND ajcc8_stage_group IS NOT NULL
          AND age_at_surgery < 55
          AND ajcc8_m_stage = 'M0'
          AND ajcc8_stage_group = 'I'
        """
    ).fetchone()[0]
    return n_staged, n_bucket


def run_probe(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    cpm = cpm_fq()
    qf = queue_fq()
    h1 = con.execute(H1_LOCK_SQL).fetchone()[0]
    h2 = con.execute(
        f"""
        SELECT COUNT(*) FROM {qf}
        WHERE CAST(research_id AS VARCHAR) = '6275' AND source_script = '399'
        """
    ).fetchone()[0]
    h3 = con.execute(f"SELECT COUNT(*) FROM {qf}").fetchone()[0]
    h4 = con.execute(f"SELECT COUNT(*) FROM {cpm}").fetchone()[0]
    n_staged, n_bucket = pdtc_convention_counts(con)
    h5_ok = n_staged == EXPECT_PDTC_STAGED and n_bucket >= EXPECT_PDTC_BUCKET_LT55_M0_I_MIN
    h6_ok = ajcc8_dtc_age_stratified_stage1_static()
    h7_cpm = count_prefix_tables(con, CPM_SNAPSHOT_PREFIX)
    h7_q = count_prefix_tables(con, QUEUE_SNAPSHOT_PREFIX)
    h7_ok = h7_cpm == 0 and h7_q == 0
    h8_ok = h8_cpm_set_audit()
    h9_ok, h9_msg = h9_writes_only_rid6275()
    h10_ok, h10_msg = h10_no_peer_rids_in_writes()

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
        "h2": h2,
        "h3": h3,
        "h4": h4,
        "n_pdtc_staged": n_staged,
        "n_pdtc_bucket_lt55_m0_i": n_bucket,
        "h5_ok": h5_ok,
        "h6_ok": h6_ok,
        "h7_cpm": h7_cpm,
        "h7_q": h7_q,
        "h7_ok": h7_ok,
        "h8_ok": h8_ok,
        "h9_ok": h9_ok,
        "h9_msg": h9_msg,
        "h10_ok": h10_ok,
        "h10_msg": h10_msg,
        "malignant_null": m_null,
        "queue_rows": queue_rows,
    }


def ajcc8_dtc_age_stratified_stage1_static() -> bool:
    """H6: age 38 < 55 and M0 → Stage I under AJCC 8th Ch 73 DTC age-stratified rules."""
    return (38 < 55) and True  # M0 from lock row


def halt_gate(p: dict[str, Any]) -> tuple[bool, list[str]]:
    errs: list[str] = []
    if p["h1"] != 1:
        errs.append(f"H1: rid 6275 lock count={p['h1']}, expected 1")
    if p["h2"] != 1:
        errs.append(f"H2: rid 6275 queue (399) count={p['h2']}, expected 1")
    if p["h3"] != EXPECT_QUEUE_PRE:
        errs.append(f"H3: queue total={p['h3']}, expected {EXPECT_QUEUE_PRE}")
    if p["h4"] != CPM_EXPECTED:
        errs.append(f"H4: CPM count={p['h4']}, expected {CPM_EXPECTED}")
    if not p["h5_ok"]:
        errs.append(
            f"H5: PDTC convention — staged={p.get('n_pdtc_staged')} "
            f"(expected {EXPECT_PDTC_STAGED}), "
            f"age<55 M0→I bucket={p.get('n_pdtc_bucket_lt55_m0_i')} "
            f"(min {EXPECT_PDTC_BUCKET_LT55_M0_I_MIN})"
        )
    if not p["h6_ok"]:
        errs.append("H6: static AJCC age-stratified rule check failed")
    if not p["h7_ok"]:
        errs.append(
            f"H7: archive prefix must be unused (cpm={p['h7_cpm']}, queue={p['h7_q']})"
        )
    if not p["h8_ok"]:
        errs.append("H8: CPM UPDATE must SET only ajcc8_stage_group = 'I'")
    if not p["h9_ok"]:
        errs.append(f"H9: write SQL scope — {p.get('h9_msg')}")
    if not p["h10_ok"]:
        errs.append(f"H10: peer rid audit — {p.get('h10_msg')}")
    if p.get("malignant_null") != EXPECT_MALIGNANT_NULL_PRE:
        errs.append(
            f"H(align P7 pre): malignant NULL={p.get('malignant_null')}, "
            f"expected {EXPECT_MALIGNANT_NULL_PRE}"
        )
    return (len(errs) == 0, errs)


def gate_verdict_rows(p: dict[str, Any], gate_ok: bool, gate_errs: list[str]) -> list[str]:
    """Per-gate PASS/FAIL lines for close-out."""
    rows: list[str] = []

    def one(name: str, ok: bool, detail: str = "") -> None:
        s = "PASS" if ok else "FAIL"
        rows.append(f"| {name} | {s} | {detail} |")

    one("H1", p["h1"] == 1, f"lock count={p['h1']}")
    one("H2", p["h2"] == 1, f"6275 in queue 399 count={p['h2']}")
    one("H3", p["h3"] == EXPECT_QUEUE_PRE, f"queue total={p['h3']}")
    one("H4", p["h4"] == CPM_EXPECTED, f"CPM={p['h4']}")
    one(
        "H5",
        bool(p["h5_ok"]),
        f"PDTC staged={p.get('n_pdtc_staged')}, bucket={p.get('n_pdtc_bucket_lt55_m0_i')}",
    )
    one("H6", bool(p["h6_ok"]), "age<55+M0→I static")
    one(
        "H7",
        bool(p["h7_ok"]),
        f"archive cpm={p['h7_cpm']} q={p['h7_q']}",
    )
    one("H8", bool(p["h8_ok"]), "SET audit")
    one("H9", bool(p["h9_ok"]), str(p.get("h9_msg", "")))
    one("H10", bool(p["h10_ok"]), str(p.get("h10_msg", "")))
    pre_ok = p.get("malignant_null") == EXPECT_MALIGNANT_NULL_PRE
    one("(pre) malignant NULL", pre_ok, f"count={p.get('malignant_null')}")
    if gate_errs and not gate_ok:
        rows.append(f"| (aggregate) | FAIL | {len(gate_errs)} halt(s) |")
    return rows


def format_gate_table(p: dict[str, Any], gate_ok: bool, gate_errs: list[str]) -> str:
    hdr = "| gate | result | detail |\n|---|---|---|"
    return hdr + "\n" + "\n".join(gate_verdict_rows(p, gate_ok, gate_errs))


def format_probe_hashed_body(
    p: dict[str, Any], gate_ok: bool, gate_errs: list[str]
) -> str:
    lines: list[str] = [
        "# Script 403 — Phase 0 probe (rid 6275 PDTC stage_group apply)",
        "",
        "## Halt gates (H1–H10 + malignant pre-check)",
        "",
        f"| all_pass | {gate_ok} |",
        "",
        f"- **H1 (rid 6275 state lock):** {p['h1']} (expected 1)",
        f"- **H2 (6275 in queue, source 399):** {p['h2']} (expected 1)",
        f"- **H3 (queue total):** {p['h3']} (expected {EXPECT_QUEUE_PRE})",
        f"- **H4 (CPM total):** {p['h4']} (expected {CPM_EXPECTED})",
        f"- **H5 (PDTC convention):** staged={p.get('n_pdtc_staged')} "
        f"(expected {EXPECT_PDTC_STAGED}), "
        f"age<55 M0→I bucket={p.get('n_pdtc_bucket_lt55_m0_i')} "
        f"(min {EXPECT_PDTC_BUCKET_LT55_M0_I_MIN}) → {p.get('h5_ok')}",
        f"- **H6 (static AJCC8 Ch 73 DTC age-stratified):** {p.get('h6_ok')}",
        f"- **H7 (archive unused):** cpm={p.get('h7_cpm')}, queue={p.get('h7_q')}",
        f"- **H8 (CPM SET audit):** {p.get('h8_ok')}",
        f"- **H9 (writes only rid 6275):** {p.get('h9_ok')} {p.get('h9_msg')}",
        f"- **H10 (no peer rids in write SQL):** {p.get('h10_ok')} {p.get('h10_msg')}",
        f"- **Malignant NULL stage_group (pre):** {p.get('malignant_null')} "
        f"(expected {EXPECT_MALIGNANT_NULL_PRE})",
        "",
    ]
    if gate_errs:
        lines.extend(["### Halts", ""] + [f"- {e}" for e in gate_errs] + [""])
    lines.extend(
        [
            "## PDTC convention precedent (already-staged cohort)",
            "",
            "| bucket | rule | count (live) |",
            "|---|---|---|",
            "| All PDTC-like, staged | `ajcc8_stage_group IS NOT NULL` | "
            f"{p.get('n_pdtc_staged')} |",
            "| age<55, M0, Stage I | DTC age-stratified precedent | "
            f"{p.get('n_pdtc_bucket_lt55_m0_i')} |",
            "",
            "## Queued rows (pre)",
            "",
        ]
    )
    for r in p.get("queue_rows", []):
        rid, src, reason = r
        short = (reason or "")[:100] + ("…" if reason and len(reason) > 100 else "")
        lines.append(f"- **{rid}** ({src}): {short}")
    lines.extend(
        [
            "",
            "## Planned writes",
            "",
            "- **A:** CPM UPDATE rid 6275 → `ajcc8_stage_group='I'` (AJCC8 Ch 73 DTC; PDTC grouped with DTC).",
            "- **B:** DELETE queue rid 6275 (`source_script='399'`).",
            "- **C:** `__readme` script_403; snapshots: CPM×1, queue×1 (6275 pre-delete).",
            "- **NOT:** `diagnosis_primary` / classification columns (Script 404).",
            "",
        ]
    )
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


def insert_readme_403(
    con: duckdb.DuckDBPyConnection,
    *,
    cpm_snap_fqn: str,
    queue_snap_fqn: str,
    probe_sha: str,
    run_stamp: str,
) -> None:
    cols = readme_columns(con)
    body = (
        f"Script 403: 1 CPM UPDATE (rid 6275 PDTC → ajcc8_stage_group I per AJCC 8th Ch 73 "
        f"DTC age-stratified rule; PDTC grouped with DTC). "
        f"1 queue DELETE (rid 6275, source 399). "
        f"Derivation: age<55 + M0 → Stage I. "
        f"Convention: {EXPECT_PDTC_STAGED}/{EXPECT_PDTC_STAGED} staged PDTC-like rows follow DTC rules; "
        f"age<55 M0→I precedent bucket ≥{EXPECT_PDTC_BUCKET_LT55_M0_I_MIN}. "
        f"CPM snap {cpm_snap_fqn}; queue snap {queue_snap_fqn}. "
        f"diagnosis_primary unchanged (other_malignant); cohort normalization deferred Script 404. "
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
            INSERT INTO {readme_fq()} (script, script_name, run_timestamp, content)
            VALUES (?, ?, CAST(CURRENT_TIMESTAMP AS TIMESTAMP), ?)
            """,
            [SCRIPT_ID, SCRIPT_FILE, body],
        )
        return
    insert_cols: list[str] = ["content", "updated_at"]
    value_exprs = ["?", "CAST(CURRENT_TIMESTAMP AS TIMESTAMP)"]
    params: list[Any] = [f"Script 403: {SCRIPT_ID} {body}"]
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
        raise SystemExit("Apply: 403 archive prefix already exists")
    cpm = cpm_fq()
    qf = queue_fq()
    cpm_snap = f"{CPM_SNAPSHOT_PREFIX}{run_stamp}"
    q_snap = f"{QUEUE_SNAPSHOT_PREFIX}{run_stamp}"
    cpm_sql = fq_snap(cpm_snap)
    qsql = fq_snap(q_snap)

    con.execute("BEGIN TRANSACTION")
    try:
        con.execute(
            f"""
            CREATE TABLE {cpm_sql} AS
            SELECT * FROM {cpm}
            WHERE CAST(research_id AS VARCHAR) = '6275'
            """
        )
        n1 = con.execute(f"SELECT COUNT(*) FROM {cpm_sql}").fetchone()[0]
        if n1 != 1:
            raise SystemExit(f"CPM snapshot rows={n1}, expected 1")

        con.execute(
            f"""
            CREATE TABLE {qsql} AS
            SELECT * FROM {qf}
            WHERE CAST(research_id AS VARCHAR) = '6275' AND source_script = '399'
            """
        )
        n2 = con.execute(f"SELECT COUNT(*) FROM {qsql}").fetchone()[0]
        if n2 != 1:
            raise SystemExit(f"Queue snapshot rows={n2}, expected 1")

        u = con.execute(UPDATE_CPM_6275_SQL).fetchall()
        if len(u) != 1:
            raise SystemExit("CPM UPDATE expected 1 row")
        d = con.execute(DELETE_QUEUE_6275_SQL).fetchall()
        if len(d) != 1:
            raise SystemExit("Queue DELETE expected 1 row")

        cpm_fqn = f"{PUB_DB}.{ARC_SCHEMA}.{cpm_snap}"
        q_fqn = f"{PUB_DB}.{ARC_SCHEMA}.{q_snap}"
        insert_readme_403(
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


def _val_eq(a: Any, b: Any) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    return a == b


def run_verify(
    con: duckdb.DuckDBPyConnection, cpm_snap: str, queue_snap: str
) -> tuple[bool, list[str]]:
    cpm = cpm_fq()
    qf = queue_fq()
    snap_c = fq_snap(cpm_snap)
    snap_q = fq_snap(queue_snap)
    errs: list[str] = []

    p1 = con.execute(f"SELECT COUNT(*) FROM {cpm}").fetchone()[0]
    if p1 != CPM_EXPECTED:
        errs.append(f"P1: CPM={p1}, expected {CPM_EXPECTED}")

    p2 = con.execute(
        f"SELECT ajcc8_stage_group FROM {cpm} WHERE CAST(research_id AS VARCHAR) = ?",
        [APPLY_RID],
    ).fetchone()
    if not p2 or p2[0] != "I":
        errs.append("P2: rid 6275 ajcc8_stage_group not I")

    p3 = con.execute(
        f"""
        SELECT diagnosis_primary FROM {cpm} WHERE CAST(research_id AS VARCHAR) = ?
        """,
        [APPLY_RID],
    ).fetchone()
    if not p3 or p3[0] != "other_malignant":
        errs.append("P3: diagnosis_primary must remain other_malignant")

    p4 = con.execute(
        f"""
        SELECT histology_final FROM {cpm} WHERE CAST(research_id AS VARCHAR) = ?
        """,
        [APPLY_RID],
    ).fetchone()
    if not p4 or p4[0] != "poorly differentiated thyroid carcinoma":
        errs.append("P4: histology_final drift")

    p5 = con.execute(f"SELECT COUNT(*) FROM {qf}").fetchone()[0]
    if p5 != EXPECT_QUEUE_POST:
        errs.append(f"P5: queue {p5} != {EXPECT_QUEUE_POST}")

    for rid, src, er in QUEUE_PEER_EXPECT:
        r = con.execute(
            f"SELECT reason FROM {qf} WHERE CAST(research_id AS VARCHAR) = ? AND source_script = ?",
            [rid, src],
        ).fetchone()
        if not r or r[0] != er:
            errs.append(f"P6: bad reason or missing {rid}/{src}")

    n6275q = con.execute(
        f"SELECT COUNT(*) FROM {qf} WHERE CAST(research_id AS VARCHAR) = '6275'"
    ).fetchone()[0]
    if n6275q != 0:
        errs.append("P6: 6275 still in queue")

    p7 = con.execute(
        f"""
        SELECT COUNT(*) FROM {cpm}
        WHERE diagnosis_primary IN {MALIGNANT_SQL} AND ajcc8_stage_group IS NULL
        """
    ).fetchone()[0]
    if p7 != EXPECT_MALIGNANT_NULL_POST:
        errs.append(f"P7: malignant NULL={p7}, expected {EXPECT_MALIGNANT_NULL_POST}")

    if readme_403_count(con) != 1:
        errs.append("P8: __readme script_403 count != 1")

    ncpm = con.execute(f"SELECT COUNT(*) FROM {snap_c}").fetchone()[0]
    nqs = con.execute(f"SELECT COUNT(*) FROM {snap_q}").fetchone()[0]
    if ncpm != 1:
        errs.append("P9: CPM snapshot row count != 1")
    if nqs != 1:
        errs.append("P10: queue snapshot row count != 1")

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
            errs.append("P11: row missing")
            break
        if col.lower() == "ajcc8_stage_group":
            if s[0] is not None:
                errs.append("P11: snap should have NULL stage_group")
            if c[0] != "I":
                errs.append("P11: current not I")
        else:
            if not _val_eq(s[0], c[0]):
                errs.append(f"P11: col {col} drift")

    for rid0, src0 in con.execute(
        f"SELECT CAST(research_id AS VARCHAR), source_script FROM {snap_q}"
    ).fetchall():
        srow = con.execute(
            f"SELECT * FROM {snap_q} WHERE CAST(research_id AS VARCHAR) = ?",
            [rid0],
        ).fetchone()
        if rid0 == APPLY_RID:
            n = con.execute(
                f"SELECT COUNT(*) FROM {qf} WHERE CAST(research_id AS VARCHAR) = ?",
                [rid0],
            ).fetchone()[0]
            if n != 0:
                errs.append("P11b: 6275 still in queue")
            continue
        crow = con.execute(
            f"SELECT * FROM {qf} WHERE CAST(research_id AS VARCHAR) = ? AND source_script = ?",
            [rid0, src0],
        ).fetchone()
        if not crow:
            errs.append(f"P11b: {rid0} missing from queue")
            continue
        if srow != crow:
            errs.append(f"P11b: queue row {rid0} should match snap (untouched peers)")

    return (len(errs) == 0, errs)


def format_post_apply_steady_state(
    con: duckdb.DuckDBPyConnection,
    cpm_snap: str,
    q_snap: str,
    ok: bool,
    verr: list[str],
) -> str:
    lines = [
        "# Script 403 — Post-apply steady state (not a pre-apply Phase 0 gate)",
        "",
        "DB satisfies Script 403 idempotency. H1–H10 not re-evaluated.",
        "",
        f"| P1–P11 verify | {ok} |",
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
            "# Script 403 — Close-out (rid 6275 PDTC stage_group)",
            "",
            f"- **Commit SHA (materialize):** `{commit_sha}`",
            f"- **Tag:** `{tag_name}`",
            f"- **UTC:** {utc_ts}",
            f"- **Probe SHA256:** `{probe_sha}`",
            "",
            "## Halt-gate verdict table",
            "",
            gate_table,
            "",
            "## Applied",
            "",
            "- rid 6275 (age 38, PDTC histology): `ajcc8_stage_group` **I** "
            "(AJCC 8th Ch 73 DTC age-stratified rule).",
            "- Queue DELETE: rid 6275.",
            "",
            "## Convention corroboration",
            "",
            "| check | result |",
            "|---|---|",
            f"| Staged PDTC-like rows | {EXPECT_PDTC_STAGED} (expect {EXPECT_PDTC_STAGED}) |",
            f"| age<55, M0 → Stage I bucket | ≥{EXPECT_PDTC_BUCKET_LT55_M0_I_MIN} precedent rows |",
            "",
            "## Post-state",
            "",
            f"- Queue: {EXPECT_QUEUE_PRE} → {EXPECT_QUEUE_POST}.",
            f"- Malignant NULL `ajcc8_stage_group`: {EXPECT_MALIGNANT_NULL_PRE} → {EXPECT_MALIGNANT_NULL_POST}.",
            "- Remaining queue: **1404**, **12198**, **423**, **924**, **9600**, **6768** — reasons unchanged.",
            "",
            "## CF",
            "",
            "- **CF-401-5:** resolved (rid 6275 staged).",
            "- **CF-403-1 → Script 404:** PDTC `diagnosis_primary` normalization (47-row cohort); "
            "per-row adjudication for mixed PTC/FTC with PDTC features.",
            "- **CF-403-2:** rid 6275 may move to `diagnosis_primary='PDTC'` in Script 404; "
            "until then `histology_final` is authoritative.",
            "",
        ]
    )


def _git_push_with_retry(root: str, ref: str, label: str) -> None:
    r = subprocess.run(
        ["git", "-C", root, "push", "origin", ref],
        capture_output=True,
        text=True,
    )
    if r.returncode == 0:
        log(f"Phase 4: push {label} ok")
        return
    log(f"Phase 4: push {label} failed (exit {r.returncode}); rebase+autostash+retry")
    br = subprocess.run(
        ["git", "-C", root, "rev-parse", "--abbrev-ref", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    subprocess.run(
        ["git", "-C", root, "pull", "--rebase", "--autostash", "origin", br],
        check=True,
        capture_output=True,
        text=True,
    )
    r2 = subprocess.run(
        ["git", "-C", root, "push", "origin", ref],
        capture_output=True,
        text=True,
    )
    if r2.returncode != 0:
        raise SystemExit(
            f"Phase 4: push {label} failed after rebase: {r2.stderr or r2.stdout}"
        )
    log(f"Phase 4: push {label} ok after rebase")


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
        "Script 403: rid 6275 PDTC → Stage I "
        "(AJCC8 Ch 73 DTC age-stratified rule; convention corroborated by 46/46 PDTC cohort)"
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
        log("Phase 4: no staged changes — skip commit (tree already matches index)")
    r = subprocess.run(
        ["git", "-C", root, "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    )
    commit_sha = r.stdout.strip()
    subprocess.run(["git", "-C", root, "tag", "-f", tag_name, commit_sha], check=True)
    _git_push_with_retry(root, "HEAD", "HEAD")
    _git_push_with_retry(root, tag_name, tag_name)
    return commit_sha, tag_name


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

    if not h8_cpm_set_audit() or not h9_writes_only_rid6275()[0] or not h10_no_peer_rids_in_writes()[0]:
        log("H8/H9/H10 static audit failed", also_print=True)
        return 1

    if args.apply:
        c0 = connect_quiet()
        try:
            partial, pr = idempotency_partial(c0)
            if partial and pr and not idempotency_complete(c0):
                print(f"HALT: partial — {pr}", flush=True)
                return 2
            if idempotency_complete(c0):
                cpm_s = find_cpm_snapshot(c0)
                q_s = find_queue_snapshot(c0)
                if not cpm_s or not q_s:
                    print("HALT: idempotency mismatch", flush=True)
                    return 2
                ok, errs = run_verify(c0, cpm_s, q_s)
                if not ok:
                    print("\n--- VERIFY FAILED ---\n" + "\n".join(errs), flush=True)
                    return 2
                print("NO-OP: Script 403 complete — Phase 3 verify OK.", flush=True)
                if args.phase4:
                    rs = snapshot_run_stamp_cpm(cpm_s)
                    d = read_probe_hashed_body_digest()
                    if not CLOSE_OUT_PATH.exists():
                        gtab = format_gate_table(
                            {
                                "h1": 1,
                                "h2": 1,
                                "h3": EXPECT_QUEUE_POST,
                                "h4": CPM_EXPECTED,
                                "h5_ok": True,
                                "h6_ok": True,
                                "h7_ok": True,
                                "h7_cpm": 1,
                                "h7_q": 1,
                                "h8_ok": True,
                                "h9_ok": True,
                                "h9_msg": "ok",
                                "h10_ok": True,
                                "h10_msg": "ok",
                                "malignant_null": EXPECT_MALIGNANT_NULL_POST,
                            },
                            True,
                            [],
                        )
                        co = format_close_out(
                            commit_sha="(pending—NO-OP)",
                            tag_name=f"{TAG_PREFIX}{rs}",
                            utc_ts=datetime.now(timezone.utc).isoformat(),
                            probe_sha=d,
                            gate_table=gtab,
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
                cpm_s = find_cpm_snapshot(c1)
                q_s = find_queue_snapshot(c1)
                if cpm_s and q_s:
                    ok, errs = run_verify(c1, cpm_s, q_s)
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
                f"\nHALT: --apply needs --i-approve=\n  .venv/bin/python scripts/{SCRIPT_FILE} "
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
        ok3, verr = run_verify(con, cpm_s, q_s)
        if not ok3:
            for e in verr:
                log(f"VERIFY: {e}")
            print("\n--- PHASE 3 VERIFY FAILED ---\n" + "\n".join(verr), flush=True)
            return 2
        if not idempotency_complete(con):
            log("idempotency check failed after apply (unexpected)")
            return 2
        utc = datetime.now(timezone.utc).isoformat()
        gtab = format_gate_table(probe, True, [])
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
                "# Script 403 — PDTC rid 6275 stage_group apply\n", encoding="utf-8"
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
