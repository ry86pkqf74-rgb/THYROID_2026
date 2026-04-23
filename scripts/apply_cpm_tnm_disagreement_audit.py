#!/usr/bin/env python3
"""Script 398 — CPM T/N/M primary↔v2 disagreement audit sidecar (read-only materialization).

Creates `manuscript_workspace.cpm_tnm_cross_source_disagreements_v1` (4256 rows) +
`main.__readme` (script_398). **Zero** writes to `main.canonical_patient_master`.

Phases
------
* --phase 0 (default) — probe + H1–H8; probe markdown + PROBE_REPORT_SHA256
  (hashed region excludes footer after ``---HASH-BOUNDARY---``).
* --apply — idempotency; else ``--i-approve`` must match probe hash; CREATE+INSERT+readme;
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
MAIN_SCHEMA = "main"
MS_SCHEMA = "manuscript_workspace"
CPM_TABLE = "canonical_patient_master"
README_TABLE = "__readme"
TARGET_TABLE = "cpm_tnm_cross_source_disagreements_v1"

SCRIPT_FILE = "apply_cpm_tnm_disagreement_audit.py"
SCRIPT_ID = "script_398"
SCRIPT_TAG = "script_398"

# Differentiated (DTC) — breakdown / close-out (317 + 1899 + 1689 = 3905; non-DTC 351)
DTC_DX: tuple[str, ...] = ("PTC", "FTC", "HCC", "DTC_NOS")
DTC_SQL = "(" + ",".join(f"'{d}'" for d in DTC_DX) + ")"

EXPECTED_T = 363
EXPECTED_N = 2055
EXPECTED_M = 1838
EXPECTED_TOTAL = 4_256
CPM_EXPECTED = 10_871

# Target column order (CREATE TABLE) — 12 data columns
TARGET_COLS: tuple[str, ...] = (
    "research_id",
    "diagnosis_primary",
    "age_at_surgery",
    "axis",
    "primary_value",
    "v2_value",
    "dominant_value",
    "disagreement_pattern",
    "current_stage_group",
    "stage_group_corrected",
    "path_stage_raw",
    "snapshot_ts",
)

# Pattern sanity (P7) — normalized LEAST↔GREATEST buckets
EXPECT_PATTERN_T3A_T3B = 135
EXPECT_PATTERN_N1A_NX = 1_423
EXPECT_PATTERN_N1A_N1B = 541
EXPECT_PATTERN_M0_M1 = 1_838

DOM_T = "dominant_tumor_ajcc8_t_stage"
DOM_N = "dominant_tumor_ajcc8_n_stage"
DOM_M = "dominant_tumor_ajcc8_m_stage"
DOMINANT_LIST = (DOM_T, DOM_N, DOM_M)

HASH_BOUNDARY = "---HASH-BOUNDARY---"

OUTPUT_DIR = REPO_ROOT / "scripts" / "output"
PROBE_PATH = OUTPUT_DIR / "apply_cpm_tnm_disagreement_audit_probe.md"
RUN_LOG_PATH = OUTPUT_DIR / "apply_cpm_tnm_disagreement_audit_run.log"
CLOSE_OUT_PATH = REPO_ROOT / "cursor_prompts" / "CLOSE_OUT_398.md"
PROMPT_PATH = REPO_ROOT / "cursor_prompts" / (
    "CURSOR_PROMPT_CPM_TNM_DISAGREEMENT_AUDIT_20260423_SCRIPT_398.md"
)
TAG_PREFIX = "v1_0-cpm-tnm-disagreement-audit-"

FORCE_ADD_PATTERNS: list[str] = [r"scripts/output/.*_run\.log$"]


def cpm_fq() -> str:
    return f'"{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"'


def readme_fq() -> str:
    return f'"{PUB_DB}"."{MAIN_SCHEMA}"."{README_TABLE}"'


def target_fq() -> str:
    return f'"{PUB_DB}"."{MS_SCHEMA}"."{TARGET_TABLE}"'


def dtc_predicate() -> str:
    return f"diagnosis_primary IN {DTC_SQL}"


def _should_force_add(path: Path) -> bool:
    try:
        s = str(path.relative_to(REPO_ROOT))
    except ValueError:
        s = str(path)
    return any(re.search(p, s) for p in FORCE_ADD_PATTERNS)


# --------------------------------------------------------------------------- #
# Apply SQL (for H5 static check — no CPM UPDATE)
# --------------------------------------------------------------------------- #

CREATE_TARGET_SQL = f"""
CREATE TABLE {target_fq()} (
  research_id           VARCHAR NOT NULL,
  diagnosis_primary     VARCHAR,
  age_at_surgery        BIGINT,
  axis                  VARCHAR NOT NULL,
  primary_value         VARCHAR NOT NULL,
  v2_value              VARCHAR NOT NULL,
  dominant_value        VARCHAR,
  disagreement_pattern  VARCHAR NOT NULL,
  current_stage_group   VARCHAR,
  stage_group_corrected  VARCHAR,
  path_stage_raw        VARCHAR,
  snapshot_ts           TIMESTAMP NOT NULL,
  PRIMARY KEY (research_id, axis)
);
""".strip()

INSERT_T_SQL = f"""
INSERT INTO {target_fq()}
SELECT
  CAST(research_id AS VARCHAR) AS research_id,
  diagnosis_primary,
  CAST(age_at_surgery AS BIGINT) AS age_at_surgery,
  'T' AS axis,
  ajcc8_t_stage AS primary_value,
  ajcc8_t_stage_v2 AS v2_value,
  {DOM_T} AS dominant_value,
  LEAST(ajcc8_t_stage, ajcc8_t_stage_v2) || '↔' || GREATEST(ajcc8_t_stage, ajcc8_t_stage_v2)
    AS disagreement_pattern,
  ajcc8_stage_group AS current_stage_group,
  ajcc8_stage_group_corrected AS stage_group_corrected,
  path_stage_raw,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS snapshot_ts
FROM {cpm_fq()}
WHERE ajcc8_t_stage IS NOT NULL AND ajcc8_t_stage_v2 IS NOT NULL
  AND ajcc8_t_stage <> ajcc8_t_stage_v2
""".strip()

INSERT_N_SQL = f"""
INSERT INTO {target_fq()}
SELECT
  CAST(research_id AS VARCHAR) AS research_id,
  diagnosis_primary,
  CAST(age_at_surgery AS BIGINT) AS age_at_surgery,
  'N' AS axis,
  ajcc8_n_stage AS primary_value,
  ajcc8_n_stage_v2 AS v2_value,
  {DOM_N} AS dominant_value,
  LEAST(ajcc8_n_stage, ajcc8_n_stage_v2) || '↔' || GREATEST(ajcc8_n_stage, ajcc8_n_stage_v2)
    AS disagreement_pattern,
  ajcc8_stage_group AS current_stage_group,
  ajcc8_stage_group_corrected AS stage_group_corrected,
  path_stage_raw,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS snapshot_ts
FROM {cpm_fq()}
WHERE ajcc8_n_stage IS NOT NULL AND ajcc8_n_stage_v2 IS NOT NULL
  AND ajcc8_n_stage <> ajcc8_n_stage_v2
""".strip()

INSERT_M_SQL = f"""
INSERT INTO {target_fq()}
SELECT
  CAST(research_id AS VARCHAR) AS research_id,
  diagnosis_primary,
  CAST(age_at_surgery AS BIGINT) AS age_at_surgery,
  'M' AS axis,
  ajcc8_m_stage AS primary_value,
  ajcc8_m_stage_v2 AS v2_value,
  {DOM_M} AS dominant_value,
  LEAST(ajcc8_m_stage, ajcc8_m_stage_v2) || '↔' || GREATEST(ajcc8_m_stage, ajcc8_m_stage_v2)
    AS disagreement_pattern,
  ajcc8_stage_group AS current_stage_group,
  ajcc8_stage_group_corrected AS stage_group_corrected,
  path_stage_raw,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS snapshot_ts
FROM {cpm_fq()}
WHERE ajcc8_m_stage IS NOT NULL AND ajcc8_m_stage_v2 IS NOT NULL
  AND ajcc8_m_stage <> ajcc8_m_stage_v2
""".strip()

ALL_APPLY_SQL = "\n\n".join(
    [CREATE_TARGET_SQL, INSERT_T_SQL, INSERT_N_SQL, INSERT_M_SQL]
)


def cpm_update_safe(sql: str) -> bool:
    return re.search(
        r"UPDATE\s+[\"']?[^\"';\n]*canonical_patient_master",
        sql,
        re.IGNORECASE,
    ) is None


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
# Connection
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


def cpm_column_names(con: duckdb.DuckDBPyConnection) -> set[str]:
    rows = con.execute(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
        """,
        [PUB_DB, MAIN_SCHEMA, CPM_TABLE],
    ).fetchall()
    return {r[0] for r in rows}


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


def target_columns(con: duckdb.DuckDBPyConnection) -> list[str] | None:
    if not table_exists(con, MS_SCHEMA, TARGET_TABLE):
        return None
    rows = con.execute(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
        ORDER BY ordinal_position
        """,
        [PUB_DB, MS_SCHEMA, TARGET_TABLE],
    ).fetchall()
    return [r[0] for r in rows]


def readme_398_count(con: duckdb.DuckDBPyConnection) -> int:
    cols = readme_columns(con)
    if not cols:
        return 0
    if "script" in cols:
        n = con.execute(
            f"SELECT COUNT(*) FROM {readme_fq()} WHERE script = ?",
            [SCRIPT_ID],
        ).fetchone()[0]
        if n:
            return n
    if "script_tag" in cols:
        n = con.execute(
            f"SELECT COUNT(*) FROM {readme_fq()} WHERE script_tag = ?",
            [SCRIPT_TAG],
        ).fetchone()[0]
        if n:
            return n
    return con.execute(
        f"""
        SELECT COUNT(*) FROM {readme_fq()}
        WHERE content LIKE 'Script 398:%' OR content LIKE ?
        """,
        [f"%{SCRIPT_ID}%"],
    ).fetchone()[0]


def audit_table_rowcount(con: duckdb.DuckDBPyConnection) -> int:
    if not table_exists(con, MS_SCHEMA, TARGET_TABLE):
        return -1
    return con.execute(f"SELECT COUNT(*) FROM {target_fq()}").fetchone()[0]


def idempotency_complete(con: duckdb.DuckDBPyConnection) -> bool:
    n = audit_table_rowcount(con)
    if n != EXPECTED_TOTAL:
        return False
    if readme_398_count(con) != 1:
        return False
    return True


def get_readme_398_run_stamp(con: duckdb.DuckDBPyConnection) -> str:
    """Return YYYYMMDD_HHMMSS from latest script_398 __readme row, else current UTC."""
    cols = readme_columns(con)
    if "run_timestamp" in cols and "script_tag" in cols:
        r = con.execute(
            f"""
            SELECT run_timestamp FROM {readme_fq()}
            WHERE script_tag = ? ORDER BY run_timestamp DESC NULLS LAST LIMIT 1
            """,
            [SCRIPT_TAG],
        ).fetchone()
        if r and r[0] is not None:
            if hasattr(r[0], "year"):
                dt = r[0]
            else:
                # assume string
                s = str(r[0])
                dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


# --------------------------------------------------------------------------- #
# Probe
# --------------------------------------------------------------------------- #


def t_nm_where(a: str) -> str:
    cpm = cpm_fq()
    if a == "T":
        return (
            f"ajcc8_t_stage IS NOT NULL AND ajcc8_t_stage_v2 IS NOT NULL "
            f"AND ajcc8_t_stage <> ajcc8_t_stage_v2"
        )
    if a == "N":
        return (
            f"ajcc8_n_stage IS NOT NULL AND ajcc8_n_stage_v2 IS NOT NULL "
            f"AND ajcc8_n_stage <> ajcc8_n_stage_v2"
        )
    if a == "M":
        return (
            f"ajcc8_m_stage IS NOT NULL AND ajcc8_m_stage_v2 IS NOT NULL "
            f"AND ajcc8_m_stage <> ajcc8_m_stage_v2"
        )
    raise ValueError(a)


def count_dup_risk(con: duckdb.DuckDBPyConnection) -> int:
    """0 means (research_id, axis) is unique in the union of T/N/M disagreements."""
    cpm = cpm_fq()
    q = f"""
    WITH u AS (
      SELECT research_id, 'T' AS axis FROM {cpm} WHERE {t_nm_where("T")}
      UNION ALL
      SELECT research_id, 'N' AS axis FROM {cpm} WHERE {t_nm_where("N")}
      UNION ALL
      SELECT research_id, 'M' AS axis FROM {cpm} WHERE {t_nm_where("M")}
    )
    SELECT COUNT(*) - COUNT(DISTINCT (CAST(research_id AS VARCHAR) || '·' || axis)) FROM u
    """
    return con.execute(q).fetchone()[0]


def dtc_non_counts(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    cpm = cpm_fq()
    out: dict[str, int] = {}
    for ax in ("T", "N", "M"):
        dtc = con.execute(
            f"""
            SELECT COUNT(*) FROM {cpm}
            WHERE {dtc_predicate()} AND ({t_nm_where(ax)})
            """
        ).fetchone()[0]
        non = con.execute(
            f"""
            SELECT COUNT(*) FROM {cpm}
            WHERE NOT ({dtc_predicate()}) AND ({t_nm_where(ax)})
            """
        ).fetchone()[0]
        out[f"dtc_{ax.lower()}"] = dtc
        out[f"non_{ax.lower()}"] = non
    return out


def run_probe(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    cpm = cpm_fq()
    cols = cpm_column_names(con)

    n_t = con.execute(
        f"SELECT COUNT(*) FROM {cpm} WHERE {t_nm_where('T')}"
    ).fetchone()[0]
    n_n = con.execute(
        f"SELECT COUNT(*) FROM {cpm} WHERE {t_nm_where('N')}"
    ).fetchone()[0]
    n_m = con.execute(
        f"SELECT COUNT(*) FROM {cpm} WHERE {t_nm_where('M')}"
    ).fetchone()[0]
    total = n_t + n_n + n_m

    h2 = con.execute(
        """
        SELECT 1 FROM information_schema.schemata
        WHERE catalog_name = ? AND schema_name = ?
        """,
        [PUB_DB, MS_SCHEMA],
    ).fetchone() is not None
    if not h2:
        alt = con.execute(
            "SELECT 1 FROM duckdb_schemas() WHERE database_name = ? AND schema_name = ?",
            [PUB_DB, MS_SCHEMA],
        ).fetchone()
        h2 = alt is not None

    h3_t_exists = table_exists(con, MS_SCHEMA, TARGET_TABLE)
    h3_n = audit_table_rowcount(con) if h3_t_exists else -1
    h3_idem = idempotency_complete(con)

    h4 = con.execute(f"SELECT COUNT(*) FROM {cpm}").fetchone()[0]

    h5 = cpm_update_safe(ALL_APPLY_SQL)

    ec = target_columns(con)
    h6_match = (ec is None) or (list(ec) == list(TARGET_COLS))

    h7_dup = count_dup_risk(con)

    h8_dom = all(c in cols for c in DOMINANT_LIST)

    dxb = dtc_non_counts(con)

    top_pat = {}
    cpm2 = cpm_fq()
    for ax, col1, col2 in (
        ("T", "ajcc8_t_stage", "ajcc8_t_stage_v2"),
        ("N", "ajcc8_n_stage", "ajcc8_n_stage_v2"),
        ("M", "ajcc8_m_stage", "ajcc8_m_stage_v2"),
    ):
        rows = con.execute(
            f"""
            SELECT
              LEAST({col1}, {col2}) || '↔' || GREATEST({col1}, {col2}) AS p,
              COUNT(*)::BIGINT
            FROM {cpm2}
            WHERE {col1} IS NOT NULL AND {col2} IS NOT NULL AND {col1} <> {col2}
            GROUP BY 1 ORDER BY 2 DESC LIMIT 3
            """
        ).fetchall()
        top_pat[ax] = [(str(r[0]), int(r[1])) for r in rows]

    return {
        "n_t": n_t,
        "n_n": n_n,
        "n_m": n_m,
        "total": total,
        "h2_ms": h2,
        "h3_exists": h3_t_exists,
        "h3_rows": h3_n,
        "h3_idem": h3_idem,
        "h4_cpm": h4,
        "h5_no_update": h5,
        "h6_match": h6_match,
        "h7_dup": h7_dup,
        "h8_dom": h8_dom,
        "top_pat": top_pat,
        "dtc_non": dxb,
        "existing_target_cols": ec,
    }


def halt_gate(p: dict[str, Any]) -> tuple[bool, list[str]]:
    errs: list[str] = []
    if p["n_t"] != EXPECTED_T:
        errs.append(f"H1: T disagreements={p['n_t']}, expected {EXPECTED_T}")
    if p["n_n"] != EXPECTED_N:
        errs.append(f"H1: N disagreements={p['n_n']}, expected {EXPECTED_N}")
    if p["n_m"] != EXPECTED_M:
        errs.append(f"H1: M disagreements={p['n_m']}, expected {EXPECTED_M}")
    if p["total"] != EXPECTED_TOTAL:
        errs.append(f"H1: T+N+M={p['total']}, expected {EXPECTED_TOTAL}")

    if not p["h2_ms"]:
        errs.append("H2: manuscript_workspace schema missing")

    # H3: absent, or complete idempotency (4256 + __readme script_398)
    if p["h3_exists"]:
        if p["h3_idem"] and p["h3_rows"] == EXPECTED_TOTAL:
            pass
        else:
            errs.append(
                f"H3: target table in bad state (rows={p['h3_rows']}, idem={p['h3_idem']}); "
                "drop table manually to rebuild, or fix partial apply"
            )

    if p["h4_cpm"] != CPM_EXPECTED:
        errs.append(f"H4: CPM count={p['h4_cpm']}, expected {CPM_EXPECTED}")
    if not p["h5_no_update"]:
        errs.append("H5: generated apply SQL must not UPDATE main.canonical_patient_master")
    if not p.get("h6_match", True):
        errs.append(
            f"H6: column list != spec (existing: {p.get('existing_target_cols')!r})"
        )
    if p["h7_dup"] != 0:
        errs.append(f"H7: duplicate (research_id,axis) in union, delta={p['h7_dup']}")
    if not p["h8_dom"]:
        errs.append("H8: missing dominant_tumor_ajcc8_{t,n,m}_stage on CPM")

    return (len(errs) == 0, errs)


def format_probe_hashed_body(p: dict[str, Any], gate_ok: bool, gate_errs: list[str]) -> str:
    lines: list[str] = [
        "# Script 398 — Phase 0 probe (CPM T/N/M cross-source disagreement audit)",
        "",
        "## Halt gates (H1–H8)",
        "",
        f"| all_pass | {gate_ok} |",
        "",
        "## Counts (must match for apply)",
        "",
        f"- **T disagreements:** {p['n_t']} (expected {EXPECTED_T})",
        f"- **N disagreements:** {p['n_n']} (expected {EXPECTED_N})",
        f"- **M disagreements:** {p['n_m']} (expected {EXPECTED_M})",
        f"- **Total:** {p['total']} (expected {EXPECTED_TOTAL})",
        f"- **CPM total (H4):** {p['h4_cpm']}",
        f"- **manuscript_workspace (H2):** {'present' if p['h2_ms'] else 'MISSING'}",
        f"- **Target table present:** {p['h3_exists']}  rows={p['h3_rows']}  idem={p['h3_idem']}",
        f"- **H5 (no CPM UPDATE in apply SQL):** {p['h5_no_update']}",
        f"- **H6 (column list vs spec if table exists):** {p.get('h6_match', True)}",
        f"- **H7 duplicate risk (0 required):** {p['h7_dup']}",
        f"- **H8 dominant columns:** {p['h8_dom']}",
        "",
    ]
    if p["dtc_non"]:
        lines.append("## DTC vs non-DTC (PTC,FTC,HCC,DTC_NOS) — per-axis disags")
        lines.append("")
        for k in ("dtc_t", "non_t", "dtc_n", "non_n", "dtc_m", "non_m"):
            lines.append(f"- **{k}:** {p['dtc_non'].get(k, '—')}")
        lines.append("")

    lines.append("## Top disagreement patterns (current CPM) — T / N / M")
    lines.append("")
    for ax, rows in p["top_pat"].items():
        lines.append(f"### Axis {ax}")
        for pat, c in rows:
            lines.append(f"- `{pat}`: {c}")
        lines.append("")

    if gate_errs:
        lines.extend(["### Halts", ""] + [f"- {e}" for e in gate_errs] + [""])

    lines.extend(
        [
            "## Planned materialization (read-only; no CPM writes)",
            "",
            "```sql",
            CREATE_TARGET_SQL,
            "",
            "-- INSERTs T, N, M (see script constants INSERT_T_SQL / N / M)",
            "```",
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


# --------------------------------------------------------------------------- #
# Apply
# --------------------------------------------------------------------------- #


def insert_readme_398(
    con: duckdb.DuckDBPyConnection,
    *,
    probe_sha: str,
    run_stamp: str,
    top_summary: str,
) -> None:
    cols = readme_columns(con)
    body = (
        f"Script 398: CPM T/N/M cross-source disagreement sidecar. Table "
        f"{PUB_DB}.{MS_SCHEMA}.{TARGET_TABLE}; rows={EXPECTED_TOTAL} (T={EXPECTED_T} N={EXPECTED_N} "
        f"M={EXPECTED_M}). {top_summary} DTC/FTC-style breakdown in script close-out. "
        f"Probe SHA256: {probe_sha}. run_stamp={run_stamp}. "
        f"Zero UPDATE to main.{CPM_TABLE}."
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
    insert_cols = ["content", "updated_at"]
    value_exprs = ["?", "CAST(CURRENT_TIMESTAMP AS TIMESTAMP)"]
    params: list[Any] = [f"Script 398: {SCRIPT_ID} {body}"]
    if "script" in cols:
        insert_cols.append("script")
        value_exprs.append("?")
        params.append(SCRIPT_ID)
    con.execute(
        f"INSERT INTO {readme_fq()} ({', '.join(insert_cols)}) VALUES ({', '.join(value_exprs)})",
        params,
    )


def apply_transaction(
    con: duckdb.DuckDBPyConnection, probe_sha: str, run_stamp: str, top_summary: str
) -> None:
    if table_exists(con, MS_SCHEMA, TARGET_TABLE):
        raise SystemExit("Apply: target table must not exist (use NO-OP if already materialized)")

    if not cpm_update_safe(ALL_APPLY_SQL):
        raise SystemExit("H5: abort — unsafe SQL")

    con.execute("BEGIN TRANSACTION")
    try:
        for stmt in (CREATE_TARGET_SQL, INSERT_T_SQL, INSERT_N_SQL, INSERT_M_SQL):
            con.execute(stmt)
        n = con.execute(f"SELECT COUNT(*) FROM {target_fq()}").fetchone()[0]
        if n != EXPECTED_TOTAL:
            raise SystemExit(f"Apply: rowcount={n}, expected {EXPECTED_TOTAL}")
        insert_readme_398(
            con, probe_sha=probe_sha, run_stamp=run_stamp, top_summary=top_summary
        )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise


# --------------------------------------------------------------------------- #
# Verify
# --------------------------------------------------------------------------- #


def run_verify(
    con: duckdb.DuckDBPyConnection, probe: dict[str, Any]
) -> tuple[bool, list[str], dict[str, Any]]:
    errs: list[str] = []
    cpm = cpm_fq()
    info: dict[str, Any] = {}

    p1 = con.execute(f"SELECT COUNT(*) FROM {cpm}").fetchone()[0]
    if p1 != CPM_EXPECTED:
        errs.append(f"P1: CPM={p1}, expected {CPM_EXPECTED}")

    if not table_exists(con, MS_SCHEMA, TARGET_TABLE):
        errs.append("P2: target table missing")
        return (False, errs, info)

    rpk = con.execute(
        """
        SELECT constraint_name FROM information_schema.table_constraints
        WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
          AND constraint_type = 'PRIMARY KEY'
        """,
        [PUB_DB, MS_SCHEMA, TARGET_TABLE],
    ).fetchone()
    if not rpk:
        errs.append("P2: PRIMARY KEY missing on target (information_schema)")

    nrows = con.execute(f"SELECT COUNT(*) FROM {target_fq()}").fetchone()[0]
    if nrows != EXPECTED_TOTAL:
        errs.append(f"P3: target rows={nrows}, expected {EXPECTED_TOTAL}")

    for ax, exp in (("T", EXPECTED_T), ("N", EXPECTED_N), ("M", EXPECTED_M)):
        a = con.execute(
            f"SELECT COUNT(*) FROM {target_fq()} WHERE axis = ?",
            [ax],
        ).fetchone()[0]
        if a != exp:
            errs.append(f"P4: axis {ax} = {a}, expected {exp}")

    n_rd = readme_398_count(con)
    if n_rd != 1:
        errs.append(f"P5: __readme {SCRIPT_TAG} rows={n_rd}, expected 1")

    p6 = con.execute(
        f"""
        SELECT COUNT(*) FROM {cpm}
        WHERE ajcc8_t_stage IS NOT NULL AND ajcc8_t_stage_v2 IS NOT NULL
          AND ajcc8_t_stage <> ajcc8_t_stage_v2
        """
    ).fetchone()[0]
    if p6 != EXPECTED_T:
        errs.append(
            f"P6: T disag = {p6}, expected {EXPECTED_T} (no silent CPM mutation)"
        )

    t_t3a_t3b = con.execute(
        f"""
        SELECT COUNT(*) FROM {target_fq()}
        WHERE axis = 'T' AND disagreement_pattern = 'T3a↔T3b'
        """
    ).fetchone()[0]
    if t_t3a_t3b != EXPECT_PATTERN_T3A_T3B:
        errs.append(
            f"P7: T3a↔T3b = {t_t3a_t3b}, expected {EXPECT_PATTERN_T3A_T3B}"
        )
    t_n1a_nx = con.execute(
        f"""
        SELECT COUNT(*) FROM {target_fq()}
        WHERE axis = 'N' AND disagreement_pattern = 'N1a↔Nx'
        """
    ).fetchone()[0]
    if t_n1a_nx != EXPECT_PATTERN_N1A_NX:
        errs.append(
            f"P7: N1a↔Nx = {t_n1a_nx}, expected {EXPECT_PATTERN_N1A_NX}"
        )
    t_n1a_n1b = con.execute(
        f"""
        SELECT COUNT(*) FROM {target_fq()}
        WHERE axis = 'N' AND disagreement_pattern = 'N1a↔N1b'
        """
    ).fetchone()[0]
    if t_n1a_n1b != EXPECT_PATTERN_N1A_N1B:
        errs.append(
            f"P7: N1a↔N1b = {t_n1a_n1b}, expected {EXPECT_PATTERN_N1A_N1B}"
        )
    t_m0_m1 = con.execute(
        f"""
        SELECT COUNT(*) FROM {target_fq()}
        WHERE axis = 'M' AND disagreement_pattern = 'M0↔M1'
        """
    ).fetchone()[0]
    if t_m0_m1 != EXPECT_PATTERN_M0_M1:
        errs.append(
            f"P7: M0↔M1 = {t_m0_m1}, expected {EXPECT_PATTERN_M0_M1}"
        )

    info = {"nrows": nrows, "p6_t_disag": p6}
    return (len(errs) == 0, errs, info)


def _top_pattern_summary(p: dict[str, Any]) -> str:
    parts: list[str] = []
    for ax, rows in p.get("top_pat", {}).items():
        top3 = ", ".join(f"{pat}={c}" for pat, c in rows[:3])
        parts.append(f"{ax}: {top3}")
    return " | ".join(parts)


def format_close_out(
    *,
    commit_sha: str,
    tag_name: str,
    utc_ts: str,
    probe_sha: str,
    p: dict[str, Any],
) -> str:
    dx = p.get("dtc_non", {})
    dtc_t = dx.get("dtc_t", "—")
    dtc_n = dx.get("dtc_n", "—")
    dtc_m = dx.get("dtc_m", "—")
    nont = dx.get("non_t", "—")
    nonn = dx.get("non_n", "—")
    nonm = dx.get("non_m", "—")
    def _tp(ax: str) -> str:
        rows = p.get("top_pat", {}).get(ax, [])
        return ", ".join(f"{a} ({b})" for a, b in rows[:3]) if rows else "—"
    lines = [
        "# Script 398 — Close-out",
        "",
        f"- **Git commit / SHA:** `{commit_sha}`",
        f"- **Tag:** `{tag_name}`",
        f"- **UTC timestamp:** {utc_ts}",
        f"- **Probe SHA256 (consumed):** `{probe_sha}`",
        f"- **Target table FQN:** `{PUB_DB}.{MS_SCHEMA}.{TARGET_TABLE}`",
        "",
        "## Halt-gate verdicts (Phase 0) — H1–H8",
        "",
        "| gate | result |",
        "|---|---|",
        "| H1 | Target T/N/M counts 363/2055/1838, total 4256 |",
        "| H2 | `manuscript_workspace` exists |",
        "| H3 | Target table absent, or 4256 rows + idempotent `__readme` |",
        "| H4 | CPM = 10,871 |",
        "| H5 | No `UPDATE` to `main.canonical_patient_master` in apply SQL |",
        "| H6 | Sidecar column list matches `TARGET_COLS` |",
        "| H7 | No duplicate (research_id, axis) in union |",
        "| H8 | `dominant_tumor_ajcc8_{t,n,m}_stage` present on CPM |",
        "",
        "## Materialized write",
        "",
        f"- **Rows:** {EXPECTED_TOTAL} (T={EXPECTED_T} + N={EXPECTED_N} + M={EXPECTED_M})",
        "- **CPM primary columns updated:** 0 (read-only to CPM)",
        "",
        f"- **DTC (PTC,FTC,HCC,DTC_NOS) per-axis (diagnosis breakdown):** T={dtc_t} N={dtc_n} M={dtc_m}",
        f"- **Non-DTC per axis:** T={nont} N={nonn} M={nonm}",
        "",
        "## Top-3 patterns (current snapshot)",
        "",
        f"- T: {_tp('T')}",
        f"- N: {_tp('N')}",
        f"- M: {_tp('M')}",
        "",
        "## Zero CPM write confirmation (H5 + P1 + P6)",
        "",
        "- P1: CPM row count unchanged; P6: T-disag pre/post query still 363.",
        "",
        "## CF-398 follow-ups",
        "",
        "- **CF-398-1:** Per-pattern triage (v2- vs primary-wins).",
        "- **CF-398-2:** Re-derive `ajcc8_stage_group` under v2 for audit rows; flag restage.",
        "- **CF-398-3:** v2 column provenance (phase 4.6, builder design).",
        "- **CF-398-4:** Script 399 — surgical `UPDATE` after adjudication.",
        "",
    ]
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
        "Script 398: CPM T/N/M disagreement audit sidecar "
        f"(4256 rows materialized, read-only)"
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
        help="SHA256 of hashed probe body (required for first apply)",
    )
    ap.add_argument(
        "--force-stamp",
        default="",
        help="Override RUN_STAMP (UTC YYYYMMDD_HHMMSS) for tag",
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
            if idempotency_complete(c0):
                line = (
                    "NO-OP: audit table 4256 rows + one script_398 __readme — "
                    "Phase 3 verify only."
                )
                print(line, flush=True)
                OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
                with open(RUN_LOG_PATH, "a", encoding="utf-8") as f:
                    f.write(f"{datetime.now(timezone.utc).isoformat()} {line}\n")
                pr = run_probe(c0)
                ok, errs, _v = run_verify(c0, pr)
                if not ok:
                    print("\n--- PHASE 3 VERIFY FAILED ---\n" + "\n".join(errs), flush=True)
                    return 2
                print("Phase 3 verify OK (NO-OP).", flush=True)
                if args.phase4:
                    rs = get_readme_398_run_stamp(c0)
                    log(f"Phase 4: NO-OP path — run_stamp from readme {rs}")
                    _, tag_n = git_commit_and_tag(rs)
                    csha = subprocess.run(
                        ["git", "-C", str(REPO_ROOT), "rev-parse", "HEAD"],
                        check=True,
                        capture_output=True,
                        text=True,
                    ).stdout.strip()
                    log(f"Phase 4: commit_sha={csha} tag={tag_n}")
                    print(f"commit_sha={csha} tag={tag_n}", flush=True)
                return 0

            if table_exists(c0, MS_SCHEMA, TARGET_TABLE):
                n = audit_table_rowcount(c0)
                if n != EXPECTED_TOTAL:
                    log(f"DRIFT: audit table row count {n} != {EXPECTED_TOTAL}")
                    print(
                        f"exit 4: audit table drifted (rows={n}); manual DROP+investigate",
                        flush=True,
                    )
                    return 4
                if readme_398_count(c0) != 1:
                    log("HALT: 4256 rows in sidecar but not exactly one __readme (script_398)")
                    print(
                        "exit 2: partial or corrupt (4256 rows without single readme); "
                        "fix __readme or DROP+re-apply",
                        flush=True,
                    )
                    return 2

            pre = run_probe(c0)
            p_ok, p_errs = halt_gate(pre)
            if not p_ok:
                for e in p_errs:
                    print(f"Pre-apply HALT: {e}", flush=True)
                return 1
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
            line = "Apply path: idempotency now complete — use NO-OP re-run of --apply."
            log(line)
            print(line, flush=True)
            return 0

        if table_exists(con, MS_SCHEMA, TARGET_TABLE):
            log("HALT: target still exists; non-idempotent state")
            return 2

        run_stamp = args.force_stamp.strip() or datetime.now(timezone.utc).strftime(
            "%Y%m%d_%H%M%S"
        )
        top = _top_pattern_summary(probe)
        apply_transaction(con, digest, run_stamp, top)

        ok3, verr, _ = run_verify(con, probe)
        if not ok3:
            for e in verr:
                log(f"VERIFY: {e}")
            print("\n--- PHASE 3 VERIFY FAILED ---\n" + "\n".join(verr), flush=True)
            return 2

        utc = datetime.now(timezone.utc).isoformat()
        cbody = format_close_out(
            commit_sha="(pending; run after Phase 4)",
            tag_name=f"{TAG_PREFIX}{run_stamp}",
            utc_ts=utc,
            probe_sha=digest,
            p=probe,
        )
        CLOSE_OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        PROMPT_PATH.parent.mkdir(parents=True, exist_ok=True)
        if not PROMPT_PATH.exists():
            PROMPT_PATH.write_text(
                "# Script 398 — CPM T/N/M disagreement audit\n\n"
                "See `scripts/apply_cpm_tnm_disagreement_audit.py` + project chat.\n",
                encoding="utf-8",
            )
        CLOSE_OUT_PATH.write_text(cbody, encoding="utf-8")
        log(f"Wrote {CLOSE_OUT_PATH}")

        csha = "(not committed)"
        tag_n = "—"
        if args.phase4:
            csha, tag_n = git_commit_and_tag(run_stamp)
            cbody2 = format_close_out(
                commit_sha=csha,
                tag_name=tag_n,
                utc_ts=utc,
                probe_sha=digest,
                p=probe,
            )
            CLOSE_OUT_PATH.write_text(cbody2, encoding="utf-8")

        log("Script 398 apply + verify complete.")
        print(f"commit_sha={csha} tag={tag_n}", flush=True)
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
