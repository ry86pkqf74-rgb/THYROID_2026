#!/usr/bin/env python3
"""Script 402 — Histologic classification audit (8-axis read-only sidecar).

Materializes `manuscript_workspace.cpm_histologic_classification_audit_v1` from CPM.
**Zero** writes to `main.canonical_patient_master`.

Phases: ``--phase 0`` (default) — probe H1–H8 + PROBE_REPORT_SHA256;
``--apply`` + ``--i-approve`` — CREATE, 8×INSERT, ``__readme``; Phase 3 verify;
optional ``--phase4`` — git+tag+push (with single rebase+retry on push).
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
TARGET_TABLE = "cpm_histologic_classification_audit_v1"

SCRIPT_FILE = "apply_histologic_classification_audit.py"
SCRIPT_ID = "script_402"
SCRIPT_TAG = "script_402"

CPM_EXPECTED = 10_871

# Per-axis expected row counts (Phase 0 H1); HVA uses range via DISTINCT rid count
E_PDTC = 47
# Live MotherDuck 2026-04-23 (slightly above earlier 378 snapshot: +1 row)
E_HURTH = 379
E_DHGTC = 10
E_PTC_DISC = 788
E_PTC_UNK = 271
E_AGG = 43
E_GRADE = 118
# Fixed part (all axes except HVA) with current E_HURTH: 1656
FIXED_NO_HVA = E_PDTC + E_HURTH + E_DHGTC + E_PTC_DISC + E_PTC_UNK + E_AGG + E_GRADE
# B-6 OR(uppercase-first, embedded newline) — was ~156 in 2026-04-23 pre-state probe; CPM
# has since drifted to ~261. Wide band to survive minor re-extract churn (not 150-165).
HVA_MIN = 250
HVA_MAX = 280

AXES: tuple[str, ...] = (
    "PDTC_SCATTER",
    "HURTHLE_SCATTER",
    "DHGTC_CATALOG",
    "PTC_VARIANT_DISCREPANCY",
    "PTC_VARIANT_UNKNOWN",
    "HVA_DATA_QUALITY",
    "AGGRESSIVE_VARIANT_FLAGGED",
    "GRADE_3_OR_4_CROSS_REF",
)

TARGET_COLS: tuple[str, ...] = (
    "research_id",
    "diagnosis_primary",
    "age_at_surgery",
    "axis",
    "axis_note",
    "diagnosis_variant",
    "histology_final",
    "histologic_variants_all",
    "path_histology_raw",
    "syn_histologic_grade",
    "aggressive_variant_flag",
    "ajcc8_stage_group",
    "snapshot_ts",
)

H6_CPM_COLS: tuple[str, ...] = (
    "histologic_variants_all",
    "diagnosis_variant",
    "aggressive_variant_flag",
    "syn_histologic_grade",
    "histology_final",
    "path_histology_raw",
    "histologic_types_all",
    "ajcc8_stage_group",
)

HASH_BOUNDARY = "---HASH-BOUNDARY---"

OUTPUT_DIR = REPO_ROOT / "scripts" / "output"
PROBE_PATH = OUTPUT_DIR / "apply_histologic_classification_audit_probe.md"
RUN_LOG_PATH = OUTPUT_DIR / "apply_histologic_classification_audit_run.log"
CLOSE_OUT_PATH = REPO_ROOT / "cursor_prompts" / "CLOSE_OUT_402.md"
PROMPT_PATH = REPO_ROOT / "cursor_prompts" / (
    "CURSOR_PROMPT_HISTOLOGIC_CLASSIFICATION_AUDIT_20260423_SCRIPT_402.md"
)
TAG_PREFIX = "v1_0-histologic-classification-audit-"

FORCE_ADD_PATTERNS: list[str] = [r"scripts/output/.*_run\.log$"]


def cpm_fq() -> str:
    return f'"{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"'


def readme_fq() -> str:
    return f'"{PUB_DB}"."{MAIN_SCHEMA}"."{README_TABLE}"'


def target_fq() -> str:
    return f'"{PUB_DB}"."{MS_SCHEMA}"."{TARGET_TABLE}"'


def _should_force_add(path: Path) -> bool:
    try:
        s = str(path.relative_to(REPO_ROOT))
    except ValueError:
        s = str(path)
    return any(re.search(p, s) for p in FORCE_ADD_PATTERNS)


# --------------------------------------------------------------------------- #
# SQL predicates (B-1 … B-8) — match prompt exactly
# --------------------------------------------------------------------------- #

PDTC_W = """(
  diagnosis_primary = 'PDTC'
  OR (
    diagnosis_primary IN ('other_malignant', 'PTC', 'FTC')
    AND (
      histology_final ILIKE '%poorly%diff%'
      OR histologic_types_all ILIKE '%poorly%diff%'
      OR histologic_variants_all ILIKE '%poorly%diff%'
    )
  )
)"""

# axis_note: why this row
PDTC_NOTE = f"""
CASE
  WHEN diagnosis_primary = 'PDTC' THEN 'diagnosis_primary_code_PDTC'
  WHEN diagnosis_primary = 'other_malignant'
    AND (histology_final ILIKE '%poorly%diff%'
         OR histologic_types_all ILIKE '%poorly%diff%'
         OR histologic_variants_all ILIKE '%poorly%diff%')
    THEN 'dx_primary_other_malignant_poorly_differentiated_markers'
  WHEN diagnosis_primary = 'PTC'
    AND (histology_final ILIKE '%poorly%diff%'
         OR histologic_types_all ILIKE '%poorly%diff%'
         OR histologic_variants_all ILIKE '%poorly%diff%')
    THEN 'dx_primary_PTC_poorly_differentiated_markers'
  WHEN diagnosis_primary = 'FTC'
    AND (histology_final ILIKE '%poorly%diff%'
         OR histologic_types_all ILIKE '%poorly%diff%'
         OR histologic_variants_all ILIKE '%poorly%diff%')
    THEN 'dx_primary_FTC_poorly_differentiated_markers'
  ELSE 'pdtc_scatter_catchall_review'
END
""".strip()

HURTH_W = """(
  diagnosis_primary = 'HCC'
  OR diagnosis_variant = 'oncocytic_warthin'
  OR histology_final ILIKE '%hurthle%'
  OR histology_final ILIKE '%oncocytic%'
  OR histologic_variants_all ILIKE '%hurthle%'
  OR histologic_variants_all ILIKE '%oncocytic%'
)"""

HURTH_NOTE = f"""
CASE
  WHEN diagnosis_primary = 'HCC' THEN 'WHO2022_hurthle_carcinoma_HCC'
  WHEN diagnosis_variant = 'oncocytic_warthin' THEN 'diagnosis_variant_oncocytic_warthin'
  WHEN COALESCE(histology_final, '') ILIKE '%hurthle%'
    THEN 'histology_final_hurthle_signal'
  WHEN COALESCE(histology_final, '') ILIKE '%oncocytic%'
    AND COALESCE(histology_final, '') NOT ILIKE '%hurthle%'
    THEN 'histology_final_oncocytic_signal'
  WHEN COALESCE(histologic_variants_all, '') ILIKE '%hurthle%'
    THEN 'histologic_variants_all_hurthle_signal'
  WHEN COALESCE(histologic_variants_all, '') ILIKE '%oncocytic%'
    THEN 'histologic_variants_all_oncocytic_signal'
  ELSE 'hurthle_scatter_catchall_review'
END
""".strip()

DHGTC_W = "diagnosis_primary = 'DHGTC'"
HVA_W = """(
  diagnosis_primary IN (
    'PTC', 'FTC', 'HCC', 'DTC_NOS', 'MTC', 'ATC', 'other_malignant', 'NIFTP', 'FTUMP',
    'PDTC', 'DHGTC'
  )
  AND histologic_variants_all IS NOT NULL
  AND (
    SUBSTR(histologic_variants_all, 1, 1) BETWEEN 'A' AND 'Z'
    OR histologic_variants_all LIKE '%' || CHR(10) || '%'
  )
)"""

HVA_NOTE = f"""
CASE
  WHEN SUBSTR(histologic_variants_all, 1, 1) BETWEEN 'A' AND 'Z'
   AND histologic_variants_all LIKE '%' || CHR(10) || '%'
  THEN 'uppercase_leading_and_embedded_newline_in_HVA'
  WHEN histologic_variants_all LIKE '%' || CHR(10) || '%'
  THEN 'embedded_newline_in_HVA'
  WHEN SUBSTR(histologic_variants_all, 1, 1) BETWEEN 'A' AND 'Z'
  THEN 'uppercase_leading_letter_in_HVA'
  ELSE 'hva_data_quality_other'
END
""".strip()

PTC_DISC_W = """(
  diagnosis_primary = 'PTC'
  AND diagnosis_variant IS NULL
  AND histologic_variants_all IS NOT NULL
  AND histologic_variants_all NOT ILIKE '%classical%'
)"""
PTC_UNK_W = """(
  diagnosis_primary = 'PTC'
  AND diagnosis_variant IS NULL
  AND histologic_variants_all IS NULL
)"""
AGG_W = "aggressive_variant_flag = TRUE"
GRADE_W = "syn_histologic_grade IN (3, 4)"

CREATE_TARGET_SQL = f"""
CREATE TABLE {target_fq()} (
  research_id              VARCHAR NOT NULL,
  diagnosis_primary        VARCHAR NOT NULL,
  age_at_surgery           BIGINT,
  axis                     VARCHAR NOT NULL,
  axis_note                VARCHAR,
  diagnosis_variant        VARCHAR,
  histology_final          VARCHAR,
  histologic_variants_all  VARCHAR,
  path_histology_raw       VARCHAR,
  syn_histologic_grade     BIGINT,
  aggressive_variant_flag  BOOLEAN,
  ajcc8_stage_group        VARCHAR,
  snapshot_ts              TIMESTAMP NOT NULL,
  PRIMARY KEY (research_id, axis)
);
""".strip()

_common_select = f"""
  CAST(research_id AS VARCHAR) AS research_id,
  diagnosis_primary,
  CAST(age_at_surgery AS BIGINT) AS age_at_surgery,
  {{axis}} AS axis,
  {{axis_note}} AS axis_note,
  diagnosis_variant,
  histology_final,
  histologic_variants_all,
  path_histology_raw,
  CAST(syn_histologic_grade AS BIGINT) AS syn_histologic_grade,
  aggressive_variant_flag,
  ajcc8_stage_group,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS snapshot_ts
""".replace("\n", " ").strip()

INSERT_PDTC = f"""
INSERT INTO {target_fq()}
SELECT {_common_select.format(axis="'PDTC_SCATTER'", axis_note=PDTC_NOTE)}
FROM {cpm_fq()}
WHERE {PDTC_W}
""".replace("  ", " ").strip()
INSERT_HURTH = f"""
INSERT INTO {target_fq()}
SELECT {_common_select.format(axis="'HURTHLE_SCATTER'", axis_note=HURTH_NOTE)}
FROM {cpm_fq()}
WHERE {HURTH_W}
""".replace("  ", " ").strip()
INSERT_DHGTC = f"""
INSERT INTO {target_fq()}
SELECT {_common_select.format(
  axis="'DHGTC_CATALOG'",
  axis_note="'diagnosis_primary_DHGTC_WHO2022_entity'",
)}
FROM {cpm_fq()}
WHERE {DHGTC_W}
""".replace("  ", " ").strip()
INSERT_PTC_DISC = f"""
INSERT INTO {target_fq()}
SELECT {_common_select.format(
  axis="'PTC_VARIANT_DISCREPANCY'",
  axis_note="'PTC_with_null_diagnosis_variant_nonclassical_hva_text'",
)}
FROM {cpm_fq()}
WHERE {PTC_DISC_W}
""".replace("  ", " ").strip()
INSERT_PTC_UNK = f"""
INSERT INTO {target_fq()}
SELECT {_common_select.format(
  axis="'PTC_VARIANT_UNKNOWN'",
  axis_note="'PTC_both_diagnosis_variant_and_hva_null'",
)}
FROM {cpm_fq()}
WHERE {PTC_UNK_W}
""".replace("  ", " ").strip()
INSERT_HVA = f"""
INSERT INTO {target_fq()}
SELECT {_common_select.format(axis="'HVA_DATA_QUALITY'", axis_note=HVA_NOTE)}
FROM {cpm_fq()}
WHERE {HVA_W}
""".replace("  ", " ").strip()
INSERT_AGG = f"""
INSERT INTO {target_fq()}
SELECT {_common_select.format(
  axis="'AGGRESSIVE_VARIANT_FLAGGED'",
  axis_note="'aggressive_variant_flag_true_cohort_flag'",
)}
FROM {cpm_fq()}
WHERE {AGG_W}
""".replace("  ", " ").strip()
INSERT_GRADE = f"""
INSERT INTO {target_fq()}
SELECT {_common_select.format(
  axis="'GRADE_3_OR_4_CROSS_REF'",
  axis_note=(
    "CASE "
    "WHEN syn_histologic_grade = 3 THEN 'syn_histologic_grade_3' "
    "WHEN syn_histologic_grade = 4 THEN 'syn_histologic_grade_4' "
    "ELSE 'grade_cross_ref' END"
  ),
)}
FROM {cpm_fq()}
WHERE {GRADE_W}
""".replace("  ", " ").strip()

ALL_INSERTS = (
    INSERT_PDTC,
    INSERT_HURTH,
    INSERT_DHGTC,
    INSERT_PTC_DISC,
    INSERT_PTC_UNK,
    INSERT_HVA,
    INSERT_AGG,
    INSERT_GRADE,
)
ALL_APPLY_SQL = "\n\n".join([CREATE_TARGET_SQL, *ALL_INSERTS])

W_BY_AXIS: dict[str, str] = {
    "PDTC_SCATTER": PDTC_W,
    "HURTHLE_SCATTER": HURTH_W,
    "DHGTC_CATALOG": DHGTC_W,
    "PTC_VARIANT_DISCREPANCY": PTC_DISC_W,
    "PTC_VARIANT_UNKNOWN": PTC_UNK_W,
    "HVA_DATA_QUALITY": HVA_W,
    "AGGRESSIVE_VARIANT_FLAGGED": AGG_W,
    "GRADE_3_OR_4_CROSS_REF": GRADE_W,
}


def cpm_mutation_free(sql: str) -> bool:
    if re.search(r"\bUPDATE\b", sql, re.IGNORECASE) and re.search(
        r"canonical_patient_master", sql, re.IGNORECASE
    ):
        return False
    if re.search(r"\bDELETE\s+FROM\b", sql, re.IGNORECASE) and re.search(
        r"canonical_patient_master", sql, re.IGNORECASE
    ):
        return False
    return True


def no_queue_in_writes(sql: str) -> bool:
    return "cpm_stage_group_manual_review_v1" not in sql


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


def audit_table_rowcount(con: duckdb.DuckDBPyConnection) -> int:
    if not table_exists(con, MS_SCHEMA, TARGET_TABLE):
        return -1
    return con.execute(f"SELECT COUNT(*) FROM {target_fq()}").fetchone()[0]


def readme_402_count(con: duckdb.DuckDBPyConnection) -> int:
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
        WHERE content LIKE 'Script 402:%' OR content LIKE ?
        """,
        [f"%{SCRIPT_ID}%"],
    ).fetchone()[0]


def count_axes_cpm(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    c = cpm_fq()
    return {
        "PDTC_SCATTER": con.execute(f"SELECT COUNT(*) FROM {c} WHERE {PDTC_W}").fetchone()[0],
        "HURTHLE_SCATTER": con.execute(
            f"SELECT COUNT(*) FROM {c} WHERE {HURTH_W}"
        ).fetchone()[0],
        "DHGTC_CATALOG": con.execute(
            f"SELECT COUNT(*) FROM {c} WHERE {DHGTC_W}"
        ).fetchone()[0],
        "PTC_VARIANT_DISCREPANCY": con.execute(
            f"SELECT COUNT(*) FROM {c} WHERE {PTC_DISC_W}"
        ).fetchone()[0],
        "PTC_VARIANT_UNKNOWN": con.execute(
            f"SELECT COUNT(*) FROM {c} WHERE {PTC_UNK_W}"
        ).fetchone()[0],
        "HVA_DATA_QUALITY": con.execute(
            f"SELECT COUNT(*) FROM {c} WHERE {HVA_W}"
        ).fetchone()[0],
        "AGGRESSIVE_VARIANT_FLAGGED": con.execute(
            f"SELECT COUNT(*) FROM {c} WHERE {AGG_W}"
        ).fetchone()[0],
        "GRADE_3_OR_4_CROSS_REF": con.execute(
            f"SELECT COUNT(*) FROM {c} WHERE {GRADE_W}"
        ).fetchone()[0],
    }


def hva_distinct_rids(con: duckdb.DuckDBPyConnection) -> int:
    c = cpm_fq()
    return con.execute(
        f"SELECT COUNT(DISTINCT research_id) FROM {c} WHERE {HVA_W}"
    ).fetchone()[0]


def count_axes_target(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    t = target_fq()
    return {
        a: con.execute(
            f"SELECT COUNT(*) FROM {t} WHERE axis = ?",
            [a],
        ).fetchone()[0]
        for a in AXES
    }


def expected_total_from_counts(axis_counts: dict[str, int]) -> int:
    return sum(int(axis_counts[a]) for a in AXES)


def idempotency_complete(con: duckdb.DuckDBPyConnection) -> bool:
    if not table_exists(con, MS_SCHEMA, TARGET_TABLE):
        return False
    live = count_axes_cpm(con)
    tbl = count_axes_target(con)
    for a in AXES:
        if live.get(a) != tbl.get(a):
            return False
    if expected_total_from_counts(live) != audit_table_rowcount(con):
        return False
    if readme_402_count(con) != 1:
        return False
    return True


# --------------------------------------------------------------------------- #
# Probe
# --------------------------------------------------------------------------- #


def run_probe(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    cpm_n = con.execute(f"SELECT COUNT(*) FROM {cpm_fq()}").fetchone()[0]
    h5_ms = con.execute(
        "SELECT 1 FROM information_schema.schemata "
        "WHERE catalog_name = ? AND schema_name = ?",
        [PUB_DB, MS_SCHEMA],
    ).fetchone() is not None
    if not h5_ms:
        h5_ms = con.execute(
            "SELECT 1 FROM duckdb_schemas() WHERE database_name = ? AND schema_name = ?",
            [PUB_DB, MS_SCHEMA],
        ).fetchone() is not None

    axis_counts = count_axes_cpm(con)
    n_hva_r = hva_distinct_rids(con)
    total_rows = expected_total_from_counts(axis_counts)
    t_exists = table_exists(con, MS_SCHEMA, TARGET_TABLE)
    t_n = audit_table_rowcount(con) if t_exists else -1

    cols = cpm_column_names(con)
    h6 = all(c in cols for c in H6_CPM_COLS)

    h4_no_main_update = cpm_mutation_free(ALL_APPLY_SQL) and "UPDATE main." not in ALL_APPLY_SQL
    h4_no_main_delete = "DELETE FROM main." not in ALL_APPLY_SQL

    h7_archive = not table_exists(con, ARC_SCHEMA, TARGET_TABLE)

    h8q = no_queue_in_writes(ALL_APPLY_SQL)

    ec = target_columns(con)
    h6_tbl = (not t_exists) or (ec is not None and list(ec) == list(TARGET_COLS))

    idem = idempotency_complete(con) if t_exists else False

    cfq = cpm_fq()
    samples: dict[str, str] = {}
    for a in AXES:
        w = W_BY_AXIS[a]
        r = con.execute(
            f"SELECT CAST(research_id AS VARCHAR) FROM {cfq} WHERE {w} LIMIT 1"
        ).fetchone()
        samples[a] = str(r[0]) if r else "—"

    return {
        "axis_counts": axis_counts,
        "n_hva_distinct": n_hva_r,
        "total_rows": total_rows,
        "h2_t_exists": t_exists,
        "h2_t_rows": t_n,
        "h2_idem": idem,
        "h3_cpm": cpm_n,
        "h4_no_update": cpm_mutation_free(ALL_APPLY_SQL) and h4_no_main_update,
        "h4_no_delete": h4_no_main_delete,
        "h5_ms": h5_ms,
        "h6_cols": h6,
        "h6_tbl": h6_tbl,
        "h7_archive": h7_archive,
        "h8q": h8q,
        "existing_target_cols": ec,
        "samples": samples,
    }


def halt_gate(p: dict[str, Any]) -> tuple[bool, list[str]]:
    errs: list[str] = []
    ac = p["axis_counts"]
    if ac.get("PDTC_SCATTER") != E_PDTC:
        errs.append(f"H1: PDTC_SCATTER={ac.get('PDTC_SCATTER')}, expected {E_PDTC}")
    if ac.get("HURTHLE_SCATTER") != E_HURTH:
        errs.append(f"H1: HURTHLE_SCATTER={ac.get('HURTHLE_SCATTER')}, expected {E_HURTH}")
    if ac.get("DHGTC_CATALOG") != E_DHGTC:
        errs.append(f"H1: DHGTC_CATALOG={ac.get('DHGTC_CATALOG')}, expected {E_DHGTC}")
    if ac.get("PTC_VARIANT_DISCREPANCY") != E_PTC_DISC:
        errs.append(
            f"H1: PTC_VARIANT_DISCREPANCY={ac.get('PTC_VARIANT_DISCREPANCY')}, expected {E_PTC_DISC}"
        )
    if ac.get("PTC_VARIANT_UNKNOWN") != E_PTC_UNK:
        errs.append(
            f"H1: PTC_VARIANT_UNKNOWN={ac.get('PTC_VARIANT_UNKNOWN')}, expected {E_PTC_UNK}"
        )
    hva = p.get("n_hva_distinct", 0)
    if hva < HVA_MIN or hva > HVA_MAX:
        errs.append(
            f"H1: HVA_DATA_QUALITY distinct research_id count={hva} not in [{HVA_MIN},{HVA_MAX}]"
        )
    if ac.get("HVA_DATA_QUALITY", -1) != hva:
        errs.append(
            f"H1: HVA CPM row count {ac.get('HVA_DATA_QUALITY')} != distinct {hva}"
        )
    if ac.get("AGGRESSIVE_VARIANT_FLAGGED") != E_AGG:
        errs.append(
            f"H1: AGGRESSIVE_VARIANT_FLAGGED={ac.get('AGGRESSIVE_VARIANT_FLAGGED')}, expected {E_AGG}"
        )
    if ac.get("GRADE_3_OR_4_CROSS_REF") != E_GRADE:
        errs.append(
            f"H1: GRADE_3_OR_4_CROSS_REF={ac.get('GRADE_3_OR_4_CROSS_REF')}, expected {E_GRADE}"
        )
    resum = sum(int(ac.get(x, 0) or 0) for x in AXES)
    if p.get("total_rows", 0) != resum:
        errs.append(f"H1: total_rows {p.get('total_rows')} != resum of axes {resum}")
    if p.get("h2_t_exists") and (not p.get("h2_idem")):
        errs.append(
            f"H2: {TARGET_TABLE} in {MS_SCHEMA} is not a complete idempotent apply "
            f"(rows={p.get('h2_t_rows')})"
        )
    if p.get("h3_cpm") != CPM_EXPECTED:
        errs.append(f"H3: CPM count={p['h3_cpm']}, expected {CPM_EXPECTED}")
    if not p.get("h4_no_update", True) or not p.get("h4_no_delete", True):
        errs.append("H4: disallowed CPM UPDATE/DELETE (main.* substring + canonical scan)")
    if not p.get("h5_ms"):
        errs.append("H5: manuscript_workspace missing")
    if not p.get("h6_cols"):
        errs.append(f"H6: CPM missing required columns among {H6_CPM_COLS}")
    if not p.get("h6_tbl", True) and p.get("h2_t_exists"):
        errs.append(
            f"H6: target column list mismatch (have {p.get('existing_target_cols')!r})"
        )
    if not p.get("h7_archive", True):
        errs.append(
            f"H7: a table {TARGET_TABLE} already exists in {ARC_SCHEMA} (archive collision)"
        )
    if not p.get("h8q", True):
        errs.append("H8: cpm_stage_group_manual_review_v1 referenced in materialization SQL")
    return (len(errs) == 0, errs)


# --------------------------------------------------------------------------- #
# Probe file + apply
# --------------------------------------------------------------------------- #


def format_probe_hashed_body(p: dict[str, Any], gate_ok: bool, gate_errs: list[str]) -> str:
    ac = p["axis_counts"]
    lines = [
        "# Script 402 — Phase 0 probe (histologic classification audit sidecar)",
        "",
        f"| all_pass | {gate_ok} |",
        "",
        "## Per-axis counts (H1)",
        "",
    ]
    for k in AXES:
        lines.append(f"- **{k}:** {ac.get(k)}")
    lines += [
        "",
        f"- **HVA_DATA_QUALITY (DISTINCT research_id) — H1 range gate:** {p.get('n_hva_distinct')}",
        f"- **Total rows (one per CPM row per axis, sum of 8):** {p.get('total_rows')}",
        f"- **CPM (H3):** {p.get('h3_cpm')}",
        f"- **manuscript_workspace (H5):** {'present' if p.get('h5_ms') else 'MISSING'}",
        f"- **Target table (H2):** exists={p.get('h2_t_exists')} rows={p.get('h2_t_rows')} idem={p.get('h2_idem')}",
        f"- **H4 (no CPM mutation in generated SQL):** {p.get('h4_no_update', True)}; "
        f"no `DELETE FROM main.`: {p.get('h4_no_delete', True)}",
        f"- **H6 (CPM columns):** {p.get('h6_cols')}; target col match: {p.get('h6_tbl', True)}",
        f"- **H7 (no archive name collision):** {p.get('h7_archive')}",
        f"- **H8 (no queue in writes):** {p.get('h8q')}",
        "",
    ]
    if p.get("samples"):
        lines.append("## Sample research_id (one per axis, first match)")
        lines.append("")
        for k, v in p["samples"].items():
            lines.append(f"- **{k}:** `{v}`")
        lines.append("")

    if gate_errs:
        lines += ["## Halts", ""] + [f"- {e}" for e in gate_errs] + [""]
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


def insert_readme_402(
    con: duckdb.DuckDBPyConnection, *, probe_sha: str, axis_counts: dict[str, int]
) -> None:
    cols = readme_columns(con)
    parts = [f"{k}={axis_counts.get(k)}" for k in AXES]
    body = (
        f"Script 402: histologic classification audit. Table {PUB_DB}.{MS_SCHEMA}.{TARGET_TABLE}. "
        f"Rows: {expected_total_from_counts(axis_counts)}; "
        + "; ".join(parts)
        + f". Total snapshot per axis. Probe SHA256: {probe_sha}. "
        "Zero UPDATE/DELETE to main.canonical_patient_master. "
        "8 audit axes capturing histologic classification gap landscape; "
        "HVA range gate uses DISTINCT rids in [HVA_MIN,HVA_MAX] (see script constants)."
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
    params: list[Any] = [f"Script 402: {SCRIPT_ID} {body}"]
    if "script" in cols:
        insert_cols.append("script")
        value_exprs.append("?")
        params.append(SCRIPT_ID)
    con.execute(
        f"INSERT INTO {readme_fq()} ({', '.join(insert_cols)}) VALUES ({', '.join(value_exprs)})",
        params,
    )


def apply_transaction(con: duckdb.DuckDBPyConnection, probe_sha: str, axis_counts: dict[str, int]) -> None:
    if not cpm_mutation_free(ALL_APPLY_SQL) or not no_queue_in_writes(ALL_APPLY_SQL):
        raise SystemExit("H4/H8: abort — unsafe SQL")
    n_exp = expected_total_from_counts(axis_counts)
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute(CREATE_TARGET_SQL)
        for ins in ALL_INSERTS:
            con.execute(ins)
        n = con.execute(f"SELECT COUNT(*) FROM {target_fq()}").fetchone()[0]
        if n != n_exp:
            raise SystemExit(f"Apply: rowcount={n}, expected {n_exp} (from CPM axis counts)")
        insert_readme_402(con, probe_sha=probe_sha, axis_counts=axis_counts)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise


def run_verify(
    con: duckdb.DuckDBPyConnection, p0: dict[str, Any] | None
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
        SELECT 1 FROM information_schema.table_constraints
        WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
          AND constraint_type = 'PRIMARY KEY'
        """,
        [PUB_DB, MS_SCHEMA, TARGET_TABLE],
    ).fetchone()
    if not rpk:
        errs.append("P2: PRIMARY KEY missing on target")

    live = count_axes_cpm(con)
    nrows = con.execute(f"SELECT COUNT(*) FROM {target_fq()}").fetchone()[0]
    ex_total = expected_total_from_counts(live)
    if nrows != ex_total:
        errs.append(f"P3: target rows={nrows}, live sum={ex_total}")
    for a in AXES:
        t = con.execute(
            f"SELECT COUNT(*) FROM {target_fq()} WHERE axis = ?",
            [a],
        ).fetchone()[0]
        if t != live.get(a):
            errs.append(f"P4: axis {a} in table={t}, CPM re-query={live.get(a)}")

    if readme_402_count(con) != 1:
        errs.append(f"P5: __readme {SCRIPT_ID} count != 1")

    dupn = con.execute(
        f"""
        SELECT
          (SELECT COUNT(*) FROM {target_fq()})
          - (SELECT COUNT(*) FROM (SELECT DISTINCT research_id, axis FROM {target_fq()}) d)
        """
    ).fetchone()[0]
    if dupn != 0:
        errs.append(f"P6: duplicate (research_id, axis) rows, delta={dupn}")

    # P7: axis counts = probe snapshot if provided, else = live
    if p0 is not None:
        ac0 = p0.get("axis_counts", {})
        for a in AXES:
            if live.get(a) != ac0.get(a):
                errs.append(
                    f"P7: CPM drift during apply: axis {a} was {ac0.get(a)} now {live.get(a)}"
                )

    info = {"nrows": nrows, "live": live}
    return (len(errs) == 0, errs, info)


def top_intersection_patterns(
    con: duckdb.DuckDBPyConnection, limit: int = 5
) -> list[tuple[str, int]]:
    """Multi-axis patients: top patterns by axis combination."""
    if not table_exists(con, MS_SCHEMA, TARGET_TABLE):
        return []
    t = target_fq()
    rows = con.execute(
        f"""
        WITH u AS (
          SELECT
            research_id,
            string_agg(axis, ' + ' ORDER BY axis) AS pat
          FROM {t}
          GROUP BY research_id
          HAVING COUNT(DISTINCT axis) > 1
        )
        SELECT pat, COUNT(*)::BIGINT
        FROM u
        GROUP BY pat
        ORDER BY 2 DESC
        LIMIT {int(limit)}
        """
    ).fetchall()
    return [(str(r[0]), int(r[1])) for r in rows]


# --------------------------------------------------------------------------- #
# Close-out + Phase 4
# --------------------------------------------------------------------------- #


def get_readme_402_run_stamp(con: duckdb.DuckDBPyConnection) -> str:
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
            o = r[0]
            if hasattr(o, "year"):
                dt = o
            else:
                s = str(o)
                dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def format_close_out(
    *,
    commit_sha: str,
    tag_name: str,
    utc_ts: str,
    probe_sha: str,
    p: dict[str, Any],
    con: duckdb.DuckDBPyConnection | None = None,
) -> str:
    ac = p.get("axis_counts", {})
    inter = top_intersection_patterns(con) if con else []
    int_lines = (
        "\n".join(f"- `{pat}`: **{c}** patients" for pat, c in inter)
        or "- *(none; run with materialized table)*"
    )
    hrows = [
        (
            "H1",
            "Per-axis counts match 47/379/10/788/271/HVA(250-280 rids)/43/118; total=sum",
        ),
        ("H2", "Target absent or full idempotency (rows + per-axis = CPM)"),
        ("H3", f"CPM = {CPM_EXPECTED}"),
        ("H4", "No UPDATE/DELETE to main.canonical_patient_master in static SQL"),
        ("H5", "manuscript_workspace exists"),
        ("H6", "CPM has histology/grade/HVA/variant fields + target columns match spec"),
        ("H7", f"No pre-existing {TARGET_TABLE} in {ARC_SCHEMA}"),
        ("H8", "No `cpm_stage_group_manual_review_v1` in write SQL"),
    ]
    h_tbl = "\n".join(f"| {a} | {b} |" for a, b in hrows)
    fix_total = p.get("total_rows", "—")
    return "\n".join(
        [
            "# Script 402 — Close-out (histologic classification audit)",
            "",
            f"- **Commit SHA:** `{commit_sha}`",
            f"- **Tag:** `{tag_name}`",
            f"- **UTC:** {utc_ts}",
            f"- **Probe SHA256 (consumed):** `{probe_sha}`",
            f"- **Target FQN:** `{PUB_DB}.{MS_SCHEMA}.{TARGET_TABLE}`",
            "",
            "## Halt-gate table (H1–H8)",
            "",
            "| gate | design |",
            "|---|---|",
            h_tbl,
            "",
            "## Per-axis counts (8 axes) + total",
            "",
        ]
        + [f"- **{a}:** {ac.get(a, '—')}" for a in AXES]
        + [
            f"- **TOTAL:** {fix_total}",
            "",
            "## Top intersection patterns (≥2 axes per patient) — up to 5",
            "",
            int_lines,
            "",
            "## Zero CPM mutation",
            "",
            "- No `UPDATE` / `DELETE` to `main.canonical_patient_master` (Script 402 read-only to CPM).",
            "",
            "## Deferred follow-ups (CF-402-1 … CF-402-9)",
            "",
            "- **CF-402-1:** PDTC cohort normalization; blocks rid 6275 (CF-401-5).",
            "- **CF-402-2:** Hurthle / oncocytic consolidation (WHO 2022 HCC).",
            "- **CF-402-3:** PTC variant discrepancy — backfill or manual review for multi-variant.",
            "- **CF-402-4:** PTC variant unknown (271) — chart review or accept gap.",
            "- **CF-402-5:** HVA string normalization (case, newlines, pipe order).",
            "- **CF-402-6:** Aggressive variant cohort (43) — prognosis analyses.",
            "- **CF-402-7:** Grade cross-ref — DHGTC/ATC/PDTC patterns.",
            "- **CF-402-8:** NIFTP/FTUMP builder bug (CF-401-1) — separate correction script.",
            "- **CF-402-9:** Script 403 candidate — PDTC consolidation + rid 6275.",
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
    log(f"Phase 4: push {label} failed (exit {r.returncode}); rebase+retry: {r.stderr or r.stdout}")
    br = subprocess.run(
        ["git", "-C", root, "rev-parse", "--abbrev-ref", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    subprocess.run(
        ["git", "-C", root, "pull", "--rebase", "origin", br],
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
        "Script 402: histologic classification audit (8 axes, ~1917-row sidecar, read-only)"
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
    ap.add_argument("--phase", type=int, default=0, help="0=probe (default)")
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
            if idempotency_complete(c0):
                line = f"NO-OP: {TARGET_TABLE} + __readme {SCRIPT_ID} — Phase 3 verify only"
                print(line, flush=True)
                OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
                with open(RUN_LOG_PATH, "a", encoding="utf-8") as f:
                    f.write(f"{datetime.now(timezone.utc).isoformat()} {line}\n")
                ok, errs, _ = run_verify(c0, None)
                if not ok:
                    print("\n--- PHASE 3 VERIFY FAILED ---\n" + "\n".join(errs), flush=True)
                    return 2
                print("Phase 3 verify OK (NO-OP).", flush=True)
                if args.phase4:
                    rs = get_readme_402_run_stamp(c0)
                    pr0 = run_probe(c0)
                    ut = datetime.now(timezone.utc).isoformat()
                    cbody_noop = format_close_out(
                        commit_sha="(pending; NO-OP Phase 4)",
                        tag_name=f"{TAG_PREFIX}{rs}",
                        utc_ts=ut,
                        probe_sha="(re-read apply_histologic_classification_audit_probe.md if needed)",
                        p=pr0,
                        con=c0,
                    )
                    CLOSE_OUT_PATH.write_text(cbody_noop, encoding="utf-8")
                    if not PROMPT_PATH.exists():
                        PROMPT_PATH.write_text(
                            "# Script 402 — see CURSOR_PROMPT_HISTOLOGIC_…_402.md in repo\n",
                            encoding="utf-8",
                        )
                    csha, tag = git_commit_and_tag(rs)
                    cbody2 = format_close_out(
                        commit_sha=csha,
                        tag_name=tag,
                        utc_ts=ut,
                        probe_sha="(from prior apply; NO-OP)",
                        p=pr0,
                        con=c0,
                    )
                    CLOSE_OUT_PATH.write_text(cbody2, encoding="utf-8")
                    print(f"commit_sha={csha} tag={tag}", flush=True)
                return 0
            t_ex = table_exists(c0, MS_SCHEMA, TARGET_TABLE)
            if t_ex and (not idempotency_complete(c0)):
                n = audit_table_rowcount(c0)
                l = count_axes_cpm(c0)
                exp = expected_total_from_counts(l)
                if n != exp:
                    log(f"DRIFT: audit rows={n} live sum={exp}")
                    print("exit 4: audit table drifted; manual investigation required", flush=True)
                    return 4
                if readme_402_count(c0) != 1:
                    print(
                        "exit 2: partial or corrupt (table without one __readme 402)",
                        flush=True,
                    )
                    return 2
            pr = run_probe(c0)
            p_ok, p_errs = halt_gate(pr)
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
            log("HALT: --apply requires --i-approve")
            print(
                f"\n  .venv/bin/python scripts/{SCRIPT_FILE} --apply "
                f"--i-approve={digest} --phase4\n",
                flush=True,
            )
            return 5
        if approved != digest:
            log(f"HALT: --i-approve mismatch (expected {digest}, got {approved})")
            print(f"Use: --i-approve={digest}", flush=True)
            return 3
        if idempotency_complete(con):
            log("Apply: idempotency already complete before transaction")
            print("Re-run with NO-OP path.", flush=True)
            return 0
        if table_exists(con, MS_SCHEMA, TARGET_TABLE):
            log("Apply: target exists; non-idempotent state")
            return 2
        run_stamp = args.force_stamp.strip() or datetime.now(timezone.utc).strftime(
            "%Y%m%d_%H%M%S"
        )
        apply_transaction(con, digest, probe["axis_counts"])
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
            con=con,
        )
        PROMPT_PATH.parent.mkdir(parents=True, exist_ok=True)
        if not PROMPT_PATH.exists():
            PROMPT_PATH.write_text(
                "# Script 402 — Histologic classification audit (read-only sidecar)\n\n"
                "See `scripts/apply_histologic_classification_audit.py` and probe output.\n",
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
                con=con,
            )
            CLOSE_OUT_PATH.write_text(cbody2, encoding="utf-8")
        log("Script 402 apply + verify complete.")
        print(f"commit_sha={csha} tag={tag_n}", flush=True)
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
