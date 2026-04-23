#!/usr/bin/env python3
"""Script 394 — DTC NULL-T stage_group fill (M-decidable subset, CPM).

Surgical in-place UPDATE on main.canonical_patient_master.ajcc8_stage_group
from ajcc8_stage_group_corrected for 20 rows (T NULL, N/M set, stage_group NULL).

Phases
------
* default           — read-only Q0-A..E; writes scripts/output/394_prestate_probe_report.md;
                    prints PROBE_REPORT_SHA256=<hex> for --i-approve on apply.
* --apply           — idempotency check (NO-OP exits without touching probe/close-out files),
                    fresh runs require --i-approve=<sha256> matching that probe body;
                    2A snapshot, 2B UPDATE, 2C __readme, 3 verify

Token: motherduck_client.get_token() (never printed).
"""
from __future__ import annotations

import argparse
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

SNAPSHOT_PREFIX = "cpm_stage_group_pre394_"

OUTPUT_DIR = REPO_ROOT / "scripts" / "output"
PRESTATE_PATH = OUTPUT_DIR / "394_prestate_probe_report.md"
RUN_LOG_PATH = OUTPUT_DIR / "394_run.log"
CLOSE_OUT_PATH = OUTPUT_DIR / "394_close_out_report.md"

CPM_EXPECTED = 10_871

NULL_T_BASE_WHERE = """
  ajcc8_t_stage IS NULL
  AND ajcc8_n_stage IS NOT NULL
  AND ajcc8_m_stage IS NOT NULL
  AND ajcc8_stage_group IS NULL
  AND diagnosis_primary IN ('PTC','FTC','HCC')
""".strip()

AJCC8_M_DECIDABLE_CASE = """
  CASE
    WHEN age_at_surgery < 55 AND ajcc8_m_stage = 'M0' THEN 'I'
    WHEN age_at_surgery < 55 AND ajcc8_m_stage = 'M1' THEN 'II'
    WHEN age_at_surgery >= 55 AND ajcc8_m_stage = 'M1' THEN 'IVB'
    ELSE NULL
  END
""".strip()


# --------------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------------- #


def log(msg: str) -> None:
    line = f"{datetime.now(timezone.utc).isoformat()} {msg}"
    print(line, flush=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(RUN_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def fresh_log() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    RUN_LOG_PATH.write_text("", encoding="utf-8")


# --------------------------------------------------------------------------- #
# Connection
# --------------------------------------------------------------------------- #


def connect() -> duckdb.DuckDBPyConnection:
    tok = get_token()
    if not tok:
        raise SystemExit(
            f"No MotherDuck RW token (token_mode={token_mode()}).  "
            "Set MD_SA_TOKEN / MOTHERDUCK_TOKEN or motherduck.local.toml."
        )
    log(f"Connecting md:{PUB_DB} (token_mode={token_mode()})")
    con = duckdb.connect(f"md:{PUB_DB}?motherduck_token={tok}")
    con.execute(f'USE "{PUB_DB}"')
    con.execute(f'USE "{PUB_DB}".main')
    dbs = {r[0] for r in con.execute("SELECT database_name FROM duckdb_databases()").fetchall()}
    if PUB_DB not in dbs:
        raise SystemExit(f"PUB DB '{PUB_DB}' not attached")
    return con


def table_exists(con: duckdb.DuckDBPyConnection, schema: str, name: str) -> bool:
    row = con.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
        """,
        [PUB_DB, schema, name],
    ).fetchone()
    return row is not None


def find_snapshot(con: duckdb.DuckDBPyConnection) -> str | None:
    rows = con.execute(
        f"""
        SELECT table_name FROM information_schema.tables
        WHERE table_catalog = ? AND table_schema = ?
          AND table_name LIKE '{SNAPSHOT_PREFIX}%'
        ORDER BY table_name
        LIMIT 1
        """,
        [PUB_DB, ARC_SCHEMA],
    ).fetchall()
    return rows[0][0] if rows else None


def readme_394_present(con: duckdb.DuckDBPyConnection) -> bool:
    if not table_exists(con, MAIN_SCHEMA, README_TABLE):
        return False
    row = con.execute(
        f"""
        SELECT 1 FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{README_TABLE}"
        WHERE content LIKE 'Script 394:%'
        LIMIT 1
        """
    ).fetchone()
    return row is not None


def run_probe(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    cpm = f'"{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"'

    n_q0a = con.execute(
        f"SELECT COUNT(*) FROM {cpm} WHERE {NULL_T_BASE_WHERE}"
    ).fetchone()[0]

    q0b = con.execute(
        f"""
        SELECT
          CASE
            WHEN age_at_surgery < 55 AND ajcc8_m_stage = 'M0' THEN 'I (age<55 M0)'
            WHEN age_at_surgery < 55 AND ajcc8_m_stage = 'M1' THEN 'II (age<55 M1)'
            WHEN age_at_surgery >= 55 AND ajcc8_m_stage = 'M1' THEN 'IVB (age>=55 M1)'
            ELSE 'NOT_DERIVABLE'
          END AS derivation_route,
          ajcc8_stage_group_corrected AS builder_corrected,
          COUNT(*) AS n
        FROM {cpm}
        WHERE {NULL_T_BASE_WHERE}
        GROUP BY 1, 2 ORDER BY 1, 2
        """
    ).fetchall()

    q0c = con.execute(
        f"""
        SELECT
          SUM(CASE
            WHEN ajcc8_stage_group_corrected = {AJCC8_M_DECIDABLE_CASE}
            THEN 1 ELSE 0 END) AS n_match,
          COUNT(*) AS n_total
        FROM {cpm}
        WHERE {NULL_T_BASE_WHERE}
          AND ajcc8_stage_group_corrected IS NOT NULL
        """
    ).fetchone()

    q0d = con.execute(
        f"""
        SELECT research_id, diagnosis_primary, age_at_surgery,
               ajcc8_n_stage, ajcc8_m_stage,
               ajcc8_stage_group_corrected AS builder_corrected,
               {AJCC8_M_DECIDABLE_CASE} AS ajcc8_derived
        FROM {cpm}
        WHERE {NULL_T_BASE_WHERE}
          AND ajcc8_stage_group_corrected IS NOT NULL
        ORDER BY age_at_surgery, research_id
        """
    ).fetchall()

    q0e = con.execute(
        f"""
        SELECT research_id, diagnosis_primary, age_at_surgery,
               ajcc8_n_stage, ajcc8_m_stage,
               ajcc8_t_stage_v2 AS t_v2_signal,
               COALESCE(CAST(path_t_stage_raw AS VARCHAR), '·') AS path_t_raw
        FROM {cpm}
        WHERE {NULL_T_BASE_WHERE}
          AND ajcc8_stage_group_corrected IS NULL
        ORDER BY age_at_surgery, research_id
        """
    ).fetchall()

    return {
        "n_q0a": n_q0a,
        "q0b": q0b,
        "q0c_n_match": q0c[0],
        "q0c_n_total": q0c[1],
        "q0d": q0d,
        "q0e": q0e,
    }


def _format_q0b_table(q0b: list) -> str:
    lines = ["| derivation_route | builder_corrected | n |", "|---|---|---:|"]
    for route, bc, n in q0b:
        bc_s = bc if bc is not None else "NULL"
        lines.append(f"| {route} | {bc_s} | {n} |")
    return "\n".join(lines)


def _format_q0d_table(q0d: list) -> str:
    lines = [
        "| research_id | diagnosis_primary | age_at_surgery | N | M | "
        "builder_corrected | ajcc8_derived |",
        "|---:|---|---:|---|---|---|---|",
    ]
    for r in q0d:
        rid, dx, age, n, m, bc, ad = r
        ad_s = ad if ad is not None else "NULL"
        lines.append(
            f"| {rid} | {dx} | {age} | {n} | {m} | {bc} | {ad_s} |"
        )
    return "\n".join(lines)


def _format_q0e_table(q0e: list) -> str:
    lines = [
        "| research_id | diagnosis_primary | age_at_surgery | N | M | "
        "t_v2_signal | path_t_raw |",
        "|---:|---|---:|---|---|---|---|",
    ]
    for r in q0e:
        rid, dx, age, n, m, tv2, praw = r
        tv2_s = tv2 if tv2 is not None else "NULL"
        lines.append(
            f"| {rid} | {dx} | {age} | {n} | {m} | {tv2_s} | {praw} |"
        )
    return "\n".join(lines)


def format_probe_md(probe: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# Script 394 — Pre-state probe (Phase 0)",
            "",
            f"**Generated (UTC):** {datetime.now(timezone.utc).isoformat()}",
            "",
            "## Q0-A. NULL-T DTC orphan count (T NULL, N/M set, stage_group NULL)",
            "",
            "| n_null_t_dtc_orphans |",
            "|---:|",
            f"| {probe['n_q0a']} |",
            "",
            "## Q0-B. Cohort partition (derivation route × builder_corrected)",
            "",
            _format_q0b_table(probe["q0b"]),
            "",
            "## Q0-C. Builder vs AJCC8 M-decidable derivation (fillable subset)",
            "",
            "| n_match | n_total |",
            "|---:|---:|",
            f"| {probe['q0c_n_match']} | {probe['q0c_n_total']} |",
            "",
            "## Q0-D. Per-row fillable cohort (20 rows expected)",
            "",
            _format_q0d_table(probe["q0d"]),
            "",
            "## Q0-E. Deferred cohort (395 handoff; builder_corrected NULL)",
            "",
            _format_q0e_table(probe["q0e"]),
            "",
        ]
    )


def halt_gate(probe: dict[str, Any]) -> tuple[bool, list[str]]:
    errs: list[str] = []
    if probe["n_q0a"] != 33:
        errs.append(f"Q0-A: n={probe['n_q0a']}, expected 33")

    # Q0-B: expect exactly 4 groups with counts 14,1,5,13
    expected_b = {
        ("I (age<55 M0)", "I", 14),
        ("II (age<55 M1)", "II", 1),
        ("IVB (age>=55 M1)", "IVB", 5),
        ("NOT_DERIVABLE", None, 13),
    }
    got_b = set()
    for route, bc, n in probe["q0b"]:
        got_b.add((route, bc, int(n)))
    if got_b != {(t[0], t[1], t[2]) for t in expected_b}:
        errs.append(f"Q0-B: split mismatch. got={sorted(got_b)} expected={expected_b}")

    nm, nt = probe["q0c_n_match"], probe["q0c_n_total"]
    if nm != 20 or nt != 20:
        errs.append(f"Q0-C: n_match={nm} n_total={nt}, expected 20/20")

    q0d = probe["q0d"]
    if len(q0d) != 20:
        errs.append(f"Q0-D: row count {len(q0d)}, expected 20")
    for r in q0d:
        rid, _, _, _, _, bc, ad = r
        if bc != ad:
            errs.append(
                f"Q0-D: research_id {rid} builder_corrected={bc!r} != ajcc8_derived={ad!r}"
            )

    if len(probe["q0e"]) != 13:
        errs.append(f"Q0-E: deferred row count {len(probe['q0e'])}, expected 13")

    return (len(errs) == 0, errs)


def idempotency_state(con: duckdb.DuckDBPyConnection) -> tuple[str | None, bool]:
    return find_snapshot(con), readme_394_present(con)


def apply_script(con: duckdb.DuckDBPyConnection, run_stamp: str) -> str:
    snap_name = f"{SNAPSHOT_PREFIX}{run_stamp}"
    snap_fq = f'"{PUB_DB}"."{ARC_SCHEMA}"."{snap_name}"'
    cpm = f'"{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"'
    readme = f'"{PUB_DB}"."{MAIN_SCHEMA}"."{README_TABLE}"'

    log(f"2A: CREATE OR REPLACE {snap_fq} AS ...")
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {snap_fq} AS
        SELECT research_id, diagnosis_primary, age_at_surgery,
               ajcc8_t_stage, ajcc8_n_stage, ajcc8_m_stage,
               ajcc8_t_stage_v2,
               ajcc8_stage_group, ajcc8_stage_group_corrected,
               CASE
                 WHEN ajcc8_stage_group_corrected IS NOT NULL THEN '394_fillable'
                 ELSE '395_deferred_needs_T'
               END AS cohort_tag,
               CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS snapshot_ts
        FROM {cpm}
        WHERE {NULL_T_BASE_WHERE}
        """
    )
    n_snap = con.execute(f"SELECT COUNT(*) FROM {snap_fq}").fetchone()[0]
    if n_snap != 33:
        raise SystemExit(f"2A: snapshot row count {n_snap}, expected 33")
    log(f"2A OK — {n_snap} rows")

    log("2B: UPDATE ajcc8_stage_group FROM builder (with AJCC8 guard) ...")
    con.execute(
        f"""
        UPDATE {cpm}
        SET ajcc8_stage_group = ajcc8_stage_group_corrected
        WHERE {NULL_T_BASE_WHERE}
          AND ajcc8_stage_group_corrected IS NOT NULL
          AND ajcc8_stage_group_corrected = {AJCC8_M_DECIDABLE_CASE}
        """
    )
    log("2B done")

    prov = (
        "Script 394: canonical_patient_master.ajcc8_stage_group NULL-T DTC orphan fill "
        "(M-decidable subset) — 20 rows builder-synced from ajcc8_stage_group_corrected "
        "(14×I, 1×II, 5×IVB), all AJCC8-validated. 13 rows deferred to future Script 395 "
        f"(age>=55 M0, needs T sync from ajcc8_t_stage_v2). Snapshot: {ARC_SCHEMA}.{snap_name}. "
        "Builder-sync validated 20/20 — same pattern as 393."
    )
    log("2C: __readme")
    con.execute(
        f"""
        INSERT INTO {readme}(content, updated_at) VALUES
        (?, CAST(CURRENT_TIMESTAMP AS TIMESTAMP))
        """,
        [prov],
    )
    return snap_name


def run_verify(con: duckdb.DuckDBPyConnection, snap_name: str) -> dict[str, Any]:
    cpm = f'"{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"'
    snap = f'"{PUB_DB}"."{ARC_SCHEMA}"."{snap_name}"'
    readme = f'"{PUB_DB}"."{MAIN_SCHEMA}"."{README_TABLE}"'

    v1 = con.execute(
        f"SELECT COUNT(*) FROM {cpm} WHERE {NULL_T_BASE_WHERE}"
    ).fetchone()[0]

    v2 = con.execute(
        f"""
        SELECT COUNT(*) FROM {cpm} c
        JOIN {snap} s USING (research_id)
        WHERE s.cohort_tag = '394_fillable'
          AND c.ajcc8_stage_group IS NOT NULL
        """
    ).fetchone()[0]

    v3 = con.execute(
        f"""
        SELECT c.ajcc8_stage_group, COUNT(*) AS n
        FROM {cpm} c
        JOIN {snap} s USING (research_id)
        WHERE s.cohort_tag = '394_fillable'
        GROUP BY 1 ORDER BY 1
        """
    ).fetchall()

    v4 = con.execute(
        f"""
        SELECT COUNT(*) FROM {cpm} c
        JOIN {snap} s USING (research_id)
        WHERE s.cohort_tag = '395_deferred_needs_T'
          AND c.ajcc8_stage_group IS NULL
        """
    ).fetchone()[0]

    v5 = con.execute(f"SELECT COUNT(*) FROM {cpm}").fetchone()[0]

    v6 = con.execute(
        f"""
        SELECT cohort_tag, COUNT(*) AS n FROM {snap}
        GROUP BY 1 ORDER BY 1
        """
    ).fetchall()

    v7 = con.execute(
        f"SELECT COUNT(*) FROM {readme} WHERE content LIKE 'Script 394:%'"
    ).fetchone()[0]

    v8 = con.execute(
        f"""
        SELECT SUM(CASE WHEN c.ajcc8_stage_group != c.ajcc8_stage_group_corrected
                   THEN 1 ELSE 0 END)
        FROM {cpm} c
        JOIN {snap} s USING (research_id)
        WHERE s.cohort_tag = '394_fillable'
        """
    ).fetchone()[0]

    v9 = con.execute(
        f"""
        SELECT COUNT(*) FROM {cpm}
        WHERE ajcc8_t_stage = 'T3b'
          AND ajcc8_n_stage IS NOT NULL AND ajcc8_m_stage IS NOT NULL
          AND ajcc8_stage_group IS NULL
          AND diagnosis_primary IN ('PTC','FTC','HCC')
        """
    ).fetchone()[0]

    dist = {r[0]: int(r[1]) for r in v3}
    dist_ok = dist == {"I": 14, "II": 1, "IVB": 5}
    v6_ok = {r[0]: int(r[1]) for r in v6} == {
        "394_fillable": 20,
        "395_deferred_needs_T": 13,
    }

    all_ok = (
        v1 == 13
        and v2 == 20
        and v4 == 13
        and v5 == CPM_EXPECTED
        and v7 == 1
        and v8 == 0
        and v9 == 0
        and dist_ok
        and v6_ok
    )

    return {
        "v1": v1,
        "v2": v2,
        "v3": v3,
        "v4": v4,
        "v5": v5,
        "v6": v6,
        "v7": v7,
        "v8": v8,
        "v9": v9,
        "dist_ok": dist_ok,
        "v6_ok": v6_ok,
        "all_ok": all_ok,
    }


def format_close_out(
    verify: dict[str, Any],
    snap_name: str,
    q0e_rows: list,
) -> str:
    q0e_md = _format_q0e_table(q0e_rows)
    lines = [
        "# Script 394 — Close-out (Phase 3)",
        "",
        f"**Snapshot:** `{ARC_SCHEMA}.{snap_name}`",
        "",
        "## Verification summary",
        "",
        "| Check | Value |",
        "|---|---|",
        f"| V1 n_orphans_remaining (NULL-T cohort) | {verify['v1']} (expect 13) |",
        f"| V2 n_filled (394_fillable) | {verify['v2']} (expect 20) |",
        f"| V3 distribution | {verify['v3']} (expect I=14, II=1, IVB=5) |",
        f"| V3 dist_ok | {verify['dist_ok']} |",
        f"| V4 deferred unchanged (stage_group NULL) | {verify['v4']} (expect 13) |",
        f"| V5 n_cpm | {verify['v5']} (expect {CPM_EXPECTED}) |",
        f"| V6 snapshot cohort_tag counts | {verify['v6']} |",
        f"| V6_ok | {verify['v6_ok']} |",
        f"| V7 __readme rows (Script 394) | {verify['v7']} (expect 1) |",
        f"| V8 n_mismatch fillable cohort | {verify['v8']} (expect 0) |",
        f"| V9 T3b DTC orphans | {verify['v9']} (expect 0) |",
        "",
        f"**Phase 3 pass:** {verify['all_ok']}",
        "",
        "## CF-394-1 — Deferred to Script 395 (Q0-E verbatim)",
        "",
        "13 age≥55 M0 DTC rows: sync `ajcc8_t_stage ← ajcc8_t_stage_v2` where populated; "
        "re-derive stage_group; research_ids 1404 and 12198 may need manual review if no T signal.",
        "",
        q0e_md,
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--apply",
        action="store_true",
        help="Run idempotency, apply, verify (default is probe only)",
    )
    ap.add_argument(
        "--force-stamp",
        default="",
        help="Override RUN_STAMP for snapshot name (default: UTC YYYYMMDD_HHMMSS)",
    )
    ap.add_argument(
        "--i-approve",
        default="",
        metavar="SHA256",
        help=(
            "Required for a fresh --apply: hex SHA256 of the prestate probe report body "
            "(from probe-only run; optional sha256: prefix). Ignored on idempotent NO-OP."
        ),
    )
    args = ap.parse_args()

    fresh_log()
    con = connect()

    # Idempotency first: NO-OP must not rewrite probe or close-out reports.
    if args.apply:
        snap, readme = idempotency_state(con)
        if snap and readme:
            log(
                f"NO-OP: snapshot {snap} and Script 394 __readme present — "
                "idempotent success (probe/close-out files untouched)."
            )
            return 0
        if snap and not readme:
            log("HALT: partial apply — snapshot exists but no __readme")
            return 2
        if readme and not snap:
            log("HALT: missing snapshot — __readme without snapshot")
            return 3

    probe = run_probe(con)
    PRESTATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    prestate_body = format_probe_md(probe)
    PRESTATE_PATH.write_text(prestate_body, encoding="utf-8")
    log(f"Wrote {PRESTATE_PATH}")
    digest = probe_report_sha256(prestate_body)
    print(f"PROBE_REPORT_SHA256={digest}", flush=True)

    ok, errs = halt_gate(probe)
    if not ok:
        for e in errs:
            log(f"HALT: {e}")
        print("\n--- HALT GATE FAILED ---\n" + "\n".join(errs), flush=True)
        return 1

    if args.apply:
        approved = normalize_i_approve(args.i_approve)
        if not approved:
            log("HALT: fresh --apply requires --i-approve=<sha256> from probe-only output")
            print(
                "\nAfter greenlight, run:\n"
                f"  python3 scripts/394_dtc_null_t_stage_group_fill.py --apply "
                f"--i-approve={digest}\n",
                flush=True,
            )
            return 5
        if approved != digest:
            log(
                f"HALT: --i-approve mismatch (file {digest}, got {approved}) — "
                "probe drift or wrong hash"
            )
            return 5

    if not args.apply:
        log("Probe only — re-run with --apply --i-approve=<sha256> after greenlight.")
        return 0

    run_stamp = args.force_stamp.strip() or datetime.now(timezone.utc).strftime(
        "%Y%m%d_%H%M%S"
    )
    snap_name = apply_script(con, run_stamp)
    verify = run_verify(con, snap_name)
    CLOSE_OUT_PATH.write_text(
        format_close_out(verify, snap_name, probe["q0e"]),
        encoding="utf-8",
    )
    log(f"Wrote {CLOSE_OUT_PATH}")
    if not verify["all_ok"]:
        log("VERIFY FAILED — see close-out report")
        return 4
    log("Script 394 apply + verify complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
