#!/usr/bin/env python3
"""Script 393 — DTC T3b stage_group orphan fill (CPM, publication DB).

Surgical in-place UPDATE on main.canonical_patient_master.ajcc8_stage_group
(+ mirror ajcc8_stage_group_corrected when NULL) for 9 DTC T3b rows.

Phases
------
* default           — read-only Q0-A/B/C/D; writes scripts/output/393_prestate_probe_report.md;
                    prints PROBE_REPORT_SHA256=<hex> for --i-approve on apply.
* --apply           — idempotency check (NO-OP exits without touching probe/close-out files),
                    fresh runs require --i-approve=<sha256> matching that probe body;
                    2A snapshot, 2B/2C UPDATE, 2D __readme, 3 verify

Token: motherduck_client.get_token() (never printed).
Auth: see docs / motherduck.local.toml
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
SCRIPT_ID = "393"
CPM_TABLE = "canonical_patient_master"
README_TABLE = "__readme"

# Snapshot base name; full name = cpm_stage_group_pre393_<RUN_STAMP>
SNAPSHOT_PREFIX = "cpm_stage_group_pre393_"

OUTPUT_DIR = REPO_ROOT / "scripts" / "output"
PRESTATE_PATH = OUTPUT_DIR / "393_prestate_probe_report.md"
RUN_LOG_PATH = OUTPUT_DIR / "393_run.log"
CLOSE_OUT_PATH = OUTPUT_DIR / "393_close_out_report.md"

CPM_EXPECTED = 10_871

# Expected research_id -> (stage_group) from spec
EXPECTED_T3B: dict[int, str] = {
    1412: "I",
    1546: "I",
    4430: "II",
    6087: "I",
    7566: "II",
    550: "II",
    1908: "II",
    5432: "II",
    11108: "IVB",
}


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


def readme_393_present(con: duckdb.DuckDBPyConnection) -> bool:
    if not table_exists(con, MAIN_SCHEMA, README_TABLE):
        return False
    row = con.execute(
        f"""
        SELECT 1 FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{README_TABLE}"
        WHERE content LIKE 'Script 393:%'
        LIMIT 1
        """
    ).fetchone()
    return row is not None


# SQL fragments (DTC, T3b orphans)
T3B_WHERE = """
  ajcc8_t_stage = 'T3b'
  AND ajcc8_n_stage IS NOT NULL
  AND ajcc8_m_stage IS NOT NULL
  AND ajcc8_stage_group IS NULL
  AND diagnosis_primary IN ('PTC','FTC','HCC')
""".strip()

# T3b DTC filter without stage_group IS NULL (for 2C after 2B has filled stage_group)
T3B_BASE_WHERE = """
  ajcc8_t_stage = 'T3b'
  AND ajcc8_n_stage IS NOT NULL
  AND ajcc8_m_stage IS NOT NULL
  AND diagnosis_primary IN ('PTC','FTC','HCC')
""".strip()

DERIVED_CASE = """
  CASE
    WHEN age_at_surgery < 55 AND ajcc8_m_stage = 'M0' THEN 'I'
    WHEN age_at_surgery < 55 AND ajcc8_m_stage = 'M1' THEN 'II'
    WHEN age_at_surgery >= 55 AND ajcc8_t_stage = 'T3b' AND ajcc8_m_stage = 'M0' THEN 'II'
    WHEN age_at_surgery >= 55 AND ajcc8_m_stage = 'M1' THEN 'IVB'
    ELSE NULL
  END
""".strip()


def run_probe(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    n_orphans = con.execute(
        f"""
        SELECT COUNT(*) FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        WHERE {T3B_WHERE}
        """
    ).fetchone()[0]

    q0b = con.execute(
        f"""
        SELECT research_id, diagnosis_primary, ajcc8_t_stage, ajcc8_n_stage, ajcc8_m_stage,
               age_at_surgery,
               {DERIVED_CASE} AS derived_stage_group
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        WHERE {T3B_WHERE}
        ORDER BY research_id
        """
    ).fetchall()

    n_corrected_also_null = con.execute(
        f"""
        SELECT COUNT(*) FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        WHERE {T3B_WHERE}
          AND ajcc8_stage_group_corrected IS NULL
        """
    ).fetchone()[0]

    q0d = con.execute(
        f"""
        SELECT ajcc8_t_stage, COUNT(*) AS n
        FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        WHERE ajcc8_n_stage IS NOT NULL
          AND ajcc8_m_stage IS NOT NULL
          AND ajcc8_stage_group IS NULL
          AND diagnosis_primary IN ('PTC','FTC','HCC')
        GROUP BY 1 ORDER BY 2 DESC
        """
    ).fetchall()

    n_non_t3b_orphan = con.execute(
        f"""
        SELECT COUNT(*) FROM "{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"
        WHERE ajcc8_t_stage != 'T3b'
          AND ajcc8_n_stage IS NOT NULL
          AND ajcc8_m_stage IS NOT NULL
          AND ajcc8_stage_group IS NULL
          AND diagnosis_primary IN ('PTC','FTC','HCC')
        """
    ).fetchone()[0]

    return {
        "n_orphans": n_orphans,
        "q0b_rows": q0b,
        "n_corrected_also_null": n_corrected_also_null,
        "q0d": q0d,
        "n_non_t3b_orphan": n_non_t3b_orphan,
    }


def format_probe_md(probe: dict[str, Any]) -> str:
    lines = [
        f"# Script 393 — Pre-state probe (Phase 0)",
        f"",
        f"**Generated (UTC):** {datetime.now(timezone.utc).isoformat()}",
        f"",
        f"## Q0-A. Orphan count (DTC, T3b, T/N/M set, stage_group NULL)",
        f"",
        f"| n_orphans |",
        f"|---:|",
        f"| {probe['n_orphans']} |",
        f"",
        f"## Q0-B. Derivation preview",
        f"",
        "| research_id | diagnosis_primary | T | N | M | age | derived_stage_group |",
        "|---:|---|---|---|---|---:|---|",
    ]
    null_derived = 0
    for r in probe["q0b_rows"]:
        rid, dx, t, n, m, age, dsg = r
        dsg_s = dsg if dsg is not None else "NULL"
        if dsg is None:
            null_derived += 1
        lines.append(
            f"| {rid} | {dx} | {t} | {n} | {m} | {age} | {dsg_s} |"
        )
    lines += [
        f"",
        f"**Rows with NULL derived_stage_group:** {null_derived}",
        f"",
        f"## Q0-C. Corrected also NULL (among T3b orphan cohort)",
        f"",
        f"| n_corrected_also_null |",
        f"|---:|",
        f"| {probe['n_corrected_also_null']} |",
        f"",
        f"## Q0-D. DTC stage_group NULL by T-stage (global)",
        f"",
        "| ajcc8_t_stage | n |",
        "|---|---:|",
    ]
    for ts, n in probe["q0d"]:
        lines.append(f"| {ts} | {n} |")
    if not probe["q0d"]:
        lines.append("| _(no rows)_ | 0 |")
    lines += [
        "",
        "## Q0-D (single count). DTC `ajcc8_t_stage != 'T3b'` (matches V4; excludes NULL T)",
        "",
        f"| n |",
        f"|---:|",
        f"| {probe['n_non_t3b_orphan']} |",
        "",
    ]
    return "\n".join(lines)


def halt_gate(probe: dict[str, Any]) -> tuple[bool, list[str]]:
    """Return (ok, error messages)."""
    errs: list[str] = []
    if probe["n_orphans"] != 9:
        errs.append(f"Q0-A: n_orphans={probe['n_orphans']}, expected 9")
    for r in probe["q0b_rows"]:
        rid, _, _, _, _, _, dsg = r
        if dsg is None:
            errs.append(f"Q0-B: research_id {rid} has NULL derived_stage_group")
        else:
            exp = EXPECTED_T3B.get(int(rid))
            if exp is not None and dsg != exp:
                errs.append(
                    f"Q0-B: research_id {rid} derived {dsg!r}, spec expected {exp!r}"
                )
    if len(probe["q0b_rows"]) == 9:
        got_ids = {int(r[0]) for r in probe["q0b_rows"]}
        if got_ids != set(EXPECTED_T3B.keys()):
            errs.append(
                f"Q0-B: research_id set mismatch. got={sorted(got_ids)} "
                f"expected={sorted(EXPECTED_T3B.keys())}"
            )
    for ts, n in probe["q0d"]:
        if ts is None or ts == "T3b":
            # NULL t_stage is a separate data-quality bucket; V4's != 'T3b' excludes NULL.
            # Halt only on a *non-NULL* T that is not T3b (e.g. T4a) — other rules apply.
            continue
        if n and int(n) > 0:
            errs.append(
                f"Q0-D: non-T3b DTC orphan — ajcc8_t_stage={ts!r} count={n} "
                "(393 halt: out of scope; separate script needed)"
            )
    return (len(errs) == 0, errs)


def idempotency_state(con: duckdb.DuckDBPyConnection) -> tuple[str | None, bool]:
    snap = find_snapshot(con)
    readme = readme_393_present(con)
    return snap, readme


def apply_script(con: duckdb.DuckDBPyConnection, run_stamp: str) -> None:
    snap_name = f"{SNAPSHOT_PREFIX}{run_stamp}"
    snap_fq = f'"{PUB_DB}"."{ARC_SCHEMA}"."{snap_name}"'
    cpm = f'"{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"'
    readme = f'"{PUB_DB}"."{MAIN_SCHEMA}"."{README_TABLE}"'

    log(f"2A: CREATE OR REPLACE {snap_fq} AS ...")
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {snap_fq} AS
        SELECT research_id, diagnosis_primary,
               ajcc8_t_stage, ajcc8_n_stage, ajcc8_m_stage, age_at_surgery,
               ajcc8_stage_group, ajcc8_stage_group_corrected,
               CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS snapshot_ts
        FROM {cpm}
        WHERE {T3B_WHERE}
        """
    )
    n_snap = con.execute(f"SELECT COUNT(*) FROM {snap_fq}").fetchone()[0]
    if n_snap != 9:
        raise SystemExit(f"2A: snapshot row count {n_snap}, expected 9")
    log(f"2A OK — {n_snap} rows")

    log("2B: UPDATE ajcc8_stage_group ...")
    con.execute(
        f"""
        UPDATE {cpm}
        SET ajcc8_stage_group = CASE
          WHEN age_at_surgery < 55 AND ajcc8_m_stage = 'M0' THEN 'I'
          WHEN age_at_surgery < 55 AND ajcc8_m_stage = 'M1' THEN 'II'
          WHEN age_at_surgery >= 55 AND ajcc8_t_stage = 'T3b' AND ajcc8_m_stage = 'M0' THEN 'II'
          WHEN age_at_surgery >= 55 AND ajcc8_m_stage = 'M1' THEN 'IVB'
        END
        WHERE {T3B_WHERE}
        """
    )
    # duckdb may not return rows_changed on all drivers — verify via V1
    log("2B done")

    log("2C: mirror ajcc8_stage_group_corrected (T3b base + snapshot ids; not stage_group IS NULL) ...")
    con.execute(
        f"""
        UPDATE {cpm}
        SET ajcc8_stage_group_corrected = ajcc8_stage_group
        WHERE {T3B_BASE_WHERE}
          AND ajcc8_stage_group_corrected IS NULL
          AND ajcc8_stage_group IS NOT NULL
          AND research_id IN (SELECT research_id FROM {snap_fq})
        """
    )
    log("2C done")

    prov = (
        "Script 393: canonical_patient_master.ajcc8_stage_group DTC T3b orphan fill — 9 rows "
        "derived deterministically from AJCC8 rules (3×I, 5×II, 1×IVB). Mirror column "
        "ajcc8_stage_group_corrected synced where also NULL. Builder-logic gap (240-builder did "
        f"not retrigger for upstream-T3b rows); no rebuild. Snapshot: {ARC_SCHEMA}.{snap_name}."
    )
    log("2D: __readme")
    con.execute(
        f"""
        INSERT INTO {readme}(content, updated_at) VALUES
        (?, CAST(CURRENT_TIMESTAMP AS TIMESTAMP))
        """,
        [prov],
    )


def run_verify(
    con: duckdb.DuckDBPyConnection,
    snap_name: str,
    non_t3b_baseline: int,
) -> dict[str, Any]:
    cpm = f'"{PUB_DB}"."{MAIN_SCHEMA}"."{CPM_TABLE}"'
    snap = f'"{PUB_DB}"."{ARC_SCHEMA}"."{snap_name}"'
    readme = f'"{PUB_DB}"."{MAIN_SCHEMA}"."{README_TABLE}"'

    v1 = con.execute(
        f"""
        SELECT COUNT(*) FROM {cpm}
        WHERE {T3B_WHERE}
        """
    ).fetchone()[0]

    v2 = con.execute(
        f"""
        SELECT COUNT(*) FROM {cpm} c
        JOIN {snap} s USING (research_id)
        WHERE c.ajcc8_stage_group IS NOT NULL
        """
    ).fetchone()[0]

    v3 = con.execute(
        f"""
        SELECT c.ajcc8_stage_group, COUNT(*) AS n
        FROM {cpm} c
        JOIN {snap} s USING (research_id)
        GROUP BY 1 ORDER BY 1
        """
    ).fetchall()

    v4 = con.execute(
        f"""
        SELECT COUNT(*) FROM {cpm}
        WHERE ajcc8_t_stage != 'T3b'
          AND ajcc8_n_stage IS NOT NULL
          AND ajcc8_m_stage IS NOT NULL
          AND ajcc8_stage_group IS NULL
          AND diagnosis_primary IN ('PTC','FTC','HCC')
        """
    ).fetchone()[0]

    v5 = con.execute(f"SELECT COUNT(*) FROM {cpm}").fetchone()[0]

    v6 = con.execute(f"SELECT COUNT(*) FROM {snap}").fetchone()[0]

    v7 = con.execute(
        f"""
        SELECT COUNT(*) FROM {readme} WHERE content LIKE 'Script 393:%'
        """
    ).fetchone()[0]

    v8 = con.execute(
        f"""
        SELECT c.research_id, c.ajcc8_stage_group, c.ajcc8_stage_group_corrected
        FROM {cpm} c
        JOIN {snap} s USING (research_id)
        WHERE c.ajcc8_stage_group IS DISTINCT FROM c.ajcc8_stage_group_corrected
        """
    ).fetchall()

    ok = (
        v1 == 0
        and v2 == 9
        and v5 == CPM_EXPECTED
        and v6 == 9
        and v7 == 1
        and len(v8) == 0
        and v4 == non_t3b_baseline
    )
    dist_ok = {r[0]: r[1] for r in v3} == {"I": 3, "II": 5, "IVB": 1}
    if not dist_ok:
        ok = False

    return {
        "v1": v1,
        "v2": v2,
        "v3": v3,
        "v4": v4,
        "v5": v5,
        "v6": v6,
        "v7": v7,
        "v8_mismatches": v8,
        "dist_ok": dist_ok,
        "all_ok": ok,
        "v4_baseline_matched": v4 == non_t3b_baseline,
    }


def format_close_out(verify: dict[str, Any], snap_name: str) -> str:
    lines = [
        "# Script 393 — Close-out (Phase 3)",
        "",
        f"**Snapshot:** `{ARC_SCHEMA}.{snap_name}`",
        "",
        "| Check | Value |",
        "|---|---|",
        f"| V1 n_orphans_remaining (T3b) | {verify['v1']} (expect 0) |",
        f"| V2 n_filled in cohort | {verify['v2']} (expect 9) |",
        f"| V3 distribution | {verify['v3']} (expect I=3, II=5, IVB=1) |",
        f"| V3 dist_ok | {verify['dist_ok']} |",
        f"| V4 non-T3b DTC orphans | {verify['v4']} (vs Q0-D baseline) |",
        f"| V5 n_cpm | {verify['v5']} (expect {CPM_EXPECTED}) |",
        f"| V6 n_snapshot | {verify['v6']} (expect 9) |",
        f"| V7 __readme rows | {verify['v7']} (expect 1) |",
        f"| V8 parity mismatches | {len(verify['v8_mismatches'])} (expect 0) |",
        "",
        f"**Phase 3 pass:** {verify['all_ok']}",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--apply",
        action="store_true",
        help="Run idempotency, apply, verify (default is --probe only)",
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
                f"NO-OP: snapshot {snap} and Script 393 __readme present — "
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
    baseline_non_t3b = int(probe["n_non_t3b_orphan"])
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
                f"  python3 scripts/393_dtc_stage_group_orphan_fill.py --apply "
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

    if probe["n_orphans"] != 9:
        log("HALT: apply blocked — orphan count not 9")
        return 1

    run_stamp = args.force_stamp.strip() or datetime.now(timezone.utc).strftime(
        "%Y%m%d_%H%M%S"
    )
    apply_script(con, run_stamp)
    snap_name = f"{SNAPSHOT_PREFIX}{run_stamp}"
    verify = run_verify(con, snap_name, non_t3b_baseline=baseline_non_t3b)
    CLOSE_OUT_PATH.write_text(
        format_close_out(verify, snap_name), encoding="utf-8"
    )
    log(f"Wrote {CLOSE_OUT_PATH}")
    if not verify["all_ok"]:
        log("VERIFY FAILED — see close-out report")
        return 4
    log("Script 393 apply + verify complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())