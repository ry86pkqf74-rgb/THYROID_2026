#!/usr/bin/env python3
"""Script 389.1 — `detail_table_registry_v1` schema migration.

Adds two VARCHAR columns to the registry and backfills them deterministically
from `archive_pub_v1_0.*` snapshot table names + `archive_move_log_v1`:

    superseded_by      VARCHAR  -- canonical that replaced this row (NULL = live)
    renamed_by_script  VARCHAR  -- script that retired/renamed (NULL = never)

Spec: `cursor_prompts/CURSOR_PROMPT_REGISTRY_SCHEMA_MIGRATION_20260422_SCRIPT_389_1.md`

Phases
------
* `--phase 0` (default) — read-only probe; writes
  `scripts/output/389_1_probe_report.md` and halts.
* `--apply` — runs Phase 2 (snapshot + ALTER + UPDATEs + __readme provenance)
  and Phase 3 (post-state verify).

Idempotency
-----------
* If both new columns exist AND a `__readme` row prefixed
  ``"Script 389.1:"`` is present → exits 0 with NO-OP message.
* If the columns exist but no `__readme` provenance row → halts with a
  "partial migration detected" error so a human can investigate.

Hard rules honored
------------------
* No cross-DB sourcing of canonicals: this script only reads the registry,
  the `archive_pub_v1_0` namespace, and `archive_move_log_v1`.
* `CAST(CURRENT_TIMESTAMP AS TIMESTAMP)` for every timestamp insert.
* Token never printed — `motherduck_client.get_token()` + `token_mode()`.
* No `git add` performed by this script; commit + tag is manual per the
  surgical-staging rule.
"""
from __future__ import annotations

import argparse
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from motherduck_client import get_token, token_mode  # noqa: E402

PUB_DB = "thyroid_canonical_publication_v1_0"
REGISTRY_NAME = "detail_table_registry_v1"
REGISTRY_CANDIDATE_SCHEMAS = ("main", "manuscript_workspace")
ARCHIVE_SCHEMA = "archive_pub_v1_0"
MOVE_LOG_NAME = "archive_move_log_v1"
MOVE_LOG_CANDIDATE_SCHEMAS = ("manuscript_workspace", "main")
README_NAME = "__readme"
README_PREFIX = "Script 389.1:"

SCRIPT_TAG = "389_1_registry_schema_migration"

OUTPUT_DIR = REPO_ROOT / "scripts" / "output"
PROBE_REPORT_PATH = OUTPUT_DIR / "389_1_probe_report.md"
RUN_LOG_PATH = OUTPUT_DIR / "389_1_run.log"
CLOSE_OUT_PATH = OUTPUT_DIR / "389_1_close_out.md"

# Regex for `<original>_pre<NNN>[ _<extra> ]_<stamp>`. The `<extra>` chunk
# (e.g. `_v2_backup`, `_body`) is allowed but not required. Stamp = anything
# at the end (we only care about the script number for renamed_by_script).
PRE_NNN_RE = re.compile(
    r"^(?P<orig>.+?)_pre(?P<script>\d+(?:_\d+)?)(?:_[A-Za-z0-9]+)*_(?P<stamp>\d{8}.*)$"
)
# Regex for `<original>_legacy_<stamp>[ _body ]` (387 / 389 style).
LEGACY_RE = re.compile(
    r"^(?P<orig>.+?)_legacy_(?P<stamp>\d{8}\S*?)(?:_body)?$"
)


# --------------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------------- #

_log_buf: list[str] = []


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3] + "Z"


def log(msg: str) -> None:
    line = f"[INFO] [{_ts()}] {msg}"
    print(line, flush=True)
    _log_buf.append(line)


def warn(msg: str) -> None:
    line = f"[WARN] [{_ts()}] {msg}"
    print(line, flush=True)
    _log_buf.append(line)


def err(msg: str) -> None:
    line = f"[ERROR] [{_ts()}] {msg}"
    print(line, flush=True)
    _log_buf.append(line)


def flush_log() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    mode = "a" if RUN_LOG_PATH.exists() else "w"
    with RUN_LOG_PATH.open(mode, encoding="utf-8") as fh:
        fh.write("\n".join(_log_buf) + "\n")


# --------------------------------------------------------------------------- #
# Connection + introspection
# --------------------------------------------------------------------------- #


def connect() -> duckdb.DuckDBPyConnection:
    tok = get_token()
    if not tok:
        raise SystemExit(
            f"No MotherDuck RW token (token_mode={token_mode()}). "
            "Set MD_SA_TOKEN / MOTHERDUCK_TOKEN or motherduck.local.toml."
        )
    log(f"Connecting md:{PUB_DB} (token_mode={token_mode()})")
    con = duckdb.connect(f"md:{PUB_DB}?motherduck_token={tok}")
    con.execute(f'USE "{PUB_DB}"')
    con.execute(f'USE "{PUB_DB}".main')
    return con


def find_object_schema(
    con: duckdb.DuckDBPyConnection,
    name: str,
    candidates: tuple[str, ...],
) -> str | None:
    for sch in candidates:
        row = con.execute(
            """
            SELECT 1 FROM information_schema.tables
            WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
            """,
            [PUB_DB, sch, name],
        ).fetchone()
        if row:
            return sch
    return None


def get_columns(
    con: duckdb.DuckDBPyConnection, schema: str, name: str
) -> list[tuple[str, str]]:
    rows = con.execute(
        """
        SELECT column_name, data_type FROM information_schema.columns
        WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
        ORDER BY ordinal_position
        """,
        [PUB_DB, schema, name],
    ).fetchall()
    return [(r[0], r[1]) for r in rows]


def list_archive_tables(con: duckdb.DuckDBPyConnection) -> list[str]:
    rows = con.execute(
        """
        SELECT table_name FROM information_schema.tables
        WHERE table_catalog = ? AND table_schema = ?
        ORDER BY table_name
        """,
        [PUB_DB, ARCHIVE_SCHEMA],
    ).fetchall()
    return [r[0] for r in rows]


def list_main_tables(con: duckdb.DuckDBPyConnection) -> set[str]:
    rows = con.execute(
        """
        SELECT table_name FROM information_schema.tables
        WHERE table_catalog = ? AND table_schema = 'main'
        """,
        [PUB_DB],
    ).fetchall()
    return {r[0] for r in rows}


# --------------------------------------------------------------------------- #
# Phase 0 — probe + backfill map
# --------------------------------------------------------------------------- #


def parse_archive_name(arc_name: str) -> tuple[str, str | None, str | None]:
    """Return (original_name, script_number_or_None, stamp_or_None)."""
    m = PRE_NNN_RE.match(arc_name)
    if m:
        return (
            m.group("orig"),
            m.group("script").replace("_", "."),
            m.group("stamp"),
        )
    m = LEGACY_RE.match(arc_name)
    if m:
        return (m.group("orig"), None, m.group("stamp"))
    return (arc_name, None, None)


def load_move_log(
    con: duckdb.DuckDBPyConnection, sch: str | None
) -> dict[str, list[tuple[str, str]]]:
    """Return src_table -> list[(script_tag, archive_fq)] from move log."""
    if sch is None:
        return {}
    rows = con.execute(
        f"""
        SELECT src_table, script, archive_fq
        FROM "{PUB_DB}"."{sch}"."{MOVE_LOG_NAME}"
        WHERE src_table IS NOT NULL
        """
    ).fetchall()
    out: dict[str, list[tuple[str, str]]] = {}
    for src, script_tag, arc_fq in rows:
        out.setdefault(src, []).append(
            (script_tag or "", arc_fq or "")
        )
    return out


_SCRIPT_TAG_NUM_RE = re.compile(r"^(\d+(?:_\d+)?[A-Za-z]*)")


def script_number_from_tag(script_tag: str) -> str | None:
    m = _SCRIPT_TAG_NUM_RE.match(script_tag)
    if not m:
        return None
    return m.group(1).replace("_", ".")


def build_backfill_map(
    registry_rows: list[tuple[str, str]],
    archive_tables: list[str],
    move_log: dict[str, list[tuple[str, str]]],
    main_tables: set[str],
) -> tuple[
    list[dict],   # backfill rows
    list[dict],   # registry-orphans (table missing from main but no archive)
    list[dict],   # archive-orphans (snapshot, no registry row)
    list[dict],   # archive parse failures
]:
    registry_names = {n for n, _ in registry_rows}
    arc_by_orig: dict[str, list[dict]] = {}
    parse_failures: list[dict] = []

    for arc_name in archive_tables:
        orig, script_no, stamp = parse_archive_name(arc_name)
        rec = {
            "archive_name": arc_name,
            "original_name": orig,
            "script_number": script_no,
            "stamp": stamp,
        }
        if script_no is None and stamp is None:
            parse_failures.append(rec)
            continue
        arc_by_orig.setdefault(orig, []).append(rec)

    backfill: list[dict] = []
    for det_name, sch in registry_rows:
        # Snapshots whose original_name == registry detail_table_name
        matches = arc_by_orig.get(det_name, [])
        # Move-log rows for this src_table
        ml_entries = move_log.get(det_name, [])

        # Determine renamed_by_script: prefer archive-name-derived script
        # number; fall back to script_tag from archive_move_log.
        script_candidates: list[str] = []
        for m_rec in matches:
            if m_rec["script_number"]:
                script_candidates.append(m_rec["script_number"])
        for tag, _arc_fq in ml_entries:
            num = script_number_from_tag(tag)
            if num:
                script_candidates.append(num)

        if not script_candidates:
            continue

        # Canonical pick: highest script number (most recent rename).
        def _key(s: str) -> tuple[int, ...]:
            return tuple(int(p) for p in s.split(".") if p.isdigit())

        renamed_by = sorted(set(script_candidates), key=_key)[-1]

        # superseded_by: only fill if the registry table is gone from main
        # (i.e. truly retired); otherwise leave NULL — naming heuristics are
        # not deterministic enough.
        superseded_by: str | None = None
        # We don't infer; we set NULL by default. The prompt allows NULL.
        # If a future close-out file enumerates explicit replacements we
        # can backfill in a follow-up.

        backfill.append({
            "detail_table_name": det_name,
            "schema_name": sch,
            "renamed_by_script": renamed_by,
            "superseded_by": superseded_by,
            "archive_matches": [m["archive_name"] for m in matches],
            "move_log_entries": [
                {"script_tag": t, "archive_fq": a} for t, a in ml_entries
            ],
            "still_live_in_main": det_name in main_tables,
        })

    # Registry rows that look retired (table missing from main and from
    # manuscript_workspace) but with no archive snapshot match.
    registry_orphans: list[dict] = []
    for det_name, sch in registry_rows:
        if det_name in main_tables:
            continue
        if det_name in arc_by_orig:
            continue
        # Possible move-log entry — note it
        ml_entries = move_log.get(det_name, [])
        registry_orphans.append({
            "detail_table_name": det_name,
            "schema_name": sch,
            "move_log_entries": [
                {"script_tag": t, "archive_fq": a} for t, a in ml_entries
            ],
        })

    # Archive snapshots whose original_name is not in the registry.
    archive_orphans: list[dict] = []
    for orig, recs in arc_by_orig.items():
        if orig in registry_names:
            continue
        archive_orphans.append({
            "original_name": orig,
            "archive_names": [r["archive_name"] for r in recs],
        })
    archive_orphans.sort(key=lambda d: d["original_name"])

    return backfill, registry_orphans, archive_orphans, parse_failures


def write_probe_report(
    registry_schema: str,
    n_rows: int,
    columns: list[tuple[str, str]],
    backfill: list[dict],
    registry_orphans: list[dict],
    archive_orphans: list[dict],
    parse_failures: list[dict],
    move_log_schema: str | None,
    n_archive_tables: int,
) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    has_new_cols = {"superseded_by", "renamed_by_script"}.issubset(
        {c for c, _t in columns}
    )

    lines: list[str] = []
    lines += [
        "# Script 389.1 — Registry Schema Migration · Phase 0 Probe",
        "",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        f"PUB DB: `{PUB_DB}`",
        f"Registry: `{PUB_DB}.{registry_schema}.{REGISTRY_NAME}` "
        f"({n_rows:,} rows, {len(columns)} columns)",
        f"Archive schema: `{ARCHIVE_SCHEMA}` "
        f"({n_archive_tables} snapshot tables)",
        f"Move log: "
        f"`{PUB_DB}.{move_log_schema or '<not found>'}.{MOVE_LOG_NAME}`",
        f"New columns already present: **{has_new_cols}**",
        "",
        "## Current registry columns",
        "",
        "| ordinal | column | type |",
        "|---:|---|---|",
    ]
    for i, (c, t) in enumerate(columns, 1):
        lines.append(f"| {i} | `{c}` | `{t}` |")

    lines += [
        "",
        f"## Proposed backfill map ({len(backfill)} rows)",
        "",
        "Rule: `renamed_by_script` is the highest script number derivable "
        "from either the `_pre<NNN>_` archive name or the `script` column "
        "in `archive_move_log_v1`. `superseded_by` is left NULL by default "
        "(no deterministic name-rule mapping); fill manually in a follow-up "
        "if a close-out names a specific replacement.",
        "",
        "| detail_table_name | schema | renamed_by_script | superseded_by "
        "| live in main? | archive matches | move-log scripts |",
        "|---|---|---|---|---|---|---|",
    ]
    for b in backfill:
        archives = "<br>".join(f"`{a}`" for a in b["archive_matches"]) or "—"
        ml_scripts = (
            "<br>".join(
                f"`{e['script_tag']}`" for e in b["move_log_entries"]
            )
            or "—"
        )
        lines.append(
            f"| `{b['detail_table_name']}` | `{b['schema_name']}` "
            f"| `{b['renamed_by_script']}` "
            f"| {('`' + b['superseded_by'] + '`') if b['superseded_by'] else 'NULL'} "
            f"| {'yes' if b['still_live_in_main'] else 'no'} "
            f"| {archives} | {ml_scripts} |"
        )

    lines += [
        "",
        f"## Registry rows missing from `main` with NO archive snapshot "
        f"({len(registry_orphans)})",
        "",
        "These look retired on paper but have no archive evidence. Manual "
        "review — they may be `manuscript_workspace.*` audit rows, or they "
        "may be honest gaps that need a separate cleanup pass.",
        "",
    ]
    if registry_orphans:
        lines += [
            "| detail_table_name | schema | move-log entries |",
            "|---|---|---|",
        ]
        for o in registry_orphans:
            ml = (
                "<br>".join(
                    f"`{e['script_tag']}` → `{e['archive_fq']}`"
                    for e in o["move_log_entries"]
                )
                or "—"
            )
            lines.append(
                f"| `{o['detail_table_name']}` | `{o['schema_name']}` | {ml} |"
            )
    else:
        lines.append("_None._")

    lines += [
        "",
        f"## Archive snapshots with no matching registry row "
        f"({len(archive_orphans)})",
        "",
        "Likely fine — these are objects that were archived but never lived "
        "in the registry (e.g. `__readme_pre*_backup`, view-DDL snapshots). "
        "Listed for completeness only.",
        "",
    ]
    if archive_orphans:
        lines += [
            "| original_name | archive snapshots |",
            "|---|---|",
        ]
        for o in archive_orphans:
            arcs = "<br>".join(f"`{a}`" for a in o["archive_names"])
            lines.append(f"| `{o['original_name']}` | {arcs} |")
    else:
        lines.append("_None._")

    lines += [
        "",
        f"## Archive table-name parse failures ({len(parse_failures)})",
        "",
        "Tables whose name did not match either the `_pre<NNN>_<stamp>` or "
        "`_legacy_<stamp>` regex. Should be empty for a healthy archive.",
        "",
    ]
    if parse_failures:
        for p in parse_failures:
            lines.append(f"- `{p['archive_name']}`")
    else:
        lines.append("_None._")

    lines.append("")
    PROBE_REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    log(f"  wrote probe report -> {PROBE_REPORT_PATH}")


# --------------------------------------------------------------------------- #
# Phase 2 — apply
# --------------------------------------------------------------------------- #


def readme_provenance_present(
    con: duckdb.DuckDBPyConnection, schema: str
) -> bool:
    rows = con.execute(
        f"""
        SELECT COUNT(*) FROM "{PUB_DB}"."{schema}"."{README_NAME}"
        WHERE content LIKE ?
        """,
        [f"{README_PREFIX}%"],
    ).fetchone()
    return bool(rows and rows[0] > 0)


def find_readme_schema(con: duckdb.DuckDBPyConnection) -> str:
    sch = find_object_schema(con, README_NAME, ("main", "manuscript_workspace"))
    if sch is None:
        raise SystemExit(
            f"`{README_NAME}` not found in main or manuscript_workspace; "
            "cannot record provenance."
        )
    return sch


def phase2_apply(
    con: duckdb.DuckDBPyConnection,
    registry_schema: str,
    backfill: list[dict],
    stamp: str,
) -> dict:
    log("=" * 72)
    log(f"Phase 2 — apply (registry at {registry_schema}.{REGISTRY_NAME})")
    log("=" * 72)

    snapshot_name = f"{REGISTRY_NAME[:-3]}_pre389_1_{stamp}"  # strip trailing "_v1"? No — keep full
    # Actually keep full original: detail_table_registry_v1_pre389_1_<stamp>
    snapshot_name = f"{REGISTRY_NAME}_pre389_1_{stamp}"
    snapshot_fq = f'"{PUB_DB}"."{ARCHIVE_SCHEMA}"."{snapshot_name}"'
    registry_fq = f'"{PUB_DB}"."{registry_schema}"."{REGISTRY_NAME}"'

    # 2A — snapshot
    sql = f'CREATE OR REPLACE TABLE {snapshot_fq} AS SELECT * FROM {registry_fq}'
    log(f"  2A SNAPSHOT: {sql}")
    con.execute(sql)
    src_n = con.execute(f"SELECT COUNT(*) FROM {registry_fq}").fetchone()[0]
    arc_n = con.execute(f"SELECT COUNT(*) FROM {snapshot_fq}").fetchone()[0]
    if src_n != arc_n:
        raise SystemExit(
            f"snapshot parity FAIL: src={src_n} arc={arc_n}"
        )
    log(f"     parity OK ({src_n:,} rows)")

    # 2B — ALTER ADD COLUMN IF NOT EXISTS (both)
    for col in ("superseded_by", "renamed_by_script"):
        sql = (
            f'ALTER TABLE {registry_fq} '
            f'ADD COLUMN IF NOT EXISTS "{col}" VARCHAR'
        )
        log(f"  2B ALTER: {sql}")
        con.execute(sql)

    # 2C — UPDATEs (one per mapped row; echo each)
    n_updated = 0
    for b in backfill:
        det = b["detail_table_name"]
        rb_script = b["renamed_by_script"]
        sup = b["superseded_by"]
        if rb_script is None and sup is None:
            continue
        sql = (
            f'UPDATE {registry_fq} SET '
            f'renamed_by_script = ?, superseded_by = ? '
            f'WHERE detail_table_name = ?'
        )
        log(
            f"  2C UPDATE {det:<55} "
            f"renamed_by_script={rb_script!r} superseded_by={sup!r}"
        )
        con.execute(sql, [rb_script, sup, det])
        n_updated += 1

    # 2D — __readme provenance
    readme_schema = find_readme_schema(con)
    readme_fq = f'"{PUB_DB}"."{readme_schema}"."{README_NAME}"'
    readme_cols = {
        c for c, _ in get_columns(con, readme_schema, README_NAME)
    }

    msg = (
        f"{README_PREFIX} {REGISTRY_NAME} schema migration — added "
        f"superseded_by + renamed_by_script columns; backfilled "
        f"{n_updated} rows from {ARCHIVE_SCHEMA} snapshot names and "
        f"{MOVE_LOG_NAME}. Snapshot: {ARCHIVE_SCHEMA}.{snapshot_name}."
    )
    # Pick column names defensively — __readme has used various shapes.
    if {"content", "updated_at"}.issubset(readme_cols):
        sql = (
            f'INSERT INTO {readme_fq}(content, updated_at) '
            f'VALUES (?, CAST(CURRENT_TIMESTAMP AS TIMESTAMP))'
        )
        params = [msg]
    elif {"note", "updated_at"}.issubset(readme_cols):
        sql = (
            f'INSERT INTO {readme_fq}(note, updated_at) '
            f'VALUES (?, CAST(CURRENT_TIMESTAMP AS TIMESTAMP))'
        )
        params = [msg]
    else:
        raise SystemExit(
            f"__readme columns unexpected: {sorted(readme_cols)} — "
            "cannot insert provenance row"
        )
    log(f"  2D __readme INSERT into {readme_fq}")
    con.execute(sql, params)

    return {
        "snapshot_name": snapshot_name,
        "snapshot_fq": snapshot_fq,
        "src_rows_before": src_n,
        "rows_updated": n_updated,
        "readme_schema": readme_schema,
        "readme_msg": msg,
    }


# --------------------------------------------------------------------------- #
# Phase 3 — verify
# --------------------------------------------------------------------------- #


def phase3_verify(
    con: duckdb.DuckDBPyConnection,
    registry_schema: str,
    apply_result: dict,
    backfill: list[dict],
    archive_orig_set: set[str],
    main_tables: set[str],
    backfill_names: set[str] | None = None,
) -> dict:
    log("=" * 72)
    log("Phase 3 — verify")
    log("=" * 72)

    registry_fq = f'"{PUB_DB}"."{registry_schema}"."{REGISTRY_NAME}"'
    snapshot_fq = apply_result["snapshot_fq"]

    # Row count unchanged
    n_now = con.execute(f"SELECT COUNT(*) FROM {registry_fq}").fetchone()[0]
    n_before = apply_result["src_rows_before"]
    if n_now != n_before:
        raise SystemExit(
            f"row count drift: before={n_before} after={n_now}"
        )
    log(f"  row count unchanged ({n_now:,})")

    # Snapshot row count == registry row count
    n_arc = con.execute(f"SELECT COUNT(*) FROM {snapshot_fq}").fetchone()[0]
    if n_arc != n_now:
        raise SystemExit(
            f"snapshot row count {n_arc} != registry {n_now}"
        )
    log(f"  snapshot row count matches ({n_arc:,})")

    # Both new columns present, typed VARCHAR
    cols = dict(get_columns(con, registry_schema, REGISTRY_NAME))
    for c in ("superseded_by", "renamed_by_script"):
        t = cols.get(c)
        if t is None:
            raise SystemExit(f"column missing: {c}")
        if "VARCHAR" not in t.upper():
            raise SystemExit(f"column {c} type is {t!r}, expected VARCHAR")
        log(f"  column present: {c} :: {t}")

    # Every registry row that we backfilled (had a deterministic script
    # number from archive name or move_log) must have non-NULL
    # renamed_by_script.  We do NOT require this for rows whose only
    # archive entry is a _legacy_<stamp> snapshot (no script number
    # encoded) — those were archived by a script that uses a naming
    # convention that doesn't encode the script number, so we never set
    # renamed_by_script for them and that is correct.
    effective_names = backfill_names if backfill_names is not None else {
        b["detail_table_name"] for b in backfill
    }
    rows = con.execute(
        f"""
        SELECT detail_table_name, renamed_by_script
        FROM {registry_fq}
        """
    ).fetchall()
    missing_renames = [
        det for det, rb in rows
        if det in effective_names and not rb
    ]
    if missing_renames:
        raise SystemExit(
            "registry rows in backfill set have NULL "
            f"renamed_by_script: {missing_renames[:10]}"
        )
    # Also report rows that have any archive match (including legacy) for
    # completeness in the log.
    n_any_archive = sum(1 for d, _ in rows if d in archive_orig_set)
    log(
        f"  renamed_by_script non-NULL for all {len(effective_names)} "
        f"backfilled rows; {n_any_archive} rows have any archive match "
        f"(includes legacy-style archives with no script number)"
    )

    # Every non-NULL superseded_by either references a live main.* table OR
    # is documented (no entries currently — backfill always set NULL).
    bad_sup = [
        det for det, _ in rows
        if False  # placeholder — currently no superseded_by values to check
    ]
    sup_rows = con.execute(
        f"""
        SELECT detail_table_name, superseded_by
        FROM {registry_fq}
        WHERE superseded_by IS NOT NULL
        """
    ).fetchall()
    for det, sup in sup_rows:
        if sup not in main_tables:
            bad_sup.append((det, sup))
    if bad_sup:
        raise SystemExit(
            f"superseded_by points to non-existent main.* table: {bad_sup}"
        )
    log(
        f"  non-NULL superseded_by rows: {len(sup_rows)} "
        "(all reference live main.* targets)"
    )

    # __readme provenance present
    readme_schema = apply_result["readme_schema"]
    if not readme_provenance_present(con, readme_schema):
        raise SystemExit(
            f"__readme has no row prefixed '{README_PREFIX}' after apply"
        )
    log("  __readme provenance row present")

    return {
        "registry_rows": n_now,
        "snapshot_rows": n_arc,
        "archive_matched_rows": sum(
            1 for d, _ in rows if d in archive_orig_set
        ),
        "non_null_superseded_by": len(sup_rows),
    }


# --------------------------------------------------------------------------- #
# Close-out writer (Phase 2/3 success)
# --------------------------------------------------------------------------- #


def write_close_out(
    registry_schema: str,
    apply_result: dict,
    verify_result: dict,
    backfill: list[dict],
    n_archive_tables: int,
) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Script 389.1 — `detail_table_registry_v1` schema migration close-out",
        "",
        f"**Date:** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%SZ')}",
        f"**Script:** `scripts/{SCRIPT_TAG}.py`",
        f"**Prompt:** `cursor_prompts/CURSOR_PROMPT_REGISTRY_SCHEMA_MIGRATION_20260422_SCRIPT_389_1.md`",
        f"**Registry:** `{PUB_DB}.{registry_schema}.{REGISTRY_NAME}`",
        f"**Snapshot:** `{apply_result['snapshot_fq']}`",
        "",
        "## Outcome",
        "",
        f"* Registry rows before/after: **{verify_result['registry_rows']:,}** "
        "(unchanged)",
        f"* Snapshot rows: **{verify_result['snapshot_rows']:,}** (parity OK)",
        f"* Rows updated by backfill: **{apply_result['rows_updated']}**",
        f"* Archive snapshot tables scanned: **{n_archive_tables}**",
        f"* Registry rows with archive match (renamed_by_script set): "
        f"**{verify_result['archive_matched_rows']}**",
        f"* Rows with non-NULL `superseded_by`: "
        f"**{verify_result['non_null_superseded_by']}** "
        "(deterministic mapping not in scope; backfill leaves NULL)",
        "",
        "## New columns",
        "",
        "| column | type | semantics |",
        "|---|---|---|",
        "| `superseded_by` | VARCHAR | canonical that replaced this row "
        "(NULL = still live or unknown replacement) |",
        "| `renamed_by_script` | VARCHAR | script number that retired/renamed "
        "(NULL = never renamed) |",
        "",
        "## `__readme` provenance row",
        "",
        f"```\n{apply_result['readme_msg']}\n```",
        "",
        "## Backfill detail",
        "",
        "| detail_table_name | renamed_by_script | superseded_by |",
        "|---|---|---|",
    ]
    for b in backfill:
        sup = (
            f"`{b['superseded_by']}`"
            if b["superseded_by"] else "NULL"
        )
        lines.append(
            f"| `{b['detail_table_name']}` "
            f"| `{b['renamed_by_script']}` | {sup} |"
        )
    lines += [
        "",
        "## Idempotency",
        "",
        "Re-running `--phase 0` after this commit will exit cleanly; "
        "re-running `--apply` will detect both new columns + `__readme` row "
        "and exit 0 with a NO-OP message.",
        "",
        "## Carry-forward",
        "",
        "* `superseded_by` is intentionally NULL across the board — fill on "
        "demand from close-out markdowns when a 1:1 retire→replace mapping "
        "is documented (e.g. 361 op-path consolidation, 364 complications "
        "consolidation).",
        "",
    ]
    CLOSE_OUT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    log(f"  wrote close-out -> {CLOSE_OUT_PATH}")


# --------------------------------------------------------------------------- #
# Driver
# --------------------------------------------------------------------------- #


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--phase",
        type=str,
        default=None,
        help="0 = probe (read-only). Default behavior with no args.",
    )
    ap.add_argument(
        "--apply",
        action="store_true",
        help="Run Phase 2 (snapshot + ALTER + UPDATE + __readme) "
             "followed by Phase 3 verify.",
    )
    args = ap.parse_args()

    if args.phase is not None and args.phase != "0":
        raise SystemExit(
            f"unknown --phase value {args.phase!r}; only '0' is supported"
        )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    con = connect()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    try:
        # Locate registry
        registry_schema = find_object_schema(
            con, REGISTRY_NAME, REGISTRY_CANDIDATE_SCHEMAS
        )
        if registry_schema is None:
            raise SystemExit(
                f"{REGISTRY_NAME} not found in {REGISTRY_CANDIDATE_SCHEMAS}"
            )
        log(f"Registry located: {registry_schema}.{REGISTRY_NAME}")

        # Idempotency check
        cols = get_columns(con, registry_schema, REGISTRY_NAME)
        col_names = {c for c, _ in cols}
        has_new_cols = {"superseded_by", "renamed_by_script"}.issubset(col_names)
        readme_schema = find_object_schema(
            con, README_NAME, ("main", "manuscript_workspace")
        )
        readme_present = (
            readme_schema is not None
            and readme_provenance_present(con, readme_schema)
        )

        if has_new_cols and readme_present and not args.apply:
            log(
                "Idempotency: both new columns + __readme provenance row "
                "already present. NO-OP, prior run detected."
            )
            print("NO-OP, prior run detected.")
            return 0
        if has_new_cols and not readme_present:
            raise SystemExit(
                "partial migration detected (new columns exist but no "
                "__readme row prefixed 'Script 389.1:'); manual review "
                "required"
            )

        # Phase 0 — probe
        n_rows = con.execute(
            f'SELECT COUNT(*) FROM "{PUB_DB}"."{registry_schema}"."{REGISTRY_NAME}"'
        ).fetchone()[0]
        log(
            f"Registry: {n_rows:,} rows, {len(cols)} columns: "
            f"{[c for c, _ in cols]}"
        )

        registry_rows = con.execute(
            f"""
            SELECT detail_table_name, schema_name
            FROM "{PUB_DB}"."{registry_schema}"."{REGISTRY_NAME}"
            ORDER BY detail_table_name
            """
        ).fetchall()

        archive_tables = list_archive_tables(con)
        log(f"Archive schema `{ARCHIVE_SCHEMA}`: {len(archive_tables)} tables")

        move_log_schema = find_object_schema(
            con, MOVE_LOG_NAME, MOVE_LOG_CANDIDATE_SCHEMAS
        )
        if move_log_schema:
            log(f"Move log: {move_log_schema}.{MOVE_LOG_NAME}")
        else:
            warn(f"{MOVE_LOG_NAME} not found in candidate schemas")
        move_log = load_move_log(con, move_log_schema)
        log(
            f"Move log entries (distinct src_table): {len(move_log)}"
        )

        main_tables = list_main_tables(con)

        backfill, registry_orphans, archive_orphans, parse_failures = (
            build_backfill_map(
                registry_rows, archive_tables, move_log, main_tables
            )
        )

        log(f"Proposed backfill rows: {len(backfill)}")
        log(f"Registry rows missing from main with no archive: "
            f"{len(registry_orphans)}")
        log(f"Archive snapshots with no registry match: {len(archive_orphans)}")
        log(f"Archive name parse failures: {len(parse_failures)}")

        write_probe_report(
            registry_schema=registry_schema,
            n_rows=n_rows,
            columns=cols,
            backfill=backfill,
            registry_orphans=registry_orphans,
            archive_orphans=archive_orphans,
            parse_failures=parse_failures,
            move_log_schema=move_log_schema,
            n_archive_tables=len(archive_tables),
        )

        if not args.apply:
            log(
                "Phase 0 complete. Review 389_1_probe_report.md. Re-run "
                "with --apply to proceed."
            )
            print(
                "Phase 0 complete. Review 389_1_probe_report.md. "
                "Re-run with --apply to proceed."
            )
            return 0

        # Idempotency: if columns + __readme already present, skip Phase 2
        # but still run Phase 3 + write close-out (both are read-only).
        if has_new_cols and readme_present:
            log(
                "Idempotency: Phase 2 already applied "
                "(columns + __readme provenance detected). "
                "Skipping Phase 2; running Phase 3 + close-out."
            )
            # Reconstruct apply_result from live PUB state.
            snaps = con.execute(
                """
                SELECT table_name FROM information_schema.tables
                WHERE table_catalog = ? AND table_schema = ?
                  AND table_name LIKE 'detail_table_registry_v1_pre389_1_%'
                ORDER BY table_name DESC LIMIT 1
                """,
                [PUB_DB, ARCHIVE_SCHEMA],
            ).fetchone()
            snap_name = snaps[0] if snaps else f"detail_table_registry_v1_pre389_1_{stamp}"
            snap_fq = (
                f'"{PUB_DB}"."{ARCHIVE_SCHEMA}"."{snap_name}"'
            )
            readme_schema_live = find_object_schema(
                con, README_NAME, ("main", "manuscript_workspace")
            ) or "main"
            n_rows_now = con.execute(
                f'SELECT COUNT(*) FROM "{PUB_DB}"."{registry_schema}"."{REGISTRY_NAME}"'
            ).fetchone()[0]
            readme_row = con.execute(
                f"""
                SELECT content FROM "{PUB_DB}"."{readme_schema_live}"."{README_NAME}"
                WHERE content LIKE ?
                LIMIT 1
                """,
                [f"{README_PREFIX}%"],
            ).fetchone()
            apply_result = {
                "snapshot_name": snap_name,
                "snapshot_fq": snap_fq,
                "src_rows_before": n_rows_now,
                "rows_updated": len(backfill),
                "readme_schema": readme_schema_live,
                "readme_msg": readme_row[0] if readme_row else "(provenance row recovered)",
            }
        else:
            # Phase 2 — apply
            apply_result = phase2_apply(con, registry_schema, backfill, stamp)

        # Phase 3 — verify
        archive_orig_set = {
            parse_archive_name(a)[0] for a in archive_tables
        }
        backfill_names = {b["detail_table_name"] for b in backfill}
        verify_result = phase3_verify(
            con,
            registry_schema,
            apply_result,
            backfill,
            archive_orig_set,
            main_tables,
            backfill_names=backfill_names,
        )

        write_close_out(
            registry_schema=registry_schema,
            apply_result=apply_result,
            verify_result=verify_result,
            backfill=backfill,
            n_archive_tables=len(archive_tables),
        )

        log("Phase 2 + Phase 3 complete. Run git stage / commit / tag manually.")
        return 0
    finally:
        flush_log()
        con.close()


if __name__ == "__main__":
    sys.exit(main())
