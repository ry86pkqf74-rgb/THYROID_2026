#!/usr/bin/env python3
"""Script 270c — Phase B planning (dry-run only).

Emits a disposition manifest for every table/view in the canonical DB
main schema and every object in the four stray schemas of the archive DB
"Thyroid 2026 UPdated" (main, mm_contract_dev, qa, v2_stage).

NO destructive writes against MotherDuck — only read queries plus one
audit row insert on completion. Output is CSV/JSON/MD artifacts for
human review before Script 270d (the destructive execute phase).

Pipeline:
  1. Connect (locked search path; CPM invariant asserted by _md_connect).
  2. Round-trip restore test against an existing archive_pub_v1_0
     snapshot — halt if it fails.
  3. Pre-fetch keep list, registry, queryable enumeration of main.
  4. Classify every main-schema base table per the 6-rule eligibility
     ladder; classify every main-schema view per its referenced tables.
  5. Classify every object in the four stray archive schemas.
  6. Budget pre-flight check — halt if any of the four budgets trip.
  7. Emit 6 artifacts.
  8. Insert one audit row.

Tag anchor: v1_0_registry_locked (commit 117f55d).

DO NOT run any destructive write here. 270d handles execution after the
manifests are reviewed by a human (and possibly a clinical collaborator).
"""
from __future__ import annotations

import csv
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
OUT_DIR = REPO / "scripts" / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_MANIFEST_CSV = OUT_DIR / "270c_phase_b_disposition_manifest.csv"
OUT_VIEWS_CSV = OUT_DIR / "270c_views_compile_impact.csv"
OUT_STRAY_CSV = OUT_DIR / "270c_stray_schema_consolidation.csv"
OUT_BUDGETS_JSON = OUT_DIR / "270c_budgets.json"
OUT_RESTORE_JSON = OUT_DIR / "270c_restore_test.json"
OUT_SUMMARY_MD = OUT_DIR / "270c_planning_summary.md"
OUT_LOG = OUT_DIR / "270c_phase_b_planning.log"

ARCHIVE_DB = "Thyroid 2026 UPdated"
ARCHIVE_SCHEMA_PUB = "archive_pub_v1_0"
ARCHIVE_SCHEMA_LEGACY = "archive_legacy"
STRAY_SCHEMAS = ("main", "mm_contract_dev", "qa", "v2_stage")

WS = f'"{PUBLICATION_DB}".manuscript_workspace'
KEEP_LIST_FQ = f"{WS}.main_schema_keep_list_v1"
REGISTRY_FQ = f"{WS}.detail_table_registry_v1"
AUDIT_FQ = f"{WS}.v1_1_finalization_audit_v1"

# Budgets
BUDGET_MAX_ARCHIVE_CANDIDATES = 250
BUDGET_MAX_STRAY_DROPS = 250
BUDGET_MAX_TOTAL_ROWS_IN_ARCHIVE = 50_000_000
BUDGET_HUMAN_REVIEW_ROW_THRESHOLD = 10_000_000

NOTE_ENTITIES_RE = re.compile(r"^note_entities_.*$")

# Snapshot-name detector for stray-vs-pub matching. Per Phase B Step 5
# bug fix: archive_pub_v1_0 snapshots are named
#   <table_name>_pre<script_num>_<UTC>            (e.g., _pre270_20260417...)
# or, for legacy backup conventions,
#   <table_name>_<anything>backup<anything>
# The original 270c matcher compared exact names only and reported zero
# DROP_ALREADY_SNAPSHOTTED. The corrected matcher uses these patterns.
SNAPSHOT_PATTERN_TPL = r"^{name}_(?:pre\d+|.*backup.*)"

CPM_NAME = "canonical_patient_master"

V1_1_TECH_DEBT_LINK = "registry_null_residual_v1_1"


# =============================================================================
# Utilities
# =============================================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_stamp(d: datetime) -> str:
    return d.strftime("%Y%m%dT%H%M%SZ")


def safe_count(con, fq_table: str) -> tuple[int | None, str | None]:
    """Return (row_count, None) or (None, error_message)."""
    try:
        n = con.execute(f"SELECT COUNT(*) FROM {fq_table}").fetchone()[0]
        return int(n), None
    except Exception as e:
        return None, str(e)[:160]


def queryable_main_objects(con) -> dict[str, dict]:
    """Return {name: {object_type, queryable, row_count, error}} for
    everything in canonical main schema. object_type ∈ {BASE TABLE, VIEW}.
    Skips catalog ghosts (info_schema present but SELECT fails).

    Uses duckdb_tables() + duckdb_views() (more reliable than
    information_schema.tables, which sometimes drops views in MotherDuck-
    attached databases per the catalog_vs_queryable_drift convention).
    """
    table_rows = con.execute(f"""
        SELECT table_name FROM duckdb_tables()
        WHERE database_name = ? AND schema_name = 'main'
        ORDER BY table_name
    """, [PUBLICATION_DB]).fetchall()
    view_rows = con.execute(f"""
        SELECT view_name FROM duckdb_views()
        WHERE database_name = ? AND schema_name = 'main'
        ORDER BY view_name
    """, [PUBLICATION_DB]).fetchall()
    out: dict[str, dict] = {}
    for (name,) in table_rows:
        fq = f'"{PUBLICATION_DB}".main."{name}"'
        n, err = safe_count(con, fq)
        out[name] = {
            "object_type": "BASE TABLE",
            "queryable": err is None,
            "row_count": n,
            "error": err,
        }
    for (name,) in view_rows:
        if name in out:
            # name collision shouldn't happen; prefer table classification
            continue
        fq = f'"{PUBLICATION_DB}".main."{name}"'
        n, err = safe_count(con, fq)
        out[name] = {
            "object_type": "VIEW",
            "queryable": err is None,
            "row_count": n,
            "error": err,
        }
    return out


def fetch_keep_list(con) -> dict[str, dict]:
    rows = con.execute(f"""
        SELECT table_name, schema_at_registration, keep_reason,
               registered_by_script, notes
        FROM {KEEP_LIST_FQ}
        ORDER BY table_name
    """).fetchall()
    out: dict[str, dict] = {}
    for name, sch, reason, by_script, notes in rows:
        out[name] = {
            "schema_at_registration": sch,
            "keep_reason": reason,
            "registered_by_script": by_script,
            "notes": notes,
        }
    return out


def fetch_registry(con) -> dict[str, dict]:
    rows = con.execute(f"""
        SELECT detail_table_name, schema_name, total_patients,
               feeds_master_columns_normalized
        FROM {REGISTRY_FQ}
    """).fetchall()
    out: dict[str, dict] = {}
    for name, schema, tp, norm in rows:
        norm_str = (norm or "").strip()
        has_norm = bool(norm_str) and "TODO" not in norm_str.upper()
        out[name] = {
            "schema_name": schema,
            "total_patients": int(tp) if tp is not None else 0,
            "normalized_str": norm_str,
            "has_normalized": has_norm,
            "n_feed_cols": (
                len({tok.strip() for tok in norm_str.split(";") if tok.strip()})
                if has_norm else 0
            ),
        }
    return out


def fetch_view_definitions_main(con) -> dict[str, str]:
    """Return {view_name: view_sql}. Empty string if definition unavailable.

    Prefer duckdb_views() (.sql column) — information_schema.views can be
    incomplete on MotherDuck-attached databases.
    """
    rows = con.execute(f"""
        SELECT view_name, sql
        FROM duckdb_views()
        WHERE database_name = ? AND schema_name = 'main'
    """, [PUBLICATION_DB]).fetchall()
    return {name: (defn or "") for name, defn in rows}


def fetch_archive_db_objects(con, db: str, schemas: tuple[str, ...]) -> list[dict]:
    """Return list of {schema, name, object_type, row_count, error} for
    every object in the named schemas of the named database."""
    placeholders = ",".join(repr(s) for s in schemas)
    rows = con.execute(f"""
        SELECT schema_name, table_name, 'BASE TABLE' AS object_type
        FROM duckdb_tables()
        WHERE database_name = ?
          AND schema_name IN ({placeholders})
        UNION ALL
        SELECT schema_name, view_name, 'VIEW' AS object_type
        FROM duckdb_views()
        WHERE database_name = ?
          AND schema_name IN ({placeholders})
        ORDER BY 1, 2
    """, [db, db]).fetchall()
    out: list[dict] = []
    for sch, name, ttype in rows:
        fq = f'"{db}"."{sch}"."{name}"'
        n, err = safe_count(con, fq)
        out.append({
            "schema": sch,
            "name": name,
            "object_type": ttype,
            "row_count": n,
            "error": err,
        })
    return out


def fetch_archive_schema_object_names(con, db: str, schema: str) -> set[str]:
    """Return set of object names (tables + views) in the given schema."""
    try:
        rows = con.execute(f"""
            SELECT table_name FROM duckdb_tables()
            WHERE database_name = ? AND schema_name = ?
            UNION
            SELECT view_name FROM duckdb_views()
            WHERE database_name = ? AND schema_name = ?
        """, [db, schema, db, schema]).fetchall()
        return {r[0] for r in rows}
    except Exception:
        return set()


def fetch_archive_schema_object_rowcounts(con, db: str, schema: str) -> dict[str, int]:
    """Return {name: row_count} for tables in schema. Views skipped."""
    try:
        rows = con.execute(f"""
            SELECT table_name FROM duckdb_tables()
            WHERE database_name = ? AND schema_name = ?
        """, [db, schema]).fetchall()
    except Exception:
        return {}
    out: dict[str, int] = {}
    for (name,) in rows:
        fq = f'"{db}"."{schema}"."{name}"'
        n, err = safe_count(con, fq)
        if err is None and n is not None:
            out[name] = n
    return out


# =============================================================================
# Round-trip restore test
# =============================================================================

def restore_test(con, log) -> dict:
    """Pick one existing canonical_patient_master_pre* snapshot in
    archive_pub_v1_0 and round-trip it through a TEMPORARY TABLE.

    Asserts row count + column count + column type match.
    Drops the temp table. Returns a JSON-serializable dict.
    """
    log("\n--- ROUND-TRIP RESTORE TEST ---")
    candidates = con.execute(f"""
        SELECT table_name FROM duckdb_tables()
        WHERE database_name = ?
          AND schema_name   = ?
          AND table_name LIKE 'canonical_patient_master_pre%'
        ORDER BY table_name DESC
    """, [ARCHIVE_DB, ARCHIVE_SCHEMA_PUB]).fetchall()

    if not candidates:
        msg = (
            f"no canonical_patient_master_pre* snapshots found in "
            f"\"{ARCHIVE_DB}\".{ARCHIVE_SCHEMA_PUB} — cannot run restore test"
        )
        log(f"  FAIL: {msg}")
        return {"status": "FAIL", "reason": msg}

    chosen = candidates[0][0]
    src_fq = f'"{ARCHIVE_DB}"."{ARCHIVE_SCHEMA_PUB}"."{chosen}"'
    log(f"  chosen snapshot: {src_fq}")

    src_n = con.execute(f"SELECT COUNT(*) FROM {src_fq}").fetchone()[0]
    src_cols_rows = con.execute(f"DESCRIBE {src_fq}").fetchall()
    src_cols = {r[0]: r[1] for r in src_cols_rows}
    log(f"  source: {src_n} rows, {len(src_cols)} cols")

    temp_name = f"restore_test_{utc_stamp(utc_now())}"
    try:
        con.execute(
            f"CREATE TEMPORARY TABLE {temp_name} AS SELECT * FROM {src_fq}"
        )
    except Exception as e:
        msg = f"CREATE TEMPORARY TABLE failed: {str(e)[:200]}"
        log(f"  FAIL: {msg}")
        return {
            "status": "FAIL",
            "reason": msg,
            "chosen_snapshot": src_fq,
            "source_row_count": int(src_n),
            "source_column_count": len(src_cols),
        }

    tmp_n = con.execute(f"SELECT COUNT(*) FROM {temp_name}").fetchone()[0]
    tmp_cols_rows = con.execute(f"DESCRIBE {temp_name}").fetchall()
    tmp_cols = {r[0]: r[1] for r in tmp_cols_rows}

    failures: list[str] = []
    if int(tmp_n) != int(src_n):
        failures.append(f"row count mismatch: src={src_n} tmp={tmp_n}")
    if len(tmp_cols) != len(src_cols):
        failures.append(
            f"column count mismatch: src={len(src_cols)} tmp={len(tmp_cols)}"
        )
    type_diffs = []
    for col, src_type in src_cols.items():
        if col not in tmp_cols:
            type_diffs.append(f"missing in tmp: {col}")
            continue
        if tmp_cols[col] != src_type:
            type_diffs.append(
                f"{col}: src={src_type} tmp={tmp_cols[col]}"
            )
    if type_diffs:
        failures.append(f"column type diffs: {type_diffs[:5]}"
                        + (" ..." if len(type_diffs) > 5 else ""))

    try:
        con.execute(f"DROP TABLE {temp_name}")
        dropped = True
    except Exception as e:
        dropped = False
        failures.append(f"drop temp failed: {str(e)[:120]}")

    result = {
        "status": "PASS" if not failures else "FAIL",
        "chosen_snapshot": src_fq,
        "source_row_count": int(src_n),
        "source_column_count": len(src_cols),
        "temp_table_name": temp_name,
        "temp_row_count": int(tmp_n),
        "temp_column_count": len(tmp_cols),
        "type_diffs": type_diffs,
        "temp_table_dropped": dropped,
        "failures": failures,
    }
    if failures:
        log(f"  FAIL: {failures}")
    else:
        log(
            f"  PASS: round-trip {tmp_n} rows / {len(tmp_cols)} cols "
            f"verified, temp table dropped"
        )
    return result


# =============================================================================
# Canonical-main eligibility
# =============================================================================

def classify_main_table(
    name: str,
    info: dict,
    keep_list: dict[str, dict],
    registry: dict[str, dict],
) -> dict:
    """Apply the 6-rule eligibility ladder. Returns a manifest row dict.

    Rules (per Phase B planning prompt):
      1. T = canonical_patient_master                     -> KEEP_SPINE
      2. T in keep_list                                   -> KEEP_KEEP_LIST
      3. T in registry with non-empty normalized          -> KEEP_REGISTRY_FEEDER
      4. T name ~ ^note_entities_.*$                      -> KEEP_NOTE_ENTITIES
      5. T in registry with EMPTY/NULL normalized         -> KEEP_PENDING_V1_1_DECISION
      6. otherwise                                         -> ARCHIVE_CANDIDATE
    """
    row = {
        "schema": "main",
        "table_name": name,
        "object_type": info["object_type"],
        "row_count": info["row_count"],
        "disposition": None,
        "justification": None,
        "feeder_mapping": None,
        "keep_list_entry": None,
        "archive_target": None,
        "phase_b_execute_order": None,
    }

    if name == CPM_NAME:
        row["disposition"] = "KEEP_SPINE"
        row["justification"] = "canonical_patient_master is the cohort spine"
        return row

    if name in keep_list:
        kl = keep_list[name]
        row["disposition"] = "KEEP_KEEP_LIST"
        row["justification"] = (
            f"main_schema_keep_list_v1 entry: keep_reason="
            f"{kl['keep_reason']!r} registered_by={kl['registered_by_script']}"
        )
        row["keep_list_entry"] = kl["keep_reason"]
        return row

    reg = registry.get(name)
    if reg and reg["has_normalized"]:
        row["disposition"] = "KEEP_REGISTRY_FEEDER"
        row["justification"] = (
            f"detail_table_registry_v1 active feeder "
            f"({reg['n_feed_cols']} normalized cols, "
            f"total_patients={reg['total_patients']})"
        )
        row["feeder_mapping"] = (
            reg["normalized_str"][:200]
            + ("..." if len(reg["normalized_str"]) > 200 else "")
        )
        return row

    if NOTE_ENTITIES_RE.match(name):
        row["disposition"] = "KEEP_NOTE_ENTITIES"
        row["justification"] = (
            "matches ^note_entities_.* (live NLP detail layer per Script 270 spec)"
        )
        return row

    if reg and not reg["has_normalized"]:
        row["disposition"] = "KEEP_PENDING_V1_1_DECISION"
        row["justification"] = (
            f"detail_table_registry_v1 row with EMPTY/NULL "
            f"feeds_master_columns_normalized — deferred to v1_1 per "
            f"v1_1_tech_debt_v1.debt_id={V1_1_TECH_DEBT_LINK!r}"
        )
        return row

    row["disposition"] = "ARCHIVE_CANDIDATE"
    row["justification"] = (
        "not CPM, not in keep_list, not a registry feeder, not "
        "note_entities_*, not in registry-NULL residual"
    )
    return row


def find_view_table_refs(view_sql: str, candidate_tables: list[str]) -> list[str]:
    """Whole-word match each candidate table name in the view SQL."""
    if not view_sql:
        return []
    hits: list[str] = []
    for t in candidate_tables:
        # Word-boundary match avoids substring false positives.
        if re.search(rf"(?<![A-Za-z0-9_]){re.escape(t)}(?![A-Za-z0-9_])",
                     view_sql):
            hits.append(t)
    return hits


def classify_main_view(
    name: str,
    info: dict,
    keep_list: dict[str, dict],
    archive_candidate_tables: set[str],
    view_defs: dict[str, str],
) -> tuple[dict, list[dict]]:
    """Returns (manifest_row, view_compile_impact_rows).

    Default disposition for views is KEEP unless every referenced
    main-schema base table is an ARCHIVE_CANDIDATE.
    """
    row = {
        "schema": "main",
        "table_name": name,
        "object_type": info["object_type"],
        "row_count": info["row_count"],
        "disposition": None,
        "justification": None,
        "feeder_mapping": None,
        "keep_list_entry": None,
        "archive_target": None,
        "phase_b_execute_order": None,
    }

    if name in keep_list:
        kl = keep_list[name]
        row["disposition"] = "KEEP_KEEP_LIST"
        row["justification"] = (
            f"main_schema_keep_list_v1 view entry: keep_reason="
            f"{kl['keep_reason']!r}"
        )
        row["keep_list_entry"] = kl["keep_reason"]
        return row, []

    sql = view_defs.get(name, "")
    refs = find_view_table_refs(sql, sorted(archive_candidate_tables))

    impact_rows: list[dict] = []
    for ref in refs:
        impact_rows.append({
            "view_name": name,
            "references_table": ref,
            "will_break_on_archive": True,
            "proposed_handling": "archive_ddl_then_drop_view",
        })

    if refs:
        row["disposition"] = "VIEW_COMPILE_WILL_BREAK"
        row["justification"] = (
            f"references {len(refs)} ARCHIVE_CANDIDATE table(s); "
            f"270d must archive view DDL before dropping underlying tables"
        )
    else:
        row["disposition"] = "KEEP_VIEW"
        row["justification"] = (
            "view does not reference any ARCHIVE_CANDIDATE tables"
        )
    return row, impact_rows


# =============================================================================
# Stray-schema disposition
# =============================================================================

def find_matching_snapshots(
    stray_name: str,
    snapshots: dict[str, int],
) -> list[tuple[str, int]]:
    """Return [(snapshot_name, row_count), ...] for snapshots in
    archive_pub_v1_0 whose names match the suffix patterns:

      <stray_name>_pre<digits>_<...>   (canonical script-snapshot pattern)
      <stray_name>_<...>backup<...>    (legacy backup convention)

    Patterns intentionally exclude bare-name match (different table) and
    require concrete suffix indicators to avoid false positives like
    `<stray_name>_predictions` matching against `_pre`.
    """
    pat = re.compile(
        SNAPSHOT_PATTERN_TPL.format(name=re.escape(stray_name)),
        re.IGNORECASE,
    )
    return [
        (snap_name, rc) for snap_name, rc in snapshots.items()
        if pat.match(snap_name)
    ]


def classify_stray_object(
    obj: dict,
    pub_names: set[str],
    legacy_names: set[str],
    pub_rowcounts: dict[str, int],
) -> dict:
    """Disposition for one stray object (table or view).

    Order of checks (per Phase B Step 5 bug fix):
      1. Snapshot-suffix match in archive_pub_v1_0 with equal row count
         -> DROP_ALREADY_SNAPSHOTTED.
      2. Snapshot-suffix match in archive_pub_v1_0 with differing row
         count -> DIVERGENT (halt for human review; 270d refuses to
         proceed if any DIVERGENT row is present).
      3. Empty base table -> DROP_NO_RESTORE_VALUE.
      4. Non-queryable -> DROP_NO_RESTORE_VALUE.
      5. Otherwise -> MIGRATE_TO_ARCHIVE_LEGACY.
    """
    name = obj["name"]
    sch = obj["schema"]
    rc = obj["row_count"]
    out = {
        "schema": sch,
        "name": name,
        "object_type": obj["object_type"],
        "row_count": rc,
        "disposition": None,
        "identical_in_archive_pub_v1_0": False,
        "identical_in_archive_legacy": False,
        "proposed_target_name": None,
        "justification": None,
    }

    legacy_match = name in legacy_names
    out["identical_in_archive_legacy"] = bool(legacy_match)

    # 1 + 2: snapshot-suffix match (corrected from exact-name match)
    matches = find_matching_snapshots(name, pub_rowcounts)
    if matches and rc is not None and obj["object_type"] == "BASE TABLE":
        exact = [(s, c) for s, c in matches if c == rc]
        if exact:
            out["disposition"] = "DROP_ALREADY_SNAPSHOTTED"
            out["identical_in_archive_pub_v1_0"] = True
            primary = exact[0]
            extra = (
                f" (+{len(exact) - 1} other exact-rowcount matches)"
                if len(exact) > 1 else ""
            )
            out["justification"] = (
                f"snapshot-suffix match in {ARCHIVE_SCHEMA_PUB} with "
                f"row_count={rc}: {primary[0]}{extra}"
            )
            return out
        else:
            out["disposition"] = "DIVERGENT"
            out["identical_in_archive_pub_v1_0"] = False
            preview = ", ".join(
                f"{s}(rc={c})" for s, c in matches[:3]
            ) + (f" +{len(matches) - 3} more" if len(matches) > 3 else "")
            out["justification"] = (
                f"snapshot-suffix match in {ARCHIVE_SCHEMA_PUB} but row "
                f"counts differ: stray={rc} vs [{preview}]; halt for "
                "human review (270d refuses to migrate DIVERGENT rows)"
            )
            return out

    # 3: empty base tables
    if obj["object_type"] == "BASE TABLE" and rc == 0:
        out["disposition"] = "DROP_NO_RESTORE_VALUE"
        out["justification"] = (
            "empty base table (0 rows) in stray archive-DB schema — "
            "no clinical/audit value to preserve"
        )
        return out

    # 4: non-queryable (broken view chains, etc.)
    if obj["error"] is not None:
        out["disposition"] = "DROP_NO_RESTORE_VALUE"
        out["justification"] = (
            f"object not queryable (error: {obj['error'][:80]}); "
            "cannot recover content for migration"
        )
        return out

    # 5: default — preserve under archive_legacy
    out["disposition"] = "MIGRATE_TO_ARCHIVE_LEGACY"
    out["justification"] = (
        f"unique to stray schema {sch!r} (no snapshot-suffix match in "
        f"{ARCHIVE_SCHEMA_PUB}); preserve under {ARCHIVE_SCHEMA_LEGACY}"
    )
    ts = utc_stamp(utc_now())
    out["proposed_target_name"] = f"{sch}__{name}_{ts}"
    return out


# =============================================================================
# Main
# =============================================================================

def main() -> int:
    log_lines: list[str] = []

    def log(msg: str) -> None:
        line = msg if msg.endswith("\n") else msg + "\n"
        log_lines.append(line)
        print(msg)

    def flush_log() -> None:
        OUT_LOG.write_text("".join(log_lines))

    started_at = utc_now()
    log("=== START Script 270c — Phase B planning (DRY-RUN) ===")
    log(f"started_at: {started_at.isoformat()}")
    log(f"publication_db: {PUBLICATION_DB}")
    log(f"archive_db:     {ARCHIVE_DB}")
    log(f"output_dir:     {OUT_DIR}")

    con = connect_locked()
    log("connected (search path locked, CPM invariant verified)")

    # ----- 1. Round-trip restore test (mandatory, halt on fail) -----
    restore_result = restore_test(con, log)
    OUT_RESTORE_JSON.write_text(json.dumps(restore_result, indent=2, default=str))
    log(f"  wrote {OUT_RESTORE_JSON}")
    if restore_result["status"] != "PASS":
        log("\nABORT — round-trip restore test failed; refusing to proceed.")
        flush_log()
        return 1

    # ----- 2. Pre-fetch ----------------------------------------------------
    log("\n--- pre-fetch ---")
    keep_list = fetch_keep_list(con)
    log(f"  main_schema_keep_list_v1: {len(keep_list)} rows")
    registry = fetch_registry(con)
    n_reg_with_norm = sum(1 for v in registry.values() if v["has_normalized"])
    n_reg_null = sum(1 for v in registry.values() if not v["has_normalized"])
    log(
        f"  detail_table_registry_v1: {len(registry)} rows "
        f"({n_reg_with_norm} with non-empty normalized, "
        f"{n_reg_null} EMPTY/NULL)"
    )

    main_objects = queryable_main_objects(con)
    n_main_total = len(main_objects)
    n_main_queryable = sum(1 for v in main_objects.values() if v["queryable"])
    n_main_ghosts = n_main_total - n_main_queryable
    n_main_tables = sum(
        1 for v in main_objects.values()
        if v["queryable"] and v["object_type"] == "BASE TABLE"
    )
    n_main_views = sum(
        1 for v in main_objects.values()
        if v["queryable"] and v["object_type"] == "VIEW"
    )
    log(
        f"  canonical main: {n_main_total} info_schema entries, "
        f"{n_main_queryable} queryable ({n_main_tables} tables, "
        f"{n_main_views} views), {n_main_ghosts} ghosts"
    )

    view_defs = fetch_view_definitions_main(con)
    log(f"  main view_definitions cached: {len(view_defs)}")

    # ----- 3. Eligibility classification (canonical main) ------------------
    log("\n--- canonical main eligibility classification ---")
    manifest_table_rows: list[dict] = []
    archive_candidate_set: set[str] = set()

    for name in sorted(main_objects):
        info = main_objects[name]
        if not info["queryable"]:
            log(f"  skip ghost: {name} (not queryable: {info['error']})")
            continue
        if info["object_type"] == "BASE TABLE":
            row = classify_main_table(name, info, keep_list, registry)
            manifest_table_rows.append(row)
            if row["disposition"] == "ARCHIVE_CANDIDATE":
                archive_candidate_set.add(name)

    # Disposition distribution for tables
    table_disp_dist: dict[str, int] = {}
    for r in manifest_table_rows:
        table_disp_dist[r["disposition"]] = (
            table_disp_dist.get(r["disposition"], 0) + 1
        )
    log(f"  base-table disposition dist: {table_disp_dist}")

    # ----- 4. Views compile-impact analysis --------------------------------
    log("\n--- main views compile-impact ---")
    manifest_view_rows: list[dict] = []
    view_impact_rows: list[dict] = []
    for name in sorted(main_objects):
        info = main_objects[name]
        if info["object_type"] != "VIEW" or not info["queryable"]:
            continue
        vrow, impact = classify_main_view(
            name, info, keep_list, archive_candidate_set, view_defs
        )
        manifest_view_rows.append(vrow)
        view_impact_rows.extend(impact)

    view_disp_dist: dict[str, int] = {}
    for r in manifest_view_rows:
        view_disp_dist[r["disposition"]] = (
            view_disp_dist.get(r["disposition"], 0) + 1
        )
    log(f"  view disposition dist: {view_disp_dist}")
    log(f"  views compile-impact rows (one per (view, ref_table)): "
        f"{len(view_impact_rows)}")

    # ----- 5. Stray-schema disposition (archive DB) ------------------------
    log("\n--- stray-schema disposition (archive DB) ---")
    stray_objects = fetch_archive_db_objects(con, ARCHIVE_DB, STRAY_SCHEMAS)
    log(f"  stray-schema objects (4 schemas): {len(stray_objects)}")

    pub_names = fetch_archive_schema_object_names(
        con, ARCHIVE_DB, ARCHIVE_SCHEMA_PUB)
    legacy_names = fetch_archive_schema_object_names(
        con, ARCHIVE_DB, ARCHIVE_SCHEMA_LEGACY)
    log(
        f"  reference: {ARCHIVE_SCHEMA_PUB} has {len(pub_names)} objects; "
        f"{ARCHIVE_SCHEMA_LEGACY} has {len(legacy_names)} objects"
    )
    pub_rowcounts = fetch_archive_schema_object_rowcounts(
        con, ARCHIVE_DB, ARCHIVE_SCHEMA_PUB)

    stray_rows: list[dict] = []
    for obj in stray_objects:
        stray_rows.append(classify_stray_object(
            obj, pub_names, legacy_names, pub_rowcounts))

    stray_disp_dist: dict[str, int] = {}
    for r in stray_rows:
        stray_disp_dist[r["disposition"]] = (
            stray_disp_dist.get(r["disposition"], 0) + 1
        )
    log(f"  stray disposition dist: {stray_disp_dist}")
    n_stray_drops = sum(
        1 for r in stray_rows
        if r["disposition"] in ("DROP_ALREADY_SNAPSHOTTED",
                                "DROP_NO_RESTORE_VALUE")
    )
    n_stray_migrate = sum(
        1 for r in stray_rows if r["disposition"] == "MIGRATE_TO_ARCHIVE_LEGACY"
    )
    n_stray_divergent = sum(
        1 for r in stray_rows if r["disposition"] == "DIVERGENT"
    )
    n_stray_already_snapshotted = sum(
        1 for r in stray_rows if r["disposition"] == "DROP_ALREADY_SNAPSHOTTED"
    )
    n_stray_no_value = sum(
        1 for r in stray_rows if r["disposition"] == "DROP_NO_RESTORE_VALUE"
    )
    log(f"  stray drops total:                  {n_stray_drops}")
    log(f"    DROP_ALREADY_SNAPSHOTTED:         {n_stray_already_snapshotted}")
    log(f"    DROP_NO_RESTORE_VALUE:            {n_stray_no_value}")
    log(f"  stray migrate total:                {n_stray_migrate}")
    log(f"  stray DIVERGENT (halt for review):  {n_stray_divergent}")

    # ----- 6. Budget pre-flight (halt on trip) -----------------------------
    log("\n--- budget pre-flight ---")
    archive_candidates = [
        r for r in manifest_table_rows if r["disposition"] == "ARCHIVE_CANDIDATE"
    ]
    n_archive = len(archive_candidates)
    total_rows_in_archive = sum(
        (r["row_count"] or 0) for r in archive_candidates
    )
    big_tables = sorted(
        [
            (r["table_name"], r["row_count"])
            for r in archive_candidates
            if r["row_count"] and r["row_count"] > BUDGET_HUMAN_REVIEW_ROW_THRESHOLD
        ],
        key=lambda x: -(x[1] or 0),
    )

    budgets = {
        "archive_candidates_count_canonical_main": {
            "actual": n_archive,
            "limit": BUDGET_MAX_ARCHIVE_CANDIDATES,
            "ok": n_archive <= BUDGET_MAX_ARCHIVE_CANDIDATES,
        },
        "drop_candidates_count_stray_archive": {
            "actual": n_stray_drops,
            "limit": BUDGET_MAX_STRAY_DROPS,
            "ok": n_stray_drops <= BUDGET_MAX_STRAY_DROPS,
        },
        "total_rows_in_archive_candidates": {
            "actual": int(total_rows_in_archive),
            "limit": BUDGET_MAX_TOTAL_ROWS_IN_ARCHIVE,
            "ok": total_rows_in_archive <= BUDGET_MAX_TOTAL_ROWS_IN_ARCHIVE,
        },
        "single_archive_candidate_over_threshold": {
            "threshold": BUDGET_HUMAN_REVIEW_ROW_THRESHOLD,
            "tables_over_threshold": [
                {"name": n, "row_count": int(c)} for n, c in big_tables
            ],
            "ok": len(big_tables) == 0,  # informational halt only if any
            "ok_severity": "human_review_required" if big_tables else "none",
        },
        "stray_divergent_rows_human_review": {
            "actual": n_stray_divergent,
            "limit": 0,
            "ok": n_stray_divergent == 0,
            "ok_severity": (
                "human_review_required" if n_stray_divergent else "none"
            ),
            "rows": [
                {
                    "schema": r["schema"],
                    "name": r["name"],
                    "row_count": r["row_count"],
                    "justification": r["justification"],
                }
                for r in stray_rows if r["disposition"] == "DIVERGENT"
            ],
        },
    }
    for k, v in budgets.items():
        log(f"  {k}: ok={v['ok']} {v}")

    OUT_BUDGETS_JSON.write_text(json.dumps(budgets, indent=2, default=str))
    log(f"  wrote {OUT_BUDGETS_JSON}")

    # DIVERGENT and large-table flags are surfaced (not halt-on-plan) so the
    # manifest still emits for human review; 270d --execute refuses to
    # migrate any DIVERGENT row.
    informational_only_budgets = {
        "single_archive_candidate_over_threshold",
        "stray_divergent_rows_human_review",
    }
    halt_keys = [
        k for k, v in budgets.items()
        if k not in informational_only_budgets and not v["ok"]
    ]
    if halt_keys:
        log(
            f"\nABORT — budget(s) tripped: {halt_keys}. "
            "Emitting partial budgets.json only; no manifest CSVs."
        )
        flush_log()
        return 1

    # ----- 7. Emit artifacts -----------------------------------------------
    log("\n--- emit artifacts ---")

    all_manifest_rows = manifest_table_rows + manifest_view_rows
    # Phase B execute order: views first (to archive DDL before dropping
    # underlying tables), then base tables. Within each group, sort by name.
    archive_targets_table = sorted(archive_candidate_set)
    view_break_names = sorted(
        r["table_name"] for r in manifest_view_rows
        if r["disposition"] == "VIEW_COMPILE_WILL_BREAK"
    )
    order_lookup: dict[tuple[str, str], int] = {}
    pos = 1
    for v in view_break_names:
        order_lookup[("VIEW", v)] = pos
        pos += 1
    for t in archive_targets_table:
        order_lookup[("BASE TABLE", t)] = pos
        pos += 1
    archive_ts = utc_stamp(started_at)
    for r in all_manifest_rows:
        key = (r["object_type"], r["table_name"])
        if key in order_lookup:
            r["phase_b_execute_order"] = order_lookup[key]
        if r["disposition"] == "ARCHIVE_CANDIDATE":
            r["archive_target"] = (
                f'"{ARCHIVE_DB}".{ARCHIVE_SCHEMA_PUB}.'
                f"{r['table_name']}_pre270d_{archive_ts}"
            )
        elif r["disposition"] == "VIEW_COMPILE_WILL_BREAK":
            r["archive_target"] = (
                f'"{ARCHIVE_DB}".{ARCHIVE_SCHEMA_PUB}.'
                f"view_ddl__{r['table_name']}_pre270d_{archive_ts}"
            )

    manifest_header = [
        "schema", "table_name", "object_type", "row_count", "disposition",
        "justification", "feeder_mapping", "keep_list_entry",
        "archive_target", "phase_b_execute_order",
    ]
    with OUT_MANIFEST_CSV.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "# generated_by", "scripts/270c_phase_b_planning.py",
            "generated_at", started_at.isoformat(),
        ])
        w.writerow(manifest_header)
        for r in sorted(
            all_manifest_rows,
            key=lambda x: (x["disposition"], x["object_type"], x["table_name"])
        ):
            w.writerow([
                r["schema"], r["table_name"], r["object_type"],
                "" if r["row_count"] is None else r["row_count"],
                r["disposition"], r["justification"] or "",
                r["feeder_mapping"] or "", r["keep_list_entry"] or "",
                r["archive_target"] or "",
                "" if r["phase_b_execute_order"] is None
                else r["phase_b_execute_order"],
            ])
    log(f"  wrote {OUT_MANIFEST_CSV} ({len(all_manifest_rows)} rows)")

    views_header = [
        "view_name", "references_table", "will_break_on_archive",
        "proposed_handling",
    ]
    with OUT_VIEWS_CSV.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "# generated_by", "scripts/270c_phase_b_planning.py",
            "generated_at", started_at.isoformat(),
        ])
        w.writerow(views_header)
        for r in sorted(view_impact_rows,
                        key=lambda x: (x["view_name"], x["references_table"])):
            w.writerow([
                r["view_name"], r["references_table"],
                r["will_break_on_archive"], r["proposed_handling"],
            ])
    log(f"  wrote {OUT_VIEWS_CSV} ({len(view_impact_rows)} rows)")

    stray_header = [
        "schema", "name", "object_type", "row_count", "disposition",
        "identical_in_archive_pub_v1_0", "identical_in_archive_legacy",
        "proposed_target_name", "justification",
    ]
    with OUT_STRAY_CSV.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "# generated_by", "scripts/270c_phase_b_planning.py",
            "generated_at", started_at.isoformat(),
        ])
        w.writerow(stray_header)
        for r in sorted(stray_rows,
                        key=lambda x: (x["schema"], x["disposition"], x["name"])):
            w.writerow([
                r["schema"], r["name"], r["object_type"],
                "" if r["row_count"] is None else r["row_count"],
                r["disposition"],
                r["identical_in_archive_pub_v1_0"],
                r["identical_in_archive_legacy"],
                r["proposed_target_name"] or "",
                r["justification"] or "",
            ])
    log(f"  wrote {OUT_STRAY_CSV} ({len(stray_rows)} rows)")

    # Planning summary markdown
    md_lines: list[str] = []
    md_lines.append("# Script 270c — Phase B planning summary\n")
    md_lines.append(
        f"Generated: {started_at.isoformat()}  \n"
        f"Tag anchor: `v1_0_registry_locked` (commit 117f55d)  \n"
        f"Mode: **dry-run only — no destructive writes**\n"
    )
    md_lines.append("## Restore test\n")
    md_lines.append(
        f"- Status: **{restore_result['status']}**\n"
        f"- Snapshot: `{restore_result.get('chosen_snapshot')}`\n"
        f"- Round-tripped {restore_result.get('source_row_count')} rows / "
        f"{restore_result.get('source_column_count')} cols cleanly\n"
    )
    md_lines.append("## Budgets\n")
    for k, v in budgets.items():
        md_lines.append(f"- `{k}`: ok={v['ok']} — {v}\n")
    md_lines.append("\n## Canonical main — base-table dispositions\n")
    for k, v in sorted(table_disp_dist.items(), key=lambda x: -x[1]):
        md_lines.append(f"- `{k}`: {v}\n")
    md_lines.append("\n## Canonical main — view dispositions\n")
    for k, v in sorted(view_disp_dist.items(), key=lambda x: -x[1]):
        md_lines.append(f"- `{k}`: {v}\n")
    md_lines.append(
        f"\nViews compile-impact rows: **{len(view_impact_rows)}**\n"
    )
    md_lines.append("\n## Stray archive-DB schemas\n")
    for k, v in sorted(stray_disp_dist.items(), key=lambda x: -x[1]):
        md_lines.append(f"- `{k}`: {v}\n")
    md_lines.append("\n## Recommended execution order for 270d\n")
    md_lines.append(
        "1. Archive view DDL for VIEW_COMPILE_WILL_BREAK rows "
        "(write to `archive_pub_v1_0.view_ddl__<view>_pre270d_<UTC>`).\n"
        "2. Drop those views.\n"
        "3. For each ARCHIVE_CANDIDATE base table, snapshot to "
        "`archive_pub_v1_0.<table>_pre270d_<UTC>` then DROP TABLE.\n"
        "4. For stray-schema DROP_ALREADY_SNAPSHOTTED + "
        "DROP_NO_RESTORE_VALUE rows, DROP without snapshot.\n"
        "5. For stray-schema MIGRATE_TO_ARCHIVE_LEGACY rows, "
        "CREATE TABLE under `archive_legacy.<schema>__<name>_<UTC>` "
        "then DROP from stray schema.\n"
        "6. Final audit row to `v1_1_finalization_audit_v1`.\n"
    )
    md_lines.append("\n## Counts to report back\n")
    md_lines.append(
        f"- archive_candidates: **{n_archive}**\n"
        f"- stray_drops: **{n_stray_drops}** "
        f"(already_snapshotted={n_stray_already_snapshotted}, "
        f"no_restore_value={n_stray_no_value})\n"
        f"- stray_migrate: **{n_stray_migrate}**\n"
        f"- stray_DIVERGENT (halt for review): **{n_stray_divergent}**\n"
        f"- view_impacts: **{len(view_impact_rows)}**\n"
        f"- restore_test: **{restore_result['status']}** "
        f"({restore_result.get('source_row_count')} rows round-tripped)\n"
    )
    OUT_SUMMARY_MD.write_text("".join(md_lines))
    log(f"  wrote {OUT_SUMMARY_MD}")

    # ----- 8. Audit row ----------------------------------------------------
    log("\n--- audit row ---")
    finding_id = "phase_b_planning_complete"
    existing = con.execute(
        f"SELECT COUNT(*) FROM {AUDIT_FQ} WHERE finding_id = ?",
        [finding_id],
    ).fetchone()[0]
    if existing:
        log(f"  audit row {finding_id!r} already present — skipping insert")
        audit_inserted = False
    else:
        notes = (
            f"Manifest emitted to scripts/output/270c_*. No destructive "
            f"writes. Restore test passed: {restore_result['chosen_snapshot']}, "
            f"{restore_result['source_row_count']} rows round-tripped cleanly. "
            f"archive_candidates={n_archive} stray_drops={n_stray_drops} "
            f"view_impacts={len(view_impact_rows)} stray_migrate={n_stray_migrate}. "
            f"Review artifacts before 270d."
        )
        con.execute(
            f"""
            INSERT INTO {AUDIT_FQ}
                (run_ts, script_num, finding_id, metric,
                 count_before, count_after, target_after, status, notes)
            VALUES (CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ["270c", finding_id, "archive_candidates_canonical_main",
             None, n_archive, None, "OK", notes],
        )
        audit_inserted = True
        log(f"  inserted audit row finding_id={finding_id!r}")

    finished_at = utc_now()
    log(
        f"\n=== END Script 270c — Phase B planning "
        f"(elapsed {(finished_at - started_at).total_seconds():.1f}s) ==="
    )
    log(
        f"REPORT: archive_candidates={n_archive} stray_drops={n_stray_drops} "
        f"view_impacts={len(view_impact_rows)} restore_test="
        f"{restore_result['status']}"
    )
    flush_log()

    # Final structured stdout for the calling agent
    print(json.dumps({
        "archive_candidates": n_archive,
        "stray_drops": n_stray_drops,
        "view_impacts": len(view_impact_rows),
        "stray_migrate": n_stray_migrate,
        "restore_test_status": restore_result["status"],
        "restore_test_rows": restore_result.get("source_row_count"),
        "audit_row_inserted": audit_inserted,
        "budgets_ok": all(
            v["ok"] for k, v in budgets.items()
            if k != "single_archive_candidate_over_threshold"
        ),
        "single_table_over_threshold_count": len(big_tables),
    }, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
