#!/usr/bin/env python3
"""
THYROID_2026 — Script 224: Compare Two Canonical Versions

Diffs two canonical publication databases and classifies the change as
PATCH, MINOR, MAJOR, or REGRESSION.

Usage:
    .venv/bin/python scripts/224_compare_canonical_versions.py --from v1_0 --to v1_1
    .venv/bin/python scripts/224_compare_canonical_versions.py --from v1_0 --to v1_1_rc

Classification rules (enforced by the versioning contract):
    PATCH:      Only additive rows in existing tables. Same schema, same shape.
    MINOR:      New columns and/or new tables added. Existing columns/tables unchanged.
    MAJOR:      Any of: column removed, column renamed, type changed, semantic drift
                detected (>0.1% of sampled rows differ in shared columns), or table removed.
    REGRESSION: Existing tables have FEWER rows than the baseline (data loss).

Outputs:
    stdout:     Human-readable summary with classification.
    reports/:   scripts/output/version_compare/v{from}_to_v{to}_{timestamp}/
                  diff_report.md   — detailed markdown report
                  diff_data.csv    — per-table CSV

Exit codes:
    0 — comparison completed without error (regardless of PATCH/MINOR/MAJOR result)
    1 — error during comparison (connection failure, missing DB, etc.)

ACCOUNT: logan.glosser.eras (TOML token, NOT env var)
"""
from __future__ import annotations

import argparse
import base64
import csv
import json
import re
import sys
from datetime import datetime
from pathlib import Path

import duckdb
import toml

REPO = Path(__file__).resolve().parent.parent
SCRIPT_TAG = "224_compare_canonical_versions"
VERSION_RE = re.compile(r'^v\d+_\d+(_\d+)?(_rc)?$')
DB_PREFIX = "thyroid_canonical_publication_"


def normalize_version(v: str) -> str:
    s = v.lower().strip()
    if not s.startswith("v"):
        s = "v" + s
    s = s.replace(".", "_")
    if not VERSION_RE.match(s):
        sys.exit(f"[{SCRIPT_TAG}] ERROR: invalid version {v!r} (expected e.g. v1_0, v1_1_rc)")
    return s


def connect_eras() -> duckdb.DuckDBPyConnection:
    toml_path = REPO / "motherduck.local.toml"
    if not toml_path.exists():
        sys.exit(f"[{SCRIPT_TAG}] ERROR: motherduck.local.toml not found")
    cfg = toml.load(str(toml_path))
    token = cfg.get("MD_SA_TOKEN") or cfg.get("MOTHERDUCK_TOKEN") or cfg.get("motherduck_token")
    if not token:
        sys.exit(f"[{SCRIPT_TAG}] ERROR: No token in motherduck.local.toml")
    padding = len(token.split(".")[1]) % 4
    payload_b64 = token.split(".")[1] + "=" * (4 - padding if padding else 0)
    payload = json.loads(base64.urlsafe_b64decode(payload_b64))
    email = payload.get("email", "unknown")
    if "eras" not in email.lower():
        sys.exit(f"[{SCRIPT_TAG}] ABORT: expected eras account, got {email}")
    return duckdb.connect(f"md:?motherduck_token={token}")


def _safe(s: str) -> str:
    return s.replace('"', '""')


def get_tables(con, db_name: str) -> dict[str, dict]:
    """Return {table_name: {rows, columns: {col: dtype}}} for all main tables."""
    rows = con.execute(f"""
        SELECT table_name, estimated_size
        FROM duckdb_tables()
        WHERE database_name = '{_safe(db_name)}' AND schema_name = 'main'
          AND table_name <> '__readme'
        ORDER BY table_name
    """).fetchall()

    result = {}
    for tbl, est_rows in rows:
        cols = con.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_catalog = '{_safe(db_name)}' AND table_schema = 'main'
              AND table_name = '{_safe(tbl)}'
            ORDER BY ordinal_position
        """).fetchall()
        result[tbl] = {
            "estimated_rows": est_rows,
            "columns": {col: dtype for col, dtype in cols},
        }
    return result


def row_hash_sample(con, db_name: str, table_name: str,
                    columns: list[str], pct: float = 2.0) -> set[str]:
    """Return a set of MD5 hashes for a percentage sample of rows."""
    col_list = ", ".join(f'"{_safe(c)}"' for c in sorted(columns))
    try:
        rows = con.execute(f"""
            SELECT md5(CAST(({col_list}) AS VARCHAR))
            FROM "{_safe(db_name)}".main."{_safe(table_name)}"
            USING SAMPLE {pct}%
        """).fetchall()
        return {r[0] for r in rows}
    except Exception:
        return set()


def compare_versions(con, from_db: str, to_db: str) -> dict:
    """Run the full diff and return a structured result dict."""
    print(f"[{SCRIPT_TAG}] Loading table schemas...")
    from_tables = get_tables(con, from_db)
    to_tables = get_tables(con, to_db)

    from_set = set(from_tables.keys())
    to_set = set(to_tables.keys())

    new_tables = sorted(to_set - from_set)
    removed_tables = sorted(from_set - to_set)
    shared_tables = sorted(from_set & to_set)

    table_diffs = []
    has_major = False
    has_minor = bool(new_tables)
    has_regression = False

    if removed_tables:
        has_major = True

    for tbl in shared_tables:
        from_cols = from_tables[tbl]["columns"]
        to_cols = to_tables[tbl]["columns"]

        from_col_set = set(from_cols.keys())
        to_col_set = set(to_cols.keys())

        new_cols = sorted(to_col_set - from_col_set)
        removed_cols = sorted(from_col_set - to_col_set)
        type_changes = {
            c: (from_cols[c], to_cols[c])
            for c in from_col_set & to_col_set
            if from_cols[c] != to_cols[c]
        }

        if new_cols:
            has_minor = True
        if removed_cols or type_changes:
            has_major = True

        # Get actual row counts
        from_rows = con.execute(
            f'SELECT COUNT(*) FROM "{_safe(from_db)}".main."{_safe(tbl)}"'
        ).fetchone()[0]
        to_rows = con.execute(
            f'SELECT COUNT(*) FROM "{_safe(to_db)}".main."{_safe(tbl)}"'
        ).fetchone()[0]
        row_delta = to_rows - from_rows

        if to_rows < from_rows:
            has_regression = True

        # Row-hash sample on shared columns (only if schema compatible and rows exist)
        value_drift_pct = None
        shared_cols = sorted(from_col_set & to_col_set - set(type_changes.keys()))
        if shared_cols and from_rows > 0 and to_rows > 0 and not removed_cols and not type_changes:
            print(f"  Sampling {tbl}...", end=" ", flush=True)
            from_hashes = row_hash_sample(con, from_db, tbl, shared_cols)
            to_hashes = row_hash_sample(con, to_db, tbl, shared_cols)
            if from_hashes and to_hashes:
                changed = len(from_hashes - to_hashes)
                total_sample = len(from_hashes)
                value_drift_pct = 100.0 * changed / max(total_sample, 1)
                if value_drift_pct > 0.1:
                    has_major = True
                print(f"drift={value_drift_pct:.2f}%")
            else:
                print("(sample failed)")

        table_diffs.append({
            "table": tbl,
            "from_rows": from_rows,
            "to_rows": to_rows,
            "row_delta": row_delta,
            "new_cols": new_cols,
            "removed_cols": removed_cols,
            "type_changes": type_changes,
            "value_drift_pct": value_drift_pct,
        })

    # Classify
    if has_regression:
        classification = "REGRESSION"
    elif has_major:
        classification = "MAJOR"
    elif has_minor:
        classification = "MINOR"
    else:
        classification = "PATCH"

    return {
        "from_db": from_db,
        "to_db": to_db,
        "classification": classification,
        "new_tables": new_tables,
        "removed_tables": removed_tables,
        "table_diffs": table_diffs,
        "has_regression": has_regression,
    }


def format_report(result: dict, from_ver: str, to_ver: str) -> str:
    cls = result["classification"]
    from_db = result["from_db"]
    to_db = result["to_db"]
    lines = []
    lines.append(f"# Canonical Version Diff: {from_ver} → {to_ver}")
    lines.append(f"\n**Classification: {cls}**")
    lines.append(f"\nGenerated: {datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}")
    lines.append(f"\nFrom: `{from_db}`")
    lines.append(f"To:   `{to_db}`")

    cls_desc = {
        "PATCH": "Additive rows only. Same schema. Existing analyses keep working.",
        "MINOR": "New columns and/or tables added. Existing columns unchanged. "
                 "Existing analyses keep working.",
        "MAJOR": "Breaking changes: column removed, column type changed, or "
                 "significant value drift. Manuscripts must be explicitly migrated.",
        "REGRESSION": "DATA LOSS DETECTED: existing tables have fewer rows in the "
                      "new version than the baseline. This must be investigated before promotion.",
    }
    lines.append(f"\n**What this means:** {cls_desc.get(cls, '')}")

    if result["new_tables"]:
        lines.append(f"\n## New Tables ({len(result['new_tables'])})")
        for t in result["new_tables"]:
            lines.append(f"- `{t}`")

    if result["removed_tables"]:
        lines.append(f"\n## Removed Tables ({len(result['removed_tables'])}) ⚠ MAJOR")
        for t in result["removed_tables"]:
            lines.append(f"- `{t}`")

    # Tables with schema changes
    schema_changed = [d for d in result["table_diffs"]
                      if d["new_cols"] or d["removed_cols"] or d["type_changes"]]
    if schema_changed:
        lines.append(f"\n## Schema Changes ({len(schema_changed)} tables)")
        for d in schema_changed:
            lines.append(f"\n### `{d['table']}`")
            if d["new_cols"]:
                lines.append(f"  **New columns ({len(d['new_cols'])}):** "
                              + ", ".join(f"`{c}`" for c in d["new_cols"]))
            if d["removed_cols"]:
                lines.append(f"  **Removed columns ({len(d['removed_cols'])}) ⚠:** "
                              + ", ".join(f"`{c}`" for c in d["removed_cols"]))
            if d["type_changes"]:
                for col, (old_t, new_t) in d["type_changes"].items():
                    lines.append(f"  **Type change ⚠:** `{col}` {old_t} → {new_t}")

    # Value drift
    drift_tables = [d for d in result["table_diffs"]
                    if d.get("value_drift_pct") is not None and d["value_drift_pct"] > 0.0]
    if drift_tables:
        lines.append("\n## Value Drift (sampled)")
        for d in drift_tables:
            flag = " ⚠ MAJOR" if d["value_drift_pct"] > 0.1 else ""
            lines.append(f"- `{d['table']}`: {d['value_drift_pct']:.2f}% of sample changed{flag}")

    # Row count summary
    lines.append("\n## Row Count Summary")
    lines.append("| Table | From | To | Delta |")
    lines.append("|-------|-----:|---:|------:|")
    for d in sorted(result["table_diffs"], key=lambda x: abs(x["row_delta"]), reverse=True)[:30]:
        flag = " ⚠" if d["to_rows"] < d["from_rows"] else ""
        lines.append(f"| `{d['table']}` | {d['from_rows']:,} | "
                     f"{d['to_rows']:,} | {d['row_delta']:+,}{flag} |")
    if len(result["table_diffs"]) > 30:
        lines.append(f"| *(+{len(result['table_diffs']) - 30} more tables with no delta)* | | | |")

    return "\n".join(lines)


def write_csv(result: dict, out_dir: Path) -> None:
    with open(out_dir / "diff_data.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "table", "status", "from_rows", "to_rows", "row_delta",
            "new_cols_count", "removed_cols_count", "type_changes_count",
            "value_drift_pct",
        ])
        for tbl in result["new_tables"]:
            w.writerow([tbl, "NEW_TABLE", "", "", "", "", "", "", ""])
        for tbl in result["removed_tables"]:
            w.writerow([tbl, "REMOVED_TABLE", "", "", "", "", "", "", ""])
        for d in result["table_diffs"]:
            if d["to_rows"] < d["from_rows"]:
                status = "REGRESSION"
            elif d["removed_cols"] or d["type_changes"]:
                status = "MAJOR_CHANGE"
            elif d["new_cols"]:
                status = "MINOR_CHANGE"
            elif d["row_delta"] != 0:
                status = "ROW_DELTA"
            else:
                status = "UNCHANGED"
            w.writerow([
                d["table"], status,
                d["from_rows"], d["to_rows"], d["row_delta"],
                len(d["new_cols"]), len(d["removed_cols"]),
                len(d["type_changes"]),
                f"{d['value_drift_pct']:.4f}" if d["value_drift_pct"] is not None else "",
            ])


def main():
    parser = argparse.ArgumentParser(
        description="Diff two canonical publication versions."
    )
    parser.add_argument("--from", dest="from_ver", required=True,
                        help="Baseline version, e.g. v1_0")
    parser.add_argument("--to", dest="to_ver", required=True,
                        help="New version, e.g. v1_1 or v1_1_rc")
    args = parser.parse_args()

    from_ver = normalize_version(args.from_ver)
    to_ver = normalize_version(args.to_ver)
    from_db = f"{DB_PREFIX}{from_ver}"
    to_db = f"{DB_PREFIX}{to_ver}"

    try:
        con = connect_eras()

        # Verify both DBs exist
        for db, label in [(from_db, "--from"), (to_db, "--to")]:
            exists = con.execute(
                "SELECT 1 FROM duckdb_databases() WHERE database_name = ?", [db]
            ).fetchone()
            if not exists:
                print(f"[{SCRIPT_TAG}] ERROR: {label} database {db!r} not found on MotherDuck",
                      file=sys.stderr)
                sys.exit(1)

        print(f"[{SCRIPT_TAG}] Comparing {from_db} → {to_db}")
        result = compare_versions(con, from_db, to_db)

        cls = result["classification"]
        print(f"\n{'='*60}")
        print(f"CLASSIFICATION: {cls}")
        print(f"{'='*60}")
        if result["new_tables"]:
            print(f"  New tables:     {len(result['new_tables'])}")
        if result["removed_tables"]:
            print(f"  Removed tables: {len(result['removed_tables'])} ⚠")
        schema_changes = sum(
            1 for d in result["table_diffs"]
            if d["new_cols"] or d["removed_cols"] or d["type_changes"]
        )
        if schema_changes:
            print(f"  Schema changes: {schema_changes} table(s)")
        row_regressions = sum(1 for d in result["table_diffs"] if d["row_delta"] < 0)
        if row_regressions:
            print(f"  Regressions:    {row_regressions} table(s) lost rows ⚠")
        print()

        # Write outputs
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        out_dir = (REPO / "scripts" / "output" / "version_compare"
                   / f"v{from_ver}_to_v{to_ver}_{ts}")
        out_dir.mkdir(parents=True, exist_ok=True)

        report_md = format_report(result, from_ver, to_ver)
        with open(out_dir / "diff_report.md", "w") as f:
            f.write(report_md)
        write_csv(result, out_dir)

        print(f"  Report: {out_dir / 'diff_report.md'}")
        print(f"  CSV:    {out_dir / 'diff_data.csv'}")

        # Return the classification for use by Script 225
        return result

    except Exception as e:
        print(f"[{SCRIPT_TAG}] ERROR: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
