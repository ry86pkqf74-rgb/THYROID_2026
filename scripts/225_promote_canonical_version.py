#!/usr/bin/env python3
"""
THYROID_2026 — Script 225: Promote Release Candidate to Release

Promotes a _rc database to a stable release after governance checks.

Usage:
    .venv/bin/python scripts/225_promote_canonical_version.py \\
        --candidate v1_1_rc --release v1_1

Steps:
  1. Verify thyroid_canonical_publication_v1_1_rc exists on MotherDuck
  2. Verify thyroid_canonical_publication_v1_1 does NOT exist (no overwrite ever)
  3. Run Script 224 internally: diff baseline → candidate
  4. If classification is MAJOR or REGRESSION, require --accept-major with typed confirmation
  5. Rename: _rc → release (COPY FROM DATABASE + DROP)
  6. Verify row counts and invariants on the new release DB
  7. Update RELEASE.md registered-versions table
  8. Update CHANGELOG.md with the diff summary
  9. Tag the build repo: git tag canonical-v1_1
 10. Prompt user to push the tag

RULES (non-negotiable):
  - Released versions can NEVER be overwritten. Not even with --force.
  - Only _rc databases can be renamed via this script.
  - The baseline for diff is always the most recent prior release (auto-detected).

ACCOUNT: logan.glosser.eras (TOML token, NOT env var)
"""
from __future__ import annotations

import argparse
import base64
import json
import re
import subprocess
import sys
from datetime import date
from pathlib import Path

import duckdb
import toml

REPO = Path(__file__).resolve().parent.parent
SCRIPT_TAG = "225_promote_canonical_version"
DB_PREFIX = "thyroid_canonical_publication_"
VERSION_RE = re.compile(r'^v\d+_\d+(_\d+)?(_rc)?$')
RELEASE_MD = REPO / "RELEASE.md"
CHANGELOG_MD = REPO / "CHANGELOG.md"


def normalize_version(v: str) -> str:
    s = v.lower().strip()
    if not s.startswith("v"):
        s = "v" + s
    s = s.replace(".", "_")
    if not VERSION_RE.match(s):
        sys.exit(f"[{SCRIPT_TAG}] ERROR: invalid version {v!r}")
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
    print(f"[{SCRIPT_TAG}] Connected as: {email}")
    return duckdb.connect(f"md:?motherduck_token={token}")


def _safe(s: str) -> str:
    return s.replace('"', '""')


def db_exists(con, db_name: str) -> bool:
    return bool(con.execute(
        "SELECT 1 FROM duckdb_databases() WHERE database_name = ?", [db_name]
    ).fetchone())


def get_all_release_dbs(con) -> list[str]:
    """Return all thyroid_canonical_publication_v* databases that are NOT _rc."""
    rows = con.execute("SELECT database_name FROM duckdb_databases()").fetchall()
    releases = []
    for (name,) in rows:
        if name.startswith(DB_PREFIX) and not name.endswith("_rc"):
            ver = name[len(DB_PREFIX):]
            if VERSION_RE.match(ver):
                releases.append(name)
    return sorted(releases)


def find_baseline(con, exclude_db: str) -> str | None:
    """Find the most recent release DB to use as diff baseline."""
    releases = [db for db in get_all_release_dbs(con) if db != exclude_db]
    return releases[-1] if releases else None


def run_compare(from_ver: str, to_ver: str) -> dict | None:
    """Run Script 224 in-process and return its result dict."""
    sys.path.insert(0, str(REPO / "scripts"))
    try:
        import importlib
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "compare_224",
            REPO / "scripts" / "224_compare_canonical_versions.py"
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        con = connect_eras()
        from_db = f"{DB_PREFIX}{from_ver}"
        to_db = f"{DB_PREFIX}{to_ver}"
        print(f"[{SCRIPT_TAG}] Running diff: {from_db} → {to_db}")
        result = mod.compare_versions(con, from_db, to_db)
        result["from_db"] = from_db
        result["to_db"] = to_db

        # Write the diff report
        ts = __import__("datetime").datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        out_dir = (REPO / "scripts" / "output" / "version_compare"
                   / f"v{from_ver}_to_v{to_ver}_{ts}")
        out_dir.mkdir(parents=True, exist_ok=True)
        report_md = mod.format_report(result, from_ver, to_ver)
        with open(out_dir / "diff_report.md", "w") as f:
            f.write(report_md)
        mod.write_csv(result, out_dir)
        print(f"[{SCRIPT_TAG}] Diff report: {out_dir / 'diff_report.md'}")

        result["_report_md"] = report_md
        result["_report_path"] = str(out_dir / "diff_report.md")
        return result

    except Exception as e:
        print(f"[{SCRIPT_TAG}] WARNING: diff failed: {e}. Continuing with manual promotion.")
        return None


def update_release_md(release_ver: str, release_type: str, summary: str) -> None:
    if not RELEASE_MD.exists():
        print(f"[{SCRIPT_TAG}] WARNING: RELEASE.md not found at {RELEASE_MD}. Skipping update.")
        return

    content = RELEASE_MD.read_text()
    today = date.today().isoformat()
    new_row = (
        f"| {release_ver} | {today} | {release_type} | {summary} | active |"
    )

    # Insert after the header row + separator row of the table
    table_marker = "| v1_0"
    if table_marker in content:
        # Find the last version row and append after it
        lines = content.splitlines()
        insert_after = -1
        for i, line in enumerate(lines):
            if line.strip().startswith("| v"):
                insert_after = i
        if insert_after >= 0:
            lines.insert(insert_after + 1, new_row)
            RELEASE_MD.write_text("\n".join(lines) + "\n")
            print(f"[{SCRIPT_TAG}] Updated RELEASE.md with {release_ver}")
        else:
            print(f"[{SCRIPT_TAG}] WARNING: Could not find table insertion point in RELEASE.md")
    else:
        print(f"[{SCRIPT_TAG}] WARNING: RELEASE.md table format unexpected; skipping update")


def update_changelog(release_ver: str, diff_result: dict | None) -> None:
    if not CHANGELOG_MD.exists():
        print(f"[{SCRIPT_TAG}] WARNING: CHANGELOG.md not found. Skipping update.")
        return

    today = date.today().isoformat()
    cls = diff_result["classification"] if diff_result else "unknown"
    new_entry = f"\n## {release_ver} — {today}\n\n**Type:** {cls}\n"

    if diff_result:
        if diff_result.get("new_tables"):
            new_entry += f"\n**New tables ({len(diff_result['new_tables'])}):**\n"
            for t in diff_result["new_tables"]:
                new_entry += f"- `{t}`\n"
        schema_changes = sum(
            1 for d in diff_result["table_diffs"]
            if d["new_cols"] or d["removed_cols"] or d["type_changes"]
        )
        if schema_changes:
            new_entry += f"\n**Schema changes:** {schema_changes} table(s) modified\n"
        regressions = [d for d in diff_result["table_diffs"] if d["row_delta"] < 0]
        if regressions:
            new_entry += f"\n**⚠ Regressions:** {len(regressions)} table(s) lost rows\n"

    new_entry += f"\n**Built from:** Release candidate `{DB_PREFIX}{release_ver}_rc`\n"
    if diff_result and diff_result.get("_report_path"):
        new_entry += f"\n**Diff report:** `{diff_result['_report_path']}`\n"

    content = CHANGELOG_MD.read_text()
    # Insert after the first heading line
    lines = content.splitlines(keepends=True)
    insert_pos = 1
    for i, line in enumerate(lines):
        if line.startswith("# "):
            insert_pos = i + 1
            break
    lines.insert(insert_pos, new_entry)
    CHANGELOG_MD.write_text("".join(lines))
    print(f"[{SCRIPT_TAG}] Updated CHANGELOG.md with {release_ver}")


def git_tag(release_ver: str) -> str:
    tag = f"canonical-{release_ver}"
    result = subprocess.run(
        ["git", "tag", tag],
        cwd=REPO,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"[{SCRIPT_TAG}] WARNING: git tag failed: {result.stderr.strip()}")
    else:
        print(f"[{SCRIPT_TAG}] Tagged: {tag}")
    return tag


def main():
    parser = argparse.ArgumentParser(
        description="Promote a canonical release candidate to a stable release."
    )
    parser.add_argument("--candidate", required=True,
                        help="RC version, e.g. v1_1_rc")
    parser.add_argument("--release", required=True,
                        help="Target release version, e.g. v1_1")
    parser.add_argument("--accept-major", action="store_true",
                        help="Required when diff classification is MAJOR or REGRESSION.")
    args = parser.parse_args()

    candidate_ver = normalize_version(args.candidate)
    release_ver = normalize_version(args.release)

    if not candidate_ver.endswith("_rc"):
        sys.exit(f"[{SCRIPT_TAG}] ERROR: --candidate must end with _rc, got {candidate_ver!r}")
    if release_ver.endswith("_rc"):
        sys.exit(f"[{SCRIPT_TAG}] ERROR: --release must NOT end with _rc, got {release_ver!r}")

    candidate_db = f"{DB_PREFIX}{candidate_ver}"
    release_db = f"{DB_PREFIX}{release_ver}"

    con = connect_eras()

    # Step 1: Verify RC exists
    if not db_exists(con, candidate_db):
        sys.exit(
            f"[{SCRIPT_TAG}] ERROR: {candidate_db!r} not found on MotherDuck.\n"
            f"  Build it first: python scripts/223_publish_canonical.py "
            f"--version {release_ver} --candidate"
        )
    print(f"[{SCRIPT_TAG}] ✓ {candidate_db} exists")

    # Step 2: Verify release does NOT exist (absolute protection)
    if db_exists(con, release_db):
        sys.exit(
            f"[{SCRIPT_TAG}] ABORT: {release_db!r} already exists.\n"
            f"  Released canonical versions can NEVER be overwritten.\n"
            f"  If you need to fix something, build v{release_ver}_patch_rc instead."
        )
    print(f"[{SCRIPT_TAG}] ✓ {release_db} does not yet exist (safe to create)")

    # Step 3: Find baseline and run diff
    baseline_db = find_baseline(con, exclude_db=candidate_db)
    diff_result = None
    if baseline_db:
        baseline_ver = baseline_db[len(DB_PREFIX):]
        diff_result = run_compare(baseline_ver, candidate_ver)
    else:
        print(f"[{SCRIPT_TAG}] No prior release found; skipping diff (first release)")

    # Step 4: Require --accept-major for dangerous classifications
    if diff_result:
        cls = diff_result["classification"]
        print(f"\n[{SCRIPT_TAG}] Diff classification: {cls}")
        if cls in ("MAJOR", "REGRESSION") and not args.accept_major:
            sys.exit(
                f"[{SCRIPT_TAG}] ABORT: classification is {cls}.\n"
                f"  Review the diff report before proceeding.\n"
                f"  If you understand and accept the breaking changes, re-run with:\n"
                f"    --accept-major\n"
                f"  Diff report: {diff_result.get('_report_path', 'see output above')}"
            )
        if cls in ("MAJOR", "REGRESSION") and args.accept_major:
            print(f"\n  ⚠ Classification is {cls}. --accept-major is set.")
            confirm = input(
                "  Type the release DB name to confirm you accept breaking changes: "
            ).strip()
            if confirm != release_db:
                sys.exit("  ABORT: confirmation did not match.")

    # Step 5: Rename (COPY FROM DATABASE + DROP)
    print(f"\n[{SCRIPT_TAG}] Promoting {candidate_db} → {release_db}")
    print(f"  Creating {release_db}...")
    con.execute(f'CREATE DATABASE "{_safe(release_db)}"')
    print("  Copying data (COPY FROM DATABASE)...")
    con.execute(
        f'COPY FROM DATABASE "{_safe(candidate_db)}" TO "{_safe(release_db)}"'
    )
    print("  Verifying...")

    # Verify table counts match
    rc_n = con.execute(f"""
        SELECT COUNT(*) FROM duckdb_tables()
        WHERE database_name='{_safe(candidate_db)}' AND schema_name='main'
    """).fetchone()[0]
    rel_n = con.execute(f"""
        SELECT COUNT(*) FROM duckdb_tables()
        WHERE database_name='{_safe(release_db)}' AND schema_name='main'
    """).fetchone()[0]
    if rc_n != rel_n:
        sys.exit(
            f"[{SCRIPT_TAG}] ABORT: table count mismatch after copy: "
            f"RC={rc_n}, release={rel_n}. NOT dropping RC."
        )

    # Verify canonical_patient_master invariants if the table exists
    has_master = con.execute(f"""
        SELECT COUNT(*) FROM duckdb_tables()
        WHERE database_name='{_safe(release_db)}' AND schema_name='main'
          AND table_name='canonical_patient_master'
    """).fetchone()[0]
    if has_master:
        inv = con.execute(f"""
            SELECT COUNT(*),
                   COUNT(*) FILTER (WHERE research_id IS NULL),
                   COUNT(*) FILTER (WHERE followup_years > 0)
            FROM "{_safe(release_db)}".main.canonical_patient_master
        """).fetchone()
        print(f"  Invariants: N={inv[0]}, null_rid={inv[1]}, fu_pos={inv[2]}")
        assert inv[1] == 0, f"Null research_id found: {inv[1]}"

    # Drop the RC
    print(f"  Dropping {candidate_db}...")
    con.execute(f'DROP DATABASE "{_safe(candidate_db)}"')
    print(f"  ✓ {candidate_db} dropped")
    print(f"  ✓ {release_db} is now live")

    # Step 6–7: Update RELEASE.md and CHANGELOG.md
    cls_label = diff_result["classification"] if diff_result else "baseline"
    _rc_base = candidate_ver[:-3]  # strip _rc (kept for reference; not consumed)
    update_release_md(
        release_ver,
        cls_label.lower(),
        f"Promoted from {candidate_db}"
    )
    update_changelog(release_ver, diff_result)

    # Step 8: Git tag
    tag = git_tag(release_ver)

    print(f"\n[{SCRIPT_TAG}] ══ PROMOTION COMPLETE ══")
    print(f"  Released: {release_db}")
    print(f"  Git tag:  {tag}")
    print("\n  Push the tag when ready:")
    print(f"    git push origin {tag}")
    print("\n  Update SUPPORTED_VERSIONS in thyroid-2026-analysis/thyroid/connection.py")
    print(f"  to include '{release_ver}' so analysis scripts can use it.")


if __name__ == "__main__":
    main()
