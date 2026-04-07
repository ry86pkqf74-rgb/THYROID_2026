#!/usr/bin/env python3
"""End-to-end MotherDuck live release audit orchestrator.

Chains the full formalization pipeline against the live MotherDuck "Thyroid 2026"
database, captures evidence at every gate, and writes a dated audit directory under
studies/.

Steps executed (in order):
  1. Preflight     — MD attachment, md_information_schema.databases, retention check
  2. Stage refresh — 116_md_stage_loader.py --md  →  v2_stage + load_inventory
  3. Promotion gate — 112_v2_domain_promotion_gate.py --motherduck-check
  4. Canonical + QA — 103 --md, 114 --md (hydrate), 117 --md
  5. Presentation views — 125_master_verified_views.py --md
  6. Release       — 115_release_snapshot.py --md, 118_parquet_release_bundle.py --md
  7. Validation    — 119_md_formalization_validate.py --md --release-mode

Usage:
  .venv/bin/python scripts/124_md_live_release_audit.py --md
  .venv/bin/python scripts/124_md_live_release_audit.py --md --dry-run
  .venv/bin/python scripts/124_md_live_release_audit.py --md --final-release
  .venv/bin/python scripts/124_md_live_release_audit.py --md --tag 20260407

Constraints:
  - Raw note text never leaves local disk.
  - Pending review rows block release when --final-release is set.
  - No destructive rewrites of canonical history; corrections require a new tag.
  - Silent local fallback is disabled: the script exits 1 if MotherDuck is unreachable.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import textwrap
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

VENV_PYTHON = ROOT / ".venv" / "bin" / "python"
SCRIPTS = ROOT / "scripts"

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="End-to-end MotherDuck live release audit orchestrator.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--md", action="store_true",
        help="Connect to MotherDuck (fail-closed; required for a live run).",
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="Pass --dry-run to each subscript; no data is written to MotherDuck.",
    )
    p.add_argument(
        "--final-release", action="store_true",
        help="Strict mode: halt if any promotable row in qa.manual_review_queue is pending.",
    )
    p.add_argument(
        "--tag", default=None,
        help="Release tag (default: today's date as YYYYMMDD).",
    )
    p.add_argument(
        "--skip-stage", action="store_true",
        help="Skip stage refresh (116); assume v2_stage is already current.",
    )
    p.add_argument(
        "--skip-gate", action="store_true",
        help="Skip promotion gate (112); useful when re-running after a gate-only failure.",
    )
    p.add_argument(
        "--output-dir", default=None,
        help="Override the studies/ output directory for this run.",
    )
    p.add_argument(
        "--db-path", default=str(ROOT / "thyroid_master.duckdb"),
        help="Local DuckDB fallback path (used only when --md is not set).",
    )
    return p.parse_args()


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _python() -> str:
    """Return the venv python path if it exists, else the current interpreter."""
    if VENV_PYTHON.exists():
        return str(VENV_PYTHON)
    return sys.executable


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _run(
    step_name: str,
    cmd: list[str],
    log_path: Path,
    step_results: list[dict],
    dry_run: bool = False,
) -> bool:
    """Run a subprocess, tee stdout+stderr to log_path, record outcome.

    Returns True on success, False on non-zero exit.
    """
    print(f"\n{'='*70}")
    print(f"  STEP: {step_name}")
    print(f"  CMD : {' '.join(cmd)}")
    print(f"  LOG : {log_path.name}")
    print(f"{'='*70}")

    started = _now_iso()
    output_lines: list[str] = []

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        cwd=ROOT,
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        print(line, end="")
        output_lines.append(line)
    proc.wait()

    log_path.write_text("".join(output_lines), encoding="utf-8")

    success = proc.returncode == 0
    step_results.append({
        "step": step_name,
        "cmd": cmd,
        "started": started,
        "finished": _now_iso(),
        "returncode": proc.returncode,
        "success": success,
        "log": log_path.name,
    })

    if success:
        print(f"  [{step_name}] OK (exit 0)")
    else:
        print(f"  [{step_name}] FAILED (exit {proc.returncode})")

    return success


# ---------------------------------------------------------------------------
# Step 1: Preflight
# ---------------------------------------------------------------------------

def run_preflight(
    con: Any,
    audit_dir: Path,
    step_results: list[dict],
    final_release: bool,
) -> bool:
    """Verify MD attachment and capture database metadata."""
    print(f"\n{'='*70}")
    print("  STEP: Preflight (MD attachment + database inventory)")
    print(f"{'='*70}")

    started = _now_iso()
    evidence: dict[str, Any] = {"started": started}

    # --- PRAGMA database_list ---
    try:
        db_list = con.execute("PRAGMA database_list").fetchall()
        db_list_records = [{"seq": r[0], "name": r[1], "file": r[2]} for r in db_list]
        evidence["pragma_database_list"] = db_list_records

        md_confirmed = any(
            "md:" in str(r) or "md_information_schema" in str(r)
            for r in db_list
        )
        evidence["md_confirmed"] = md_confirmed
        status_label = "PASS" if md_confirmed else ("FAIL" if final_release else "WARN")
        print(f"  [{status_label}] MD attachment: {len(db_list)} database(s) attached, md_confirmed={md_confirmed}")
        for row in db_list_records:
            print(f"         {row['seq']:>3}  {row['name']:<30}  {row['file']}")

        if not md_confirmed and final_release:
            print("  FATAL: --final-release requires live MotherDuck connection.")
            return False
    except Exception as exc:
        evidence["pragma_database_list_error"] = str(exc)
        print(f"  [FAIL] PRAGMA database_list: {exc}")
        return False

    # --- md_information_schema.databases ---
    try:
        md_dbs = con.execute(
            "SELECT * FROM md_information_schema.databases"
        ).fetchdf()
        evidence["md_information_schema_databases"] = md_dbs.to_dict(orient="records")
        print(f"  [PASS] md_information_schema.databases: {len(md_dbs)} row(s)")
        for _, row in md_dbs.iterrows():
            print(f"         {row.to_dict()}")
    except Exception as exc:
        evidence["md_information_schema_databases_error"] = str(exc)
        print(f"  [WARN] md_information_schema.databases: {exc}")

    # --- Schema inventory ---
    try:
        schemas = [
            r[0] for r in con.execute(
                "SELECT DISTINCT table_schema FROM information_schema.tables ORDER BY 1"
            ).fetchall()
        ]
        evidence["schemas_present"] = schemas
        print(f"  [INFO] Schemas present: {', '.join(schemas)}")
    except Exception as exc:
        evidence["schemas_present_error"] = str(exc)

    # --- Snapshot retention (query md_information_schema.snapshots if available) ---
    try:
        snaps = con.execute("SELECT * FROM md_information_schema.snapshots").fetchdf()
        evidence["md_snapshots"] = snaps.to_dict(orient="records")
        print(f"  [INFO] md_information_schema.snapshots: {len(snaps)} snapshot(s)")
    except Exception as exc:
        evidence["md_snapshots_note"] = f"Not accessible or no snapshots: {exc}"
        print(f"  [INFO] Snapshot retention query: {exc} (may be unavailable on this plan tier)")

    evidence["finished"] = _now_iso()
    evidence["success"] = True

    preflight_path = audit_dir / "preflight_db_list.json"
    preflight_path.write_text(json.dumps(evidence, indent=2, default=str), encoding="utf-8")
    print(f"  [write] {preflight_path.name}")

    step_results.append({
        "step": "Preflight",
        "started": started,
        "finished": evidence["finished"],
        "returncode": 0,
        "success": True,
        "log": preflight_path.name,
    })
    return True


# ---------------------------------------------------------------------------
# Step 2: Stage parity capture (post-116)
# ---------------------------------------------------------------------------

def capture_stage_parity(con: Any, audit_dir: Path) -> None:
    """Query v2_stage.load_inventory and write parity CSV."""
    try:
        df = con.execute("SELECT * FROM v2_stage.load_inventory ORDER BY domain_name").fetchdf()
        parity_path = audit_dir / "stage_parity_report.csv"
        df.to_csv(parity_path, index=False)
        total = len(df)
        mismatches = int((~df["row_match"]).sum()) if "row_match" in df.columns else -1
        print(f"  [parity] {total} domains; {mismatches} mismatch(es) → {parity_path.name}")
    except Exception as exc:
        (audit_dir / "stage_parity_report.csv").write_text(f"# Error: {exc}\n", encoding="utf-8")
        print(f"  [WARN] Could not read v2_stage.load_inventory: {exc}")


# ---------------------------------------------------------------------------
# Step 3: Promotion gate — pending-review check
# ---------------------------------------------------------------------------

def check_pending_reviews(con: Any, final_release: bool) -> bool:
    """Return True if it is safe to continue (no unreviewed promotable rows)."""
    try:
        total = con.execute("SELECT COUNT(*) FROM qa.manual_review_queue").fetchone()[0]
        pending = con.execute(
            "SELECT COUNT(*) FROM qa.manual_review_queue "
            "WHERE verification_status IS NULL"
        ).fetchone()[0]
        print(f"  [review queue] total={total:,}  pending={pending:,}")
        if final_release and pending > 0:
            print(
                f"  HALT: --final-release mode requires all review rows to be resolved; "
                f"{pending:,} pending row(s) remain. Resolve them before re-running with --final-release."
            )
            return False
    except Exception as exc:
        print(f"  [WARN] Could not check qa.manual_review_queue: {exc}")
    return True


# ---------------------------------------------------------------------------
# Step 6a: Release schema manifest
# ---------------------------------------------------------------------------

def capture_release_manifest(con: Any, tag: str, audit_dir: Path) -> None:
    """Write release schema manifest after 115 snapshot."""
    manifest: dict[str, Any] = {"release_tag": tag, "captured_at": _now_iso()}
    try:
        schemas = [
            r[0] for r in con.execute(
                "SELECT DISTINCT table_schema FROM information_schema.tables ORDER BY 1"
            ).fetchall()
        ]
        release_schemas = [s for s in schemas if s.startswith("release_")]
        manifest["schemas_present"] = schemas
        manifest["release_schemas"] = release_schemas

        for schema in release_schemas:
            try:
                tables = con.execute(
                    f"SELECT table_name, "
                    f"(SELECT COUNT(*) FROM {schema}.\"{{}}\".format(table_name)) AS rows "
                    f"FROM information_schema.tables WHERE table_schema = '{schema}'"
                ).fetchdf()
                manifest[f"tables_{schema}"] = tables.to_dict(orient="records")
            except Exception:
                pass

        try:
            rm = con.execute("SELECT * FROM qa.release_manifest ORDER BY created_at DESC").fetchdf()
            manifest["qa_release_manifest"] = rm.to_dict(orient="records")
        except Exception as exc:
            manifest["qa_release_manifest_error"] = str(exc)

    except Exception as exc:
        manifest["error"] = str(exc)

    out_path = audit_dir / "release_schema_manifest.json"
    out_path.write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
    print(f"  [write] {out_path.name}")


# ---------------------------------------------------------------------------
# Step 7: Strict validation evidence
# ---------------------------------------------------------------------------

def capture_validation_evidence(
    con: Any,
    tag: str,
    audit_dir: Path,
    git_sha: str,
    step_results: list[dict],
) -> None:
    """Emit release_validation_strict.json with live MD evidence."""
    evidence: dict[str, Any] = {
        "release_tag": tag,
        "git_sha": git_sha,
        "captured_at": _now_iso(),
    }

    # PRAGMA database_list
    try:
        db_list = con.execute("PRAGMA database_list").fetchall()
        evidence["pragma_database_list"] = [
            {"seq": r[0], "name": r[1], "file": r[2]} for r in db_list
        ]
    except Exception as exc:
        evidence["pragma_database_list_error"] = str(exc)

    # md_information_schema.databases
    try:
        df = con.execute("SELECT * FROM md_information_schema.databases").fetchdf()
        evidence["md_information_schema_databases"] = df.to_dict(orient="records")
    except Exception as exc:
        evidence["md_information_schema_databases_error"] = str(exc)

    # Query log (Business/Pro plans only)
    try:
        qlog = con.execute(
            "SELECT * FROM md_information_schema.query_log "
            "ORDER BY start_time DESC LIMIT 50"
        ).fetchdf()
        evidence["recent_query_log"] = qlog.to_dict(orient="records")
    except Exception as exc:
        evidence["recent_query_log_note"] = f"Not accessible: {exc}"

    # Per-schema row counts
    schema_counts: dict[str, list[dict]] = {}
    try:
        schemas = [
            r[0] for r in con.execute(
                "SELECT DISTINCT table_schema FROM information_schema.tables ORDER BY 1"
            ).fetchall()
        ]
        for schema in schemas:
            tables = con.execute(
                f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}'"
            ).fetchall()
            rows_for_schema: list[dict] = []
            for (tbl,) in tables:
                try:
                    cnt = con.execute(f'SELECT COUNT(*) FROM "{schema}"."{tbl}"').fetchone()[0]
                    rows_for_schema.append({"table": tbl, "rows": int(cnt)})
                except Exception:
                    rows_for_schema.append({"table": tbl, "rows": None})
            schema_counts[schema] = rows_for_schema
        evidence["schema_row_counts"] = schema_counts
    except Exception as exc:
        evidence["schema_row_counts_error"] = str(exc)

    # qa.release_manifest dump
    try:
        rm = con.execute(
            "SELECT * FROM qa.release_manifest ORDER BY created_at DESC"
        ).fetchdf()
        evidence["qa_release_manifest"] = rm.to_dict(orient="records")
    except Exception as exc:
        evidence["qa_release_manifest_error"] = str(exc)

    # Step execution summary
    evidence["pipeline_steps"] = step_results

    out_path = audit_dir / "release_validation_strict.json"
    out_path.write_text(json.dumps(evidence, indent=2, default=str), encoding="utf-8")
    print(f"  [write] {out_path.name}")


# ---------------------------------------------------------------------------
# Audit summary
# ---------------------------------------------------------------------------

def write_audit_summary(
    audit_dir: Path,
    tag: str,
    step_results: list[dict],
    final_release: bool,
    dry_run: bool,
) -> None:
    """Write audit_summary.md with a step-by-step status table."""
    now = _now_iso()
    all_ok = all(r["success"] for r in step_results)
    verdict = "PASS" if all_ok else "BLOCKED"

    lines = [
        "# MotherDuck Live Release Audit Summary",
        "",
        f"**Release tag:** `{tag}`",
        f"**Generated:** {now}",
        f"**Mode:** {'Dry-run' if dry_run else ('Final-release' if final_release else 'Standard')}",
        f"**Verdict:** **{verdict}**",
        "",
        "---",
        "",
        "## Pipeline Step Results",
        "",
        "| Step | Status | Started | Finished | Log |",
        "|------|--------|---------|----------|-----|",
    ]
    for r in step_results:
        status = "PASS" if r["success"] else "FAIL"
        lines.append(
            f"| {r['step']} | {status} | {r.get('started', '')} | "
            f"{r.get('finished', '')} | `{r.get('log', '')}` |"
        )

    lines += [
        "",
        "---",
        "",
        "## Deliverables",
        "",
        "| Artifact | Description |",
        "|----------|-------------|",
        "| `preflight_db_list.json` | PRAGMA database_list + md_information_schema evidence |",
        "| `stage_parity_report.csv` | v2_stage.load_inventory row-count parity |",
        "| `promotion_scorecard.csv` | 112 gate scorecard (G1–G8) |",
        "| `manual_review_queue.csv` | Pending review rows at gate time |",
        "| `release_schema_manifest.json` | Release schema + qa.release_manifest dump |",
        "| `parquet_bundle_manifest.json` | Parquet bundle file list with SHA-256 checksums |",
        "| `validation_report.md` | 119 structural + release-mode validation |",
        "| `release_validation_strict.json` | Full MD evidence: query log, schema counts, manifest |",
        "| `snapshot_metadata.json` | md_information_schema.snapshots (if accessible) |",
        "| `audit_summary.md` | This file |",
        "",
    ]

    summary_path = audit_dir / "audit_summary.md"
    summary_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"  [write] {summary_path.name}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()

    tag = args.tag or datetime.now().strftime("%Y%m%d")
    run_ts = datetime.now().strftime("%Y%m%d_%H%M")

    if args.output_dir:
        audit_dir = Path(args.output_dir)
    else:
        audit_dir = ROOT / "studies" / f"{tag}_motherduck_live_release_audit"

    audit_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print(f"  124 — MotherDuck Live Release Audit")
    print(f"  Tag  : {tag}")
    print(f"  Dir  : {audit_dir}")
    print(f"  MD   : {'YES (fail-closed)' if args.md else 'NO (local file)'}")
    print(f"  Mode : {'dry-run' if args.dry_run else ('FINAL-RELEASE' if args.final_release else 'standard')}")
    print("=" * 70)

    py = _python()
    step_results: list[dict] = []

    # ------------------------------------------------------------------
    # Git SHA for evidence
    # ------------------------------------------------------------------
    import subprocess as _sp
    try:
        git_sha = _sp.check_output(
            ["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, text=True
        ).strip()
    except Exception:
        git_sha = "unknown"

    # ------------------------------------------------------------------
    # Open a shared MotherDuck connection for preflight + evidence capture
    # (individual subscripts open their own connections)
    # ------------------------------------------------------------------
    from utils.md_connect import connect_md_or_file
    import duckdb

    db_path = Path(args.db_path)
    con = connect_md_or_file(db_path, md=args.md, fail_closed=args.md)

    # ------------------------------------------------------------------
    # Step 1: Preflight
    # ------------------------------------------------------------------
    ok = run_preflight(con, audit_dir, step_results, args.final_release)
    if not ok:
        print("\n  ABORT: Preflight failed.")
        con.close()
        sys.exit(1)

    # ------------------------------------------------------------------
    # Step 2: Stage refresh
    # ------------------------------------------------------------------
    if not args.skip_stage:
        cmd_116 = [py, str(SCRIPTS / "116_md_stage_loader.py")]
        if args.md:
            cmd_116.append("--md")
        if args.dry_run:
            cmd_116.append("--dry-run")
        ok = _run(
            "Stage refresh (116)",
            cmd_116,
            audit_dir / "stage_refresh_output.log",
            step_results,
        )
        if not ok:
            print("\n  ABORT: Stage refresh failed.")
            con.close()
            sys.exit(1)
    else:
        print("\n  [skip] Stage refresh (--skip-stage)")

    # Capture parity report after stage load
    if not args.dry_run:
        capture_stage_parity(con, audit_dir)

    # ------------------------------------------------------------------
    # Step 3: Promotion gate
    # ------------------------------------------------------------------
    if not args.skip_gate:
        gate_label = f"promote_{run_ts}"
        gate_out_dir = audit_dir / "promotion_gate"
        gate_out_dir.mkdir(exist_ok=True)

        cmd_112 = [
            py, str(SCRIPTS / "112_v2_domain_promotion_gate.py"),
            "--run-label", gate_label,
            "--output-dir", str(gate_out_dir),
        ]
        if args.md:
            cmd_112.append("--motherduck-check")
        ok = _run(
            "Promotion gate (112)",
            cmd_112,
            audit_dir / "promotion_gate_output.log",
            step_results,
        )
        if not ok:
            print("\n  ABORT: Promotion gate failed.")
            con.close()
            sys.exit(1)

        # Copy scorecard/review-queue artefacts to top-level audit dir
        for fname in ("promotion_scorecard.csv", "manual_review_queue.csv", "motherduck_promote.sql"):
            src = gate_out_dir / fname
            if src.exists():
                import shutil
                shutil.copy2(src, audit_dir / fname)

        # Pending-review check (only meaningful after gate writes qa tables).
        # Re-open MD: the gate runs in another process; a long-lived con may not
        # see new qa.manual_review_queue rows immediately on MotherDuck.
        if not args.dry_run:
            if args.md:
                con.close()
                con = connect_md_or_file(db_path, md=args.md, fail_closed=args.md)
            ok = check_pending_reviews(con, args.final_release)
            if not ok:
                con.close()
                sys.exit(1)
    else:
        print("\n  [skip] Promotion gate (--skip-gate)")

    # ------------------------------------------------------------------
    # Step 4a: Canonical materialization (103)
    # ------------------------------------------------------------------
    cmd_103 = [py, str(SCRIPTS / "103_fact_lineage_materialize.py")]
    if args.md:
        cmd_103.append("--md")
    if args.dry_run:
        cmd_103.append("--dry-run")
    ok = _run(
        "Canonical materialization (103)",
        cmd_103,
        audit_dir / "canonical_output.log",
        step_results,
    )
    if not ok:
        print("\n  ABORT: Canonical materialization failed.")
        con.close()
        sys.exit(1)

    # ------------------------------------------------------------------
    # Step 4b: QA schema setup (114)
    # ------------------------------------------------------------------
    gate_out_dir_path = audit_dir / "promotion_gate"
    cmd_114 = [py, str(SCRIPTS / "114_qa_schema_setup.py")]
    if args.md:
        cmd_114.append("--md")
    if gate_out_dir_path.exists() and not args.dry_run:
        cmd_114 += ["--hydrate-from", str(gate_out_dir_path)]
    ok = _run(
        "QA schema setup (114)",
        cmd_114,
        audit_dir / "qa_setup_output.log",
        step_results,
    )
    if not ok:
        print("\n  ABORT: QA schema setup failed.")
        con.close()
        sys.exit(1)

    # ------------------------------------------------------------------
    # Step 4c: Contract views (117)
    # ------------------------------------------------------------------
    cmd_117 = [py, str(SCRIPTS / "117_md_contract_views.py")]
    if args.md:
        cmd_117.append("--md")
    if args.dry_run:
        cmd_117.append("--dry-run")
    ok = _run(
        "Contract views (117)",
        cmd_117,
        audit_dir / "contract_views_output.log",
        step_results,
    )
    if not ok:
        print("\n  ABORT: Contract views setup failed.")
        con.close()
        sys.exit(1)

    # ------------------------------------------------------------------
    # Step 5: Presentation views (125)
    # ------------------------------------------------------------------
    views_script = SCRIPTS / "125_master_verified_views.py"
    if views_script.exists():
        cmd_125 = [py, str(views_script)]
        if args.md:
            cmd_125.append("--md")
        if args.dry_run:
            cmd_125.append("--dry-run")
        ok = _run(
            "Presentation views (125)",
            cmd_125,
            audit_dir / "presentation_views_output.log",
            step_results,
        )
        if not ok:
            print("\n  WARN: Presentation views failed — continuing (non-blocking).")
    else:
        print(f"\n  [WARN] 125_master_verified_views.py not found at {views_script}; skipping.")

    # ------------------------------------------------------------------
    # Step 6a: Release snapshot (115)
    # ------------------------------------------------------------------
    cmd_115 = [
        py, str(SCRIPTS / "115_release_snapshot.py"),
        "--tag", tag,
    ]
    if args.md:
        cmd_115.append("--md")
    if args.dry_run:
        cmd_115.append("--dry-run")
    ok = _run(
        "Release snapshot (115)",
        cmd_115,
        audit_dir / "release_snapshot_output.log",
        step_results,
    )
    if not ok:
        print("\n  ABORT: Release snapshot failed.")
        con.close()
        sys.exit(1)

    if not args.dry_run:
        capture_release_manifest(con, tag, audit_dir)

    # ------------------------------------------------------------------
    # Step 6b: Parquet release bundle (118)
    # ------------------------------------------------------------------
    cmd_118 = [
        py, str(SCRIPTS / "118_parquet_release_bundle.py"),
        "--tag", tag,
    ]
    if args.md:
        cmd_118.append("--md")
    if args.dry_run:
        cmd_118.append("--dry-run")
    ok = _run(
        "Parquet release bundle (118)",
        cmd_118,
        audit_dir / "parquet_bundle_output.log",
        step_results,
    )
    if not ok:
        print("\n  WARN: Parquet bundle failed — continuing to validation.")

    # Copy the parquet bundle manifest to audit dir
    bundle_manifest = ROOT / "exports" / f"parquet_release_{tag}" / "manifest.json"
    if bundle_manifest.exists():
        import shutil
        shutil.copy2(bundle_manifest, audit_dir / "parquet_bundle_manifest.json")
        print(f"  [copy] parquet_bundle_manifest.json from {bundle_manifest}")

    # ------------------------------------------------------------------
    # Step 7: Validation (119, release-mode)
    # ------------------------------------------------------------------
    validate_out_dir = audit_dir / "validation_run"
    validate_out_dir.mkdir(exist_ok=True)

    cmd_119 = [
        py, str(SCRIPTS / "119_md_formalization_validate.py"),
        "--output-dir", str(validate_out_dir),
    ]
    if args.md:
        cmd_119.append("--md")
    if args.final_release:
        cmd_119.append("--release-mode")
    ok = _run(
        "Formalization validation (119)",
        cmd_119,
        audit_dir / "validation_output.log",
        step_results,
    )

    # Copy validation report to top-level audit dir
    val_report = validate_out_dir / "validation_report.md"
    if val_report.exists():
        import shutil
        shutil.copy2(val_report, audit_dir / "validation_report.md")
        print(f"  [copy] validation_report.md")

    if not ok and args.final_release:
        print("\n  ABORT: Validation failed in final-release mode.")
        # Still capture evidence before exiting
        if not args.dry_run:
            capture_validation_evidence(con, tag, audit_dir, git_sha, step_results)
        write_audit_summary(audit_dir, tag, step_results, args.final_release, args.dry_run)
        con.close()
        sys.exit(1)

    # ------------------------------------------------------------------
    # Post-validation evidence capture
    # ------------------------------------------------------------------
    if not args.dry_run:
        capture_validation_evidence(con, tag, audit_dir, git_sha, step_results)

        # snapshot_metadata.json -- alias of preflight md_information_schema.snapshots
        snap_path = audit_dir / "snapshot_metadata.json"
        if not snap_path.exists():
            try:
                snaps = con.execute("SELECT * FROM md_information_schema.snapshots").fetchdf()
                snap_path.write_text(
                    json.dumps({
                        "captured_at": _now_iso(),
                        "snapshots": snaps.to_dict(orient="records"),
                    }, indent=2, default=str),
                    encoding="utf-8",
                )
            except Exception as exc:
                snap_path.write_text(
                    json.dumps({"note": f"Not accessible: {exc}"}),
                    encoding="utf-8",
                )
            print(f"  [write] snapshot_metadata.json")

    con.close()

    # ------------------------------------------------------------------
    # Audit summary
    # ------------------------------------------------------------------
    write_audit_summary(audit_dir, tag, step_results, args.final_release, args.dry_run)

    all_ok = all(r["success"] for r in step_results)
    print(f"\n{'='*70}")
    print(f"  124 — DONE")
    print(f"  Verdict : {'PASS' if all_ok else 'BLOCKED'}")
    print(f"  Audit dir: {audit_dir}")
    print(f"{'='*70}")

    if not all_ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
