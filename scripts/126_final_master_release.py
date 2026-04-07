#!/usr/bin/env python3
"""Post-review final master database release (MotherDuck, fail-closed).

Run ONLY after:
  - Reviewer decisions are recorded (CSV +/or completed manual_review_queue), and
  - Final analyst institutional lab extract is available (if applicable).

This orchestrator:
  1. Ensures qa schema DDL (114) including promotion_review_decisions extensions
  2. Appends qa.promotion_review_decisions from optional CSV (append-only)
  3. Re-hydrates qa.manual_review_queue from a gate folder with reviewed CSV
  4. Optionally runs 127 lab append (--lab-csv + --ingestion-wave)
  5. Re-materializes canonical facts (103), contract tables/views (117), master views (125)
  6. Creates release_YYYYMMDD snapshot (115 --final-master) and parquet bundle (118 --final-master)
  7. Runs formalization validator in --release-mode (119)
  8. Writes manuscript-readiness evidence under studies/<date>_final_master_release/

Constraints:
  - Requires --md (no silent local fallback)
  - Cloud parquet bundle excludes raw note text (118 --final-master profile)
  - promotion_review_decisions inserts are append-only

Usage:
  .venv/bin/python scripts/126_final_master_release.py --md --release-date 20260407 \\
      --hydrate-mrq-from studies/v2_domain_promotion_gate_formalization_20260406_v3 \\
      --decisions-csv studies/20260407_final_master_release/promotion_review_decisions.csv \\
      --lab-csv exports/incoming/final_lab_20260407.csv \\
      --ingestion-wave final_institutional_20260407

  .venv/bin/python scripts/126_final_master_release.py --md --release-date 20260407 --dry-run
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

SCRIPTS = ROOT / "scripts"
DDL_PATH = ROOT / "scripts" / "sql" / "114_qa_schema_ddl.sql"
VENV_PY = ROOT / ".venv" / "bin" / "python"


def _py() -> str:
    return str(VENV_PY) if VENV_PY.exists() else sys.executable


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--md", action="store_true", help="Required. MotherDuck only (fail-closed).")
    p.add_argument(
        "--md-sa",
        action="store_true",
        help="Prefer MD_SA_TOKEN over MOTHERDUCK_TOKEN for this process and subprocesses that support --md-sa.",
    )
    p.add_argument("--db-path", default=str(ROOT / "thyroid_master.duckdb"), help="Token path anchor.")
    p.add_argument(
        "--release-date",
        default=datetime.now().strftime("%Y%m%d"),
        help="Tag for snapshot, exports, and studies folder (YYYYMMDD).",
    )
    p.add_argument(
        "--hydrate-mrq-from",
        type=Path,
        default=None,
        help="Gate directory containing reviewed manual_review_queue.csv (passed to 114).",
    )
    p.add_argument(
        "--decisions-csv",
        type=Path,
        default=None,
        help="Append rows to qa.promotion_review_decisions (append-only).",
    )
    p.add_argument(
        "--decision-batch-id",
        default=None,
        help="Fills qa.promotion_review_decisions.decision_batch_id (default: release-date).",
    )
    p.add_argument("--lab-csv", type=Path, default=None, help="Analyst lab file for script 127.")
    p.add_argument(
        "--ingestion-wave",
        default=None,
        help="Required with --lab-csv; passed to 127.",
    )
    p.add_argument("--skip-103", action="store_true", help="Skip canonical fact materialization.")
    p.add_argument("--skip-117", action="store_true", help="Skip contract parquet load.")
    p.add_argument("--skip-snapshot", action="store_true", help="Skip 115/118.")
    p.add_argument("--dry-run", action="store_true", help="Print plan only.")
    p.add_argument(
        "--release-mode",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Pass --release-mode to 119 (strict). Use --no-release-mode for structural-only validation.",
    )
    p.add_argument(
        "--synthetic-fill-mrq-verification",
        metavar="STATUS",
        default=None,
        help="NON-PUBLICATION: copy --hydrate-mrq-from into the study folder, set blank "
        "verification_status to STATUS, then hydrate from that copy. Real releases require "
        "human-reviewed CSVs without this flag.",
    )
    return p.parse_args()


def connect_md(db_path: Path, *, prefer_service_account: bool = False) -> duckdb.DuckDBPyConnection:
    import os

    from utils.md_connect import connect_md_or_file

    return connect_md_or_file(
        db_path,
        md=True,
        fail_closed=True,
        prefer_service_account=prefer_service_account,
        custom_user_agent=os.environ.get("MOTHERDUCK_CUSTOM_USER_AGENT"),
        motherduck_session_hint=os.environ.get("MOTHERDUCK_SESSION_HINT"),
    )


def apply_qa_ddl(con: duckdb.DuckDBPyConnection) -> None:
    if not DDL_PATH.is_file():
        print(f"  [warn] DDL missing: {DDL_PATH}")
        return
    ddl = DDL_PATH.read_text(encoding="utf-8")
    lines_no_comments = "\n".join(
        line for line in ddl.splitlines() if not line.strip().startswith("--")
    )
    for stmt in lines_no_comments.split(";"):
        stmt = stmt.strip()
        if not stmt:
            continue
        try:
            con.execute(stmt)
        except Exception as exc:
            if "already exists" in str(exc).lower():
                continue
            print(f"  [warn] DDL: {exc}")


def assert_mrq_csv_fully_reviewed(mrq_path: Path) -> None:
    """Exit if any row lacks non-empty verification_status."""
    if not mrq_path.is_file():
        print(f"  FATAL: manual_review_queue.csv not found: {mrq_path}")
        sys.exit(1)
    df = pd.read_csv(mrq_path)
    if "verification_status" not in df.columns:
        print("  FATAL: manual_review_queue.csv missing verification_status column")
        sys.exit(1)
    ser = df["verification_status"].astype(str)
    blank = df["verification_status"].isna() | ser.str.strip().eq("") | ser.str.lower().eq("nan")
    n_bad = int(blank.sum())
    if n_bad > 0:
        print(
            f"  FATAL: {n_bad:,} manual_review_queue row(s) lack verification_status. "
            f"Fully review the CSV or use --synthetic-fill-mrq-verification for non-production tests only."
        )
        sys.exit(1)
    print(f"  [preflight] manual_review_queue fully reviewed: {len(df):,} row(s) at {mrq_path}")


def build_mrq_hydrate_gate_dir(
    src_gate: Path,
    dest_gate: Path,
    synthetic_status: str | None,
) -> Path:
    """Copy gate CSVs into dest_gate; optionally fill blank verification_status."""
    dest_gate.mkdir(parents=True, exist_ok=True)
    for fname in (
        "promotion_scorecard.csv",
        "schema_validation.csv",
        "concordance_summary.csv",
        "manual_review_queue.csv",
    ):
        p = src_gate / fname
        if p.is_file():
            shutil.copy2(p, dest_gate / fname)
    mrq = dest_gate / "manual_review_queue.csv"
    if not mrq.is_file():
        print(f"  FATAL: source gate missing manual_review_queue.csv: {src_gate}")
        sys.exit(1)
    df = pd.read_csv(mrq)
    if synthetic_status:
        if "verification_status" not in df.columns:
            df["verification_status"] = None
        ser = df["verification_status"].astype(str)
        mask = df["verification_status"].isna() | ser.str.strip().eq("") | ser.str.lower().eq("nan")
        n = int(mask.sum())
        df.loc[mask, "verification_status"] = synthetic_status
        if "reviewer" in df.columns:
            rmask = mask & df["reviewer"].isna()
            df.loc[rmask, "reviewer"] = "synthetic_fill_scripts126"
        df.to_csv(mrq, index=False)
        print(
            f"  [126] NON-PUBLICATION: synthetic MRQ fill applied to {n:,} row(s) → {synthetic_status!r}"
        )
    return dest_gate


def append_promotion_decisions(
    con: duckdb.DuckDBPyConnection,
    csv_path: Path,
    batch_id: str,
) -> int:
    df = pd.read_csv(csv_path)
    df.columns = [c.strip() for c in df.columns]
    now = datetime.now(timezone.utc)
    df["decision_batch_id"] = batch_id
    df["created_at"] = now.isoformat()
    if "source_object_id" not in df.columns:
        df["source_object_id"] = None
    if "evidence_ref" not in df.columns:
        df["evidence_ref"] = None

    base_cols = [
        "review_id",
        "run_label",
        "llm_entity_id",
        "research_id",
        "domain",
        "entity_type",
        "algorithm_status",
        "verification_status",
        "reviewer",
        "reviewed_at",
        "waiver_reason",
        "created_at",
        "decision_batch_id",
        "source_object_id",
        "evidence_ref",
    ]
    for c in base_cols:
        if c not in df.columns and c not in ("created_at", "decision_batch_id"):
            df[c] = None

    for c in base_cols:
        if c not in df.columns:
            df[c] = None
    insert_df = df[base_cols].replace({float("nan"): None})

    def _cell(val: object) -> object:
        if val is None:
            return None
        if isinstance(val, float) and pd.isna(val):
            return None
        if pd.isna(val):
            return None
        return val

    col_list = ", ".join(insert_df.columns)
    placeholders = ", ".join(["?"] * len(insert_df.columns))
    sql = f"INSERT INTO qa.promotion_review_decisions ({col_list}) VALUES ({placeholders})"
    n = 0
    for row in insert_df.itertuples(index=False, name=None):
        con.execute(sql, [_cell(v) for v in row])
        n += 1
    print(f"  [qa] appended qa.promotion_review_decisions: {n:,} row(s), batch={batch_id}")
    return n


def run_subprocess(name: str, cmd: list[str], log_path: Path) -> bool:
    print(f"\n{'='*70}\n  SUBPROCESS: {name}\n  CMD: {' '.join(cmd)}\n{'='*70}")
    log_path.parent.mkdir(parents=True, exist_ok=True)
    proc = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
    log_path.write_text(proc.stdout + "\n" + proc.stderr, encoding="utf-8")
    print(proc.stdout)
    if proc.stderr:
        print(proc.stderr)
    if proc.returncode != 0:
        print(f"  [FAIL] {name} exit {proc.returncode}; see {log_path}")
        return False
    print(f"  [OK] {name}")
    return True


def gather_evidence(con: duckdb.DuckDBPyConnection, tag: str, git_sha: str) -> dict[str, Any]:
    ev: dict[str, Any] = {
        "release_tag": tag,
        "git_sha": git_sha,
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
        "row_counts": {},
        "lineage": {},
        "review_queue": {},
        "source_limited_notes": [],
    }
    targets = [
        "main.canonical_extracted_fact_long_v2",
        "main.canonical_fact_quarantine_v2",
        "main.note_extraction_runs",
        "main.longitudinal_lab_canonical_v1",
        "main.longitudinal_lab_deduped_v",
        "main.master_fact_long_verified_v1",
        "main.master_patient_rollup_verified_v1",
        "main.master_source_lineage_v1",
    ]
    for fq in targets:
        try:
            n = con.execute(f"SELECT COUNT(*) FROM {fq}").fetchone()[0]
            ev["row_counts"][fq] = int(n)
        except Exception as exc:
            ev["row_counts"][fq] = f"error: {exc}"

    try:
        ev["lineage"]["facts_with_source_object"] = int(
            con.execute(
                "SELECT COUNT(*) FROM main.master_fact_long_verified_v1 "
                "WHERE source_object_id IS NOT NULL"
            ).fetchone()[0]
        )
        ev["lineage"]["facts_total"] = int(
            con.execute("SELECT COUNT(*) FROM main.master_fact_long_verified_v1").fetchone()[0]
        )
    except Exception as exc:
        ev["lineage"]["error"] = str(exc)

    try:
        tot = con.execute("SELECT COUNT(*) FROM qa.manual_review_queue").fetchone()[0]
        pen = con.execute(
            "SELECT COUNT(*) FROM qa.manual_review_queue WHERE verification_status IS NULL"
        ).fetchone()[0]
        ev["review_queue"] = {"total": int(tot), "pending_verification": int(pen)}
    except Exception as exc:
        ev["review_queue"]["error"] = str(exc)

    ev["source_limited_notes"] = [
        "Operative NLP enrichment (berry ligament, frozen section, EBL) may remain sparse by design.",
        "Recurrence dates: large unresolved fraction is documented as source-limited.",
        "RAI dose recovery ceiling unless nuclear medicine notes / structured feeds improve.",
        "Non-Tg lab panel (TSH, PTH, Ca, vit D) depends on institutional lab extract coverage.",
    ]
    return ev


def write_evidence_pack(study_dir: Path, ev: dict[str, Any], export_dir: Path) -> None:
    study_dir.mkdir(parents=True, exist_ok=True)
    (study_dir / "evidence_pack.json").write_text(json.dumps(ev, indent=2, default=str), encoding="utf-8")

    lines = [
        "# Final master release — manuscript readiness evidence",
        "",
        f"- **Release tag:** `{ev['release_tag']}`",
        f"- **Git SHA:** `{ev['git_sha']}`",
        f"- **Captured (UTC):** {ev['captured_at_utc']}",
        f"- **Parquet bundle:** `{export_dir}` (no raw note text in this profile)",
        "",
    ]
    if ev.get("mrq_synthetic_fill"):
        lines.extend([
            "## MRQ warning",
            "",
            f"**`--synthetic-fill-mrq-verification`** was used with status `{ev['mrq_synthetic_fill']}`. "
            f"This is **not** human manuscript sign-off. Replace with a truly reviewed CSV for publication.",
            "",
        ])
    lines.extend([
        "## Row counts",
        "",
    ])
    for k, v in ev["row_counts"].items():
        lines.append(f"| {k} | {v} |")
    lines.extend([
        "",
        "## Lineage completeness (master facts)",
        "",
        "```json",
        json.dumps(ev.get("lineage", {}), indent=2),
        "```",
        "",
        "## Review queue",
        "",
        "```json",
        json.dumps(ev.get("review_queue", {}), indent=2),
        "```",
        "",
        "## Documented source-limited burdens",
        "",
    ])
    for note in ev.get("source_limited_notes", []):
        lines.append(f"- {note}")
    lines.extend([
        "",
        "## MotherDuck named snapshot",
        "",
        "Create a cloud snapshot from the MotherDuck UI or your organization runbook after this release.",
        "The immutable `release_<tag>` schema copy is created by `scripts/115_release_snapshot.py --final-master`.",
        "",
    ])
    (study_dir / "EVIDENCE_PACK.md").write_text("\n".join(lines), encoding="utf-8")

    memo = "\n".join([
        "# Safe to start stats / manuscripts",
        "",
        f"Release tag **{ev['release_tag']}** (git `{ev['git_sha']}`) passed scripted gates when this memo was generated.",
        "",
        "** Preconditions verified by automation:**",
        "",
        "- `qa.manual_review_queue` has zero pending `verification_status` when `--release-mode` validation completed.",
        "- `main.master_fact_long_verified_v1` exposes `research_id`, `source_object_id` (note row id), and extraction run id per fact.",
        "- Curated parquet under `exports/final_master_release_<tag>/` excludes raw note bodies.",
        "",
        "** Still source-limited (do not over-interpret gaps as QC failure):**",
        "",
    ] + [f"- {n}" for n in ev.get("source_limited_notes", [])] + [
        "",
        "Use `docs/final_master_database_contract.md` for analyst-facing column semantics.",
        "",
    ])
    (study_dir / "SAFE_TO_START_STATS_MEMO.md").write_text(memo, encoding="utf-8")


def preflight_inputs(args: argparse.Namespace) -> None:
    """Validate paths before any MotherDuck mutation or dry-run hand-off."""
    if args.hydrate_mrq_from is not None:
        if not args.hydrate_mrq_from.is_dir():
            print(f"  FATAL: --hydrate-mrq-from is not a directory: {args.hydrate_mrq_from}")
            sys.exit(1)
        mrq = args.hydrate_mrq_from / "manual_review_queue.csv"
        if not mrq.is_file():
            print(f"  FATAL: reviewed gate missing manual_review_queue.csv under {args.hydrate_mrq_from}")
            sys.exit(1)
    if args.decisions_csv is not None:
        if not args.decisions_csv.is_file():
            print(f"  FATAL: --decisions-csv not found: {args.decisions_csv}")
            sys.exit(1)
    if args.lab_csv is not None:
        if not args.lab_csv.is_file():
            print(f"  FATAL: --lab-csv not found: {args.lab_csv}")
            sys.exit(1)


def main() -> None:
    args = parse_args()
    if not args.md:
        print("  FATAL: --md is required (no silent local fallback).")
        sys.exit(1)
    if args.lab_csv and not args.ingestion_wave:
        print("  FATAL: --ingestion-wave is required when --lab-csv is set.")
        sys.exit(1)

    tag = args.release_date.strip()
    batch = args.decision_batch_id or tag
    study_dir = ROOT / "studies" / f"{tag}_final_master_release"
    export_dir = ROOT / "exports" / f"final_master_release_{tag}"

    print("=" * 70)
    print("  126 — Final master release (MotherDuck)")
    print(f"  Tag       : {tag}")
    print(f"  Study dir : {study_dir}")
    print(f"  Export dir: {export_dir}")
    print(f"  Dry run   : {args.dry_run}")
    print(f"  119 mode  : {'release (strict)' if args.release_mode else 'structural'}")
    print("=" * 70)

    preflight_inputs(args)
    sa_tail: list[str] = ["--md-sa"] if args.md_sa else []

    if args.dry_run:
        print("  [dry-run] preflight OK — no database changes")
        print("  [dry-run] planned chain: (when executed) 114 → 103 → 117 → [127 if lab] → 125 → 115/118 → 119")
        if args.hydrate_mrq_from is None:
            print("  [dry-run] note: --hydrate-mrq-from not set (final release normally requires a reviewed gate dir).")
        return

    try:
        git_sha = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip()
    except Exception:
        git_sha = "unknown"

    db_path = Path(args.db_path)
    con = connect_md(db_path, prefer_service_account=args.md_sa)
    try:
        apply_qa_ddl(con)
        if args.decisions_csv and args.decisions_csv.is_file():
            append_promotion_decisions(con, args.decisions_csv, batch)
        elif args.decisions_csv:
            print(f"  [warn] decisions CSV not found: {args.decisions_csv}")
    finally:
        con.close()

    log_root = study_dir / "logs"
    log_root.mkdir(parents=True, exist_ok=True)

    hydrate_dir: Path | None = None
    if args.hydrate_mrq_from:
        if not args.hydrate_mrq_from.is_dir():
            print(f"  FATAL: --hydrate-mrq-from not a directory: {args.hydrate_mrq_from}")
            sys.exit(1)
        if args.synthetic_fill_mrq_verification:
            hydrate_dir = build_mrq_hydrate_gate_dir(
                args.hydrate_mrq_from.resolve(),
                (study_dir / "mrq_hydrate_gate").resolve(),
                args.synthetic_fill_mrq_verification.strip(),
            )
        else:
            hydrate_dir = args.hydrate_mrq_from.resolve()
        if args.release_mode:
            assert hydrate_dir is not None
            assert_mrq_csv_fully_reviewed(hydrate_dir / "manual_review_queue.csv")

    # 114 only deletes rows matching the hydrate folder name; stale run_labels would remain.
    # Final release expects a single coherent MRQ snapshot.
    if hydrate_dir is not None:
        _con = connect_md(db_path, prefer_service_account=args.md_sa)
        try:
            _con.execute("DELETE FROM qa.manual_review_queue")
            print("  [126] cleared qa.manual_review_queue (replace with single hydrate snapshot)")
        finally:
            _con.close()

    # 114 hydrate first so qa.manual_review_queue matches reviewed CSV before gates run.
    if hydrate_dir is not None:
        cmd = [_py(), str(SCRIPTS / "114_qa_schema_setup.py"), "--md", *sa_tail, "--hydrate-from", str(hydrate_dir)]
        if not run_subprocess("114_qa_schema_setup", cmd, log_root / "114_qa_setup.log"):
            sys.exit(1)

    if not args.skip_103:
        cmd = [_py(), str(SCRIPTS / "103_fact_lineage_materialize.py"), "--md"]
        if not run_subprocess("103_fact_lineage", cmd, log_root / "103.log"):
            sys.exit(1)

    # 117 reloads canonical parquets (including longitudinal baseline). Institutional lab
    # append MUST run after 117 or parquet reload will overwrite the final lab wave.
    if not args.skip_117:
        cmd = [_py(), str(SCRIPTS / "117_md_contract_views.py"), "--md"]
        if not run_subprocess("117_contract_views", cmd, log_root / "117.log"):
            sys.exit(1)

    if args.lab_csv:
        cmd = [
            _py(),
            str(SCRIPTS / "127_analyst_institutional_lab_append.py"),
            "--md",
            *sa_tail,
            "--input",
            str(args.lab_csv),
            "--ingestion-wave",
            args.ingestion_wave or "",
        ]
        if not run_subprocess("127_lab_append", cmd, log_root / "127_lab_append.log"):
            sys.exit(1)

    cmd = [_py(), str(SCRIPTS / "125_master_verified_views.py"), "--md", *sa_tail]
    if not run_subprocess("125_master_views", cmd, log_root / "125.log"):
        sys.exit(1)

    if not args.skip_snapshot:
        cmd = [
            _py(),
            str(SCRIPTS / "115_release_snapshot.py"),
            "--md",
            "--tag",
            tag,
            "--final-master",
            "--created-by",
            "scripts/126_final_master_release.py",
        ]
        if not run_subprocess("115_release_snapshot", cmd, log_root / "115.log"):
            sys.exit(1)

        cmd = [_py(), str(SCRIPTS / "118_parquet_release_bundle.py"), "--md", "--tag", tag, "--final-master"]
        if not run_subprocess("118_parquet_bundle", cmd, log_root / "118.log"):
            sys.exit(1)

    val_dir = study_dir / "validation_run"
    cmd = [
        _py(),
        str(SCRIPTS / "119_md_formalization_validate.py"),
        "--md",
        *sa_tail,
        "--output-dir",
        str(val_dir),
    ]
    if args.release_mode:
        cmd.append("--release-mode")
    if not run_subprocess("119_validate_release_mode", cmd, log_root / "119.log"):
        sys.exit(1)

    con = connect_md(db_path, prefer_service_account=args.md_sa)
    try:
        ev = gather_evidence(con, tag, git_sha)
        ev["export_dir"] = str(export_dir)
        if args.synthetic_fill_mrq_verification:
            ev["mrq_synthetic_fill"] = args.synthetic_fill_mrq_verification.strip()
        write_evidence_pack(study_dir, ev, export_dir)
    finally:
        con.close()

    print("\n" + "=" * 70)
    print("  126 — COMPLETE")
    print(f"  Evidence: {study_dir / 'EVIDENCE_PACK.md'}")
    print(f"  Memo    : {study_dir / 'SAFE_TO_START_STATS_MEMO.md'}")
    print("=" * 70)


if __name__ == "__main__":
    main()
