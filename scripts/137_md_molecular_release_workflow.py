#!/usr/bin/env python3
"""Production-safe molecular release workflow — MotherDuck orchestration.

Wires existing scripts in promotion order:

  130 prepromote-backup  — DuckLake-safe rollback clone (deterministic name)
  130 snapshot           — optional named snapshot (native catalog only; skipped for DUCKLAKE)
  136 writer             — CREATE SNAPSHOT OF prod for read-scaling visibility
  119 --release-mode     — formal QA validation (qa catalog)
  124 --final-release    — live prod release audit (116→…→119)
  136 reader             — REFRESH DATABASE for share-backed dashboards

Usage::

  .venv/bin/python scripts/137_md_molecular_release_workflow.py backup-prod --label 20260407_1530
  .venv/bin/python scripts/137_md_molecular_release_workflow.py --execute backup-prod --label rel_20260409
  .venv/bin/python scripts/137_md_molecular_release_workflow.py qa-validate --tag 20260409
  .venv/bin/python scripts/137_md_molecular_release_workflow.py prod-audit --tag 20260409
  .venv/bin/python scripts/137_md_molecular_release_workflow.py refresh-readers
  .venv/bin/python scripts/137_md_molecular_release_workflow.py promote --tag 20260409 --execute

`--execute` passes ``--execute`` to **130** for mutating DDL and runs **136 writer**
without ``--dry-run``. Without ``--execute``, backup / named-snapshot print SQL only
(130 default). ``promote`` chains steps; pass ``--dry-run`` to forward dry-run to **124**
and **136** only.

See docs/release_runbook.md.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

VENV_PYTHON = ROOT / ".venv" / "bin" / "python"
SCRIPTS = ROOT / "scripts"


def _python() -> str:
    return str(VENV_PYTHON) if VENV_PYTHON.exists() else sys.executable


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _manifest_path(args: argparse.Namespace) -> Path:
    tag = args.tag or datetime.now(timezone.utc).strftime("%Y%m%d")
    base = Path(args.output_dir) if args.output_dir else ROOT / "studies" / f"{tag}_molecular_release_workflow"
    base.mkdir(parents=True, exist_ok=True)
    return base / "workflow_manifest.json"


def _run(cmd: list[str], *, label: str) -> int:
    print(f"\n{'='*70}\n  [{label}]\n  $ {' '.join(cmd)}\n{'='*70}\n")
    return subprocess.call(cmd, cwd=ROOT)


def cmd_backup_prod(args: argparse.Namespace) -> int:
    cmd = [_python(), str(SCRIPTS / "130_md_env_bootstrap.py")]
    if args.md_sa:
        cmd.append("--md-sa")
    if args.execute:
        cmd.append("--execute")
    cmd += ["prepromote-backup", "--label", args.label]
    return _run(cmd, label="130 prepromote-backup")


def cmd_try_named_snapshot(args: argparse.Namespace) -> int:
    cmd = [_python(), str(SCRIPTS / "130_md_env_bootstrap.py")]
    if args.md_sa:
        cmd.append("--md-sa")
    if args.execute:
        cmd.append("--execute")
    cmd += ["snapshot", "--name", args.snapshot_name]
    if getattr(args, "force_native_snapshot", False):
        cmd.append("--force-native-snapshot")
    return _run(cmd, label="130 snapshot (named, native-only)")


def cmd_writer_snapshot(args: argparse.Namespace) -> int:
    cmd = [_python(), str(SCRIPTS / "136_md_read_scaling_snapshot_refresh.py"), "writer", "--md-env", "prod"]
    if args.snapshot_name:
        cmd.extend(["--snapshot-name", args.snapshot_name])
    if args.md_sa:
        cmd.append("--prefer-sa")
    if args.dry_run:
        cmd.append("--dry-run")
    return _run(cmd, label="136 writer (CREATE SNAPSHOT OF prod)")


def cmd_qa_validate(args: argparse.Namespace) -> int:
    tag = args.tag or datetime.now(timezone.utc).strftime("%Y%m%d")
    vod = getattr(args, "validation_output_dir", None)
    out = Path(vod) if vod else (ROOT / "studies" / f"{tag}_molecular_qa_release_mode")
    out.mkdir(parents=True, exist_ok=True)
    cmd = [
        _python(),
        str(SCRIPTS / "119_md_formalization_validate.py"),
        "--md",
        "--md-env", "qa",
        "--release-mode",
        "--output-dir",
        str(out),
    ]
    if args.md_sa:
        cmd.append("--md-sa")
    return _run(cmd, label="119 formal QA (--release-mode)")


def cmd_prod_audit(args: argparse.Namespace) -> int:
    cmd = [
        _python(),
        str(SCRIPTS / "124_md_live_release_audit.py"),
        "--md",
        "--md-env", "prod",
        "--tag",
        args.tag or datetime.now(timezone.utc).strftime("%Y%m%d"),
    ]
    if not getattr(args, "relaxed", False):
        cmd.append("--final-release")
    if args.dry_run:
        cmd.append("--dry-run")
    if getattr(args, "skip_stage", False):
        cmd.append("--skip-stage")
    if getattr(args, "skip_gate", False):
        cmd.append("--skip-gate")
    return _run(cmd, label="124 live prod release audit")


def cmd_refresh_readers(args: argparse.Namespace) -> int:
    cmd = [_python(), str(SCRIPTS / "136_md_read_scaling_snapshot_refresh.py"), "reader", "--md-env", "prod"]
    if getattr(args, "refresh_all", False):
        cmd.append("--all")
    if args.dry_run:
        cmd.append("--dry-run")
    return _run(cmd, label="136 reader (REFRESH DATABASE)")


def cmd_promote(args: argparse.Namespace) -> int:
    manifest: dict = {"steps": [], "tag": args.tag, "started": datetime.now(timezone.utc).isoformat()}
    rc = 0

    def record(step: str, code: int | None, *, skipped: bool = False) -> None:
        manifest["steps"].append({"step": step, "returncode": code, "skipped": skipped})

    tag = args.tag or datetime.now(timezone.utc).strftime("%Y%m%d")
    label = args.label or f"{tag}_{_utc_stamp()}_promote"

    if not args.execute and not args.dry_run:
        print(
            "\n[INFO] promote: no --execute — rehearsal mode: 130 prints SQL only; "
            "124 / 136 writer / 136 reader run with --dry-run.\n"
            "        Pass --execute to allow MotherDuck writes in those steps.\n"
        )

    mut_ns = argparse.Namespace(**vars(args))
    if not args.execute:
        mut_ns.dry_run = True

    if not args.skip_backup:
        ns = argparse.Namespace(**vars(args))
        ns.label = label
        code = cmd_backup_prod(ns)
        record("backup-prod", code)
        if code != 0:
            rc = code
    else:
        record("backup-prod", None, skipped=True)

    if not args.skip_named_snapshot:
        snap = args.snapshot_name or f"pre_promote_{tag}"
        ns = argparse.Namespace(**vars(args))
        ns.snapshot_name = snap
        code = cmd_try_named_snapshot(ns)
        record("try-named-snapshot", code)
        if code != 0 and rc == 0:
            rc = code
    else:
        record("try-named-snapshot", None, skipped=True)

    if not args.skip_writer_snapshot:
        ns = argparse.Namespace(**vars(mut_ns))
        ns.snapshot_name = None
        code = cmd_writer_snapshot(ns)
        record("writer-snapshot", code)
        if code != 0:
            rc = code
    else:
        record("writer-snapshot", None, skipped=True)

    if not args.skip_qa:
        code = cmd_qa_validate(args)
        record("qa-validate", code)
        if code != 0:
            rc = code
    else:
        record("qa-validate", None, skipped=True)

    code = cmd_prod_audit(mut_ns)
    record("prod-audit", code)
    if code != 0:
        rc = code

    if not args.skip_reader_refresh:
        code = cmd_refresh_readers(mut_ns)
        record("refresh-readers", code)
        if code != 0:
            rc = code
    else:
        record("refresh-readers", None, skipped=True)

    manifest["finished"] = datetime.now(timezone.utc).isoformat()
    manifest["overall_returncode"] = rc
    mp = _manifest_path(args)
    mp.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"\n[manifest] {mp}")
    return rc


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--execute",
        action="store_true",
        help="Mutating steps: pass --execute to 130; run 136 writer without --dry-run.",
    )
    p.add_argument("--md-sa", action="store_true", help="Prefer MD_SA_TOKEN on subprocesses that support it.")
    p.add_argument(
        "--tag",
        default=None,
        help="Release tag YYYYMMDD (124, QA report folder, promote manifest).",
    )
    p.add_argument(
        "--output-dir",
        default=None,
        help="Studies subdirectory root for workflow_manifest.json when using promote.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Forward to 124 / 136 (writer + reader) where supported.",
    )

    sub = p.add_subparsers(dest="command", required=True)

    b = sub.add_parser("backup-prod", help="130 prepromote-backup (rollback clone).")
    b.add_argument("--label", required=True, help="Suffix for Thyroid 2026 Molecular PrePromote <label>")
    b.set_defaults(_fn=cmd_backup_prod)

    s = sub.add_parser("try-named-snapshot", help="130 snapshot --name (native DB only; skip msg for DUCKLAKE).")
    s.add_argument("--snapshot-name", required=True)
    s.add_argument(
        "--force-native-snapshot",
        action="store_true",
        help="Pass --force-native-snapshot to 130.",
    )
    s.set_defaults(_fn=cmd_try_named_snapshot)

    w = sub.add_parser("writer-snapshot", help="136 writer — snapshot prod for read-scaling.")
    w.add_argument("--snapshot-name", default=None, help="Optional named snapshot (see utils/md_read_scaling_refresh).")
    w.set_defaults(_fn=cmd_writer_snapshot)

    q = sub.add_parser("qa-validate", help="119 --md --md-env qa --release-mode")
    q.add_argument(
        "--validation-output-dir",
        default=None,
        help="Override directory for validation_report.md",
    )
    q.set_defaults(_fn=cmd_qa_validate)

    pr = sub.add_parser("prod-audit", help="124 --md --md-env prod (default: --final-release).")
    pr.add_argument(
        "--relaxed",
        action="store_true",
        help="Omit 124 --final-release (queue gate less strict).",
    )
    pr.add_argument("--skip-stage", action="store_true")
    pr.add_argument("--skip-gate", action="store_true")
    pr.set_defaults(_fn=cmd_prod_audit, relaxed=False)

    r = sub.add_parser("refresh-readers", help="136 reader — refresh read-scaling DB attachment.")
    r.add_argument("--refresh-all", action="store_true", help="REFRESH DATABASES")
    r.set_defaults(_fn=cmd_refresh_readers)

    prm = sub.add_parser(
        "promote",
        help="backup → named snapshot try → writer snapshot → QA 119 → 124 prod → reader refresh",
    )
    # Allow natural `promote --tag …` (runbook); parent-parser --tag must otherwise precede `promote`.
    prm.add_argument(
        "--tag",
        default=None,
        help="Release tag YYYYMMDD (same as global --tag / 124 --tag).",
    )
    prm.add_argument(
        "--output-dir",
        default=None,
        help="Studies base dir for workflow_manifest.json (same as global --output-dir).",
    )
    prm.add_argument(
        "--label",
        default=None,
        help="prepromote-backup label (default: <tag>_<UTC>_promote).",
    )
    prm.add_argument(
        "--snapshot-name",
        default=None,
        help="try-named-snapshot name (default: pre_promote_<tag>).",
    )
    prm.add_argument("--skip-backup", action="store_true")
    prm.add_argument("--skip-named-snapshot", action="store_true")
    prm.add_argument("--skip-writer-snapshot", action="store_true")
    prm.add_argument("--skip-qa", action="store_true")
    prm.add_argument("--skip-reader-refresh", action="store_true")
    prm.add_argument("--skip-stage", action="store_true")
    prm.add_argument("--skip-gate", action="store_true")
    prm.add_argument(
        "--relaxed",
        action="store_true",
        help="124 without --final-release",
    )
    prm.set_defaults(_fn=cmd_promote, relaxed=False)

    return p


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    fn = args._fn
    return int(fn(args))


if __name__ == "__main__":
    raise SystemExit(main())
