#!/usr/bin/env python3
"""Regenerate M044 deliverables after canonical validation passes.

Runs, in order:
  1. ``m044_validate_canonical_v1_runner.py`` (unless ``--skip-validation``)
  2. ``m044_ete_fit_models.py --force`` (parquet, tables workbook, models, forest PNGs/CSVs, manuscript md)
  3. ``m044_make_figures.py`` (Figures 1–7 PNG + figure CSVs)
  4. ``build_per_patient_with_sources.py`` (05b per-patient xlsx)
  5. Sync into ``M044_submission_package_v1_0/`` (04_tables, 08_analysis_outputs, 06_figures, parquet snapshot)
  6. ``studies/m044_validation/m044_validation_summary.{json,md}``

Usage:
  .venv/bin/python scripts/m044_regenerate_outputs.py
  .venv/bin/python scripts/m044_regenerate_outputs.py --skip-validation
  .venv/bin/python scripts/m044_regenerate_outputs.py --local  # per-patient only uses --local; models still need --md unless you adjust

Connection: publication DB uses ``_md_connect`` / TOML token like other ``--md`` scripts.
"""

from __future__ import annotations

import argparse
import json
import hashlib
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


def _discover_repo_root(start: Path) -> Path:
    for p in [start, *start.parents]:
        if (p / "scripts" / "m044_validate_canonical_v1_runner.py").is_file():
            return p
    raise SystemExit(
        "Could not find THYROID_2026 repo root "
        "(missing scripts/m044_validate_canonical_v1_runner.py). Run from the repo checkout."
    )


ROOT = _discover_repo_root(Path(__file__).resolve().parent)
SCRIPTS = ROOT / "scripts"
PKG = ROOT / "M044_submission_package_v1_0"
DATA_M044 = ROOT / "data" / "m044"
FIG_ROOT = ROOT / "figures"
VAL_DIR = ROOT / "studies" / "m044_validation"
TABLES_ROOT = ROOT / "M044_ETE_tables.xlsx"


def _run(cmd: list[str], cwd: Path = ROOT) -> None:
    print("+", " ".join(cmd), flush=True)
    r = subprocess.run(cmd, cwd=cwd)
    if r.returncode != 0:
        raise SystemExit(r.returncode)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _sync_submission_package() -> dict[str, Any]:
    synced: dict[str, Any] = {}
    if TABLES_ROOT.is_file():
        dest = PKG / "04_tables.xlsx"
        shutil.copy2(TABLES_ROOT, dest)
        synced["04_tables.xlsx"] = {"path": str(dest.relative_to(ROOT)), "sha256": _sha256(dest)}

    out_dir = PKG / "08_analysis_outputs"
    out_dir.mkdir(parents=True, exist_ok=True)
    for name in (
        "m044_cox_primary_summary.csv",
        "m044_cox_primary_with_rai_summary.csv",
        "m044_inclusion_flow_qc.csv",
        "m044_run_snapshot.json",
    ):
        src = DATA_M044 / name
        if src.is_file():
            d = out_dir / name
            shutil.copy2(src, d)
            synced[name] = {"path": str(d.relative_to(ROOT)), "sha256": _sha256(d)}

    pq_in = DATA_M044 / "analytic_file_v1.parquet"
    if pq_in.is_file():
        pq_out = out_dir / "analytic_file_v1.parquet"
        shutil.copy2(pq_in, pq_out)
        synced["analytic_file_v1.parquet"] = {
            "path": str(pq_out.relative_to(ROOT)),
            "sha256": _sha256(pq_out),
        }

    fig_dest = PKG / "06_figures"
    fig_dest.mkdir(parents=True, exist_ok=True)
    nfig = 0
    if FIG_ROOT.is_dir():
        for src in sorted(FIG_ROOT.glob("m044_*")):
            if src.is_file():
                shutil.copy2(src, fig_dest / src.name)
                nfig += 1
    synced["figures_copied"] = nfig
    return synced


def _load_json(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _build_summary_payload(
    *,
    canonical_audit_path: Path,
    run_snapshot_path: Path,
    synced: dict[str, Any],
) -> dict[str, Any]:
    sys.path.insert(0, str(SCRIPTS))
    from m044_validate_canonical_v1_runner import (  # noqa: E402
        EXPECTED_ETE_CONSISTENCY,
        EXPECTED_MAIN,
        EXPECTED_MEMBERSHIP,
        EXPECTED_RECURRENCE_COHERENCE,
        EXPECTED_SURGERY_DATE_LINEAGE,
        EXPECTED_TABLE1B_TT_ETE,
    )

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    audit = _load_json(canonical_audit_path)
    snap = _load_json(run_snapshot_path)

    pq = DATA_M044 / "analytic_file_v1.parquet"
    parquet_stats: dict[str, Any] = {"path": str(pq.relative_to(ROOT)) if pq.is_file() else None}
    if pq.is_file():
        df = pd.read_parquet(pq)
        parquet_stats["rows"] = int(len(df))
        parquet_stats["distinct_research_id"] = int(df["research_id"].nunique())

    inc_path = DATA_M044 / "m044_inclusion_flow_qc.csv"
    inclusion_tail: list[dict[str, Any]] = []
    if inc_path.is_file():
        qcdf = pd.read_csv(inc_path)
        inclusion_tail = qcdf.tail(3).to_dict(orient="records")

    expected_block = {
        "main_audit": EXPECTED_MAIN,
        "cohort_membership": EXPECTED_MEMBERSHIP,
        "cpm_ete_consistency": EXPECTED_ETE_CONSISTENCY,
        "surgery_date_lineage": EXPECTED_SURGERY_DATE_LINEAGE,
        "recurrence_coherence": EXPECTED_RECURRENCE_COHERENCE,
        "table1b_tt_ete": EXPECTED_TABLE1B_TT_ETE,
    }

    return {
        "generated_at_utc": ts,
        "repo_root": str(ROOT),
        "canonical_audit": {
            "path": str(canonical_audit_path.relative_to(ROOT)),
            "overall_status": (audit or {}).get("overall_status"),
            "failure_count": len((audit or {}).get("failures", [])),
        },
        "expected_counts_registry": expected_block,
        "canonical_audit_snapshots": (audit or {}).get("snapshots"),
        "analytic_parquet": parquet_stats,
        "m044_run_snapshot": snap,
        "inclusion_flow_qc_last_rows": inclusion_tail,
        "package_sync": synced,
        "notes": [
            "Supplement prose (M044_ETE_supplement.md / 03_supplement.docx) is not auto-regenerated by this script.",
            "Root M044_ETE_tables.xlsx is the authoring workbook; 04_tables.xlsx in the package is a synced copy.",
        ],
    }


def _summary_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# M044 validation + output regeneration summary",
        "",
        f"- **Generated (UTC):** {payload['generated_at_utc']}",
        f"- **Canonical validation status:** `{payload['canonical_audit']['overall_status']}` "
        f"({payload['canonical_audit']['failure_count']} failures)",
        "",
        "## Expected counts (manuscript-frozen registry)",
        "",
        "Values in `expected_counts_registry` match `m044_validate_canonical_v1_runner.py`. "
        "Live snapshots from the last audit run are under `canonical_audit_snapshots` in the JSON file.",
        "",
        "## Analytic parquet",
        "",
        f"```json\n{json.dumps(payload.get('analytic_parquet'), indent=2)}\n```",
        "",
        "## Latest model snapshot (`m044_run_snapshot.json`)",
        "",
        f"```json\n{json.dumps(payload.get('m044_run_snapshot'), indent=2, default=str)}\n```",
        "",
        "## Inclusion flow QC (last 3 rows)",
        "",
    ]
    for row in payload.get("inclusion_flow_qc_last_rows") or []:
        lines.append(f"- {row}")
    lines += [
        "",
        "## Package sync",
        "",
        f"```json\n{json.dumps(payload.get('package_sync'), indent=2)}\n```",
        "",
        "## Notes",
        "",
    ]
    for n in payload.get("notes", []):
        lines.append(f"- {n}")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--skip-validation",
        action="store_true",
        help="Do not run m044_validate_canonical_v1_runner (run after you already have PASS)",
    )
    ap.add_argument(
        "--skip-per-patient",
        action="store_true",
        help="Skip build_per_patient_with_sources.py",
    )
    ap.add_argument(
        "--local-per-patient",
        action="store_true",
        help="Use --local for per-patient workbook only (MotherDuck still used for main pipeline unless you change steps)",
    )
    args = ap.parse_args()

    VAL_DIR.mkdir(parents=True, exist_ok=True)
    audit_json = VAL_DIR / "m044_canonical_audit.json"

    if not args.skip_validation:
        _run([sys.executable, str(SCRIPTS / "m044_validate_canonical_v1_runner.py"), "--md"])
    elif not audit_json.is_file():
        print(
            "WARN: --skip-validation but m044_canonical_audit.json missing; "
            "summary will show unknown status",
            file=sys.stderr,
        )

    _run([sys.executable, str(SCRIPTS / "m044_ete_fit_models.py"), "--force"])
    _run([sys.executable, str(SCRIPTS / "m044_make_figures.py")])

    if not args.skip_per_patient:
        cmd = [sys.executable, str(SCRIPTS / "build_per_patient_with_sources.py")]
        if args.local_per_patient:
            cmd.append("--local")
        else:
            cmd.append("--md")
        _run(cmd)

    synced = _sync_submission_package()

    snap_path = DATA_M044 / "m044_run_snapshot.json"
    payload = _build_summary_payload(
        canonical_audit_path=audit_json,
        run_snapshot_path=snap_path,
        synced=synced,
    )
    j_path = VAL_DIR / "m044_validation_summary.json"
    m_path = VAL_DIR / "m044_validation_summary.md"
    j_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    m_path.write_text(_summary_markdown(payload), encoding="utf-8")
    print(f"Wrote {j_path}")
    print(f"Wrote {m_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
