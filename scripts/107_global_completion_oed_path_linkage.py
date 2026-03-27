#!/usr/bin/env python3
"""Build global patient-level completion linkage: OED vs path_synoptics (all DB patients).

Reuses cohort_logic from the 2–4 cm manuscript study (same definitions as table7 /
completion audits). Writes:
  - exports/patient_completion_oed_path_linkage_v1/  (parquet, csv, manifest.json)
  - local DuckDB tables patient_completion_oed_path_linkage_v1 + md_* mirror (--md)

Spine = distinct research_id appearing in operative_episode_detail_v2 OR path_synoptics.
Completion fields apply to lobectomy-first patients only; others have NULL completion
columns (not applicable).

Usage:
  .venv/bin/python scripts/107_global_completion_oed_path_linkage.py
  .venv/bin/python scripts/107_global_completion_oed_path_linkage.py --md
  .venv/bin/python scripts/107_global_completion_oed_path_linkage.py --md --sa   # CI: prefer LOCAL_DB_PATH
  .venv/bin/python scripts/107_global_completion_oed_path_linkage.py --skip-md   # exports only
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
STUDY = ROOT / "studies" / "proposal_2to4cm_extent_molecular_20260326"
EXPORT_DIR = ROOT / "exports" / "patient_completion_oed_path_linkage_v1"
STUDY_PATIENT_CSV = STUDY / "patient_level_dataset.csv"

sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(STUDY))

from local DuckDB_client import local DuckDBClient, local DuckDBConfig  # noqa: E402

import cohort_logic as cl  # noqa: E402


def _git_sha() -> str:
    try:
        return (
            subprocess.check_output(
                ["git", "-C", str(ROOT), "rev-parse", "HEAD"],
                text=True,
            ).strip()
        )
    except (subprocess.CalledProcessError, OSError):
        return "unknown"


def load_ops_path(con) -> tuple[pd.DataFrame, pd.DataFrame]:
    ops = con.execute(
        """
        SELECT research_id, surgery_episode_id, resolved_surgery_date,
               procedure_normalized, procedure_raw, laterality
        FROM operative_episode_detail_v2
        """
    ).df()
    ops["research_id"] = pd.to_numeric(ops["research_id"], errors="coerce").astype("Int64")

    path_syn = con.execute(
        """
        SELECT research_id, surg_date, thyroid_procedure, completion
        FROM path_synoptics
        """
    ).df()
    path_syn["research_id"] = pd.to_numeric(path_syn["research_id"], errors="coerce").astype("Int64")
    return ops, path_syn


def build_linkage_frame(ops: pd.DataFrame, path_syn: pd.DataFrame) -> pd.DataFrame:
    first = cl.first_qualifying_surgeries(
        ops, frozenset({"hemithyroidectomy", "total_thyroidectomy"})
    )
    comp = cl.completion_after_lobectomy(ops, first)
    path_comp = cl.path_synoptic_completion_after_lobectomy(path_syn, first)
    oed_later = cl.oed_any_later_episode_after_index(ops, first)

    spine = np.union1d(
        ops["research_id"].dropna().astype(np.int64).unique(),
        path_syn["research_id"].dropna().astype(np.int64).unique(),
    )
    out = pd.DataFrame({"research_id": spine}).sort_values("research_id")

    fc = first.rename(
        columns={
            "surgery_episode_id": "first_surgery_episode_id",
            "procedure_normalized": "first_procedure_normalized",
        }
    )
    keep_fc = [
        "research_id",
        "first_surgery_episode_id",
        "first_procedure_normalized",
        "index_surgery_date",
        "procedure_raw",
        "laterality",
    ]
    keep_fc = [c for c in keep_fc if c in fc.columns]
    out = out.merge(fc[keep_fc], how="left", on="research_id")

    comp_re = comp.rename(
        columns={
            "completion_total_flag": "oed_completion_total_flag",
            "completion_days": "oed_completion_days",
            "completion_within_30": "oed_completion_within_30",
            "completion_within_90": "oed_completion_within_90",
            "completion_within_365": "oed_completion_within_365",
        }
    )
    out = out.merge(comp_re, how="left", on="research_id")

    out = out.merge(path_comp, how="left", on="research_id")
    out = out.merge(oed_later, how="left", on="research_id")

    out["lobectomy_first_flag"] = out["first_procedure_normalized"].eq("hemithyroidectomy")
    out["has_first_qualifying_oed_row_flag"] = out["first_procedure_normalized"].notna()
    # N/A mask: completion metrics only for lobectomy-first
    not_lob = ~out["lobectomy_first_flag"].fillna(False)
    na_cols = [
        "oed_completion_total_flag",
        "oed_completion_days",
        "oed_completion_within_30",
        "oed_completion_within_90",
        "oed_completion_within_365",
        "path_completion_definite_flag",
        "path_completion_days",
        "path_completion_within_30",
        "path_completion_within_90",
        "path_completion_within_365",
        "path_synoptic_any_later_row_flag",
        "path_completion_ambiguous_later_only_flag",
        "oed_any_later_episode_flag",
    ]
    for c in na_cols:
        if c not in out.columns:
            continue
        out.loc[not_lob, c] = pd.NA

    out["linkage_definition_version"] = "v1"
    out["linkage_source_script"] = "107_global_completion_oed_path_linkage.py"
    out["built_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    dup = int(out["research_id"].duplicated().sum())
    if dup:
        raise RuntimeError(f"duplicate research_id in linkage frame: {dup}")
    return out


def cohort_consistency_check(out: pd.DataFrame) -> dict:
    """Cross-check lobectomy rows in manuscript patient_level_dataset when present."""
    if not STUDY_PATIENT_CSV.exists():
        return {"status": "SKIP", "reason": f"missing {STUDY_PATIENT_CSV.name}"}
    pl = pd.read_csv(STUDY_PATIENT_CSV, low_memory=False)
    pl["research_id"] = pd.to_numeric(pl["research_id"], errors="coerce").astype("Int64")
    lob = pl[pl["initial_lobectomy"] == 1].copy()
    if lob.empty:
        return {"status": "SKIP", "reason": "no lobectomy rows in study CSV"}
    m = lob.merge(
        out[
            [
                "research_id",
                "oed_completion_total_flag",
                "path_completion_definite_flag",
            ]
        ],
        on="research_id",
        how="left",
    )
    # study CSV column from pipeline
    if "completion_total_flag" not in m.columns:
        return {"status": "SKIP", "reason": "completion_total_flag not in study CSV"}
    a = m["completion_total_flag"].fillna(False).astype(bool)
    b = m["oed_completion_total_flag"].fillna(False).astype(bool)
    mism = int((a != b).sum())
    return {
        "status": "PASS" if mism == 0 else "FAIL",
        "n_lobectomy_study": int(len(m)),
        "oed_flag_mismatch_vs_study_csv": mism,
    }


def push_local DuckDB(con, df: pd.DataFrame) -> None:
    """Materialize RW tables via in-process register (avoids local path read on md:)."""
    con.register("_linkage_upload_v1", df)
    try:
        con.execute("DROP TABLE IF EXISTS patient_completion_oed_path_linkage_v1")
        con.execute(
            "CREATE TABLE patient_completion_oed_path_linkage_v1 AS "
            "SELECT * FROM _linkage_upload_v1"
        )
        con.execute("DROP TABLE IF EXISTS md_patient_completion_oed_path_linkage_v1")
        con.execute(
            "CREATE TABLE md_patient_completion_oed_path_linkage_v1 AS "
            "SELECT * FROM patient_completion_oed_path_linkage_v1"
        )
    finally:
        try:
            con.unregister("_linkage_upload_v1")
        except Exception:
            pass
    n = con.execute(
        "SELECT COUNT(*) FROM patient_completion_oed_path_linkage_v1"
    ).fetchone()[0]
    print(f"local DuckDB patient_completion_oed_path_linkage_v1 rows: {n}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--md",
        action="store_true",
        help="Write to local DuckDB (requires LOCAL_DB_PATH)",
    )
    ap.add_argument(
        "--skip-md",
        action="store_true",
        help="Only write exports (default if --md not passed)",
    )
    ap.add_argument(
        "--sa",
        action="store_true",
        help="Prefer LOCAL_DB_PATH over LOCAL_DB_PATH (match GitHub Actions)",
    )
    args = ap.parse_args()
    do_md = bool(args.md) and not args.skip_md

    client = local DuckDBClient(local DuckDBConfig(use_service_account=bool(args.sa)))
    con = client.connect_rw()

    ops, path_syn = load_ops_path(con)
    out = build_linkage_frame(ops, path_syn)

    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    out.to_parquet(EXPORT_DIR / "patient_completion_oed_path_linkage_v1.parquet", index=False)
    out.to_csv(EXPORT_DIR / "patient_completion_oed_path_linkage_v1.csv", index=False)

    check = cohort_consistency_check(out)
    lob_n = int(out["lobectomy_first_flag"].fillna(False).sum())
    manifest = {
        "table": "patient_completion_oed_path_linkage_v1",
        "n_rows": int(len(out)),
        "n_lobectomy_first": lob_n,
        "n_oed_completion_true": int(
            out["oed_completion_total_flag"].fillna(False).astype(bool).sum()
        ),
        "n_path_synoptic_definite": int(
            out["path_completion_definite_flag"].fillna(False).astype(bool).sum()
        ),
        "cohort_consistency_vs_study_csv": check,
        "git_sha": _git_sha(),
        "built_utc": out["built_utc"].iloc[0],
        "definitions": {
            "first_surgery": "first_qualifying_surgeries hemithyroidectomy|total_thyroidectomy",
            "oed_completion": "later OED row with total_thyroidectomy or completion_thyroidectomy",
            "path_synoptic_definite": "path_synoptic_completion_after_lobectomy in cohort_logic.py",
        },
    }
    (EXPORT_DIR / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    (EXPORT_DIR / "README.md").write_text(
        "# patient_completion_oed_path_linkage_v1\n\n"
        "Global spine: all `research_id` in `operative_episode_detail_v2` ∪ `path_synoptics`. "
        "OED vs path-synoptic completion flags follow "
        "`studies/proposal_2to4cm_extent_molecular_20260326/cohort_logic.py`.\n\n"
        "Rebuild: `.venv/bin/python scripts/107_global_completion_oed_path_linkage.py --md`\n",
        encoding="utf-8",
    )

    print(json.dumps(manifest, indent=2))
    if check.get("status") == "FAIL":
        raise SystemExit("cohort consistency FAIL — see manifest cohort_consistency_vs_study_csv")

    if do_md:
        push_local DuckDB(con, out)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
