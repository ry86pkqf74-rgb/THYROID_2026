#!/usr/bin/env python3
"""149_md_canonical_nodule_linkage_study.py — TI-RADS / FNA / molecular / pathology linkage study.

Read-only against MotherDuck (or local DuckDB): inventories candidate tables, runs the
deterministic linkage pipeline in :mod:`utils.canonical_nodule_linkage`, and writes
CSV/Parquet artifacts plus validation markdown under ``studies/``.

Does **not** create or replace remote views/tables. Safe for prod attach with SELECT-only.

Example:
  .venv/bin/python scripts/149_md_canonical_nodule_linkage_study.py --md
  .venv/bin/python scripts/149_md_canonical_nodule_linkage_study.py --db-path thyroid_master.duckdb
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "thyroid_master.duckdb"

sys.path.insert(0, str(ROOT))


CANDIDATE_INPUTS = (
    "us_nodules_tirads",
    "serial_imaging_us",
    "ultrasound_reports",
    "imaging_nodule_master_v1",
    "fna_history",
    "fna_cytology",
    "molecular_testing",
    "path_synoptics",
    "tumor_pathology",
    "benign_pathology",
    "operative_details",
    "thyroid_weights",
    "tumor_episode_master_v2",
    "molecular_test_episode_v2",
    "operative_episode_detail_v2",
    "fna_episode_master_v2",
    "imaging_fna_linkage_mm_v1",
    "imaging_fna_linkage_v3",
    "fna_molecular_linkage_v3",
    "preop_surgery_linkage_v3",
    "surgery_pathology_linkage_v3",
)

LINKAGE_ASSETS = (
    ("scripts/49_enhanced_linkage_v3.py", "v3 scored linkage tables + linkage_ambiguity_review_v1"),
    ("scripts/129_imaging_fna_linkage_mm_v1.py", "imaging_fna_linkage_mm_v1 + QA/review"),
    ("utils/imaging_fna_linkage_mm_v1.py", "specimen key normalization"),
    ("utils/canonical_nodule_linkage.py", "canonical nodule chain SQL (this study)"),
    ("scripts/sql/139_specimen_identity_layer_ddl.sql", "specimen spine uses v3 linkages"),
    ("scripts/sql/140_specimen_genomics_binding_ddl.sql", "genomics binding over v3"),
    ("scripts/117_md_contract_views.py", "MotherDuck contract / episode surfaces"),
)


def _section(title: str) -> None:
    print(f"\n{'=' * 72}\n  {title}\n{'=' * 72}\n")


def _table_exists(con, name: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {name} LIMIT 1")
        return True
    except Exception:
        return False


def _profile_table(con, name: str) -> dict[str, object]:
    row: dict[str, object] = {"table_name": name, "present": False, "n_rows": None, "n_cols": None}
    if not _table_exists(con, name):
        return row
    row["present"] = True
    try:
        row["n_rows"] = int(con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0])
    except Exception as exc:
        row["n_rows"] = f"error:{exc}"
    try:
        cols = con.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'main' AND table_name = ?
            ORDER BY ordinal_position
            """,
            [name],
        ).fetchall()
        row["n_cols"] = len(cols)
        row["columns_csv"] = ",".join(f"{c[0]}:{c[1]}" for c in cols)
    except Exception as exc:
        row["columns_csv"] = f"error:{exc}"
    return row


def _write_source_profile_csv(study_dir: Path, profiles: list[dict[str, object]]) -> Path:
    import pandas as pd

    p = study_dir / "source_profile.csv"
    pd.DataFrame(profiles).to_csv(p, index=False)
    return p


def _write_schema_inventory_md(study_dir: Path, profiles: list[dict[str, object]]) -> Path:
    lines = [
        "# Schema / grain inventory",
        "",
        f"Generated UTC: {datetime.now(timezone.utc).isoformat()}",
        "",
        "| table | present | n_rows | n_cols | notes |",
        "|-------|---------|--------|--------|-------|",
    ]
    for r in profiles:
        cols = r.get("columns_csv")
        note = ""
        if isinstance(cols, str) and len(cols) > 200:
            note = "see source_profile.csv for full column list"
            cols = str(cols)[:200] + "…"
        lines.append(
            f"| {r['table_name']} | {r['present']} | {r.get('n_rows')} | {r.get('n_cols')} | {note} |"
        )
    lines.append("")
    lines.append("## Grain expectations")
    lines.append("")
    lines.append("- `imaging_nodule_master_v1`: one row per nodule per ultrasound exam (long).")
    lines.append("- `fna_episode_master_v2`: one row per FNA episode (ordered per patient).")
    lines.append("- v3 linkages: ranked candidates with `score_rank`; `n_candidates` flags ambiguity.")
    lines.append("")
    p = study_dir / "schema_inventory.md"
    p.write_text("\n".join(lines), encoding="utf-8")
    return p


def _write_existing_linkage_assets_md(study_dir: Path) -> Path:
    lines = [
        "# Existing linkage-related repo assets",
        "",
        "Reuse these before inventing new join logic:",
        "",
        "| path | role |",
        "|------|------|",
    ]
    for path, role in LINKAGE_ASSETS:
        lines.append(f"| `{path}` | {role} |")
    lines.append("")
    p = study_dir / "existing_linkage_assets.md"
    p.write_text("\n".join(lines), encoding="utf-8")
    return p


def main() -> int:
    from motherduck_client import read_scaling_token_mode, token_mode

    ap = argparse.ArgumentParser(description="Canonical nodule linkage study (read-only)")
    ap.add_argument("--md", action="store_true", help="Attach MotherDuck (fail-closed)")
    ap.add_argument("--md-env", default=None, help="MotherDuck env: dev|qa|prod")
    ap.add_argument("--md-sa", action="store_true", help="Prefer MD_SA_TOKEN ordering")
    ap.add_argument("--db-path", type=Path, default=DB_PATH, help="Local DuckDB path (non-md)")
    ap.add_argument(
        "--study-dir",
        type=Path,
        default=ROOT / "studies" / "20260407_tirads_fna_molecular_path_linkage",
        help="Output directory for artifacts",
    )
    ap.add_argument("--dry-run", action="store_true", help="Inventory only; skip linkage queries")
    ap.add_argument(
        "--compare-parquet",
        type=Path,
        default=None,
        help="Optional prior canonical_nodule_linkage.parquet for row parity check",
    )
    args = ap.parse_args()

    print("token_mode()               :", token_mode())
    print("read_scaling_token_mode() :", read_scaling_token_mode())

    study_dir: Path = args.study_dir
    study_dir.mkdir(parents=True, exist_ok=True)

    import duckdb

    if args.md:
        from utils.md_connect import connect_md_fail_closed

        con = connect_md_fail_closed(
            args.db_path,
            env=args.md_env,
            prefer_service_account=args.md_sa,
            custom_user_agent="canonical_nodule_linkage_study_v1",
        )
    else:
        con = duckdb.connect(str(args.db_path))
        print(f"  Using local file DB: {args.db_path}")

    try:
        profiles = [_profile_table(con, t) for t in CANDIDATE_INPUTS]
        _write_source_profile_csv(study_dir, profiles)
        _write_schema_inventory_md(study_dir, profiles)
        _write_existing_linkage_assets_md(study_dir)

        if args.dry_run:
            _section("Dry-run: inventory only")
            print(f"  Wrote: {study_dir / 'source_profile.csv'}")
            print(f"  Wrote: {study_dir / 'schema_inventory.md'}")
            return 0

        if not _table_exists(con, "imaging_nodule_master_v1"):
            print("FATAL: imaging_nodule_master_v1 missing — cannot build linkage spine.", file=sys.stderr)
            return 1
        if not _table_exists(con, "imaging_fna_linkage_mm_v1"):
            print(
                "FATAL: imaging_fna_linkage_mm_v1 missing — run scripts/129_imaging_fna_linkage_mm_v1.py "
                "against this catalog first.",
                file=sys.stderr,
            )
            return 1
        for req in (
            "fna_episode_master_v2",
            "fna_molecular_linkage_v3",
            "preop_surgery_linkage_v3",
            "surgery_pathology_linkage_v3",
            "tumor_episode_master_v2",
            "molecular_test_episode_v2",
            "operative_episode_detail_v2",
        ):
            if not _table_exists(con, req):
                print(f"FATAL: required table missing: {req}", file=sys.stderr)
                return 1

        from utils.canonical_nodule_linkage import (
            candidate_pairs_sql,
            canonical_nodule_linkage_sql,
            discordance_sql,
            manual_review_queue_sql,
            qc_summary_sql,
        )

        import pandas as pd

        _section("Linkage extracts")
        canonical_sql = canonical_nodule_linkage_sql()
        df_can = con.execute(canonical_sql).df()
        can_parq = study_dir / "canonical_nodule_linkage.parquet"
        can_csv = study_dir / "canonical_nodule_linkage.csv"
        df_can.to_parquet(can_parq, index=False)
        df_can.to_csv(can_csv, index=False)
        print(f"  canonical_nodule_linkage rows: {len(df_can):,}")

        df_pairs = con.execute(candidate_pairs_sql()).df()
        df_pairs.to_parquet(study_dir / "candidate_match_pairs.parquet", index=False)
        df_pairs.to_csv(study_dir / "candidate_match_pairs.csv", index=False)

        df_mrq = con.execute(manual_review_queue_sql()).df()
        df_mrq.to_parquet(study_dir / "manual_review_queue.parquet", index=False)
        df_mrq.to_csv(study_dir / "manual_review_queue.csv", index=False)

        df_disc = con.execute(discordance_sql()).df()
        df_disc.to_parquet(study_dir / "discordance_summary.parquet", index=False)
        df_disc.to_csv(study_dir / "discordance_summary.csv", index=False)

        df_qc = con.execute(qc_summary_sql()).df()
        df_qc.to_parquet(study_dir / "linkage_qc_summary.parquet", index=False)
        df_qc.to_csv(study_dir / "linkage_qc_summary.csv", index=False)

        # Validation report
        n_spine = int(df_can.shape[0])
        n_fna_linked = int(df_can["fna_date_first"].notna().sum())
        n_mol = int(df_can["molecular_episode_id"].notna().sum())
        n_surg = int(df_can["surgery_date_index"].notna().sum())
        n_path = int(df_can["final_histology"].notna().sum())
        n_review = int(df_can["manual_review_needed_flag"].sum())
        n_discord = int(df_disc["n_rows"].sum()) if len(df_disc) else 0

        compare_note = "n/a"
        if args.compare_parquet and args.compare_parquet.exists():
            prior = pd.read_parquet(args.compare_parquet)
            if prior.shape[0] > 0 and df_can.shape[0] > 0:
                key = "canonical_row_hash"
                if key in prior.columns and key in df_can.columns:
                    a = set(prior[key].astype(str))
                    b = set(df_can[key].astype(str))
                    diff = len(a.symmetric_difference(b)) / max(len(a | b), 1)
                    compare_note = f"symmetric_diff_fraction={diff:.4f}"

        validation = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "inputs": {"md": args.md, "db_path": str(args.db_path)},
            "counts": {
                "canonical_rows": n_spine,
                "with_fna_date": n_fna_linked,
                "with_molecular_episode": n_mol,
                "with_surgery_date": n_surg,
                "with_final_histology": n_path,
                "manual_review_flagged": n_review,
                "discordance_total_rows": n_discord,
                "candidate_pair_rows": int(df_pairs.shape[0]),
                "manual_review_queue_rows": int(df_mrq.shape[0]),
            },
            "qc_summary": df_qc.iloc[0].to_dict() if len(df_qc) else {},
            "compare_prior": compare_note,
        }
        (study_dir / "validation_report.json").write_text(
            json.dumps(validation, indent=2, default=str), encoding="utf-8"
        )
        vlines = [
            "# Validation report",
            "",
            f"Generated: `{validation['generated_at_utc']}`",
            "",
            "## Counts",
            "",
            json.dumps(validation["counts"], indent=2),
            "",
            "## QC summary (upstream yields)",
            "",
            json.dumps(validation.get("qc_summary", {}), indent=2),
            "",
            "## Prior export comparison",
            "",
            compare_note,
            "",
            "## NIFTP sensitivity",
            "",
            "Primary malignancy accounting excludes `niftp_flag=true` rows per policy;",
            "include them in secondary sensitivity sets via `discordance_summary` + `niftp_flag`.",
            "",
        ]
        (study_dir / "validation_report.md").write_text("\n".join(vlines), encoding="utf-8")
        print(f"  Artifacts under: {study_dir}")
    finally:
        con.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
