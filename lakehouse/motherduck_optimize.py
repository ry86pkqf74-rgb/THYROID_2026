#!/usr/bin/env python3
"""
MotherDuck trial optimization: curated gold/canonical tables (≤15), views,
ANALYZE, timestamped Parquet export, LLM gold provenance check.

  .venv/bin/python lakehouse/motherduck_optimize.py
  MOTHERDUCK_DATABASE="Thyroid 2026" .venv/bin/python lakehouse/motherduck_optimize.py --sa

Env:
  MD_SA_TOKEN / MOTHERDUCK_TOKEN — auth
  MOTHERDUCK_DATABASE — catalog override when default DB name differs
  MOTHERDUCK_OPTIMIZE_TABLES — optional comma-separated whitelist (max 15)
  GOLD_LLM_VERIFIED_FACTS_PARQUET — default path for gold_llm hydration if unset on CLI
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# --- Curated whitelist (max 15) — manuscript/resolved gold + canonical + outcomes ---
# 1–5: resolved / manuscript   6–7: canonical + LLM slot   8–10: labs/outcomes/complications
# 11–15: lesions, survival, demo, imaging, complication long
GOLD_OPTIMIZE_TABLES: tuple[str, ...] = (
    "manuscript_cohort_v1",
    "patient_analysis_resolved_v1",
    "episode_analysis_resolved_v1_dedup",
    "thyroid_scoring_py_v1",
    "analysis_cancer_cohort_v1",
    "canonical_extracted_fact_long_v1",
    "longitudinal_lab_canonical_v1",
    "recurrence_event_clean_v1",
    "complication_patient_summary_v1",
    "gold_llm_verified_facts",
    "lesion_analysis_resolved_v1",
    "survival_cohort_enriched",
    "demographics_harmonized_v2",
    "imaging_patient_summary_v1",
    "complication_phenotype_v1",
)

PARQUET_HYDRATION_DEFAULTS: dict[str, Path] = {
    "canonical_extracted_fact_long_v1": ROOT / "processed" / "canonical_extracted_fact_long_v1.parquet",
}

def _v2_stage_tables_from_registry() -> tuple[str, ...]:
    """Derive v2_stage table list from the extraction domain registry."""
    try:
        from llm_extraction.registry import load_registry
        reg = load_registry()
        stems = [
            f"v2_stage.{spec.parquet_stem}"
            for spec in reg.domains.values()
            if spec.tier == "v2" and spec.canonical_output
        ]
        return tuple(sorted(stems)) if stems else _V2_STAGE_FALLBACK
    except Exception:
        return _V2_STAGE_FALLBACK


_V2_STAGE_FALLBACK: tuple[str, ...] = (
    "v2_stage.note_entities_llm_airway_invasion",
    "v2_stage.note_entities_llm_cervical_ln_detail",
    "v2_stage.note_entities_llm_dynamic_risk_response",
    "v2_stage.note_entities_llm_frozen_section_detail",
    "v2_stage.note_entities_llm_functional_outcomes",
    "v2_stage.note_entities_llm_imaging",
    "v2_stage.note_entities_llm_labs",
    "v2_stage.note_entities_llm_parathyroid_detail",
    "v2_stage.note_entities_llm_past_medical_hx",
    "v2_stage.note_entities_llm_past_surgical_hx",
    "v2_stage.note_entities_llm_pathology",
    "v2_stage.note_entities_llm_patient_decision_adherence",
    "v2_stage.note_entities_llm_physical_exam",
    "v2_stage.note_entities_llm_presenting_symptoms",
    "v2_stage.note_entities_llm_rad_treatment",
    "v2_stage.note_entities_llm_rai_detailed",
    "v2_stage.note_entities_llm_recurrence",
    "v2_stage.note_entities_llm_survival_followup",
    "v2_stage.note_entities_llm_synoptic_pathology_enrichment",
    "v2_stage.note_entities_llm_tg_kinetics",
    "v2_stage.note_entities_llm_tirads_granular",
    "v2_stage.note_entities_llm_us_nodule_dynamics",
    "v2_stage.note_entities_llm_vascular_invasion",
)


def _split_table_ref(ref: str) -> tuple[str, str]:
    ref = ref.strip()
    if "." in ref:
        s, t = ref.split(".", 1)
        return s.strip(), t.strip()
    return "main", ref


def _sql_table_id(schema: str, table: str) -> str:
    return f'"{schema}"."{table}"'


def _export_filename(ref: str) -> str:
    return ref.replace(".", "__") + ".parquet"


def _resolve_table_list(max_tables: int) -> list[str]:
    raw = (os.environ.get("MOTHERDUCK_OPTIMIZE_TABLES") or "").strip()
    if raw:
        names = [t.strip() for t in raw.split(",") if t.strip()]
    else:
        names = list(GOLD_OPTIMIZE_TABLES)
    if len(names) > max_tables:
        raise SystemExit(
            f"Table whitelist length {len(names)} exceeds --max-tables={max_tables}. "
            "Trim MOTHERDUCK_OPTIMIZE_TABLES or increase cap (trial cost)."
        )
    return names


def _git_sha() -> str | None:
    try:
        return (
            subprocess.check_output(
                ["git", "-C", str(ROOT), "rev-parse", "HEAD"],
                stderr=subprocess.DEVNULL,
                text=True,
            ).strip()
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


def _table_exists(con: Any, table_ref: str) -> bool:
    schema, tbl = _split_table_ref(table_ref)
    row = con.execute(
        """
        SELECT COUNT(*)::BIGINT
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
        """,
        [schema, tbl],
    ).fetchone()
    return row is not None and int(row[0]) > 0


def _hydrate_from_parquet(con: Any, table_name: str, pq: Path, dry_run: bool) -> bool:
    if not pq.is_file():
        print(f"  [hydrate] skip {table_name}: no file {pq}")
        return False
    sql = (
        f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM "
        f"read_parquet('{pq.as_posix()}')"
    )
    if dry_run:
        print(f"  [dry-run] would run: {sql[:120]}...")
        return True
    con.execute(sql)
    n = con.execute(f"SELECT COUNT(*) FROM {_sql_table_id('main', table_name)}").fetchone()[0]
    print(f"  [hydrate] {table_name} from {pq.name}: {n:,} rows")
    return True


def _ensure_gold_llm(
    con: Any,
    pq_path: Path | None,
    dry_run: bool,
) -> None:
    if _table_exists(con, "gold_llm_verified_facts"):
        return
    env_pq = (os.environ.get("GOLD_LLM_VERIFIED_FACTS_PARQUET") or "").strip()
    path = pq_path
    if path is None and env_pq:
        path = Path(env_pq)
    if path is None or not path.is_file():
        print(
            "  [warn] gold_llm_verified_facts missing; provide --gold-llm-parquet "
            "or GOLD_LLM_VERIFIED_FACTS_PARQUET for hydration"
        )
        return
    _hydrate_from_parquet(con, "gold_llm_verified_facts", path, dry_run)


def _ensure_canonical_fact_long(con: Any, dry_run: bool) -> None:
    if _table_exists(con, "canonical_extracted_fact_long_v1"):
        return
    pq = PARQUET_HYDRATION_DEFAULTS.get("canonical_extracted_fact_long_v1")
    if pq and pq.is_file():
        _hydrate_from_parquet(con, "canonical_extracted_fact_long_v1", pq, dry_run)
    else:
        print(
            "  [warn] canonical_extracted_fact_long_v1 missing; "
            "run scripts/103_fact_lineage_materialize.py or place parquet under processed/"
        )


def _create_views(con: Any, dry_run: bool) -> None:
    specs = [
        (
            "gold_master_patient_facts_v1",
            "patient_analysis_resolved_v1",
        ),
        (
            "gold_master_episode_events_v1",
            "episode_analysis_resolved_v1_dedup",
        ),
    ]
    for view_name, base in specs:
        if not _table_exists(con, base):
            print(f"  [skip view {view_name}] base table missing: {base}")
            continue
        bs, bt = _split_table_ref(base)
        sql = f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM {_sql_table_id(bs, bt)}"
        if dry_run:
            print(f"  [dry-run] {sql}")
        else:
            con.execute(sql)
            print(f"  [view] {view_name} -> {base}")


def _analyze_tables(con: Any, tables: list[str], dry_run: bool) -> None:
    for t in tables:
        if not _table_exists(con, t):
            continue
        if dry_run:
            print(f"  [dry-run] ANALYZE {t}")
            continue
        s, tbl = _split_table_ref(t)
        qid = _sql_table_id(s, tbl)
        try:
            con.execute(f"ANALYZE {qid}")
            print(f"  [analyze] {t}")
        except Exception as e:
            msg = str(e).lower()
            if "vacuum" in msg or "not implemented" in msg:
                print(f"  [analyze] {t}: skipped (MotherDuck/remote: {e})")
            else:
                print(f"  [analyze] {t}: {e}")


def _export_parquet(
    con: Any,
    tables: list[str],
    export_dir: Path,
    dry_run: bool,
) -> dict[str, Any]:
    export_dir.mkdir(parents=True, exist_ok=True)
    counts: dict[str, int] = {}
    exported: list[str] = []
    for t in tables:
        if not _table_exists(con, t):
            continue
        out = export_dir / _export_filename(t)
        s, tbl = _split_table_ref(t)
        qid = _sql_table_id(s, tbl)
        if dry_run:
            print(f"  [dry-run] COPY {t} -> {out}")
            continue
        con.execute(f"COPY (SELECT * FROM {qid}) TO '{out.as_posix()}' (FORMAT PARQUET)")
        cnt = con.execute(f"SELECT COUNT(*)::BIGINT FROM {qid}").fetchone()[0]
        counts[t] = int(cnt)
        exported.append(t)
        print(f"  [export] {t} -> {out.name} ({cnt:,} rows)")
    return {"row_counts": counts, "exported_tables": exported}


def _provenance_check(con: Any, export_dir: Path, dry_run: bool) -> int | None:
    if not _table_exists(con, "gold_llm_verified_facts"):
        print("  [provenance] skip: gold_llm_verified_facts not present")
        return None
    cols = con.execute(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'main' AND table_name = 'gold_llm_verified_facts'
        """
    ).fetchdf()["column_name"].str.lower().tolist()
    if "research_id" not in cols or "note_row_id" not in cols:
        raise SystemExit(
            "[provenance] gold_llm_verified_facts must have research_id and note_row_id "
            f"(found: {cols})"
        )
    sql = (
        "SELECT research_id, COUNT(*) AS row_ct FROM gold_llm_verified_facts "
        "GROUP BY research_id HAVING COUNT(DISTINCT note_row_id) > 0"
    )
    if dry_run:
        print("  [dry-run] provenance query + save provenance_llm_gold_note_linkage.parquet")
        return None
    out = export_dir / "provenance_llm_gold_note_linkage.parquet"
    con.execute(
        f"COPY ({sql}) TO '{out.as_posix()}' (FORMAT PARQUET)"
    )
    n = con.execute(f"SELECT COUNT(*)::BIGINT FROM ({sql}) sub").fetchone()[0]
    print(f"  [provenance] provenance_llm_gold_note_linkage.parquet rows: {n:,}")
    return int(n)


def main() -> None:
    parser = argparse.ArgumentParser(description="MotherDuck gold/canonical optimize (trial).")
    parser.add_argument("--dry-run", action="store_true", help="Print steps only; no DDL/DML.")
    parser.add_argument(
        "--sa",
        action="store_true",
        help="Prefer MD_SA_TOKEN (CI / service account).",
    )
    parser.add_argument("--env", default="prod", choices=["dev", "qa", "prod"])
    parser.add_argument("--max-tables", type=int, default=15)
    parser.add_argument(
        "--gold-llm-parquet",
        type=Path,
        default=None,
        help="Parquet for gold_llm_verified_facts when table missing.",
    )
    parser.add_argument(
        "--export-dir",
        type=Path,
        default=None,
        help="Export directory (default exports/motherduck_gold_daily_<UTC>).",
    )
    parser.add_argument("--skip-export", action="store_true")
    parser.add_argument("--skip-analyze", action="store_true")
    parser.add_argument(
        "--v2-stage",
        action="store_true",
        help="Use registry-derived v2_stage.note_entities_llm_* tables.",
    )
    args = parser.parse_args()

    if args.v2_stage:
        tables = list(_v2_stage_tables_from_registry())
        if os.environ.get("MOTHERDUCK_OPTIMIZE_TABLES", "").strip():
            raise SystemExit("Do not combine --v2-stage with MOTHERDUCK_OPTIMIZE_TABLES.")
    else:
        tables = _resolve_table_list(args.max_tables)
    if not args.v2_stage and len(tables) > args.max_tables:
        raise SystemExit(f"Resolved table list length {len(tables)} > --max-tables={args.max_tables}")
    if len(GOLD_OPTIMIZE_TABLES) > 15:
        raise SystemExit("Internal GOLD_OPTIMIZE_TABLES exceeds trial cap of 15; fix constants.")

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    export_dir = args.export_dir or (ROOT / "exports" / f"motherduck_gold_daily_{ts}")

    md_db = (
        (os.environ.get("MOTHERDUCK_DATABASE") or os.environ.get("MOTHERDUCK_DB") or "")
        .strip()
        or "(default from motherduck_client / env)"
    )

    print(f"[motherduck_optimize] whitelist ({len(tables)}): {', '.join(tables)}")
    print(f"[motherduck_optimize] export_dir={export_dir}")
    print(f"[motherduck_optimize] motherduck catalog hint={md_db}")

    if args.dry_run:
        print("[motherduck_optimize] dry-run: skipping MotherDuck connection")
        print("  [dry-run] would create views gold_master_patient_facts_v1, gold_master_episode_events_v1")
        for t in tables:
            print(f"  [dry-run] would ANALYZE / export if exists: {t}")
        print("  [dry-run] provenance: SELECT ... FROM gold_llm_verified_facts ... -> provenance_llm_gold_note_linkage.parquet")
        return

    from motherduck_client import MotherDuckClient  # noqa: E402

    client = MotherDuckClient.for_env(args.env, use_service_account=args.sa)
    con = client.connect_rw()
    try:
        _ensure_canonical_fact_long(con, dry_run=False)
        _ensure_gold_llm(con, args.gold_llm_parquet, dry_run=False)
        _create_views(con, dry_run=False)

        if not args.skip_analyze:
            _analyze_tables(con, tables, dry_run=False)
        else:
            print("  [--skip-analyze]")

        export_meta: dict[str, Any] = {}
        if not args.skip_export:
            export_meta = _export_parquet(con, tables, export_dir, dry_run=False)
            prov_rows = _provenance_check(con, export_dir, dry_run=False)
            manifest = {
                "utc_timestamp": datetime.now(timezone.utc).isoformat(),
                "git_sha": _git_sha(),
                "motherduck_database_env": (
                    os.environ.get("MOTHERDUCK_DATABASE")
                    or os.environ.get("MOTHERDUCK_DB")
                    or ""
                ).strip(),
                "tables_whitelist": tables,
                "exported_tables": export_meta.get("exported_tables", []),
                "row_counts": export_meta.get("row_counts", {}),
                "provenance_llm_gold_rows": prov_rows,
            }
            man_path = export_dir / "manifest.json"
            man_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
            print(f"  [manifest] {man_path}")
        else:
            print("  [--skip-export]")
            prov_dir = ROOT / "exports" / f"motherduck_provenance_{ts}"
            prov_dir.mkdir(parents=True, exist_ok=True)
            _provenance_check(con, prov_dir, dry_run=False)
    finally:
        con.close()


if __name__ == "__main__":
    main()
