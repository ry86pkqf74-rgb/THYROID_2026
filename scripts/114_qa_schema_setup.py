#!/usr/bin/env python3
"""Create and hydrate the MotherDuck qa schema.

Usage:
  .venv/bin/python scripts/114_qa_schema_setup.py --md
  .venv/bin/python scripts/114_qa_schema_setup.py --md --hydrate-from studies/v2_domain_promotion_gate_<label>
  .venv/bin/python scripts/114_qa_schema_setup.py --db-path thyroid_master.duckdb
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DDL_PATH = ROOT / "scripts" / "sql" / "114_qa_schema_ddl.sql"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Create qa schema and optionally hydrate from gate artifacts.")
    p.add_argument("--md", action="store_true", help="Target MotherDuck (fail-closed).")
    p.add_argument(
        "--md-sa",
        action="store_true",
        help="Prefer MD_SA_TOKEN over MOTHERDUCK_TOKEN.",
    )
    p.add_argument("--db-path", default=str(ROOT / "thyroid_master.duckdb"), help="Local DuckDB path.")
    p.add_argument(
        "--hydrate-from",
        type=Path,
        default=None,
        help="Gate run output directory to hydrate qa tables from.",
    )
    return p.parse_args()


def get_connection(args: argparse.Namespace) -> duckdb.DuckDBPyConnection:
    if args.md:
        import os
        from utils.md_connect import connect_md_or_file

        return connect_md_or_file(
            Path(args.db_path),
            md=True,
            fail_closed=True,
            prefer_service_account=args.md_sa,
            custom_user_agent=os.environ.get(
                "MOTHERDUCK_CUSTOM_USER_AGENT",
                "THYROID_2026_qa_schema_setup/1.0",
            ),
            motherduck_session_hint=os.environ.get("MOTHERDUCK_SESSION_HINT"),
        )
    return duckdb.connect(args.db_path)


def apply_ddl(con: duckdb.DuckDBPyConnection) -> None:
    ddl = DDL_PATH.read_text(encoding="utf-8")
    lines_no_comments = "\n".join(
        line for line in ddl.splitlines()
        if not line.strip().startswith("--")
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
            print(f"  [warn] DDL statement failed: {exc}")
            print(f"         Statement: {stmt[:120]}...")
    print("  [ddl] qa schema and tables created/verified")


def hydrate_scorecard(con: duckdb.DuckDBPyConnection, gate_dir: Path) -> None:
    csv_path = gate_dir / "promotion_scorecard.csv"
    if not csv_path.exists():
        print(f"  [skip] {csv_path} not found")
        return
    df = pd.read_csv(csv_path)
    run_label = gate_dir.name
    manifest_path = gate_dir / "manifest.json"
    git_sha = ""
    registry_version = ""
    if manifest_path.exists():
        m = json.loads(manifest_path.read_text())
        git_sha = m.get("git_sha", "")
        registry_version = ""

    df["run_label"] = run_label
    df["generated_at"] = datetime.now(timezone.utc).isoformat()
    df["git_sha"] = git_sha
    df["registry_version"] = registry_version

    cols = ["run_label", "gate_id", "criterion", "status", "detail", "generated_at", "git_sha", "registry_version"]
    insert_df = df[[c for c in cols if c in df.columns]]

    con.execute(f"DELETE FROM qa.promotion_scorecard WHERE run_label = '{run_label}'")
    con.register("_scorecard_tmp", insert_df)
    con.execute("INSERT INTO qa.promotion_scorecard SELECT * FROM _scorecard_tmp")
    con.unregister("_scorecard_tmp")
    print(f"  [hydrate] qa.promotion_scorecard: {len(insert_df)} rows from {run_label}")


def hydrate_domain_validation(con: duckdb.DuckDBPyConnection, gate_dir: Path) -> None:
    csv_path = gate_dir / "schema_validation.csv"
    if not csv_path.exists():
        print(f"  [skip] {csv_path} not found")
        return
    df = pd.read_csv(csv_path)
    run_label = gate_dir.name

    insert_df = pd.DataFrame({
        "run_label": run_label,
        "domain_name": df["domain_name"],
        "parquet_stem": df.get("parquet_stem", ""),
        "total_rows": df.get("total_rows", 0),
        "unique_patients": df.get("unique_patients", 0),
        "schema_ok": df.get("schema_ok", True),
        "dup_rows": df.get("dup_rows", 0),
        "dup_rate": df.get("dup_rate", 0.0),
        "provenance_cols_present": df.get("provenance_cols_present", 0),
        "entity_date_fill_pct": df.get("entity_date_fill_pct", 0.0),
        "note_date_fill_pct": df.get("note_date_fill_pct", 0.0),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    })

    con.execute(f"DELETE FROM qa.domain_validation WHERE run_label = '{run_label}'")
    con.register("_dv_tmp", insert_df)
    con.execute("INSERT INTO qa.domain_validation SELECT * FROM _dv_tmp")
    con.unregister("_dv_tmp")
    print(f"  [hydrate] qa.domain_validation: {len(insert_df)} rows from {run_label}")


def hydrate_concordance(con: duckdb.DuckDBPyConnection, gate_dir: Path) -> None:
    csv_path = gate_dir / "concordance_summary.csv"
    if not csv_path.exists():
        print(f"  [skip] {csv_path} not found")
        return
    df = pd.read_csv(csv_path)
    run_label = gate_dir.name

    insert_df = pd.DataFrame({
        "run_label": run_label,
        "comparison_domain": df.get("comparison_domain", ""),
        "algorithm_status": df.get("algorithm_comparison_status", ""),
        "llm_rows": df.get("llm_rows", 0),
        "unique_patients": df.get("unique_patients", 0),
        "structured_matches": df.get("structured_matches", 0),
        "fill_candidates": df.get("fill_candidates", 0),
        "review_conflicts": df.get("review_conflicts", 0),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    })

    con.execute(f"DELETE FROM qa.concordance_summary WHERE run_label = '{run_label}'")
    con.register("_cs_tmp", insert_df)
    con.execute("INSERT INTO qa.concordance_summary SELECT * FROM _cs_tmp")
    con.unregister("_cs_tmp")
    print(f"  [hydrate] qa.concordance_summary: {len(insert_df)} rows from {run_label}")


def hydrate_manual_review_queue(con: duckdb.DuckDBPyConnection, gate_dir: Path) -> None:
    csv_path = gate_dir / "manual_review_queue.csv"
    if not csv_path.exists():
        print(f"  [skip] {csv_path} not found")
        return
    df = pd.read_csv(csv_path)
    if df.empty:
        print("  [skip] manual_review_queue.csv is empty")
        return
    run_label = gate_dir.name

    col_map = {
        "review_row_id": "review_row_id",
        "research_id": "research_id",
        "domain": "domain",
        # v2 gate CSV uses source_domain as the promotable v2 domain key
        "source_domain": "domain",
        "comparison_domain": "domain",
        "entity_type": "entity_type",
        "entity_value_norm": "entity_value_norm",
        "llm_value": "entity_value_norm",
        "algorithm_status": "algorithm_status",
        "algorithm_comparison_status": "algorithm_status",
        "review_reason": "review_reason",
        "original_source_link": "review_reason",
        "verification_status": "verification_status",
        "reviewer": "reviewer",
        "reviewer_id": "reviewer",
        "reviewed_at": "reviewed_at",
        "reviewer_decision_at": "reviewed_at",
        "promotion_approved": "promotion_approved",
        "reviewer_evidence_span": "reviewer_evidence_span",
        "reviewer_comment": "reviewer_comment",
        "reason_code": "reason_code",
    }

    # Column order must match qa.manual_review_queue physical column order:
    # base cols ... reviewed_at, loaded_at, then ALTER extension cols.
    insert_data: dict[str, list] = {
        "review_row_id": [],
        "run_label": [],
        "research_id": [],
        "domain": [],
        "entity_type": [],
        "entity_value_norm": [],
        "algorithm_status": [],
        "review_reason": [],
        "verification_status": [],
        "reviewer": [],
        "reviewed_at": [],
        "loaded_at": [],
        "promotion_approved": [],
        "reviewer_evidence_span": [],
        "reviewer_comment": [],
        "reason_code": [],
    }

    target_columns = [
        "research_id",
        "domain",
        "entity_type",
        "entity_value_norm",
        "algorithm_status",
        "review_reason",
        "verification_status",
        "reviewer",
        "reviewed_at",
    ]

    target_tail_columns = [
        "promotion_approved",
        "reviewer_evidence_span",
        "reviewer_comment",
        "reason_code",
    ]

    now = datetime.now(timezone.utc).isoformat()
    for idx, row in df.iterrows():
        insert_data["review_row_id"].append(int(idx))
        insert_data["run_label"].append(run_label)
        for target_col in target_columns:
            val = None
            for src_col, mapped in col_map.items():
                if mapped == target_col and src_col in df.columns:
                    val = row.get(src_col)
                    if pd.isna(val):
                        val = None
                    elif target_col == "reviewed_at" and val not in (None, ""):
                        val = str(val)
                    break
            insert_data[target_col].append(val)
        insert_data["loaded_at"].append(now)
        for target_col in target_tail_columns:
            val = None
            for src_col, mapped in col_map.items():
                if mapped == target_col and src_col in df.columns:
                    val = row.get(src_col)
                    if pd.isna(val):
                        val = None
                    break
            insert_data[target_col].append(val)

    insert_df = pd.DataFrame(insert_data)

    con.execute(f"DELETE FROM qa.manual_review_queue WHERE run_label = '{run_label}'")
    con.register("_mrq_tmp", insert_df)
    con.execute("INSERT INTO qa.manual_review_queue SELECT * FROM _mrq_tmp")
    con.unregister("_mrq_tmp")
    print(f"  [hydrate] qa.manual_review_queue: {len(insert_df)} rows from {run_label}")


def main() -> None:
    args = parse_args()
    con = get_connection(args)
    try:
        apply_ddl(con)

        if args.hydrate_from and args.hydrate_from.is_dir():
            print(f"  [hydrate] from {args.hydrate_from}")
            hydrate_scorecard(con, args.hydrate_from)
            hydrate_domain_validation(con, args.hydrate_from)
            hydrate_concordance(con, args.hydrate_from)
            hydrate_manual_review_queue(con, args.hydrate_from)
        elif args.hydrate_from:
            print(f"  [warn] --hydrate-from path not found: {args.hydrate_from}")

        try:
            schemas = [r[0] for r in con.execute(
                "SELECT DISTINCT table_schema FROM information_schema.tables ORDER BY 1"
            ).fetchall()]
            print(f"  [verify] schemas with tables: {schemas}")
        except Exception:
            print("  [verify] schema listing not available")
        qa_tables = con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='qa' ORDER BY 1"
        ).fetchall()
        print(f"  [verify] qa tables: {[r[0] for r in qa_tables]}")
    finally:
        con.close()

    print("  [done] qa schema setup complete")


if __name__ == "__main__":
    main()
