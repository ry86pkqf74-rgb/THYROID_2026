#!/usr/bin/env python3
"""126_release_candidate_motherduck_audit.py — RC evidence pack (fail-closed MotherDuck).

Runs read-mostly inventory queries, optional named snapshot, row-count reconciliation,
and writes artifacts under studies/<YYYYMMDD>_release_candidate_audit/.

No raw note text is queried or written.

Usage:
  .venv/bin/python scripts/126_release_candidate_motherduck_audit.py --md --md-sa \\
      --out-root studies --create-named-snapshot rc_thyroid_2026_YYYYMMDD
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from llm_extraction.registry import load_registry  # noqa: E402
from utils.md_connect import connect_md_or_file  # noqa: E402

DEFAULT_DB = ROOT / "thyroid_master.duckdb"
V2_DIR = ROOT / "processed" / "output" / "v2_parquets"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--md", action="store_true", required=True, help="MotherDuck only (fail-closed).")
    p.add_argument("--md-sa", action="store_true", help="Prefer MD_SA_TOKEN.")
    p.add_argument("--db-path", default=str(DEFAULT_DB))
    p.add_argument("--v2-parquets-dir", default=str(V2_DIR))
    p.add_argument("--out-root", type=Path, default=ROOT / "studies")
    p.add_argument(
        "--create-named-snapshot",
        default=None,
        metavar="NAME",
        help="If set, run CREATE SNAPSHOT name OF database (MotherDuck Business).",
    )
    p.add_argument(
        "--session-hint",
        default=None,
        help="Override motherduck_session_hint (default rc_release_candidate_audit).",
    )
    return p.parse_args()


def _safe_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    print(f"  [write] {path}")


def main() -> None:
    args = parse_args()
    audit_day = datetime.now(timezone.utc).strftime("%Y%m%d")
    out = args.out_root / f"{audit_day}_release_candidate_audit"
    out.mkdir(parents=True, exist_ok=True)

    hint = args.session_hint or os.environ.get(
        "MOTHERDUCK_SESSION_HINT",
        f"rc_release_candidate_audit_{audit_day}",
    )
    ua = os.environ.get("MOTHERDUCK_CUSTOM_USER_AGENT", "THYROID_2026_rc_audit/1.0")

    con = connect_md_or_file(
        Path(args.db_path),
        md=True,
        fail_closed=True,
        prefer_service_account=args.md_sa,
        custom_user_agent=ua,
        motherduck_session_hint=hint,
    )

    chunks: list[str] = []

    def snap(sql: str, title: str) -> None:
        chunks.append(f"## {title}\n\n```sql\n{sql.strip()}\n```\n")
        try:
            df = con.execute(sql).df()
            chunks.append(df.to_markdown(index=False))
            chunks.append("\n")
        except Exception as e:
            chunks.append(f"_Error: {e}_\n\n")

    try:
        db_name = con.execute("SELECT current_database()").fetchone()[0]
        snap("SELECT current_database() AS current_database, current_timestamp AS ts", "Session")
        chunks.append(f"- **custom_user_agent (connection):** `{ua}`\n")
        chunks.append(f"- **motherduck_session_hint:** `{hint}`\n")
        chunks.append(f"- **current_database:** `{db_name}`\n\n")

        snap("SELECT * FROM MD_INFORMATION_SCHEMA.DATABASES", "MD_INFORMATION_SCHEMA.DATABASES")
        snap(
            "SELECT * FROM MD_INFORMATION_SCHEMA.DATABASE_SNAPSHOTS "
            "WHERE database_name ILIKE '%thyroid%' ORDER BY created_ts DESC LIMIT 50",
            "DATABASE_SNAPSHOTS (thyroid filter)",
        )
        snap(
            "SELECT table_schema, COUNT(*) AS n_tables "
            "FROM information_schema.tables "
            "WHERE table_catalog = current_database() "
            "GROUP BY 1 ORDER BY 1",
            "Table counts by schema",
        )

        registry = load_registry()
        rows = []
        v2_dir = Path(args.v2_parquets_dir)
        for name, spec in registry.v2_domains.items():
            if not spec.canonical_output:
                continue
            stem = spec.parquet_stem
            pq = v2_dir / f"{stem}.parquet"
            local_n = int(len(pd.read_parquet(pq))) if pq.exists() else -1
            st_m = ma_m = -1
            try:
                st_m = con.execute(f"SELECT COUNT(*) FROM v2_stage.{stem}").fetchone()[0]
            except Exception:
                pass
            try:
                ma_m = con.execute(f"SELECT COUNT(*) FROM main.{stem}").fetchone()[0]
            except Exception:
                pass
            rows.append({
                "domain": name,
                "stem": stem,
                "qa_tier": spec.qa_tier,
                "local_parquet": local_n,
                "v2_stage": st_m,
                "main": ma_m,
                "stage_eq_local": local_n == st_m if local_n >= 0 and st_m >= 0 else None,
                "main_eq_local": local_n == ma_m if local_n >= 0 and ma_m >= 0 else None,
            })

        counts_md = pd.DataFrame(rows).to_markdown(index=False)
        chunks.append("## Per-domain row counts (local vs v2_stage vs main)\n\n")
        chunks.append(counts_md)
        chunks.append("\n")

        distinct_counts = {r["v2_stage"] for r in rows if r["v2_stage"] >= 0}
        uniform = len(distinct_counts) == 1 and len(rows) > 1
        all_match_local = all(
            r["local_parquet"] == r["v2_stage"] == r["main"]
            for r in rows
            if r["local_parquet"] >= 0 and r["v2_stage"] >= 0 and r["main"] >= 0
        )
        if uniform and all_match_local:
            root_cause = (
                "All v2 domains share the same row count on disk and in MD (here: one count across all stems). "
                "This matches **current** parquets — not an MD loader bug vs an older promotion report. "
                "Typical explanation: note-level (one row per note) exports across domains for the same cohort. "
                "Validate grain with COUNT(*) vs COUNT(DISTINCT note_row_id) per stem if needed."
            )
        elif uniform:
            root_cause = (
                "All v2_stage counts identical but local parquet differs — investigate staging/load mapping."
            )
        else:
            root_cause = "v2_stage counts vary by domain (expected for entity-level per-domain files)."
        _safe_write(
            out / "row_count_reconciliation.md",
            f"# Row count reconciliation\n\n{counts_md}\n\n## Interpretation\n\n{root_cause}\n",
        )

        snap(
            "SELECT run_label, domain, COUNT(*) AS n, "
            "COUNT(*) FILTER (WHERE verification_status IS NOT NULL) AS reviewed, "
            "COUNT(*) FILTER (WHERE verification_status IS NULL) AS pending "
            "FROM qa.manual_review_queue GROUP BY 1, 2 ORDER BY 1, 2",
            "manual_review_queue by run_label and domain",
        )
        snap(
            "SELECT algorithm_status, COUNT(*) AS n "
            "FROM qa.manual_review_queue GROUP BY 1 ORDER BY 2 DESC",
            "manual_review_queue by algorithm_status",
        )
        snap(
            "SELECT release_tag, created_at, created_by FROM qa.release_manifest ORDER BY created_at DESC LIMIT 10",
            "release_manifest (latest)",
        )

        # Master views
        for v in (
            "master_fact_long_verified_v1",
            "master_patient_rollup_verified_v1",
            "master_source_lineage_v1",
        ):
            snap(f"SELECT COUNT(*) AS n FROM main.{v}", f"View {v}")

        schema_lines = ["# Schema inventory (main promoted v2 stems)\n"]
        for _, spec in registry.v2_domains.items():
            if not spec.canonical_output:
                continue
            stem = spec.parquet_stem
            try:
                cols = con.execute(
                    f"SELECT column_name, data_type FROM information_schema.columns "
                    f"WHERE table_schema = 'main' AND table_name = '{stem}' ORDER BY ordinal_position"
                ).fetchdf()
                schema_lines.append(f"## main.{stem}\n\n{cols.to_markdown(index=False)}\n\n")
            except Exception as e:
                schema_lines.append(f"## main.{stem}\n\n_Error: {e}_\n\n")
        _safe_write(out / "schema_inventory.md", "".join(schema_lines))

        _safe_write(out / "snapshot_metadata.md", "".join(chunks))

        if args.create_named_snapshot:
            snap_sql = (
                f'CREATE SNAPSHOT "{args.create_named_snapshot}" OF "{db_name}";'
                if " " in str(db_name)
                else f"CREATE SNAPSHOT {args.create_named_snapshot} OF {db_name};"
            )
            try:
                con.execute(snap_sql)
                _safe_write(out / "named_snapshot_ddl_applied.sql", snap_sql + "\n")
                print(f"  [ok] {snap_sql}")
            except Exception as e:
                _safe_write(out / "named_snapshot_error.txt", f"{snap_sql}\n\n{e}\n")
                print(f"  [warn] named snapshot: {e}")

        # Query history notes (operator fills from UI)
        _safe_write(
            out / "query_history_notes.md",
            "# Query history\n\n"
            f"Filter MotherDuck query history by **session hint** `{hint}` or **user agent** `{ua}`.\n",
        )

    finally:
        con.close()

    print(f"  [done] evidence under {out}")


if __name__ == "__main__":
    main()
