#!/usr/bin/env python3
"""Deploy source-truth confirmation views (v1) to MotherDuck and emit metrics.

Creates non-destructive VIEWs from scripts/sql/source_truth_confirmation_v1.sql:
  v_fna_episode_bethesda_resolved_v1
  v_imaging_nodule_linkage_classification_v1
  v_imaging_nodule_tirads_gap_v1
  v_canonical_us_nodule_scope_v1

Run:
  .venv/bin/python scripts/151_source_truth_confirmation_v1.py --md

Token: motherduck_client.get_token() / motherduck.local.toml (same as other --md scripts).
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from utils.md_connect import connect_md_fail_closed  # noqa: E402

SQL_PATH = ROOT / "scripts" / "sql" / "source_truth_confirmation_v1.sql"
OUT_JSON = (
    ROOT
    / "studies"
    / "20260413_source_truth_completeness_audit"
    / "confirmation_v1_metrics.json"
)
OUT_MD = (
    ROOT
    / "studies"
    / "20260413_source_truth_completeness_audit"
    / "confirmation_v1_executive.md"
)


def _iter_statements(sql: str):
    """Split on semicolons; strip leading -- comment blocks before each CREATE."""
    for raw in sql.split(";"):
        lines: list[str] = []
        for line in raw.splitlines():
            if not lines:
                stripped = line.strip()
                if not stripped:
                    continue
                if stripped.startswith("--"):
                    continue
            lines.append(line)
        stmt = "\n".join(lines).strip()
        if not stmt:
            continue
        if not stmt.upper().startswith("CREATE"):
            continue
        yield stmt + ";"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--md", action="store_true", help="MotherDuck (fail-closed)")
    parser.add_argument("--local", action="store_true", help="Local thyroid_master.duckdb file")
    args = parser.parse_args()

    if not args.md and not args.local:
        args.md = True

    db_path = ROOT / "thyroid_master.duckdb"
    if args.md:
        con = connect_md_fail_closed(db_path)
    else:
        import duckdb

        con = duckdb.connect(str(db_path))

    sql_text = SQL_PATH.read_text(encoding="utf-8")
    for stmt in _iter_statements(sql_text):
        con.execute(stmt)

    metrics: dict = {
        "deployed_at_utc": datetime.now(timezone.utc).isoformat(),
        "sql_file": str(SQL_PATH.relative_to(ROOT)),
    }

    # KPI queries
    metrics["bethesda"] = con.execute(
        """
        SELECT
            COUNT(*)::BIGINT AS n_episodes,
            SUM(CASE WHEN bethesda_resolved_num IS NOT NULL THEN 1 ELSE 0 END)::BIGINT AS n_resolved,
            SUM(CASE WHEN bethesda_unscorable_reason IS NOT NULL THEN 1 ELSE 0 END)::BIGINT AS n_unscorable_tagged
        FROM v_fna_episode_bethesda_resolved_v1
        """
    ).fetchdf().to_dict(orient="records")[0]

    metrics["linkage"] = con.execute(
        """
        SELECT linkage_state, linkage_reason_code, COUNT(*)::BIGINT AS n
        FROM v_imaging_nodule_linkage_classification_v1
        GROUP BY 1, 2
        ORDER BY n DESC
        """
    ).fetchdf().to_dict(orient="records")

    metrics["tirads_gaps"] = con.execute(
        """
        SELECT
            SUM(CASE WHEN gap_sufficient_features_null_scores THEN 1 ELSE 0 END)::BIGINT AS n_gap,
            COUNT(*)::BIGINT AS n_total
        FROM v_imaging_nodule_tirads_gap_v1
        """
    ).fetchdf().to_dict(orient="records")[0]

    metrics["scope"] = con.execute("SELECT * FROM v_canonical_us_nodule_scope_v1").fetchdf().to_dict(
        orient="records"
    )[0]

    unresolved = 0
    for row in metrics["linkage"]:
        if row.get("linkage_state") == "unresolved_linkage_gap":
            unresolved += int(row.get("n") or 0)

    metrics["unresolved_linkage_gap_total"] = unresolved

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(metrics, indent=2), encoding="utf-8")

    # Executive summary (scoped confirmation)
    n_res = int(metrics["bethesda"]["n_resolved"])
    n_ep = int(metrics["bethesda"]["n_episodes"])
    n_gap = int(metrics["tirads_gaps"]["n_gap"])
    md_lines = [
        "# Confirmation v1 (deterministic joins + exhaustive linkage classification)",
        "",
        f"- Deployed (UTC): `{metrics['deployed_at_utc']}`",
        "",
        "## Scoped answers (fail-closed global YES still blocked if any `unresolved_linkage_gap` is unacceptable)",
        "",
        "### Q1 — COMPLETE corpus → `imaging_nodule_master_v1`",
        "",
        f"- `v_canonical_us_nodule_scope_v1`: **{metrics['scope'].get('n_nodule_rows')}** rows; source_table = `{metrics['scope'].get('source_table_example')}`.",
        "- Deterministic parity was already **19891/19891** in the 20260413 audit.",
        "",
        "### Q2 — TI-RADS when ≥5 ACR fields populated",
        "",
        f"- Rows with sufficient features but **both** `tirads_reported` and `tirads_acr_recalculated` null: **{n_gap}** (see `v_imaging_nodule_tirads_gap_v1`).",
        "",
        "### Q3 — Imaging ↔ FNA classification",
        "",
        "Every `imaging_nodule_master_v1` row appears in `v_imaging_nodule_linkage_classification_v1` with:",
        "",
        "- `linked_to_fna` — primary multimodal link exists",
        "- `no_eligible_fna` — documented reason (no patient FNA, US after surgery, all FNA before index US, or only FNA beyond 90d window)",
        "- `unresolved_linkage_gap` — candidate FNA in 0–90d after index US but **no** `imaging_fna_linkage_mm_v1` primary row (requires algorithm/review follow-up)",
        "",
        f"- **Unresolved linkage gap total: {unresolved}**",
        "",
        "### Q4 — Lymph node (exam-level text)",
        "",
        "- Structured per-level LN model is **not** in scope for v1; exam-level `lymph_node_assessment` remains the capture mechanism.",
        "",
        "### Q5 — Bethesda",
        "",
        f"- Episodes with numeric Bethesda resolved (`bethesda_resolved_num`): **{n_res} / {n_ep}**.",
        "- Remaining rows carry `bethesda_unscorable_reason` in `v_fna_episode_bethesda_resolved_v1`.",
        "",
        "## Views created",
        "",
        "- `v_fna_episode_bethesda_resolved_v1`",
        "- `v_imaging_nodule_linkage_classification_v1`",
        "- `v_imaging_nodule_tirads_gap_v1`",
        "- `v_canonical_us_nodule_scope_v1`",
        "",
        f"Machine-readable metrics: `{OUT_JSON.relative_to(ROOT)}`",
        "",
    ]
    OUT_MD.write_text("\n".join(md_lines), encoding="utf-8")

    print(json.dumps(metrics, indent=2))
    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
