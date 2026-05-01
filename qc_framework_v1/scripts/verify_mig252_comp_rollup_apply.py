#!/usr/bin/env python3
"""Post-apply verification for mig_252 CPM complication rollup repair."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from motherduck_client import get_token, token_mode  # noqa: E402

CANONICAL_DB = "thyroid_canonical_publication_v1_0"
RUN_ID = "mig_252_comp_confirmed_rollup_fix_20260501"


def connect() -> duckdb.DuckDBPyConnection:
    token = get_token()
    if not token:
        raise SystemExit(f"No MotherDuck token available (token_mode={token_mode()}).")
    con = duckdb.connect(f"md:{CANONICAL_DB}?motherduck_token={token}")
    con.execute(f'USE "{CANONICAL_DB}"')
    return con


def one(con: duckdb.DuckDBPyConnection, sql: str) -> dict[str, object]:
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    row = cur.fetchone()
    return dict(zip(cols, row, strict=False)) if row else {}


def rows(con: duckdb.DuckDBPyConnection, sql: str) -> list[dict[str, object]]:
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row, strict=False)) for row in cur.fetchall()]


def main() -> int:
    con = connect()
    result: dict[str, object] = {"run_id": RUN_ID, "token_mode": token_mode()}

    print("checking cpm_counts", flush=True)
    result["cpm_counts"] = one(
        con,
        """
            SELECT COUNT(*) AS cpm_rows,
                   COUNT(DISTINCT research_id) AS cpm_distinct,
                   SUM(CASE WHEN any_confirmed_complication_flag THEN 1 ELSE 0 END) AS any_confirmed_n,
                   SUM(COALESCE(n_confirmed_complications, 0)) AS n_confirmed_sum,
                   SUM(CASE WHEN cpm_built_at IS NULL THEN 1 ELSE 0 END) AS null_cpm_built_at
            FROM main.canonical_patient_master
            """,
    )
    print(json.dumps({"cpm_counts": result["cpm_counts"]}, default=str), flush=True)

    print("checking m038_ge200", flush=True)
    result["m038_ge200"] = one(
        con,
        """
            SELECT COUNT(*) AS n,
                   SUM(CASE WHEN any_confirmed_complication_flag THEN 1 ELSE 0 END) AS events
            FROM manuscript_workspace.cohort_m038_massive_goiter_v1
            WHERE gland_weight_final_g >= 200
            """,
    )
    print(json.dumps({"m038_ge200": result["m038_ge200"]}, default=str), flush=True)

    print("checking registry_status", flush=True)
    result["registry_status"] = rows(
        con,
        f"""
            SELECT verification_status, COUNT(*) AS n
            FROM main.canonical_column_verification_registry_v1
            WHERE schema_name='main'
              AND table_name='canonical_patient_master'
              AND batch_id='{RUN_ID}'
            GROUP BY 1
            ORDER BY 1
            """,
    )
    print(json.dumps({"registry_status": result["registry_status"]}, default=str), flush=True)

    print("checking table_signoff", flush=True)
    result["table_signoff"] = rows(
        con,
        """
            SELECT table_status, signoff_migration, n_verified, n_not_started, n_failed, n_na
            FROM main.canonical_table_signoff_registry_v1
            WHERE schema_name='main' AND table_name='canonical_patient_master'
            """,
    )
    print(json.dumps({"table_signoff": result["table_signoff"]}, default=str), flush=True)

    print("checking provenance", flush=True)
    result["provenance"] = rows(
        con,
        f"""
            SELECT run_id, phases_applied, high_findings_cleared, held_for_adjudication
            FROM manuscript_workspace.cpm_reconciliation_provenance_v1
            WHERE run_id='{RUN_ID}'
            """,
    )
    print(json.dumps({"provenance": result["provenance"]}, default=str), flush=True)

    print(json.dumps(result, indent=2, default=str))

    cpm = result["cpm_counts"]
    m038 = result["m038_ge200"]
    ok = (
        cpm.get("cpm_rows") == 10871
        and cpm.get("cpm_distinct") == 10871
        and cpm.get("any_confirmed_n") == 400
        and cpm.get("n_confirmed_sum") == 460
        and cpm.get("null_cpm_built_at") == 0
        and m038.get("n") == 475
        and m038.get("events") == 10
        and result["registry_status"] == [{"verification_status": "verified", "n": 57}]
        and len(result["provenance"]) == 1
    )
    if not ok:
        print("mig_252_verify=FAIL", file=sys.stderr)
        return 1
    print("mig_252_verify=OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())