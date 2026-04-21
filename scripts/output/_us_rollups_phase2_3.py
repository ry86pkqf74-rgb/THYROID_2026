#!/usr/bin/env python3
"""Phase 2 + Phase 3 helper for US rollups -> views refactor.

Phase 2: build candidate views in manuscript_workspace using the EXACT
SQL bodies from scripts/366 and scripts/367 (CREATE OR REPLACE TABLE
swapped to CREATE OR REPLACE VIEW; target name swapped to
manuscript_workspace.candidate_*). Preop logic is verbatim: same probe,
same JOIN, same coercion.

Phase 3: parity-check candidate views vs current main.* tables.
Reports row count + EXCEPT diffs in both directions, plus a per-column
breakdown of which columns disagree on the first 20 mismatched rows.

Stops after Phase 3 — no destructive operation on main.
"""
from __future__ import annotations

import datetime
import importlib
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
SCRIPTS = HERE.parent
sys.path.insert(0, str(SCRIPTS))

mod366 = importlib.import_module("366_canonical_us_exam_master_VIEW_v2")
mod367 = importlib.import_module("367_canonical_us_patient_master_VIEW_v2")
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

PUB = PUBLICATION_DB
CAND_EXAM = f"{PUB}.manuscript_workspace.candidate_us_exam_master_v2"
CAND_PT = f"{PUB}.manuscript_workspace.candidate_us_patient_master_v2"
TBL_EXAM = f"{PUB}.main.canonical_us_exam_master_VIEW_v2"
TBL_PT = f"{PUB}.main.canonical_us_patient_master_VIEW_v2"

TIRADS_COL = "acr2017_tirads_category"
TIRADS_PTS_COL = "acr2017_tirads_points"

RUN_TS = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")
DECISION_LOG = HERE / f"_us_rollups_phase2_3_{RUN_TS}.json"


def log(msg: str) -> None:
    now = datetime.datetime.now(datetime.UTC).strftime("%H:%M:%S.%f")[:-3]
    print(f"[{now}Z] {msg}", flush=True)


def to_view_sql(create_table_sql: str, table_target: str, view_target: str) -> str:
    """Swap CREATE OR REPLACE TABLE <X> to CREATE OR REPLACE VIEW <Y>."""
    out = create_table_sql.replace(
        f"CREATE OR REPLACE TABLE {table_target}",
        f"CREATE OR REPLACE VIEW {view_target}",
    )
    if out == create_table_sql:
        raise SystemExit(
            f"Failed to rewrite CREATE TABLE -> CREATE VIEW for {table_target}"
        )
    return out


def correct_tirads_column(sql: str) -> str:
    """Script 366 references tirads_category_v2 / tirads_score_2017 which do
    not exist on the current canonical_us_nodule_v2 schema (post Script 376).
    Substitute the post-376 column names per the prompt spec.

    This is the ONE intentional deviation from "verbatim builder SQL".
    """
    out = sql.replace("tirads_category_v2", TIRADS_COL)
    out = out.replace("tirads_score_2017", TIRADS_PTS_COL)
    if out == sql:
        raise SystemExit(
            "TIRADS column substitution found nothing to replace; "
            "Script 366 may have been updated."
        )
    return out


def phase2(con) -> dict[str, str]:
    log("CREATE SCHEMA IF NOT EXISTS manuscript_workspace")
    con.execute(f'CREATE SCHEMA IF NOT EXISTS {PUB}.manuscript_workspace')

    surg_rows = con.execute(mod366.SURG_COL_PROBE_SQL).fetchall()
    surg_col = surg_rows[0][0] if surg_rows else None
    log(f"  surgery date column on CPM: {surg_col}")

    exam_table_sql = mod366.build_sql(surg_col)
    exam_table_sql = correct_tirads_column(exam_table_sql)
    exam_view_sql = to_view_sql(exam_table_sql, mod366.TARGET, CAND_EXAM)

    pt_table_sql = mod367.BUILD_SQL
    pt_view_sql = to_view_sql(pt_table_sql, mod367.TARGET, CAND_PT)
    n_swap = pt_view_sql.count(mod366.TARGET)
    pt_view_sql = pt_view_sql.replace(mod366.TARGET, CAND_EXAM)
    log(f"  patient view: rewired {n_swap} reference(s) of "
        f"{mod366.TARGET} -> {CAND_EXAM}")
    if n_swap == 0:
        raise SystemExit(
            "Expected patient builder to read from main.canonical_us_exam_master_VIEW_v2;"
            " no occurrence found. Check Script 367 for changes."
        )

    log(f"CREATE OR REPLACE VIEW {CAND_EXAM}")
    con.execute(exam_view_sql)
    log(f"CREATE OR REPLACE VIEW {CAND_PT}")
    con.execute(pt_view_sql)

    return {"exam_view_sql": exam_view_sql, "pt_view_sql": pt_view_sql,
            "surg_col": surg_col or ""}


def _row_counts(con, table: str, view: str) -> dict[str, int]:
    t = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    v = con.execute(f"SELECT COUNT(*) FROM {view}").fetchone()[0]
    return {"table_rows": t, "view_rows": v}


def _except_diff(con, table: str, view: str) -> dict[str, int]:
    in_t_not_v = con.execute(
        f"SELECT COUNT(*) FROM ("
        f"  SELECT * FROM {table} EXCEPT SELECT * FROM {view}"
        f")"
    ).fetchone()[0]
    in_v_not_t = con.execute(
        f"SELECT COUNT(*) FROM ("
        f"  SELECT * FROM {view} EXCEPT SELECT * FROM {table}"
        f")"
    ).fetchone()[0]
    return {"in_table_not_view": in_t_not_v, "in_view_not_table": in_v_not_t}


def _column_breakdown(con, table: str, view: str, key_cols: list[str],
                      max_rows: int = 20) -> dict:
    """For rows where (key_cols) match but content differs, report which cols
    disagree on the first <=max_rows rows."""
    cols = [r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_catalog = ? AND table_schema = ? AND table_name = ? "
        "ORDER BY ordinal_position",
        [PUB, table.split(".")[1], table.split(".")[2]],
    ).fetchall()]

    non_key = [c for c in cols if c not in key_cols]
    on_clause = " AND ".join([f't."{k}" IS NOT DISTINCT FROM v."{k}"' for k in key_cols])
    diff_predicates = " OR ".join(
        [f't."{c}" IS DISTINCT FROM v."{c}"' for c in non_key]
    ) if non_key else "FALSE"

    sql = (
        f"SELECT {', '.join([f't.\"{k}\"' for k in key_cols])}, "
        f"       {', '.join([f'(t.\"{c}\" IS DISTINCT FROM v.\"{c}\") AS diff_{c}' for c in non_key])} "
        f"FROM {table} t INNER JOIN {view} v ON {on_clause} "
        f"WHERE {diff_predicates} "
        f"LIMIT {max_rows}"
    )

    rows = con.execute(sql).fetchall()
    if not rows:
        return {"diff_rows_sampled": 0, "by_column": {}}

    sample_cols = [d[0] for d in con.execute(sql).description]
    diff_col_names = [c[len("diff_"):] for c in sample_cols if c.startswith("diff_")]
    n_disagreements = {c: 0 for c in diff_col_names}
    for r in rows:
        for i, c in enumerate(diff_col_names):
            val = r[len(key_cols) + i]
            if val:
                n_disagreements[c] += 1

    return {
        "diff_rows_sampled": len(rows),
        "by_column": {k: v for k, v in n_disagreements.items() if v > 0},
        "key_cols": key_cols,
    }


def _row_count_by_key(con, table: str, view: str, key_cols: list[str]) -> dict:
    """Anti-join row counts: rows in t with no key match in v, and vice versa."""
    on = " AND ".join([f't."{k}" IS NOT DISTINCT FROM v."{k}"' for k in key_cols])
    only_t = con.execute(
        f"SELECT COUNT(*) FROM {table} t LEFT JOIN {view} v ON {on} "
        f"WHERE v.{key_cols[0]} IS NULL"
    ).fetchone()[0]
    only_v = con.execute(
        f"SELECT COUNT(*) FROM {view} v LEFT JOIN {table} t ON {on} "
        f"WHERE t.{key_cols[0]} IS NULL"
    ).fetchone()[0]
    return {"keys_only_in_table": only_t, "keys_only_in_view": only_v}


def phase3(con) -> dict:
    out: dict = {}
    log("=" * 60)
    log("PHASE 3 PARITY: canonical_us_exam_master_VIEW_v2")
    log("=" * 60)

    out["exam"] = {}
    out["exam"]["counts"] = _row_counts(con, TBL_EXAM, CAND_EXAM)
    log(f"  row counts: {out['exam']['counts']}")

    out["exam"]["except_diff"] = _except_diff(con, TBL_EXAM, CAND_EXAM)
    log(f"  EXCEPT diff (full row): {out['exam']['except_diff']}")

    out["exam"]["key_diff"] = _row_count_by_key(
        con, TBL_EXAM, CAND_EXAM, ["research_id", "exam_date"]
    )
    log(f"  key-level diff (research_id, exam_date): {out['exam']['key_diff']}")

    if (out["exam"]["except_diff"]["in_table_not_view"] > 0 or
            out["exam"]["except_diff"]["in_view_not_table"] > 0):
        out["exam"]["column_breakdown"] = _column_breakdown(
            con, TBL_EXAM, CAND_EXAM, ["research_id", "exam_date"], max_rows=20
        )
        log(f"  column-level breakdown (first 20 mismatched rows): "
            f"{out['exam']['column_breakdown']}")

    log("")
    log("=" * 60)
    log("PHASE 3 PARITY: canonical_us_patient_master_VIEW_v2")
    log("=" * 60)

    out["pt"] = {}
    out["pt"]["counts"] = _row_counts(con, TBL_PT, CAND_PT)
    log(f"  row counts: {out['pt']['counts']}")

    out["pt"]["except_diff"] = _except_diff(con, TBL_PT, CAND_PT)
    log(f"  EXCEPT diff (full row): {out['pt']['except_diff']}")

    out["pt"]["key_diff"] = _row_count_by_key(
        con, TBL_PT, CAND_PT, ["research_id"]
    )
    log(f"  key-level diff (research_id): {out['pt']['key_diff']}")

    if (out["pt"]["except_diff"]["in_table_not_view"] > 0 or
            out["pt"]["except_diff"]["in_view_not_table"] > 0):
        out["pt"]["column_breakdown"] = _column_breakdown(
            con, TBL_PT, CAND_PT, ["research_id"], max_rows=20
        )
        log(f"  column-level breakdown (first 20 mismatched rows): "
            f"{out['pt']['column_breakdown']}")

    return out


def main() -> int:
    log("Phase 2 + Phase 3 helper start")
    con = connect_locked()

    phase2_out = phase2(con)
    phase3_out = phase3(con)

    DECISION_LOG.write_text(json.dumps({
        "run_ts_utc": RUN_TS,
        "phase2": {"surg_col": phase2_out["surg_col"]},
        "phase3": phase3_out,
    }, indent=2, default=str))
    log(f"decision log: {DECISION_LOG}")

    diffs_zero = (
        phase3_out["exam"]["except_diff"]["in_table_not_view"] == 0
        and phase3_out["exam"]["except_diff"]["in_view_not_table"] == 0
        and phase3_out["pt"]["except_diff"]["in_table_not_view"] == 0
        and phase3_out["pt"]["except_diff"]["in_view_not_table"] == 0
    )
    log(f"all four EXCEPT diffs zero: {diffs_zero}")
    log("STOPPING per agreed checkpoint. Review parity output before "
        "proceeding to Phase 4.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
