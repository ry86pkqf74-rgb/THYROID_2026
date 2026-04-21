#!/usr/bin/env python3
"""Script 381 — Phase 6: rebuild manuscript_workspace.us_llm_absorption_gap_v1.

After Scripts 377+378+380, the original gap_v1 (a 15-row sample table written
by Script 372) is stale. Rebuild it from current state:

  Gap = patients with mapped LLM entities (per
        us_llm_absorption_mapping_v1) who are STILL not represented in
        canonical_us_nodule_v2.

Since the held LLM tables are now DROPPED in main, the only durable source
for entity inventory is "Thyroid 2026 UPdated".us_legacy_20260421.archived_*.

If 0 rows after rebuild → drop gap_v1 entirely.
If rows remain → keep and report.

Also drops manuscript_workspace.us_raw_index0_conflict_v1 if Logan has
nothing to look at there (unlikely; Phase 3 logged 32K conflicts).
"""
from __future__ import annotations

import argparse
import datetime
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

PUB = PUBLICATION_DB
ARCH_DB = "Thyroid 2026 UPdated"
ARCH_SCHEMA = "us_legacy_20260421"
SCRIPT_TAG = "Script 381"
GAP = f"{PUB}.manuscript_workspace.us_llm_absorption_gap_v1"
V2 = f"{PUB}.main.canonical_us_nodule_v2"

OUT_DIR = HERE / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)
RUN_TS = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")
DECISION_LOG = OUT_DIR / f"381_rebuild_gap_{RUN_TS}.json"

LLM_ARCHIVES = [
    "archived_note_entities_llm_tirads_granular",
    "archived_note_entities_llm_us_nodule_dynamics",
    "archived_note_entities_llm_imaging",
]


def log(msg: str) -> None:
    now = datetime.datetime.now(datetime.UTC).strftime("%H:%M:%S.%f")[:-3]
    print(f"[{now}Z] {msg}", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    log(f"{SCRIPT_TAG} start  commit={args.commit}")
    con = connect_locked()

    # Build a list of (research_id, source_table) from archives where the
    # patient has any non-empty entity array AND is NOT now in v2.
    # Reuse the parsing pattern from Script 377.
    union_sql = " UNION ALL ".join(f"""
SELECT
  '{tbl}' AS source_table,
  TRY_CAST(research_id AS INTEGER) AS research_id,
  json_array_length(json_extract(TRY_CAST(result_json AS JSON), '$.entities')) AS n_entities
FROM "{ARCH_DB}"."{ARCH_SCHEMA}"."{tbl}"
WHERE result_json IS NOT NULL
  AND TRY_CAST(result_json AS JSON) IS NOT NULL
  AND json_type(TRY_CAST(result_json AS JSON), '$.entities') = 'ARRAY'
  AND json_array_length(json_extract(TRY_CAST(result_json AS JSON), '$.entities')) > 0
  AND TRY_CAST(research_id AS INTEGER) IS NOT NULL
""" for tbl in LLM_ARCHIVES)

    if not args.commit:
        # Just count
        n = con.execute(f"""
WITH unabsorbed AS (
  SELECT DISTINCT source_table, research_id, SUM(n_entities) AS total_entities
  FROM ({union_sql}) s
  WHERE research_id NOT IN (SELECT DISTINCT research_id FROM {V2})
  GROUP BY 1, 2
)
SELECT COUNT(*) FROM unabsorbed
""").fetchone()[0]
        log(f"  dry-run gap count: {n}")
        return 0

    log(f"CREATE OR REPLACE TABLE {GAP}")
    con.execute(f"""
CREATE OR REPLACE TABLE {GAP} AS
WITH all_archives AS (
    {union_sql}
)
SELECT source_table, research_id, SUM(n_entities) AS total_entities,
       CURRENT_TIMESTAMP AS rebuilt_at
FROM all_archives
WHERE research_id NOT IN (SELECT DISTINCT research_id FROM {V2})
GROUP BY 1, 2
ORDER BY 3 DESC, 1
""")

    n = con.execute(f"SELECT COUNT(*) FROM {GAP}").fetchone()[0]
    by_src = dict(con.execute(
        f"SELECT source_table, COUNT(*) FROM {GAP} GROUP BY 1 ORDER BY 1"
    ).fetchall())
    log(f"  gap rebuilt: {n} (patient, source_table) rows")
    log(f"  by source: {by_src}")

    # Also report Phase 2 deferred (multi-nodule) and Phase 3 conflict tables
    extras = {}
    for tbl in (
        "us_llm_absorption_deferred_multi_nodule_v1",
        "us_raw_index0_conflict_v1",
        "us_raw_index_mismatch_v1",
        "us_llm_absorption_mapping_v1",
    ):
        try:
            cnt = con.execute(
                f"SELECT COUNT(*) FROM {PUB}.manuscript_workspace.{tbl}"
            ).fetchone()[0]
            extras[tbl] = cnt
        except Exception:
            extras[tbl] = "absent"
    log(f"  related audit tables: {extras}")

    if n == 0:
        log(f"  gap is empty — DROP TABLE {GAP}")
        con.execute(f"DROP TABLE {GAP}")
    else:
        log(f"  gap retained ({n} rows) — review before further drops")

    DECISION_LOG.write_text(json.dumps({
        "script": SCRIPT_TAG, "run_ts_utc": RUN_TS,
        "gap_rows_after_rebuild": n, "gap_by_source": by_src,
        "related_audit_table_counts": extras,
        "gap_dropped": (n == 0),
    }, indent=2, default=str))
    log(f"decision log: {DECISION_LOG}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
