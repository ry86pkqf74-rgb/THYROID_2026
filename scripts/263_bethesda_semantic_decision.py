#!/usr/bin/env python3
"""
Script 263 - Bethesda semantic ambiguity recon (NO MUTATION; STOP for Logan).

Per Prompt 13 dry-run finding #5: canonical_patient_master.bethesda_final does
not match MAX(bethesda_category) from fna_episode_master_v2 in 795+ patients
(795 in the dry-run, 796 measured live), and instead tracks worst_bethesda_num
more closely. This implies bethesda_final is currently 'worst observed across
cytology + path' rather than 'final cytology'.

This script:
  - Re-measures the discordance (live SELECT, no caching).
  - Writes scripts/output/263_semantic_decision_pending.md with the decision.
  - Does NOT mutate canonical_patient_master, manuscript_workspace views, or
    manuscript_workspace.__conventions.

Exit 0. Awaiting Logan's decision before any apply pass.
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402
from _v1_1_helpers import make_logger, utc_ts, write_decision_log  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
OUTPUT_DIR = REPO / "scripts" / "output"
RUN_LOG = OUTPUT_DIR / "263_run.log"
DECISION_LOG = OUTPUT_DIR / "263_decision_log.json"
PENDING_MD = OUTPUT_DIR / "263_semantic_decision_pending.md"
SCRIPT_TAG = "Script 263"
SCRIPT_NUM = "263"
RUN_DATE = "2026-04-17"

CPM = f'{PUBLICATION_DB}.main.canonical_patient_master'
FEM = f'{PUBLICATION_DB}.main.fna_episode_master_v2'


def measure(con) -> dict:
    out: dict = {}
    out["cpm_dim"] = con.execute(f"""
        SELECT COUNT(*),
               COUNT(*) FILTER (WHERE bethesda_final IS NOT NULL),
               COUNT(*) FILTER (WHERE worst_bethesda_num IS NOT NULL),
               COUNT(*) FILTER (WHERE bethesda_final = worst_bethesda_num),
               COUNT(*) FILTER (WHERE bethesda_final IS DISTINCT FROM worst_bethesda_num)
          FROM {CPM}
    """).fetchone()
    out["vs_fem"] = con.execute(f"""
        WITH x AS (
          SELECT TRY_CAST(research_id AS VARCHAR) AS rid,
                 MAX(bethesda_category) AS m
            FROM {FEM}
           WHERE bethesda_category IS NOT NULL
           GROUP BY 1
        )
        SELECT
          COUNT(*) FILTER (WHERE cpm.bethesda_final = x.m)               AS bf_eq_fem,
          COUNT(*) FILTER (WHERE cpm.bethesda_final IS DISTINCT FROM x.m) AS bf_ne_fem,
          COUNT(*) FILTER (WHERE cpm.worst_bethesda_num = x.m)            AS wb_eq_fem,
          COUNT(*) FILTER (WHERE cpm.worst_bethesda_num IS DISTINCT FROM x.m) AS wb_ne_fem
          FROM {CPM} cpm
          JOIN x ON x.rid = cpm.research_id
    """).fetchone()
    out["sample_disc"] = con.execute(f"""
        WITH x AS (
          SELECT TRY_CAST(research_id AS VARCHAR) AS rid,
                 MAX(bethesda_category) AS m
            FROM {FEM}
           WHERE bethesda_category IS NOT NULL
           GROUP BY 1
        )
        SELECT cpm.research_id, cpm.bethesda_final, cpm.worst_bethesda_num,
               x.m AS fem_max_bethesda
          FROM {CPM} cpm
          JOIN x ON x.rid = cpm.research_id
         WHERE cpm.bethesda_final IS DISTINCT FROM x.m
         ORDER BY cpm.research_id
         LIMIT 10
    """).fetchall()
    return out


PENDING_TEMPLATE = """# Script 263 - Decision Pending: bethesda_final semantic ambiguity

**Status:** dry-run only. No mutations applied. No snapshot written.
**Run date (UTC):** {run_ts}
**Branch:** `cleanup/v1_1_finalization-20260416`

## What was verified live (post Scripts 261 + 262)

CPM rows={n_rows}; bethesda_final populated={bf_pop}; worst_bethesda_num populated={wb_pop}.
- bethesda_final = worst_bethesda_num: **{eq}** patients
- bethesda_final IS DISTINCT FROM worst_bethesda_num: **{disc}** patients

Compared against `MAX(bethesda_category)` from `fna_episode_master_v2` (the
canonical FNA-cytology source, joined on `research_id` after the dtype align):

| Column                | Equal to MAX(FEM) | Not equal | Pct equal (of joined) |
|-----------------------|------------------:|----------:|----------------------:|
| `bethesda_final`      | {bf_eq:>17,} | {bf_ne:>9,} | {bf_pct:>21.2f}% |
| `worst_bethesda_num`  | {wb_eq:>17,} | {wb_ne:>9,} | {wb_pct:>21.2f}% |

`worst_bethesda_num` matches FNA-only MAX more often than `bethesda_final` does.
This is consistent with the dry-run finding that `bethesda_final` is currently
populated as "worst observed across cytology + path" rather than "final cytology
only".

Sample of 10 discordant rows (bethesda_final vs MAX(FEM.bethesda_category)):

| research_id | bethesda_final | worst_bethesda_num | MAX(FEM.bethesda_category) |
|-------------|---------------:|-------------------:|---------------------------:|
{sample_table}

## Decision required

Reply in chat with one of:

- **Path A (final cytology semantics):**
  - Rename current `bethesda_final` to `bethesda_worst_across_sources_legacy`
    (status='legacy' in `data_dictionary_v240`).
  - Create a new `bethesda_final` column = `MAX(fna_episode_master_v2.bethesda_category)`
    grouped by `CAST(research_id AS VARCHAR)`.
  - Snapshot CPM and any `manuscript_workspace` view referencing `bethesda_final`
    before rewrite. Re-verify view counts post-rewrite.

- **Path B (worst-across-sources semantics, the de facto current behavior):**
  - Keep column name. Update `data_dictionary_v240.description` to make explicit:
    "worst Bethesda category observed across cytology + path".
  - Snapshot then drop `worst_bethesda_num` (now duplicative).
  - Snapshot CPM and any `manuscript_workspace` view referencing
    `worst_bethesda_num` before drop. Re-verify view counts post-rewrite.

Either path:
  - Append a row to `manuscript_workspace.__conventions` with
    `convention_id='bethesda_semantics'` documenting the chosen interpretation.
  - Re-run Script 263 with `--apply` (and `--path A|B`) once the decision is set.

## What this script DID do (no mutation)

- Verified live ground truth above.
- Wrote `scripts/output/263_run.log` with the same numbers.
- Wrote `scripts/output/263_decision_log.json` flagging the decision as pending.
- Wrote this `scripts/output/263_semantic_decision_pending.md`.
- Did **not** snapshot, alter, or rewrite any table/view.
"""


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    log, fh = make_logger(RUN_LOG)
    t0 = time.time()
    log("=" * 78)
    log(f"=== START {Path(__file__).name}  mode=DRY-RUN")
    con = connect_locked()
    log(f"connected to {PUBLICATION_DB}")
    run_ts = utc_ts()
    decision: dict = {
        "script": SCRIPT_NUM, "run_ts": run_ts, "run_date": RUN_DATE,
        "mode": "DRY-RUN", "phases": {}, "decision_status": "pending_logan",
    }
    try:
        m = measure(con)
        n_rows, bf_pop, wb_pop, eq, disc = m["cpm_dim"]
        bf_eq, bf_ne, wb_eq, wb_ne = m["vs_fem"]
        bf_total = bf_eq + bf_ne
        wb_total = wb_eq + wb_ne
        bf_pct = (100.0 * bf_eq / bf_total) if bf_total else 0.0
        wb_pct = (100.0 * wb_eq / wb_total) if wb_total else 0.0
        log(f"  CPM rows={n_rows}  bf_pop={bf_pop}  wb_pop={wb_pop}  "
            f"bf=wb {eq}  bf<>wb {disc}")
        log(f"  bethesda_final  vs MAX(FEM): eq={bf_eq} ne={bf_ne} ({bf_pct:.2f}%)")
        log(f"  worst_bethesda_num vs MAX(FEM): eq={wb_eq} ne={wb_ne} ({wb_pct:.2f}%)")
        sample = m["sample_disc"]
        sample_md = "\n".join(
            f"| {r[0]} | {r[1]} | {r[2]} | {r[3]} |" for r in sample
        )
        body = PENDING_TEMPLATE.format(
            run_ts=run_ts, n_rows=n_rows, bf_pop=bf_pop, wb_pop=wb_pop,
            eq=eq, disc=disc,
            bf_eq=bf_eq, bf_ne=bf_ne, bf_pct=bf_pct,
            wb_eq=wb_eq, wb_ne=wb_ne, wb_pct=wb_pct,
            sample_table=sample_md,
        )
        PENDING_MD.write_text(body, encoding="utf-8")
        log(f"  wrote {PENDING_MD.relative_to(REPO)}")
        decision["phases"]["measure"] = {
            "n_rows": n_rows, "bf_pop": bf_pop, "wb_pop": wb_pop,
            "eq": eq, "disc": disc,
            "bf_eq_fem": bf_eq, "bf_ne_fem": bf_ne,
            "wb_eq_fem": wb_eq, "wb_ne_fem": wb_ne,
            "bf_pct": round(bf_pct, 2), "wb_pct": round(wb_pct, 2),
        }
    except Exception as exc:
        log(f"FATAL: {exc!r}")
        decision["error"] = str(exc)
        write_decision_log(DECISION_LOG, decision)
        fh.close()
        raise

    write_decision_log(DECISION_LOG, decision)
    log(f"=== END  elapsed={time.time()-t0:.1f}s")
    fh.close()


if __name__ == "__main__":
    main()
