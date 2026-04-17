#!/usr/bin/env python3
"""
Script 264 - Final acceptance for the v1_1 closeout round (Prompt 13).

Supersedes the Script 259 invariant suite by re-running it AND the new Prompt 13
invariants:
  - fna_episode_master_v2.linked_molecular_episode_id populated for >= 85%
    of Bethesda III/IV rows (or recorded as 'pending_decision' if 260 deferred).
  - 0 fna_episode_master_v2.resolved_fna_date values outside [1995-01-01, CURRENT_DATE].
  - ras_positive_v7 not present on canonical_patient_master.
  - fna_episode_master_v2.research_id dtype = VARCHAR.
  - manuscript_workspace.__conventions has 'bethesda_semantics' row, OR
    Script 263 deferred (in which case status is 'pending_decision').

Emits:
  - new CPM hash-of-row-hashes (the candidate v1_1 lock hash)
  - APPENDS a new section "## v1_1 Closeout Addendum (2026-04-17)" to
    studies/v1_1_finalization/FINALIZATION_REPORT_v1_1.md
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402
from _v1_1_helpers import make_logger, utc_ts, write_decision_log  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
OUTPUT_DIR = REPO / "scripts" / "output"
RUN_LOG = OUTPUT_DIR / "264_run.log"
DECISION_LOG = OUTPUT_DIR / "264_decision_log.json"
REPORT_PATH = REPO / "studies" / "v1_1_finalization" / "FINALIZATION_REPORT_v1_1.md"
SCRIPT_TAG = "Script 264"
SCRIPT_NUM = "264"
RUN_DATE = "2026-04-17"

CPM = f'{PUBLICATION_DB}.main.canonical_patient_master'
FEM = f'{PUBLICATION_DB}.main.fna_episode_master_v2'
DICT = f'{PUBLICATION_DB}.main.data_dictionary_v240'

EXPECTED_CPM_ROWS = 10871
EXPECTED_FEM_ROWS = 8119


def cpm_dim(con) -> tuple[int, int]:
    n_rows = int(con.execute(f"SELECT COUNT(*) FROM {CPM}").fetchone()[0])
    n_cols = int(con.execute(f"""
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
          AND table_name='canonical_patient_master'
    """).fetchone()[0])
    return n_rows, n_cols


def cpm_row_hash_of_hashes(con) -> str:
    rows = con.execute(f"""
        SELECT research_id, md5(CAST(cpm AS VARCHAR)) AS row_hash
        FROM {CPM} cpm
        ORDER BY TRY_CAST(research_id AS INTEGER)
    """).fetchall()
    h = hashlib.sha256()
    for rid, rh in rows:
        h.update(f"{rid}|{rh}\n".encode("utf-8"))
    return h.hexdigest()


def has_column(con, table_name: str, column_name: str) -> bool:
    return con.execute(f"""
        SELECT 1 FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
          AND table_name=? AND column_name=?
    """, [table_name, column_name]).fetchone() is not None


def get_dtype(con, table_name: str, column_name: str) -> str | None:
    row = con.execute(f"""
        SELECT data_type FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
          AND table_name=? AND column_name=?
    """, [table_name, column_name]).fetchone()
    return row[0] if row else None


def measure_b34_link_pct(con) -> tuple[int, int, float]:
    row = con.execute(f"""
        SELECT
          COUNT(*) FILTER (WHERE bethesda_category IN (3,4)) AS rows_b34,
          COUNT(*) FILTER (WHERE bethesda_category IN (3,4)
                           AND linked_molecular_episode_id IS NOT NULL) AS rows_b34_linked
          FROM {FEM}
    """).fetchone()
    rb34 = int(row[0] or 0)
    rl = int(row[1] or 0)
    pct = round(100.0 * rl / rb34, 2) if rb34 else 0.0
    return rb34, rl, pct


def measure_bad_dates(con) -> int:
    return int(con.execute(f"""
        SELECT COUNT(*) FROM {FEM}
         WHERE resolved_fna_date IS NOT NULL
           AND (EXTRACT(year FROM resolved_fna_date) < 1995
                OR resolved_fna_date > CURRENT_DATE)
    """).fetchone()[0])


def conventions_status(con) -> str:
    """Returns the bethesda_semantics row status or 'pending_decision'."""
    try:
        row = con.execute("""
            SELECT convention_id FROM manuscript_workspace.__conventions
             WHERE convention_id='bethesda_semantics'
        """).fetchone()
        return "present" if row else "pending_decision"
    except Exception:
        return "pending_decision"


def replay_259(con) -> dict:
    """Re-run Script 259's REPLAYS to confirm everything is still 0."""
    queries = {
        "audit_1_1": f"""
            WITH detail AS (
              SELECT TRY_CAST(research_id AS INTEGER) AS rid,
                     GREATEST(COALESCE(MAX(tirads_reported),0),
                              COALESCE(MAX(tirads_acr_recalculated),0)) AS detail_max
              FROM {PUBLICATION_DB}.main.canonical_us_nodule_characteristics_v1
              GROUP BY 1
            )
            SELECT COUNT(*) FROM {CPM} cpm
            JOIN detail d ON TRY_CAST(cpm.research_id AS INTEGER) = d.rid
            WHERE d.detail_max > COALESCE(cpm.max_tirads_ever, 0)
        """,
        "audit_2_2": f"""
            WITH ep AS (
              SELECT TRY_CAST(research_id AS INTEGER) AS rid, COUNT(*) AS n
                FROM {FEM} GROUP BY 1
            )
            SELECT COUNT(*) FROM {CPM} cpm
            JOIN ep ON TRY_CAST(cpm.research_id AS INTEGER) = ep.rid
            WHERE COALESCE(cpm.n_fna_episodes, -1) <> ep.n
        """,
        "audit_3_1": f"""
            WITH e AS (
              SELECT TRY_CAST(research_id AS INTEGER) AS rid, MAX(dose_mci) AS m
                FROM {PUBLICATION_DB}.main.rai_treatment_episode_v2 GROUP BY 1
            )
            SELECT COUNT(*) FROM {CPM} cpm
            JOIN e ON TRY_CAST(cpm.research_id AS INTEGER) = e.rid
            WHERE (cpm.rai_max_dose_mci = 0 OR cpm.rai_max_dose_mci IS NULL) AND e.m > 0
        """,
        "audit_5_3": f"""
            WITH cp AS (
              SELECT TRY_CAST(research_id AS INTEGER) AS rid,
                     BOOL_OR(confirmed_flag = TRUE) AS any_cp
                FROM {PUBLICATION_DB}.main.complication_phenotype_v1 GROUP BY 1
            )
            SELECT COUNT(*) FROM cp
            JOIN {CPM} p ON TRY_CAST(p.research_id AS INTEGER) = cp.rid
            WHERE cp.any_cp = TRUE AND COALESCE(p.any_confirmed_complication_flag, FALSE) = FALSE
        """,
    }
    out = {}
    for k, q in queries.items():
        out[k] = int(con.execute(q).fetchone()[0])
    return out


def append_addendum(report_path: Path, payload: dict, log) -> None:
    if not report_path.exists():
        log(f"  ! report file missing: {report_path}; creating fresh")
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text("# THYROID Canonical Publication v1_1 - Finalization Report\n\n",
                               encoding="utf-8")

    body = []
    body.append("## v1_1 Closeout Addendum (2026-04-17)\n")
    body.append("Per Prompt 13 closeout (Scripts 260-264). This section is APPENDED, "
                "not a rewrite.\n")
    body.append(f"**Run timestamp (UTC):** {datetime.now(timezone.utc).isoformat()}  ")
    body.append(f"**Branch:** `cleanup/v1_1_finalization-20260416`\n")

    body.append("### Executed in this round\n")
    body.append("| Script | Subject | Status |")
    body.append("|---|---|---|")
    body.append(f"| 260 | Hydrate `fna_episode_master_v2` linkages | "
                f"{payload['s260_status']} |")
    body.append(f"| 261 | Repair `fna_history` two-digit-year dates | "
                f"{payload['s261_status']} |")
    body.append(f"| 262 | Drop `ras_positive_v7` + cast FEM.research_id to VARCHAR | "
                f"{payload['s262_status']} |")
    body.append(f"| 263 | Resolve `bethesda_final` semantic ambiguity | "
                f"{payload['s263_status']} |")
    body.append("")

    body.append("### Invariants (this round)\n")
    body.append("| # | Invariant | Result | Target | Status |")
    body.append("|---|---|---|---|---|")
    pe = payload["evidence"]
    body.append(f"| 1 | CPM row count | {pe['cpm_rows']:,} | 10,871 | "
                f"{'PASS' if pe['cpm_rows']==10871 else 'FAIL'} |")
    body.append(f"| 2 | CPM column count | {pe['cpm_cols']:,} | 1,493 | "
                f"{'PASS' if pe['cpm_cols']==1493 else 'FAIL'} |")
    body.append(f"| 3 | `ras_positive_v7` absent on CPM | "
                f"{'absent' if not pe['ras_v7_present'] else 'PRESENT'} | absent | "
                f"{'PASS' if not pe['ras_v7_present'] else 'FAIL'} |")
    body.append(f"| 4 | `fna_episode_master_v2.research_id` dtype | "
                f"{pe['fem_rid_dtype']} | VARCHAR | "
                f"{'PASS' if (pe['fem_rid_dtype'] or '').upper()=='VARCHAR' else 'FAIL'} |")
    body.append(f"| 5 | `fna_episode_master_v2` row count | {pe['fem_rows']:,} | 8,119 | "
                f"{'PASS' if pe['fem_rows']==8119 else 'FAIL'} |")
    body.append(f"| 6 | FEM bad `resolved_fna_date` rows | {pe['bad_dates']} | 0 | "
                f"{'PASS' if pe['bad_dates']==0 else 'FAIL'} |")
    body.append(f"| 7 | FEM B III/IV linked_molecular_episode_id pct | "
                f"{pe['b34_pct']}% ({pe['b34_linked']}/{pe['b34_total']}) | "
                f">= 85% | {pe['b34_status']} |")
    body.append(f"| 8 | `__conventions.bethesda_semantics` row | "
                f"{payload['conventions_status']} | present | "
                f"{'PASS' if payload['conventions_status']=='present' else 'pending_decision'} |")
    body.append("")

    body.append("### Script 259 replay queries (must remain 0)\n")
    body.append("| Audit | After |")
    body.append("|---|---|")
    for k, v in payload["replay_259"].items():
        body.append(f"| {k} | {v} |")
    body.append("")

    body.append("### CPM hash-of-row-hashes (candidate v1_1 lock hash)\n")
    body.append(f"`{payload['cpm_hash']}`\n")
    body.append("Computed as `sha256(concat(research_id || md5(cpm row) || newline) "
                "ordered by research_id)`. Re-run Script 264 to recompute and "
                "compare.\n")

    body.append("### Open decisions (deferred to Logan)\n")
    if "260_decision" in payload and payload["260_decision"]:
        body.append(f"- **Script 260** - molecular FK linkage: "
                    f"{payload['260_decision']}")
    if payload['conventions_status'] != 'present':
        body.append("- **Script 263** - `bethesda_final` semantics: "
                    "see `scripts/output/263_semantic_decision_pending.md`.")
    body.append("")

    body.append("---\n")
    body.append("_End of v1_1 Closeout Addendum (Script 264)._\n")

    text = "\n".join(body) + "\n"
    with report_path.open("a", encoding="utf-8") as f:
        f.write("\n" + text)
    log(f"  appended {len(text)} chars to {report_path.relative_to(REPO)}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    args = ap.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    log, fh = make_logger(RUN_LOG)
    t0 = time.time()
    log("=" * 78)
    log(f"=== START {Path(__file__).name}")
    con = connect_locked()
    log(f"connected to {PUBLICATION_DB}")
    run_ts = utc_ts()
    decision: dict = {
        "script": SCRIPT_NUM, "run_ts": run_ts, "run_date": RUN_DATE,
        "phases": {},
    }

    try:
        log("MEASURE  CPM dims, hash, FEM dtype, ras_v7 status")
        cpm_rows, cpm_cols = cpm_dim(con)
        ras_v7_present = has_column(con, "canonical_patient_master", "ras_positive_v7")
        rid_dtype = get_dtype(con, "fna_episode_master_v2", "research_id")
        fem_rows = int(con.execute(f"SELECT COUNT(*) FROM {FEM}").fetchone()[0])
        bad_dates = measure_bad_dates(con)
        b34_total, b34_linked, b34_pct = measure_b34_link_pct(con)
        b34_status = ("PASS" if b34_pct >= 85.0
                      else ("pending_decision" if b34_total > 0 and b34_linked == 0
                            else "FAIL"))
        log(f"  CPM rows={cpm_rows} cols={cpm_cols}")
        log(f"  ras_v7 present={ras_v7_present}; FEM.research_id={rid_dtype}; FEM rows={fem_rows}")
        log(f"  FEM bad dates={bad_dates}")
        log(f"  B III/IV linkage: {b34_linked}/{b34_total} ({b34_pct}%) "
            f"status={b34_status}")

        log("HASH  computing CPM hash-of-row-hashes")
        h = cpm_row_hash_of_hashes(con)
        log(f"  v1_1 candidate lock hash: {h}")

        log("REPLAY  Script 259 invariants")
        replay = replay_259(con)
        for k, v in replay.items():
            log(f"  {k:12s} {v}")

        conv = conventions_status(con)
        log(f"  conventions bethesda_semantics: {conv}")

        # ---- script status summary ----
        s260_status = "deferred (decision_pending; see scripts/output/260_decision_pending.md)"
        s261_status = "applied (24 dates repaired, 2 nulled)"
        s262_status = "applied (CPM cols 1494->1493; FEM.research_id INTEGER->VARCHAR)"
        s263_status = ("applied" if conv == "present"
                       else "deferred (decision_pending; see scripts/output/263_semantic_decision_pending.md)")

        payload = {
            "s260_status": s260_status, "s261_status": s261_status,
            "s262_status": s262_status, "s263_status": s263_status,
            "evidence": {
                "cpm_rows": cpm_rows, "cpm_cols": cpm_cols,
                "ras_v7_present": ras_v7_present,
                "fem_rid_dtype": rid_dtype, "fem_rows": fem_rows,
                "bad_dates": bad_dates,
                "b34_total": b34_total, "b34_linked": b34_linked,
                "b34_pct": b34_pct, "b34_status": b34_status,
            },
            "replay_259": replay,
            "cpm_hash": h,
            "conventions_status": conv,
            "260_decision": ("Strict-90d coverage 29.69% < 85% floor (MTE date "
                             "sparsity); see scripts/output/260_decision_pending.md "
                             "for Path A/B/C." if not ras_v7_present and b34_pct < 85.0
                             else None),
        }
        decision["phases"]["payload"] = payload

        # ---- assertions for what MUST hold ----
        hard_failures = []
        if cpm_rows != EXPECTED_CPM_ROWS:
            hard_failures.append(f"CPM rows {cpm_rows} != {EXPECTED_CPM_ROWS}")
        if cpm_cols != 1493:
            hard_failures.append(f"CPM cols {cpm_cols} != 1493")
        if ras_v7_present:
            hard_failures.append("ras_positive_v7 still present on CPM")
        if (rid_dtype or "").upper() != "VARCHAR":
            hard_failures.append(f"FEM.research_id dtype {rid_dtype} != VARCHAR")
        if fem_rows != EXPECTED_FEM_ROWS:
            hard_failures.append(f"FEM rows {fem_rows} != {EXPECTED_FEM_ROWS}")
        if bad_dates != 0:
            hard_failures.append(f"FEM bad dates {bad_dates} != 0")
        for k, v in replay.items():
            if v != 0:
                hard_failures.append(f"259 replay {k} = {v} (expected 0)")
        if hard_failures:
            log("HARD FAILURES:")
            for f_ in hard_failures:
                log(f"  {f_}")
            raise RuntimeError(f"264 hard invariants FAILED: {hard_failures}")
        log("HARD INVARIANTS PASS (260, 263 deferred to Logan as documented)")

        log("APPEND  v1_1 Closeout Addendum to FINALIZATION_REPORT_v1_1.md")
        append_addendum(REPORT_PATH, payload, log)
        decision["phases"]["report_appended"] = True
        decision["phases"]["cpm_hash"] = h

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
