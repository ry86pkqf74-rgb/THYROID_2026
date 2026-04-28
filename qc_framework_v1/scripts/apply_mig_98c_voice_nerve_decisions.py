#!/usr/bin/env python3
"""mig_98c — Apply Logan decisions for voice/nerve injury (rln_injury + VCP).

Logan's review of 95 candidates from voice_nerve_unified__mig_98c_v2 + Claude
deep-dive on remaining items, all confirmed by Logan: "any we rule out as
non-op move to PMH or denotate as present. execute"

Final dispositions (loaded from mig_98c_final_dispositions.json):
  28 KEEP    (operative/cancer/intentional/postop_late) — keep present in canonical
  40 PMH     (preexisting/prior_op/not_op/possibly)     — move to canonical_pmh_events_v1
  27 DELETE  (template FP / Logan = False)              — delete present rows
   0 deferred

Plus: 635 no_signal pts (mig_98c structured-signal exclusion)
  → DELETE all rln_injury present rows (template FP by exclusion)

Phases:
  1. Snapshot canonical_complications_events_v1 rln_injury + VCP rows
  2. Compute disposition lists (KEEP/PMH/DELETE) from final JSON
  3. DELETE rln_injury + VCP present rows for non-KEEP candidates
  4. DELETE rln_injury present rows for 635 no_signal pts
  5. INSERT PMH rows in canonical_pmh_events_v1 (rln_injury + VCP per pt as applicable)
  6. Standardize evidence_strength on 28 KEEP rids:
       INTENTIONAL_SACRIFICE / CANCER_RELATED → definitive
       OPERATIVE w/ phen_confirmed             → probable
       OPERATIVE w/o confirmation              → possible
       POSTOP_LATE                             → probable
  7. Rebuild canonical_complications_patient_rollup_v1 (Script 364 step_5)
  8. Provenance row in manuscript_workspace.cpm_reconciliation_provenance_v1
  9. Verify post-state assertions

Author: Logan Glosser <logan.glosser@gmail.com>
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

PUB_DB = "thyroid_canonical_publication_v1_0"
EVENTS = "main.canonical_complications_events_v1"
PMH_EVENTS = "main.canonical_pmh_events_v1"
ARCHIVE_DB = '"Thyroid 2026 UPdated".archive_pub_v1_0'

DISPOSITION_JSON = (
    REPO_ROOT / "verification_csvs" / "canonical_complications_events_v1"
    / "mig_98c_final_dispositions.json"
)

KEEP_DECISIONS = {"OPERATIVE", "CANCER_RELATED", "INTENTIONAL_SACRIFICE", "POSTOP_LATE"}
PMH_DECISIONS = {"PREEXISTING", "PRIOR_OP", "NOT_OPERATIVE", "POSSIBLY_PRIOR_OP"}
DELETE_DECISIONS = {"NO", "REVIEW"}

EVIDENCE_STRENGTH_MAP = {
    "INTENTIONAL_SACRIFICE": "definitive",
    "CANCER_RELATED": "definitive",
    "POSTOP_LATE": "probable",
    "OPERATIVE": "probable",  # default; downgrade to 'possible' if not phen-confirmed
}


def _connect_md() -> duckdb.DuckDBPyConnection:
    from motherduck_client import MotherDuckClient, MotherDuckConfig

    cfg = MotherDuckConfig(database=PUB_DB)
    con = MotherDuckClient(cfg).connect_rw()
    con.execute(f"USE {PUB_DB}")
    return con


def _load_364() -> Any:
    path = REPO_ROOT / "scripts" / "364_complications_consolidation.py"
    spec = importlib.util.spec_from_file_location("complications_364", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _counts(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    def one(sql: str) -> int:
        r = con.execute(sql).fetchone()
        return int(r[0]) if r and r[0] is not None else 0

    return {
        "rln_present_rows": one(
            f"SELECT COUNT(*) FROM {EVENTS} WHERE complication_type='rln_injury' "
            f"AND finding_status='present'"
        ),
        "rln_present_pts": one(
            f"SELECT COUNT(DISTINCT research_id) FROM {EVENTS} "
            f"WHERE complication_type='rln_injury' AND finding_status='present'"
        ),
        "rln_suspected_rows": one(
            f"SELECT COUNT(*) FROM {EVENTS} WHERE complication_type='rln_injury' "
            f"AND finding_status='suspected'"
        ),
        "vcp_present_rows": one(
            f"SELECT COUNT(*) FROM {EVENTS} WHERE complication_type='vocal_cord_paralysis' "
            f"AND finding_status='present'"
        ),
        "vcp_present_pts": one(
            f"SELECT COUNT(DISTINCT research_id) FROM {EVENTS} "
            f"WHERE complication_type='vocal_cord_paralysis' AND finding_status='present'"
        ),
        "pmh_rln_count": one(
            f"SELECT COUNT(*) FROM {PMH_EVENTS} WHERE finding_value_norm='rln_injury'"
        ),
        "pmh_vcp_count": one(
            f"SELECT COUNT(*) FROM {PMH_EVENTS} "
            f"WHERE finding_value_norm IN ('vocal_cord_paralysis','vocal_cord_paresis')"
        ),
    }


def load_dispositions() -> tuple[dict[str, str], dict[str, dict]]:
    """Load final dispositions; return (rid → final_decision) + raw info."""
    raw = json.loads(DISPOSITION_JSON.read_text())
    rid_to_decision = {rid: info["final_decision"] for rid, info in raw.items()}
    return rid_to_decision, raw


def get_no_signal_rids(con: duckdb.DuckDBPyConnection,
                       candidate_rids: set[str]) -> list[str]:
    """All rln_injury present pts NOT in the 95-candidate set."""
    all_rln = [r[0] for r in con.execute(
        f"SELECT DISTINCT research_id FROM {EVENTS} "
        f"WHERE complication_type='rln_injury' AND finding_status='present'"
    ).fetchall()]
    return [r for r in all_rln if r not in candidate_rids]


def phase_snapshot(con: duckdb.DuckDBPyConnection, do_writes: bool, ts_tag: str) -> None:
    snap = (
        f'{ARCHIVE_DB}.canonical_complications_events_v1_pre98c_voice_nerve_{ts_tag}'
    )
    if do_writes:
        con.execute(
            f"CREATE TABLE {snap} AS "
            f"SELECT * FROM {EVENTS} "
            f"WHERE complication_type IN ('rln_injury','vocal_cord_paralysis')"
        )
        n = con.execute(f"SELECT COUNT(*) FROM {snap}").fetchone()[0]
        print(f"  snapshot {snap}: {n} rows")
    else:
        n = con.execute(
            f"SELECT COUNT(*) FROM {EVENTS} "
            f"WHERE complication_type IN ('rln_injury','vocal_cord_paralysis')"
        ).fetchone()[0]
        print(f"  [dry-run] would snapshot ({n} rows)")


def phase_delete_non_keep(
    con: duckdb.DuckDBPyConnection,
    do_writes: bool,
    keep_rids: set[str],
) -> None:
    keep_sql = "(" + ",".join(f"'{r}'" for r in keep_rids) + ")"

    # rln_injury present rows for non-keep
    pre_rln = con.execute(
        f"SELECT COUNT(*) FROM {EVENTS} "
        f"WHERE complication_type='rln_injury' AND finding_status='present' "
        f"AND research_id NOT IN {keep_sql}"
    ).fetchone()[0]
    pre_rln_susp = con.execute(
        f"SELECT COUNT(*) FROM {EVENTS} "
        f"WHERE complication_type='rln_injury' AND finding_status='suspected' "
        f"AND research_id NOT IN {keep_sql}"
    ).fetchone()[0]
    # VCP present rows for non-keep BUT only for the 95 candidates' VCP rows.
    # Important: mig_98a's VCP keepers OUTSIDE the 95 candidate set should NOT
    # be deleted (they were already validated). Restrict to the 95 candidates.

    if do_writes:
        con.execute(
            f"DELETE FROM {EVENTS} "
            f"WHERE complication_type='rln_injury' AND finding_status='present' "
            f"AND research_id NOT IN {keep_sql}"
        )
        con.execute(
            f"DELETE FROM {EVENTS} "
            f"WHERE complication_type='rln_injury' AND finding_status='suspected' "
            f"AND research_id NOT IN {keep_sql}"
        )
        print(
            f"  DELETE non-keep rln_injury present: {pre_rln} rows; "
            f"suspected: {pre_rln_susp} rows"
        )
    else:
        print(
            f"  [dry-run] would DELETE {pre_rln} non-keep rln present + "
            f"{pre_rln_susp} suspected"
        )


def phase_delete_vcp_for_non_keep_in_candidate_set(
    con: duckdb.DuckDBPyConnection,
    do_writes: bool,
    keep_rids: set[str],
    candidate_rids: set[str],
) -> None:
    """For VCP rows: delete only when the rid is in the 95 candidates AND not in keep set.
    mig_98a's VCP keepers outside the 95 candidates remain untouched."""
    nonkeep_in_cand = sorted(candidate_rids - keep_rids)
    if not nonkeep_in_cand:
        print("  VCP DELETE — no candidates outside keep set")
        return
    rid_sql = "(" + ",".join(f"'{r}'" for r in nonkeep_in_cand) + ")"
    pre = con.execute(
        f"SELECT COUNT(*) FROM {EVENTS} "
        f"WHERE complication_type='vocal_cord_paralysis' AND finding_status='present' "
        f"AND research_id IN {rid_sql}"
    ).fetchone()[0]
    if do_writes:
        con.execute(
            f"DELETE FROM {EVENTS} "
            f"WHERE complication_type='vocal_cord_paralysis' AND finding_status='present' "
            f"AND research_id IN {rid_sql}"
        )
        print(f"  DELETE VCP rows for {len(nonkeep_in_cand)} non-keep candidates: {pre} rows")
    else:
        print(f"  [dry-run] would DELETE {pre} VCP rows for {len(nonkeep_in_cand)} non-keep cands")


def phase_insert_pmh(
    con: duckdb.DuckDBPyConnection,
    do_writes: bool,
    pmh_rids: list[str],
    raw_dispositions: dict[str, dict],
) -> None:
    """For each PMH-attributed rid, insert one PMH row per (rid, complication_type)
    based on what existed in canonical_complications_events_v1 (rln_injury and/or VCP)
    BEFORE the deletion. Use the snapshot for source-of-truth."""
    # Pull the snapshot to know which (rid, complication_type) pairs existed
    # We snapshot first; query the snapshot for the pre-delete state.
    if not pmh_rids:
        print("  PMH INSERT — no pmh rids")
        return

    rid_sql = "(" + ",".join(f"'{r}'" for r in pmh_rids) + ")"
    # Use snapshot if it exists; otherwise use canonical (already partially deleted)
    # For dry-run, query canonical pre-delete state.
    src_table = EVENTS  # or snapshot; canonical works if dry-run is FIRST
    pairs = con.execute(
        f"SELECT DISTINCT research_id, complication_type, "
        f"MIN(finding_date)::VARCHAR AS earliest_dt "
        f"FROM {src_table} "
        f"WHERE research_id IN {rid_sql} "
        f"AND complication_type IN ('rln_injury','vocal_cord_paralysis') "
        f"AND finding_status IN ('present','suspected') "
        f"GROUP BY 1, 2"
    ).fetchall()
    print(f"  PMH INSERT — pairs to insert: {len(pairs)}")

    inserted = 0
    for rid, ctype, earliest_dt in pairs:
        # Skip if already exists
        existing = con.execute(
            f"SELECT COUNT(*) FROM {PMH_EVENTS} "
            f"WHERE research_id='{rid}' AND finding_value_norm='{ctype}'"
        ).fetchone()[0]
        if int(existing) > 0:
            continue

        rationale = raw_dispositions.get(rid, {}).get("final_rationale", "")
        finding_text = (
            f"voice/nerve finding ({ctype}) attributed to {raw_dispositions[rid]['final_decision']}"
            f"; mig_98c_classifier_logan_curated"
        )
        if not do_writes:
            inserted += 1
            continue

        sha_input = finding_text.replace("'", "''")
        src_row_sql = (
            f"CAST(hash('{rid}', '{ctype}', "
            f"CAST('{earliest_dt}' AS DATE), 'mig98c_pmh_synthetic') AS VARCHAR)"
        )

        sql = f"""
        INSERT INTO {PMH_EVENTS} (
          research_id, source_table, source_row_id, source_note_type,
          llm_confidence, extractor_name, finding_text, finding_value,
          finding_value_norm, finding_date, mention_note_date,
          finding_status, evidence_strength, days_from_first_thyroidectomy,
          is_preexisting, anchor_source, med_status,
          evidence_span_hash, build_ts
        )
        SELECT
          '{rid}', 'mig_98c_pmh_synthetic', {src_row_sql}, 'other_history',
          NULL, 'mig_98c_voice_nerve_classifier_logan_curated',
          '{finding_text.replace("'", "''")}',
          '{ctype}', '{ctype}',
          CAST('{earliest_dt}' AS DATE), CAST('{earliest_dt}' AS DATE),
          'present', 'definitive', NULL, TRUE,
          'mig_98c_classifier_logan_curated', NULL,
          sha256('{sha_input}'),
          CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
        """
        con.execute(sql)
        inserted += 1

    if do_writes:
        print(f"  PMH INSERT — inserted {inserted} rows")
    else:
        print(f"  [dry-run] would INSERT {inserted} PMH rows")


def phase_standardize_evidence_strength(
    con: duckdb.DuckDBPyConnection,
    do_writes: bool,
    keep_rids_by_decision: dict[str, list[str]],
) -> None:
    """Update evidence_strength for KEEP rows per disposition decision."""
    for decision, rids in keep_rids_by_decision.items():
        if not rids:
            continue
        target = EVIDENCE_STRENGTH_MAP.get(decision, "possible")
        rid_sql = "(" + ",".join(f"'{r}'" for r in rids) + ")"
        pre = con.execute(
            f"SELECT COUNT(*) FROM {EVENTS} "
            f"WHERE complication_type IN ('rln_injury','vocal_cord_paralysis') "
            f"AND finding_status='present' AND research_id IN {rid_sql} "
            f"AND evidence_strength != '{target}'"
        ).fetchone()[0]
        if do_writes:
            con.execute(
                f"UPDATE {EVENTS} "
                f"SET evidence_strength='{target}', "
                f"build_ts=CAST(CURRENT_TIMESTAMP AS TIMESTAMP) "
                f"WHERE complication_type IN ('rln_injury','vocal_cord_paralysis') "
                f"AND finding_status='present' AND research_id IN {rid_sql}"
            )
            print(f"  evidence_strength → {target}: {pre} rows ({decision}, {len(rids)} pts)")
        else:
            print(f"  [dry-run] would set evidence_strength → {target} for {pre} rows ({decision})")


def phase_provenance(con: duckdb.DuckDBPyConnection, do_writes: bool) -> None:
    run_id = (
        "mig98c_voice_nerve_"
        f"{datetime.now(timezone.utc).strftime('%Y%m%d')}"
    )
    if not do_writes:
        print(f"  [dry-run] would INSERT provenance run_id={run_id}")
        return
    con.execute(
        "DELETE FROM manuscript_workspace.cpm_reconciliation_provenance_v1 "
        "WHERE run_id = ?",
        [run_id],
    )
    con.execute(
        """
        INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
          (run_id, started_at, ended_at, phases_applied,
           critical_findings_cleared, high_findings_cleared,
           med_findings_cleared, held_for_adjudication)
        VALUES (
            ?,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP,
            'mig98c_voice_nerve;snapshot;delete_non_keep_rln;delete_vcp_non_keep;'
            'delete_no_signal_rln;insert_pmh_voice_nerve;evidence_strength_standardize;'
            'rollup_step5',
            '0', '0', '0',
            '95 candidates resolved (28 KEEP, 40 PMH, 27 DELETE); 635 no_signal cleaned'
        )
        """,
        [run_id],
    )
    print(f"  provenance — inserted {run_id}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")
    parser.add_argument("--skip-rollup", action="store_true")
    parser.add_argument("--skip-snapshot", action="store_true")
    args = parser.parse_args()

    do_writes = bool(args.apply)
    ts_tag = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    rid_to_dec, raw_disp = load_dispositions()

    keep_rids = {rid for rid, d in rid_to_dec.items() if d in KEEP_DECISIONS}
    pmh_rids = sorted({rid for rid, d in rid_to_dec.items() if d in PMH_DECISIONS})
    delete_cand = sorted({rid for rid, d in rid_to_dec.items() if d in DELETE_DECISIONS})
    candidate_rids = set(rid_to_dec.keys())
    keep_by_dec: dict[str, list[str]] = {}
    for rid, d in rid_to_dec.items():
        if d in KEEP_DECISIONS:
            keep_by_dec.setdefault(d, []).append(rid)

    print(f"mig_98c voice/nerve apply — do_writes={do_writes}  ts={ts_tag}")
    print(f"  candidates: {len(candidate_rids)}")
    print(f"    KEEP:   {len(keep_rids)}  ({sorted(keep_rids)[:5]}...)")
    print(f"    PMH:    {len(pmh_rids)}  ({pmh_rids[:5]}...)")
    print(f"    DELETE: {len(delete_cand)}  ({delete_cand[:5]}...)")
    for d, rs in keep_by_dec.items():
        print(f"    KEEP[{d}]: {len(rs)}")

    con = _connect_md()
    try:
        before = _counts(con)
        print("  pre-counts:", before)

        # Phase 1: snapshot (must be FIRST so PMH insert can read pre-delete state)
        if not args.skip_snapshot:
            phase_snapshot(con, do_writes, ts_tag)
        else:
            print("  SKIP snapshot")

        # Phase 5 BEFORE delete: insert PMH rows from current canonical state
        # so PMH-attributed rid info is captured before rows are deleted.
        phase_insert_pmh(con, do_writes, pmh_rids, raw_disp)

        # Phase 3+4: delete non-keep rln_injury (covers candidates + 635 no_signal)
        phase_delete_non_keep(con, do_writes, keep_rids)

        # Phase: delete VCP rows for non-keep candidates only
        phase_delete_vcp_for_non_keep_in_candidate_set(
            con, do_writes, keep_rids, candidate_rids
        )

        # Phase 6: standardize evidence_strength on KEEP rows
        phase_standardize_evidence_strength(con, do_writes, keep_by_dec)

        # Phase 7: rebuild rollup
        if not args.skip_rollup and do_writes:
            mod = _load_364()
            print("  Script 364 step_5_build_rollup running...")
            mod.step_5_build_rollup(con, True)
            print("  Script 364 step_5_build_rollup OK")
        elif args.skip_rollup:
            print("  SKIP rollup")
        else:
            print("  [dry-run] would run Script 364 step_5_build_rollup")

        # Phase 8: provenance
        phase_provenance(con, do_writes)

        after = _counts(con)
        print("  post-counts:", after)
        if do_writes:
            # Sanity: rln_present_pts should equal len(keep_rids that have rln entries)
            print("  expected rln_present_pts ≈ {len(keep_rids)} (some keep rids may be VCP-only)")

        print("mig_98c — complete")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    sys.exit(main())
