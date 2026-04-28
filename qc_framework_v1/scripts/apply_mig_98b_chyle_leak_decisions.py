#!/usr/bin/env python3
"""mig_98b — Apply Logan decisions for chyle_leak in canonical_complications_events_v1.

Logan's decisions on the 7 REAL candidates from the note-text classifier
(see chyle_leak_real_candidates_v2__mig_98b.xlsx):

    rid     decision
    8597    YES (gold)
    8333    YES
    10949   YES
    8815    YES
    11011   YES
    5673    PMH (move to canonical_pmh_events_v1; delete from complications)
    10376   NO (delete; consent-list false positive)

Net effect:
  - 5 patients keep `present` chyle_leak in canonical_complications_events_v1
  - 1 patient (5673) gets chyle_leak history row in canonical_pmh_events_v1
  - 1,570 patients flip to absent (DELETE present rows; absent rollup retained)

Phases:
  1. Snapshot chyle_leak rows to archive_pub_v1_0
  2. DELETE chyle_leak `present` rows for all rids NOT IN keep+pmh set
  3. DELETE chyle_leak rows for rid 5673 (move to PMH)
  4. INSERT PMH chyle_leak row for rid 5673 in canonical_pmh_events_v1
  5. Standardize evidence_strength on the 5 kept rids
  6. Rebuild canonical_complications_patient_rollup_v1 (Script 364 step_5)
  7. Write provenance row to manuscript_workspace.cpm_reconciliation_provenance_v1
  8. Verify post-state assertions

Usage:
    python3 apply_mig_98b_chyle_leak_decisions.py --dry-run
    python3 apply_mig_98b_chyle_leak_decisions.py --apply
    python3 apply_mig_98b_chyle_leak_decisions.py --apply --skip-rollup

Author: Logan Glosser <logan.glosser@gmail.com>
"""
from __future__ import annotations

import argparse
import importlib.util
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

KEEP_PRESENT = ("8597", "8333", "10949", "8815", "11011")
PMH_RID = "5673"
KEEP_OR_PMH = KEEP_PRESENT + (PMH_RID,)

# evidence_strength upgrades per Logan's clinical adjudication
DEFINITIVE_RIDS = ("8597",)            # gold-standard JP-bulb / pressure dressing / dietary advance
PROBABLE_RIDS = ("8333", "10949")      # clinical sequelae documented (clear liquid x ~30d, hospitalization + 4w low-fat)
# 8815, 11011 stay 'possible' (borderline / intra-op only)

# PMH synthetic insert for rid 5673 — chyle leak history from prior MRND in 3/19
PMH_SYNTHETIC = {
    "research_id": PMH_RID,
    "source_table": "mig_98b_pmh_synthetic",
    "source_note_type": "other_history",
    "extractor_name": "mig_98b_chyle_leak_classifier_logan_curated",
    "finding_text": "Left modified radical neck dissection in 3/19, complicated by Chyle leak",
    "finding_value": "chyle_leak",
    "finding_value_norm": "chyle_leak",
    "finding_date_iso": "2019-03-15",  # approx midpoint of 3/19; finding_date_inferred via PMH note
    "mention_note_date_iso": "2024-08-21",  # date of OTHER_HISTORY note that mentioned it
    "finding_status": "present",
    "evidence_strength": "definitive",
    "is_preexisting": True,
    "anchor_source": "mig_98b_classifier_logan_curated",
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

    keep_sql = "(" + ",".join(f"'{r}'" for r in KEEP_PRESENT) + ")"
    return {
        "present_rows": one(
            f"SELECT COUNT(*) FROM {EVENTS} "
            f"WHERE complication_type='chyle_leak' AND finding_status='present'"
        ),
        "present_pts": one(
            f"SELECT COUNT(DISTINCT research_id) FROM {EVENTS} "
            f"WHERE complication_type='chyle_leak' AND finding_status='present'"
        ),
        "present_rows_for_keep": one(
            f"SELECT COUNT(*) FROM {EVENTS} "
            f"WHERE complication_type='chyle_leak' AND finding_status='present' "
            f"AND research_id IN {keep_sql}"
        ),
        "absent_rows": one(
            f"SELECT COUNT(*) FROM {EVENTS} "
            f"WHERE complication_type='chyle_leak' AND finding_status='absent'"
        ),
        "rid_5673_total": one(
            f"SELECT COUNT(*) FROM {EVENTS} "
            f"WHERE complication_type='chyle_leak' AND research_id='{PMH_RID}'"
        ),
        "rid_5673_pmh_chyle": one(
            f"SELECT COUNT(*) FROM {PMH_EVENTS} "
            f"WHERE research_id='{PMH_RID}' AND finding_value_norm='chyle_leak'"
        ),
    }


def phase_snapshot(con: duckdb.DuckDBPyConnection, do_writes: bool, ts_tag: str) -> None:
    snap = (
        f'{ARCHIVE_DB}.canonical_complications_events_v1_pre98b_chyle_{ts_tag}'
    )
    if do_writes:
        con.execute(
            f"CREATE TABLE {snap} AS "
            f"SELECT * FROM {EVENTS} WHERE complication_type='chyle_leak'"
        )
        n = con.execute(f"SELECT COUNT(*) FROM {snap}").fetchone()[0]
        print(f"  snapshot {snap}: {n} rows")
    else:
        n = con.execute(
            f"SELECT COUNT(*) FROM {EVENTS} WHERE complication_type='chyle_leak'"
        ).fetchone()[0]
        print(f"  [dry-run] would snapshot {snap} ({n} rows)")


def phase_delete_non_keep(con: duckdb.DuckDBPyConnection, do_writes: bool) -> int:
    keep_sql = "(" + ",".join(f"'{r}'" for r in KEEP_OR_PMH) + ")"
    pre = con.execute(
        f"SELECT COUNT(*) FROM {EVENTS} "
        f"WHERE complication_type='chyle_leak' AND finding_status='present' "
        f"AND research_id NOT IN {keep_sql}"
    ).fetchone()[0]
    if do_writes:
        con.execute(
            f"DELETE FROM {EVENTS} "
            f"WHERE complication_type='chyle_leak' AND finding_status='present' "
            f"AND research_id NOT IN {keep_sql}"
        )
        print(f"  DELETE non-keep present chyle_leak rows: {pre} rows removed")
    else:
        print(f"  [dry-run] would DELETE {pre} non-keep present chyle_leak rows")
    return int(pre)


def phase_delete_5673(con: duckdb.DuckDBPyConnection, do_writes: bool) -> int:
    pre = con.execute(
        f"SELECT COUNT(*) FROM {EVENTS} "
        f"WHERE complication_type='chyle_leak' AND research_id='{PMH_RID}'"
    ).fetchone()[0]
    if do_writes:
        con.execute(
            f"DELETE FROM {EVENTS} "
            f"WHERE complication_type='chyle_leak' AND research_id='{PMH_RID}'"
        )
        print(f"  DELETE rid {PMH_RID} chyle_leak (move to PMH): {pre} rows removed")
    else:
        print(
            f"  [dry-run] would DELETE rid {PMH_RID} chyle_leak: {pre} rows"
        )
    return int(pre)


def phase_insert_pmh(con: duckdb.DuckDBPyConnection, do_writes: bool) -> None:
    existing = con.execute(
        f"SELECT COUNT(*) FROM {PMH_EVENTS} "
        f"WHERE research_id='{PMH_RID}' AND finding_value_norm='chyle_leak'"
    ).fetchone()[0]
    if int(existing) > 0:
        print(f"  PMH INSERT — skip (already {existing} chyle PMH row(s) for {PMH_RID})")
        return

    if not do_writes:
        print(f"  [dry-run] would INSERT chyle_leak PMH row for {PMH_RID}")
        return

    p = PMH_SYNTHETIC
    src_row_id_sql = (
        f"CAST(hash('{p['research_id']}', 'chyle_leak', "
        f"CAST('{p['finding_date_iso']}' AS DATE)) AS VARCHAR)"
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
        '{p['research_id']}',
        '{p['source_table']}',
        {src_row_id_sql},
        '{p['source_note_type']}',
        NULL,
        '{p['extractor_name']}',
        '{p['finding_text'].replace("'", "''")}',
        '{p['finding_value']}',
        '{p['finding_value_norm']}',
        CAST('{p['finding_date_iso']}' AS DATE),
        CAST('{p['mention_note_date_iso']}' AS DATE),
        '{p['finding_status']}',
        '{p['evidence_strength']}',
        NULL,
        TRUE,
        '{p['anchor_source']}',
        NULL,
        sha256('{p['finding_text'].replace("'", "''")}'),
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
    """
    con.execute(sql)
    print(f"  PMH INSERT — rid {PMH_RID} chyle_leak (synthetic, is_preexisting=TRUE)")


def phase_standardize_evidence_strength(
    con: duckdb.DuckDBPyConnection, do_writes: bool
) -> None:
    def_sql = "(" + ",".join(f"'{r}'" for r in DEFINITIVE_RIDS) + ")"
    prob_sql = "(" + ",".join(f"'{r}'" for r in PROBABLE_RIDS) + ")"

    pre_def = con.execute(
        f"SELECT COUNT(*) FROM {EVENTS} "
        f"WHERE complication_type='chyle_leak' AND finding_status='present' "
        f"AND research_id IN {def_sql} AND evidence_strength != 'definitive'"
    ).fetchone()[0]
    pre_prob = con.execute(
        f"SELECT COUNT(*) FROM {EVENTS} "
        f"WHERE complication_type='chyle_leak' AND finding_status='present' "
        f"AND research_id IN {prob_sql} AND evidence_strength != 'probable'"
    ).fetchone()[0]

    if do_writes:
        con.execute(
            f"UPDATE {EVENTS} "
            f"SET evidence_strength='definitive', build_ts=CAST(CURRENT_TIMESTAMP AS TIMESTAMP) "
            f"WHERE complication_type='chyle_leak' AND finding_status='present' "
            f"AND research_id IN {def_sql}"
        )
        con.execute(
            f"UPDATE {EVENTS} "
            f"SET evidence_strength='probable', build_ts=CAST(CURRENT_TIMESTAMP AS TIMESTAMP) "
            f"WHERE complication_type='chyle_leak' AND finding_status='present' "
            f"AND research_id IN {prob_sql}"
        )
        print(
            f"  evidence_strength upgrades — definitive: {pre_def} rows  probable: {pre_prob} rows"
        )
    else:
        print(
            f"  [dry-run] would upgrade evidence_strength — "
            f"definitive: {pre_def} rows, probable: {pre_prob} rows"
        )


def phase_provenance(con: duckdb.DuckDBPyConnection, do_writes: bool) -> None:
    run_id = (
        "mig98b_chyle_leak_"
        f"{datetime.now(timezone.utc).strftime('%Y%m%d')}"
    )
    if not do_writes:
        print(f"  [dry-run] would INSERT provenance run_id={run_id}")
        return
    con.execute(
        """
        DELETE FROM manuscript_workspace.cpm_reconciliation_provenance_v1
        WHERE run_id = ?
        """,
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
            'mig98b_chyle_leak;snapshot;delete_non_keep_present;delete_5673;'
            'insert_pmh_5673;evidence_strength_standardize;rollup_step5',
            '0', '0', '0',
            'all 7 candidates adjudicated; 5 keep, 1 PMH, 1 FP'
        )
        """,
        [run_id],
    )
    print(f"  provenance — inserted {run_id}")


def phase_verify_post_state(con: duckdb.DuckDBPyConnection) -> None:
    counts = _counts(con)
    print("  post-counts:", counts)
    assertions = [
        ("present_pts", 5),
        ("present_rows_for_keep", counts["present_rows"]),  # all present rows are keep rows
        ("rid_5673_total", 0),
        ("rid_5673_pmh_chyle", 1),
    ]
    for k, expected in assertions:
        actual = counts.get(k)
        if actual != expected:
            raise SystemExit(
                f"POST-STATE ASSERTION FAILED: {k} expected {expected}, got {actual}"
            )
    print("  post-state assertions OK")


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

    print(f"mig_98b chyle_leak apply — do_writes={do_writes}  ts={ts_tag}")
    con = _connect_md()
    try:
        before = _counts(con)
        print("  pre-counts:", before)
        if before["present_pts"] not in (1576, 5):
            print(
                f"  WARNING: expected 1576 (fresh) or 5 (already applied) present pts; "
                f"got {before['present_pts']}"
            )

        if not args.skip_snapshot:
            phase_snapshot(con, do_writes, ts_tag)
        else:
            print("  SKIP snapshot (--skip-snapshot)")
        phase_delete_non_keep(con, do_writes)
        phase_delete_5673(con, do_writes)
        phase_insert_pmh(con, do_writes)
        phase_standardize_evidence_strength(con, do_writes)

        if not args.skip_rollup and do_writes:
            mod = _load_364()
            print("  Script 364 step_5_build_rollup running...")
            mod.step_5_build_rollup(con, True)
            print("  Script 364 step_5_build_rollup OK")
        elif args.skip_rollup:
            print("  SKIP rollup (--skip-rollup)")
        else:
            print("  [dry-run] would run Script 364 step_5_build_rollup")

        phase_provenance(con, do_writes)

        if do_writes:
            phase_verify_post_state(con)
        else:
            print("  [dry-run] post-state verification skipped")

        print("mig_98b — complete")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    sys.exit(main())
