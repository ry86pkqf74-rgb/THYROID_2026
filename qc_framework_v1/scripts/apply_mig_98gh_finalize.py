#!/usr/bin/env python3
"""mig_98g+h — Finalize hypocalcemia_clinical and mortality (small cohorts).

98g hypocalcemia_clinical: 9 pts, all 0-13d post-op with phenotype-confirmed-
treat-required OR lab-confirmed Ca nadir. All KEEP_OPERATIVE. Standardize
evidence_strength: phenotype rows -> probable (treat_req=TRUE); lab rows
remain definitive.

98h mortality: 1 pt (rid 8254), timing 4 days post-op, registry_match.
Already definitive. KEEP_OPERATIVE.

Phases:
  1. Snapshot
  2. UPDATE evidence_strength on 98g phenotype rows -> probable
  3. Provenance rows for both 98g and 98h
  4. Rebuild canonical_complications_patient_rollup_v1

Author: Logan Glosser <logan.glosser@gmail.com>
"""
from __future__ import annotations

import argparse
import importlib.util
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

PUB_DB = "thyroid_canonical_publication_v1_0"
EVENTS = "main.canonical_complications_events_v1"
ARCHIVE_DB = '"Thyroid 2026 UPdated".archive_pub_v1_0'


def _connect_md():
    from motherduck_client import MotherDuckClient, MotherDuckConfig

    cfg = MotherDuckConfig(database=PUB_DB)
    con = MotherDuckClient(cfg).connect_rw()
    con.execute(f"USE {PUB_DB}")
    return con


def _load_364():
    path = REPO_ROOT / "scripts" / "364_complications_consolidation.py"
    spec = importlib.util.spec_from_file_location("complications_364", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")
    parser.add_argument("--skip-rollup", action="store_true")
    args = parser.parse_args()

    do_writes = bool(args.apply)
    ts_tag = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    print(f"mig_98g+h finalize — do_writes={do_writes}  ts={ts_tag}")
    con = _connect_md()
    try:
        # Pre-counts
        for ct in ("hypocalcemia_clinical", "mortality"):
            r = con.execute(
                f"SELECT COUNT(*), COUNT(DISTINCT research_id) FROM {EVENTS} "
                f"WHERE complication_type='{ct}' AND finding_status='present'"
            ).fetchone()
            print(f"  {ct}: {r[0]} rows / {r[1]} pts present")

        # Snapshot
        snap = (
            f'{ARCHIVE_DB}.canonical_complications_events_v1_pre98gh_'
            f'hypoCa_mortality_{ts_tag}'
        )
        if do_writes:
            con.execute(
                f"CREATE TABLE {snap} AS SELECT * FROM {EVENTS} "
                f"WHERE complication_type IN ('hypocalcemia_clinical','mortality')"
            )
            n = con.execute(f"SELECT COUNT(*) FROM {snap}").fetchone()[0]
            print(f"  snapshot {snap}: {n} rows")
        else:
            print(f"  [dry-run] would snapshot")

        # 98g: standardize phenotype rows for hypocalcemia_clinical to 'probable'
        # (treat_req=TRUE on all phen rows, but evidence_strength was 'possible').
        # Lab rows (canonical_labs_calcium_v1) already 'definitive'.
        upd_q = (
            f"UPDATE {EVENTS} "
            f"SET evidence_strength='probable', "
            f"build_ts=CAST(CURRENT_TIMESTAMP AS TIMESTAMP) "
            f"WHERE complication_type='hypocalcemia_clinical' "
            f"AND finding_status='present' "
            f"AND source_table='complication_phenotype_v1' "
            f"AND evidence_strength != 'probable'"
        )
        pre = con.execute(
            f"SELECT COUNT(*) FROM {EVENTS} "
            f"WHERE complication_type='hypocalcemia_clinical' "
            f"AND finding_status='present' "
            f"AND source_table='complication_phenotype_v1' "
            f"AND evidence_strength != 'probable'"
        ).fetchone()[0]
        if do_writes:
            con.execute(upd_q)
            print(f"  98g standardize phenotype evidence_strength -> probable: {pre} rows")
        else:
            print(f"  [dry-run] would standardize 98g phenotype rows: {pre}")

        # 98h: mortality already definitive; no update needed
        print("  98h mortality: already definitive (registry match) — no update")

        # Rebuild rollup
        if not args.skip_rollup and do_writes:
            mod = _load_364()
            print("  Script 364 step_5_build_rollup running...")
            mod.step_5_build_rollup(con, True)
            print("  Script 364 step_5_build_rollup OK")
        elif args.skip_rollup:
            print("  SKIP rollup")
        else:
            print("  [dry-run] would run rollup")

        # Provenance
        for sub_id, label in (("98g", "hypocalcemia_clinical"),
                              ("98h", "mortality")):
            run_id = f"mig{sub_id}_{label}_{datetime.now(timezone.utc).strftime('%Y%m%d')}"
            if do_writes:
                con.execute(
                    "DELETE FROM manuscript_workspace.cpm_reconciliation_provenance_v1 WHERE run_id = ?",
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
                        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
                        ?,
                        '0', '0', '0',
                        ?
                    )
                    """,
                    [run_id,
                     f'mig{sub_id}_{label};snapshot;evidence_standardize;rollup_step5',
                     f'small cohort all-KEEP_OPERATIVE; bulk standardize'],
                )
                print(f"  provenance — inserted {run_id}")

        # Post-counts
        for ct in ("hypocalcemia_clinical", "mortality"):
            r = con.execute(
                f"SELECT evidence_strength, COUNT(*) AS n FROM {EVENTS} "
                f"WHERE complication_type='{ct}' AND finding_status='present' "
                f"GROUP BY 1 ORDER BY 1"
            ).fetchall()
            print(f"  {ct} evidence_strength dist: {r}")

        print("mig_98g+h — complete")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    sys.exit(main())
