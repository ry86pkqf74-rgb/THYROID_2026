#!/usr/bin/env python3
"""mig_103 — Apply REAL/TEMPLATE classifier dispositions to canonical_medications_events_v1.

Reads verification_csvs/.../mig_103_decisions.json from build_medications_review.py.

Phases:
  1. Fail if meta.summary_counts.REVIEW > 50 (Logan adjudication required)
  2. Archive snapshot of meds table (optional --skip-snapshot)
  3. INSERT PMH rows (disposition=PMH) into canonical_pmh_events_v1
  4. DELETE meds rows where disposition in (DELETE, PMH)
  5. Rebuild canonical_medications_patient_rollup_v1 (Script 365 step 2, meds only)
  6. UPDATE verification + table signoff registries (inline SQL)
  7. INSERT manuscript_workspace.cpm_reconciliation_provenance_v1

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
MEDS = "main.canonical_medications_events_v1"
PMH = "main.canonical_pmh_events_v1"
ARCHIVE_DB = '"Thyroid 2026 UPdated".archive_pub_v1_0'
DECISION_JSON = (
    REPO_ROOT / "verification_csvs" / "canonical_medications_events_v1"
    / "mig_103_decisions.json"
)


def _connect_md() -> duckdb.DuckDBPyConnection:
    from motherduck_client import MotherDuckClient, MotherDuckConfig

    cfg = MotherDuckConfig(database=PUB_DB)
    con = MotherDuckClient(cfg).connect_rw()
    con.execute(f"USE {PUB_DB}")
    return con


def _load_365():
    path = REPO_ROOT / "scripts" / "365_psh_pmh_meds_consolidation.py"
    spec = importlib.util.spec_from_file_location("s365", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _sql_escape(s: str) -> str:
    return s.replace("'", "''")


def _parse_key(k: str) -> tuple[str, str]:
    rid, _, sri = k.partition("|")
    return rid, sri


def _load_decisions(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload["meta"], payload["decisions"]


def phase_registry_signoff(con: duckdb.DuckDBPyConnection, do_writes: bool) -> None:
    sql_cols = """
    UPDATE main.canonical_column_verification_registry_v1
    SET verification_status = 'verified',
        verified_by = 'logan',
        verification_method = 'note_text_real_template_classifier_mig103',
        batch_id = 'mig_103_medications_signoff_20260428',
        verified_ts = CURRENT_TIMESTAMP,
        notes = COALESCE(notes,'')
          || ' | mig_103: note-text REAL/TEMPLATE classifier on clinical_notes_long; '
          || 'bulk disposition Logan-ratified (mig_98 family pattern).'
    WHERE schema_name='main'
      AND table_name='canonical_medications_events_v1'
      AND verification_status = 'not_started'
    """
    sql_table = """
    UPDATE main.canonical_table_signoff_registry_v1 ts
    SET n_columns_total = subq.n_total,
        n_verified      = subq.n_verified,
        n_not_started   = subq.n_not_started,
        n_failed        = COALESCE(subq.n_failed, 0),
        n_na            = subq.n_na,
        table_status    = CASE
          WHEN subq.n_not_started + COALESCE(subq.n_failed,0) = 0 THEN 'verified'
          WHEN subq.n_verified > 0 THEN 'in_progress'
          ELSE 'not_started'
        END,
        signed_off_ts     = CURRENT_TIMESTAMP,
        signoff_migration = 'qc_framework_v1/migrations/103_medications_events_table_signoff.sql',
        notes             = 'mig_103: Protocol v2 note-text classifier verification; '
                          || 'pre-surgery supplement moves to PMH; template/negation/absent DELETE.'
    FROM (
      SELECT schema_name, table_name,
             COUNT(*) AS n_total,
             SUM(CASE WHEN verification_status='verified' THEN 1 ELSE 0 END) AS n_verified,
             SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
             SUM(CASE WHEN verification_status='failed' THEN 1 ELSE 0 END) AS n_failed,
             SUM(CASE WHEN verification_status='na' THEN 1 ELSE 0 END) AS n_na
      FROM main.canonical_column_verification_registry_v1
      WHERE schema_name='main' AND table_name='canonical_medications_events_v1'
      GROUP BY 1,2
    ) subq
    WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name
    """
    if do_writes:
        con.execute(sql_cols)
        con.execute(sql_table)
        print("  registry sign-off OK")
    else:
        print("  [dry-run] would UPDATE column + table signoff registries")


def phase_provenance(con: duckdb.DuckDBPyConnection, do_writes: bool,
                     summary: str) -> None:
    run_id = f"mig103_medications_{datetime.now(timezone.utc).strftime('%Y%m%d')}"
    if not do_writes:
        print(f"  [dry-run] provenance run_id={run_id}")
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
        VALUES (?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?, '0', '0', '0', ?)
        """,
        [
            run_id,
            "mig103_meds;snapshot;pmh_insert;delete;rollup;registry",
            summary[:2000],
        ],
    )
    print(f"  provenance {run_id}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")
    parser.add_argument("--decisions", type=Path, default=DECISION_JSON)
    parser.add_argument("--skip-snapshot", action="store_true")
    parser.add_argument("--skip-rollup", action="store_true")
    parser.add_argument("--skip-registry", action="store_true")
    args = parser.parse_args()

    do_writes = bool(args.apply)
    meta, decisions = _load_decisions(args.decisions)
    rev = int(meta.get("summary_counts", {}).get("REVIEW", 0))
    if rev > 50:
        print(f"STOP: REVIEW={rev} > 50 — await Logan adjudication (per prompt §6)")
        return 2

    ts_tag = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    delete_keys = [k for k, v in decisions.items()
                   if v["disposition"] == "DELETE"]
    pmh_keys = [k for k, v in decisions.items() if v["disposition"] == "PMH"]
    drop_keys = delete_keys + pmh_keys

    print(
        f"mig_103 apply  writes={do_writes}  DELETE={len(delete_keys)}  "
        f"PMH={len(pmh_keys)}  ts={ts_tag}"
    )

    con = _connect_md()
    try:
        if not args.skip_snapshot:
            snap = f"{ARCHIVE_DB}.canonical_medications_events_v1_pre_mig103_{ts_tag}"
            if do_writes:
                con.execute(f"CREATE TABLE {snap} AS SELECT * FROM {MEDS}")
                n = con.execute(f"SELECT COUNT(*) FROM {snap}").fetchone()[0]
                print(f"  snapshot {snap}: {n}")
            else:
                n = con.execute(f"SELECT COUNT(*) FROM {MEDS}").fetchone()[0]
                print(f"  [dry-run] would snapshot {n} rows → {snap}")

        # PMH inserts — pull full rows from meds
        if pmh_keys:
            for key in pmh_keys:
                rid, sri = _parse_key(key)
                res = con.execute(
                    f"SELECT * FROM {MEDS} WHERE research_id = ? AND source_row_id = ?",
                    [rid, sri],
                )
                desc = [d[0] for d in res.description]
                rows = res.fetchall()
                if not rows:
                    print(f"  WARN PMH row missing {rid} {sri[:20]}…")
                    continue
                rec = dict(zip(desc, rows[0]))
                basis = decisions[key].get("basis", "")
                ftext = (
                    f"{rec.get('finding_value_norm','')} — PMH move ({basis}); "
                    f"mig_103_classifier_logan_curated"
                )
                ft_esc = _sql_escape(str(ftext))
                fv_esc = _sql_escape(str(rec.get("finding_value") or ""))
                fvn = _sql_escape(str(rec.get("finding_value_norm") or ""))
                snt = _sql_escape(str(rec.get("source_note_type") or ""))
                fst = _sql_escape(str(rec.get("finding_status") or "present"))
                evs = _sql_escape(str(rec.get("evidence_strength") or "definitive"))
                anch = "mig_103_classifier_logan_curated"
                med_st = rec.get("med_status")
                med_sql = "NULL" if med_st is None else f"'{_sql_escape(str(med_st))}'"
                fd = rec.get("finding_date")
                fd_sql = "NULL" if fd is None else f"DATE '{fd}'"
                mnd = rec.get("mention_note_date")
                mnd_sql = "NULL" if mnd is None else f"DATE '{mnd}'"
                dfs = rec.get("days_from_first_thyroidectomy")
                dfs_sql = "NULL" if dfs is None else str(int(dfs))

                src_row_sql = (
                    f"CAST(hash('{_sql_escape(rid)}', '{_sql_escape(sri)}', "
                    f"'mig103_pmh') AS VARCHAR)"
                )
                llm = rec.get("llm_confidence")
                llm_sql = "NULL" if llm is None else str(float(llm))
                if do_writes:
                    con.execute(
                        f"""
                        INSERT INTO {PMH} (
                          research_id, source_table, source_row_id, source_note_type,
                          llm_confidence, extractor_name, finding_text, finding_value,
                          finding_value_norm, finding_date, mention_note_date,
                          finding_status, evidence_strength,
                          days_from_first_thyroidectomy, is_preexisting, anchor_source,
                          med_status, evidence_span_hash, build_ts
                        )
                        SELECT
                          '{_sql_escape(rid)}',
                          'mig_103_pmh_synthetic',
                          {src_row_sql},
                          '{snt}',
                          {llm_sql},
                          'mig_103_medications_classifier_logan_curated',
                          '{ft_esc}',
                          '{fv_esc}',
                          '{fvn}',
                          {fd_sql},
                          {mnd_sql},
                          '{fst}',
                          '{evs}',
                          {dfs_sql},
                          TRUE,
                          '{anch}',
                          {med_sql},
                          sha256('{ft_esc}'),
                          CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
                        """
                    )
            print(f"  PMH INSERT attempted for {len(pmh_keys)} keys")

        # DELETE meds (DELETE + PMH dispositions)
        if drop_keys:
            chunk = 400
            for i in range(0, len(drop_keys), chunk):
                part = drop_keys[i : i + chunk]
                or_parts = []
                for k in part:
                    rid, sri = _parse_key(k)
                    or_parts.append(
                        f"(research_id = '{_sql_escape(rid)}' "
                        f"AND source_row_id = '{_sql_escape(sri)}')"
                    )
                wh = " OR ".join(or_parts)
                if do_writes:
                    con.execute(f"DELETE FROM {MEDS} WHERE {wh}")
            if do_writes:
                n_left = con.execute(f"SELECT COUNT(*) FROM {MEDS}").fetchone()[0]
                print(
                    f"  DELETE meds — keys={len(drop_keys)}  rows_remaining={n_left}"
                )
            else:
                print(f"  [dry-run] would DELETE {len(drop_keys)} meds row keys")

        if not args.skip_rollup:
            if do_writes:
                mod = _load_365()
                mod.step_2_build_rollups(con, True, ("meds",))
                print("  rollup rebuild OK")
            else:
                print("  [dry-run] Script 365 step_2_build_rollups meds")

        if not args.skip_registry:
            phase_registry_signoff(con, do_writes)

        summ = json.dumps(meta.get("summary_counts", {}), sort_keys=True)
        phase_provenance(
            con,
            do_writes,
            f"mig_103 meds: {summ} rows_deleted_keys={len(drop_keys)}",
        )

        print("mig_103 — done")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    sys.exit(main())
