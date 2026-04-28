#!/usr/bin/env python3
"""mig_98a — Apply Logan decisions for vocal_cord_paralysis in canonical_complications_events_v1.

Mutates MotherDuck `thyroid_canonical_publication_v1_0` only when passed `--apply`.

Phases:
  1) ADD COLUMN finding_date_source (if missing) + backfill
  2) FLIP 23 NEGATION_RISK rows (indeterminate → present), excluding research_id 7306
  3) INSERT synthetic CF91 row for research_id 5048 (if absent)
  4) AUTO_DERIVE onset_class for all present vocal_cord_paralysis with onset_class = unspecified
  5) Rebuild canonical_complications_patient_rollup_v1 (Script 364 step 5)
  6) INSERT cpm_reconciliation_provenance_v1 row

See qc_framework_v1/migrations/98a_mig_vocal_cord_paralysis_apply.md
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
# Live NEC was dropped in Script 364 phase 7; join archive for date provenance.
ARCH_NEC = (
    '"Thyroid 2026 UPdated".archive_pub_v1_0.'
    "note_entities_complications_pre364_20260422_050902"
)
ONSET_WINDOW_VCP = 30
ACCEPT_INDETERMINATE_RID = "7306"
CF91_RID = "5048"
CF91_FINDING_DATE = "2015-10-08"
CF91_EVIDENCE_PHRASE = (
    "medialization of the right true vocal cord and arytenoid"
)
SYNTH_SOURCE_TAG = "mig98a_cf91_vcp"


def _connect_md() -> duckdb.DuckDBPyConnection:
    from motherduck_client import MotherDuckClient, MotherDuckConfig

    cfg = MotherDuckConfig(database=PUB_DB)
    con = MotherDuckClient(cfg).connect_rw()
    con.execute(f"USE {PUB_DB}")
    con.execute(f"USE {PUB_DB}.main")
    return con


def _col_exists(con: duckdb.DuckDBPyConnection, table: str, col: str) -> bool:
    row = con.execute(
        """
        SELECT 1 FROM information_schema.columns
        WHERE table_catalog = ? AND table_schema = 'main'
          AND table_name = ? AND column_name = ?
        """,
        [PUB_DB, table, col],
    ).fetchone()
    return row is not None


def _load_364() -> Any:
    path = REPO_ROOT / "scripts" / "364_complications_consolidation.py"
    spec = importlib.util.spec_from_file_location("complications_364", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _counts(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    def one(sql: str) -> int:
        r = con.execute(sql).fetchone()
        return int(r[0]) if r and r[0] is not None else 0

    return {
        "vcp_indeterminate": one(
            f"SELECT COUNT(*) FROM {EVENTS} "
            f"WHERE complication_type = 'vocal_cord_paralysis' "
            f"AND finding_status = 'indeterminate'"
        ),
        "vcp_present_unspec_onset": one(
            f"SELECT COUNT(*) FROM {EVENTS} "
            f"WHERE complication_type = 'vocal_cord_paralysis' "
            f"AND finding_status = 'present' AND onset_class = 'unspecified'"
        ),
        "cf91_5048": one(
            f"SELECT COUNT(*) FROM {EVENTS} "
            f"WHERE research_id = '{CF91_RID}' "
            f"AND complication_type = 'vocal_cord_paralysis'"
        ),
    }


def phase_add_and_backfill_finding_date_source(
    con: duckdb.DuckDBPyConnection, do_writes: bool
) -> None:
    if not _col_exists(con, "canonical_complications_events_v1", "finding_date_source"):
        print("  finding_date_source: column missing")
        if do_writes:
            con.execute(
                f"ALTER TABLE {EVENTS} ADD COLUMN finding_date_source VARCHAR"
            )
            print("  ALTER TABLE ADD finding_date_source — done")
    else:
        print("  finding_date_source: already present")

    sql = f"""
    UPDATE {EVENTS} AS c
    SET finding_date_source = s.val
    FROM (
        SELECT
            c2.research_id AS rid,
            c2.source_table AS st,
            c2.source_row_id AS srow,
            c2.complication_type AS ct,
            CASE
                WHEN c2.source_table = 'note_entities_complications' THEN
                    CASE
                        WHEN e.research_id IS NULL THEN 'note_entity_join_unresolved'
                        WHEN TRY_CAST(e.entity_date AS DATE) IS NOT NULL
                            THEN 'entity_date'
                        WHEN TRY_CAST(e.note_date AS DATE) IS NOT NULL
                            AND TRY_CAST(e.entity_date AS DATE) IS NULL
                            THEN 'note_date_fallback'
                        ELSE 'first_surgery_fallback'
                    END
                WHEN c2.source_table = 'complication_phenotype_v1' THEN
                    CASE WHEN COALESCE(c2.detection_date_inferred, FALSE)
                        THEN 'phenotype_date_inferred'
                        ELSE 'phenotype_date_native'
                    END
                WHEN c2.source_table IN (
                    'extracted_complications_refined_v5',
                    'extracted_rln_injury_refined_v2'
                ) THEN
                    CASE WHEN COALESCE(c2.detection_date_inferred, FALSE)
                        THEN 'extracted_detection_inferred'
                        ELSE 'extracted_detection_native'
                    END
                WHEN c2.source_table = 'canonical_labs_calcium_v1'
                    THEN 'lab_collection_date'
                WHEN c2.source_table = 'canonical_survival_followup_v1'
                    THEN 'survival_registry_date'
                WHEN c2.source_table = 'note_entities_llm_survival_followup'
                    THEN 'llm_survival_followup_anchor'
                ELSE 'source_table:' || COALESCE(c2.source_table, 'NULL')
            END AS val
        FROM {EVENTS} c2
        LEFT JOIN {ARCH_NEC} e
            ON CAST(e.research_id AS VARCHAR) = c2.research_id
           AND CAST(hash(
                e.research_id, e.note_row_id, e.source_line,
                e.entity_value_norm, e.evidence_start
            ) AS VARCHAR) = c2.source_row_id
    ) AS s
    WHERE c.research_id = s.rid
      AND c.source_table = s.st
      AND c.source_row_id = s.srow
      AND c.complication_type = s.ct
    """
    if do_writes:
        con.execute(sql)
        n = con.execute(
            f"SELECT COUNT(*) FROM {EVENTS} WHERE finding_date_source IS NOT NULL"
        ).fetchone()[0]
        print(f"  backfill finding_date_source — populated rows: {n}")
    else:
        print("  [dry-run] would backfill finding_date_source for all event rows")


def phase_flip_negation_risk(con: duckdb.DuckDBPyConnection, do_writes: bool) -> int:
    pre = con.execute(
        f"SELECT COUNT(*) FROM {EVENTS} "
        f"WHERE complication_type = 'vocal_cord_paralysis' "
        f"AND finding_status = 'indeterminate' "
        f"AND research_id != '{ACCEPT_INDETERMINATE_RID}'"
    ).fetchone()[0]
    if int(pre) != 23:
        raise SystemExit(
            f"Precondition failed: expected 23 indeterminate VCP rows "
            f"(excl {ACCEPT_INDETERMINATE_RID}), got {pre}"
        )
    if do_writes:
        con.execute(
            f"""
            UPDATE {EVENTS}
            SET finding_status = 'present',
                build_ts = CURRENT_TIMESTAMP
            WHERE complication_type = 'vocal_cord_paralysis'
              AND finding_status = 'indeterminate'
              AND research_id != '{ACCEPT_INDETERMINATE_RID}'
            """
        )
        print(f"  FLIP_TO_PRESENT — updated {pre} rows")
    else:
        print(f"  [dry-run] would FLIP_TO_PRESENT {pre} rows")
    return int(pre)


def phase_insert_cf91_5048(con: duckdb.DuckDBPyConnection, do_writes: bool) -> None:
    n = con.execute(
        f"SELECT COUNT(*) FROM {EVENTS} "
        f"WHERE research_id = '{CF91_RID}' "
        f"AND complication_type = 'vocal_cord_paralysis'"
    ).fetchone()[0]
    if int(n) > 0:
        print(f"  CF91 INSERT — skip (already {n} VCP row(s) for {CF91_RID})")
        return
    if not do_writes:
        print(f"  [dry-run] would INSERT synthetic VCP row for rid {CF91_RID}")
        return

    # Match Script 364 entity source_row_id pattern (research_id is BIGINT in NEC).
    src_row_sql = (
        f"CAST(hash(CAST(5048 AS BIGINT), '{SYNTH_SOURCE_TAG}', "
        f"CAST('{CF91_FINDING_DATE}' AS DATE)) AS VARCHAR)"
    )
    ins = f"""
    INSERT INTO {EVENTS} (
        research_id, source_table, source_row_id, source_modality, source_kind,
        complication_type, source_evidence_type, evidence_strength,
        onset_class, permanence_class, finding_status, finding_date,
        detection_date_inferred, evidence_span_hash, confidence,
        lab_value_at_detection, lab_units, build_ts, finding_date_source
    )
    SELECT
        '{CF91_RID}',
        'note_entities_complications',
        {src_row_sql},
        'op_note',
        'entity_legacy',
        'vocal_cord_paralysis',
        'operative_note',
        'possible',
        'unspecified',
        'indeterminate',
        'present',
        CAST('{CF91_FINDING_DATE}' AS DATE),
        FALSE,
        sha256('{CF91_EVIDENCE_PHRASE}'),
        NULL,
        NULL,
        NULL,
        CURRENT_TIMESTAMP,
        'cf91_invasion_absorption_synthetic'
    """
    con.execute(ins)
    print(f"  CF91 INSERT — rid {CF91_RID} (synthetic entity row)")


def phase_derive_onset(con: duckdb.DuckDBPyConnection, do_writes: bool) -> None:
    """Mirror Script 364 onset refinement for vocal_cord_paralysis only."""
    sql = f"""
    WITH op AS (
        SELECT
            CAST(research_id AS VARCHAR) AS research_id,
            CAST(surgery_date_native AS DATE) AS surgery_date
        FROM main.canonical_operative_events_v1
        WHERE surgery_date_native IS NOT NULL
    ),
    ranked AS (
        SELECT
            c.research_id AS rid,
            c.source_table AS st,
            c.source_row_id AS srow,
            c.complication_type AS ct,
            c.source_modality AS sm,
            DATE_DIFF('day', op.surgery_date, c.finding_date) AS days_since_surgery,
            ROW_NUMBER() OVER (
                PARTITION BY c.research_id, c.source_table, c.source_row_id,
                             c.complication_type
                ORDER BY op.surgery_date DESC
            ) AS rn
        FROM {EVENTS} c
        LEFT JOIN op
            ON op.research_id = c.research_id
           AND op.surgery_date <= c.finding_date
           AND DATE_DIFF('day', op.surgery_date, c.finding_date) <= {ONSET_WINDOW_VCP}
           AND DATE_DIFF('day', op.surgery_date, c.finding_date) >= 0
        WHERE c.complication_type = 'vocal_cord_paralysis'
          AND c.finding_status = 'present'
          AND c.onset_class = 'unspecified'
    )
    UPDATE {EVENTS} AS c
    SET onset_class = CASE
            WHEN r.days_since_surgery IS NOT NULL
                 AND r.days_since_surgery = 0
                 AND r.sm = 'op_note'
                THEN 'intraop'
            WHEN r.days_since_surgery IS NOT NULL
                 AND r.days_since_surgery > 0
                 AND r.days_since_surgery <= 30
                THEN 'early_postop'
            WHEN r.days_since_surgery IS NOT NULL
                 AND r.days_since_surgery > 30
                THEN 'late_postop'
            ELSE 'unspecified'
        END,
        build_ts = CURRENT_TIMESTAMP
    FROM ranked AS r
    WHERE r.rn = 1
      AND c.research_id = r.rid
      AND c.source_table = r.st
      AND c.source_row_id = r.srow
      AND c.complication_type = r.ct
    """
    if do_writes:
        con.execute(sql)
        remain = con.execute(
            f"SELECT COUNT(*) FROM {EVENTS} "
            f"WHERE complication_type = 'vocal_cord_paralysis' "
            f"AND finding_status = 'present' AND onset_class = 'unspecified'"
        ).fetchone()[0]
        print(f"  onset AUTO_DERIVE — remaining present/unspecified: {remain}")
    else:
        n = con.execute(
            f"SELECT COUNT(*) FROM {EVENTS} "
            f"WHERE complication_type = 'vocal_cord_paralysis' "
            f"AND finding_status = 'present' AND onset_class = 'unspecified'"
        ).fetchone()[0]
        print(f"  [dry-run] would derive onset for {n} present/unspecified VCP rows")


def phase_provenance(con: duckdb.DuckDBPyConnection, do_writes: bool) -> None:
    run_id = (
        "mig98a_vocal_cord_paralysis_"
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
            'mig98a_vocal_cord_paralysis;finding_date_source;negation_flip;cf91_insert;onset_derive;rollup_step5',
            '0', '0', '0',
            'CF-VC-PERMANENCE pending mig_99;11862 NO_ADD'
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
    parser.add_argument(
        "--skip-rollup",
        action="store_true",
        help="Skip Script 364 step_5 (rollup rebuild).",
    )
    args = parser.parse_args()
    do_writes = bool(args.apply)

    print(f"mig_98a vocal_cord_paralysis apply — do_writes={do_writes}")
    con = _connect_md()
    try:
        before = _counts(con)
        print("  pre-counts:", before)
        if before["vcp_indeterminate"] != 24:
            raise SystemExit(
                f"Expected 24 indeterminate VCP rows pre-apply, "
                f"got {before['vcp_indeterminate']}"
            )

        phase_add_and_backfill_finding_date_source(con, do_writes)
        phase_flip_negation_risk(con, do_writes)
        phase_insert_cf91_5048(con, do_writes)
        phase_derive_onset(con, do_writes)

        if not args.skip_rollup and do_writes:
            mod = _load_364()
            mod.step_5_build_rollup(con, True)
        elif args.skip_rollup:
            print("  SKIP rollup (--skip-rollup)")
        else:
            print("  [dry-run] would run Script 364 step_5_build_rollup")

        phase_provenance(con, do_writes)

        after = _counts(con)
        print("  post-counts:", after)
        if do_writes:
            if after["vcp_indeterminate"] != 1:
                raise SystemExit(
                    f"Expected 1 indeterminate VCP (rid {ACCEPT_INDETERMINATE_RID}), "
                    f"got {after['vcp_indeterminate']}"
                )
        print("mig_98a — complete")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    sys.exit(main())
