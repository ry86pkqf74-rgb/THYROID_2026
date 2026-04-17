#!/usr/bin/env python3
"""
Script 269 - Backfill ~527 missing molecular episodes into molecular_test_episode_v2.

Three sources, ALL inserts tagged ingestion_source='script_269_backfill':

  1. ThyroSeq (~443):
       Source: thyroseq_molecular_enrichment, restricted to patients flagged
               canonical_molecular_tested_v1.has_thyroseq=TRUE who have NO
               existing mte_v2 row with platform LIKE '%thyroseq%'
       Tag:    platform='ThyroSeq', source_table='thyroseq_molecular_enrichment'

  2. NGS-BRAF (~46):
       Source: extracted_braf_recovery_v1 WHERE braf_status='positive' AND
               detection_method LIKE '%NGS%', collapsed DISTINCT per patient
       Filter: WHERE NOT EXISTS mte_v2 row with same RID + braf_flag=TRUE
       Tag:    platform='NGS_unspecified', braf_flag=TRUE

  3. RET (~38):
       Source: ret_patient_adjudicated_v226 WHERE ret_note_true_positive=TRUE
       Filter: WHERE NOT EXISTS mte_v2 row with same RID + (ret_flag OR
               ret_fusion_flag)=TRUE
       Tag:    platform='NGS_unspecified', ret_flag=TRUE

CRITICAL NON-ACTION:
  Do NOT re-derive mol_has_thyroseq, mol_has_afirma, mol_platform,
  molecular_tested_confirmed, mol_n_tests on CPM. Pinned feeders stay.

Pre-flight:
  ALTER TABLE mte_v2 ADD COLUMN ingestion_source VARCHAR (idempotent).
  All research_id casts: source -> CAST(... AS INTEGER) to match mte_v2.research_id.
"""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
OUT_DIR = REPO / "scripts" / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)

RUN_LOG = OUT_DIR / "269_run.log"
PRE_JSON = OUT_DIR / "269_pre.json"
REPORT = OUT_DIR / "269_backfill_report.md"
DECISION_LOG = OUT_DIR / "269_decision_log.json"

SCRIPT_TAG = "Script 269"
SCRIPT_NUM = "269"
RUN_DATE = "2026-04-17"
INGEST_TAG = "script_269_backfill"

CPM = "canonical_patient_master"
MTE = "molecular_test_episode_v2"


class TeeLogger:
    def __init__(self, path: Path) -> None:
        self.fh = path.open("w", encoding="utf-8")

    def __call__(self, msg: str = "") -> None:
        print(msg)
        self.fh.write(msg + "\n")
        self.fh.flush()

    def close(self) -> None:
        self.fh.close()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def preflight(con, log) -> dict:
    log("\n--- PREFLIGHT ---")
    n_cpm = con.execute(f"SELECT COUNT(*) FROM {CPM}").fetchone()[0]
    if n_cpm != 10871:
        raise SystemExit(f"PREFLIGHT FAIL: CPM rows={n_cpm} != 10871")
    log(f"  CPM: {n_cpm} rows (pass)")

    n_mte = con.execute(f"SELECT COUNT(*) FROM {MTE}").fetchone()[0]
    log(f"  mte_v2 starting rows: {n_mte}")

    out = {
        "captured_at": utc_now(),
        "cpm_rows": int(n_cpm),
        "mte_starting_rows": int(n_mte),
    }
    PRE_JSON.write_text(json.dumps(out, indent=2))
    return out


def schema_discovery(con, log) -> dict:
    log("\n--- SCHEMA DISCOVERY ---")
    discovery: dict = {}
    for tbl in (MTE, "thyroseq_molecular_enrichment",
                "extracted_braf_recovery_v1",
                "ret_patient_adjudicated_v226",
                "canonical_molecular_tested_v1"):
        cols = con.execute(f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
              AND table_name='{tbl}'
            ORDER BY ordinal_position
        """).fetchall()
        discovery[tbl] = [{"col": c, "type": t, "nullable": n} for c, t, n in cols]
        log(f"  {tbl}: {len(cols)} cols")

    has_ing = any(c["col"] == "ingestion_source" for c in discovery[MTE])
    if not has_ing:
        log("  ingestion_source column ABSENT - will ALTER TABLE")
    else:
        log("  ingestion_source column already present")
    discovery["ingestion_source_present_pre"] = has_ing
    return discovery


def add_ingestion_source(con, log, present: bool) -> None:
    if present:
        log("  skipping ALTER (column exists)")
        return
    log("\n--- ALTER TABLE ADD ingestion_source ---")
    con.execute(f"ALTER TABLE {MTE} ADD COLUMN ingestion_source VARCHAR")
    con.execute(
        f"COMMENT ON COLUMN {MTE}.ingestion_source IS "
        f"'Ingest-script tag. NULL = original ingest (pre-provenance tracking). "
        f"Non-NULL: script_NNN_backfill or similar. Added 2026-04-17 by Script 269.'"
    )
    log("  added ingestion_source VARCHAR + comment")


def backfill_thyroseq(con, log) -> int:
    log("\n--- BACKFILL ThyroSeq ---")
    expected = con.execute("""
        WITH ts_pts AS (
          SELECT DISTINCT CAST(cmt.research_id AS VARCHAR) AS rid
          FROM canonical_molecular_tested_v1 cmt
          WHERE cmt.has_thyroseq = TRUE
        ),
        existing AS (
          SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid
          FROM molecular_test_episode_v2
          WHERE LOWER(COALESCE(platform, '')) LIKE '%thyroseq%'
             OR LOWER(COALESCE(platform_raw, '')) LIKE '%thyroseq%'
        )
        SELECT COUNT(*) FROM ts_pts t
        WHERE NOT EXISTS (SELECT 1 FROM existing e WHERE e.rid = t.rid)
    """).fetchone()[0]
    log(f"  pre-insert candidate count: {expected}")

    pre = con.execute(
        f"SELECT COUNT(*) FROM {MTE} WHERE ingestion_source='{INGEST_TAG}' "
        f"AND source_table='thyroseq_molecular_enrichment'"
    ).fetchone()[0]
    if pre > 0:
        log(f"  detected {pre} prior backfill rows for ThyroSeq; deleting "
            "for clean idempotent re-run")
        con.execute(
            f"DELETE FROM {MTE} WHERE ingestion_source='{INGEST_TAG}' "
            f"AND source_table='thyroseq_molecular_enrichment'"
        )

    # Build the INSERT. Use the FIRST enrichment row per patient (DISTINCT ON
    # pattern via ROW_NUMBER) to avoid multi-row blow-up.
    con.execute(f"""
        INSERT INTO {MTE} (
          research_id, platform, platform_raw, source_table,
          braf_flag, ras_flag, ret_flag, tert_flag, ntrk_flag, alk_flag,
          fusion_flag, tp53_flag,
          ingestion_source
        )
        WITH ts_pts AS (
          SELECT DISTINCT CAST(cmt.research_id AS BIGINT) AS rid_int
          FROM canonical_molecular_tested_v1 cmt
          WHERE cmt.has_thyroseq = TRUE
        ),
        existing AS (
          SELECT DISTINCT research_id AS rid_int
          FROM {MTE}
          WHERE LOWER(COALESCE(platform, '')) LIKE '%thyroseq%'
             OR LOWER(COALESCE(platform_raw, '')) LIKE '%thyroseq%'
        ),
        gap_pts AS (
          SELECT t.rid_int FROM ts_pts t
          WHERE NOT EXISTS (SELECT 1 FROM existing e WHERE e.rid_int = t.rid_int)
        ),
        ranked AS (
          SELECT
            tse.research_id,
            tse.molecular_platform,
            tse.braf_flag, tse.ras_flag, tse.ret_flag, tse.tert_flag,
            tse.ntrk_flag, tse.alk_flag, tse.fusion_flag, tse.tp53_flag,
            ROW_NUMBER() OVER (PARTITION BY tse.research_id
                               ORDER BY tse.imported_at, tse.source_row_hash) AS rn
          FROM thyroseq_molecular_enrichment tse
          JOIN gap_pts gp ON gp.rid_int = tse.research_id
        )
        SELECT
          CAST(research_id AS INTEGER)                       AS research_id,
          'ThyroSeq'                                         AS platform,
          COALESCE(molecular_platform, 'ThyroSeq')           AS platform_raw,
          'thyroseq_molecular_enrichment'                    AS source_table,
          braf_flag, ras_flag, ret_flag, tert_flag,
          ntrk_flag, alk_flag, fusion_flag, tp53_flag,
          '{INGEST_TAG}'                                     AS ingestion_source
        FROM ranked WHERE rn = 1
    """)

    n_after = con.execute(
        f"SELECT COUNT(*) FROM {MTE} WHERE ingestion_source='{INGEST_TAG}' "
        f"AND source_table='thyroseq_molecular_enrichment'"
    ).fetchone()[0]
    log(f"  inserted ThyroSeq backfill rows: {n_after}")
    return int(n_after)


def backfill_ngs_braf(con, log) -> int:
    log("\n--- BACKFILL NGS-BRAF ---")
    pre = con.execute(
        f"SELECT COUNT(*) FROM {MTE} WHERE ingestion_source='{INGEST_TAG}' "
        f"AND source_table='extracted_braf_recovery_v1'"
    ).fetchone()[0]
    if pre > 0:
        log(f"  deleting {pre} prior backfill rows for NGS-BRAF (idempotent)")
        con.execute(
            f"DELETE FROM {MTE} WHERE ingestion_source='{INGEST_TAG}' "
            f"AND source_table='extracted_braf_recovery_v1'"
        )

    con.execute(f"""
        INSERT INTO {MTE} (
          research_id, platform, platform_raw, source_table,
          braf_flag, braf_variant,
          ingestion_source
        )
        WITH src AS (
          SELECT
            ebr.research_id                                  AS rid_int,
            (ARRAY_AGG(ebr.braf_variant
                       ORDER BY (ebr.braf_variant IS NULL),
                                ebr.confidence DESC NULLS LAST))[1]
                                                             AS best_variant,
            (ARRAY_AGG(ebr.detection_method
                       ORDER BY ebr.confidence DESC NULLS LAST))[1]
                                                             AS best_method
          FROM extracted_braf_recovery_v1 ebr
          WHERE UPPER(COALESCE(ebr.braf_status, '')) = 'POSITIVE'
            AND UPPER(COALESCE(ebr.detection_method, '')) LIKE '%NGS%'
          GROUP BY 1
        ),
        existing AS (
          SELECT DISTINCT research_id AS rid_int FROM {MTE}
          WHERE braf_flag = TRUE
        ),
        gap AS (
          SELECT * FROM src WHERE NOT EXISTS (
            SELECT 1 FROM existing e WHERE e.rid_int = src.rid_int)
        )
        SELECT
          CAST(rid_int AS INTEGER)                           AS research_id,
          'NGS_unspecified'                                  AS platform,
          best_method                                        AS platform_raw,
          'extracted_braf_recovery_v1'                       AS source_table,
          TRUE                                               AS braf_flag,
          best_variant                                       AS braf_variant,
          '{INGEST_TAG}'                                     AS ingestion_source
        FROM gap
    """)

    n_after = con.execute(
        f"SELECT COUNT(*) FROM {MTE} WHERE ingestion_source='{INGEST_TAG}' "
        f"AND source_table='extracted_braf_recovery_v1'"
    ).fetchone()[0]
    log(f"  inserted NGS-BRAF backfill rows: {n_after}")
    return int(n_after)


def backfill_ret(con, log) -> int:
    log("\n--- BACKFILL RET ---")
    pre = con.execute(
        f"SELECT COUNT(*) FROM {MTE} WHERE ingestion_source='{INGEST_TAG}' "
        f"AND source_table='ret_patient_adjudicated_v226'"
    ).fetchone()[0]
    if pre > 0:
        log(f"  deleting {pre} prior backfill rows for RET (idempotent)")
        con.execute(
            f"DELETE FROM {MTE} WHERE ingestion_source='{INGEST_TAG}' "
            f"AND source_table='ret_patient_adjudicated_v226'"
        )

    con.execute(f"""
        INSERT INTO {MTE} (
          research_id, platform, platform_raw, source_table,
          ret_flag,
          ingestion_source
        )
        WITH src AS (
          SELECT
            CAST(rpa.research_id AS INTEGER)                 AS rid_int,
            (ARRAY_AGG(rpa.alteration_types
                       ORDER BY rpa.max_tp_confidence DESC NULLS LAST))[1]
                                                             AS alts
          FROM ret_patient_adjudicated_v226 rpa
          WHERE rpa.ret_note_true_positive = TRUE
            AND TRY_CAST(rpa.research_id AS INTEGER) IS NOT NULL
          GROUP BY 1
        ),
        existing AS (
          SELECT DISTINCT research_id AS rid_int FROM {MTE}
          WHERE ret_flag = TRUE OR ret_fusion_flag = TRUE
        ),
        gap AS (
          SELECT * FROM src WHERE NOT EXISTS (
            SELECT 1 FROM existing e WHERE e.rid_int = src.rid_int)
        )
        SELECT
          rid_int                                            AS research_id,
          'NGS_unspecified'                                  AS platform,
          'ret_adjudication_v226'                            AS platform_raw,
          'ret_patient_adjudicated_v226'                     AS source_table,
          TRUE                                               AS ret_flag,
          '{INGEST_TAG}'                                     AS ingestion_source
        FROM gap
    """)

    n_after = con.execute(
        f"SELECT COUNT(*) FROM {MTE} WHERE ingestion_source='{INGEST_TAG}' "
        f"AND source_table='ret_patient_adjudicated_v226'"
    ).fetchone()[0]
    log(f"  inserted RET backfill rows: {n_after}")
    return int(n_after)


def post_count_and_check(con, log, n_pre: int, by_source: dict) -> dict:
    log("\n--- POST-COUNT + DEVIATION CHECK ---")
    n_post = con.execute(f"SELECT COUNT(*) FROM {MTE}").fetchone()[0]
    n_backfilled = con.execute(
        f"SELECT COUNT(*) FROM {MTE} WHERE ingestion_source='{INGEST_TAG}'"
    ).fetchone()[0]
    log(f"  mte_v2 rows: pre={n_pre} post={n_post} delta={n_post - n_pre}")
    log(f"  backfilled rows (ingestion_source tag): {n_backfilled}")
    log(f"  by source: {by_source}")

    # Allow ±5% tolerance: 527 expected -> 500..555
    if not (500 <= n_backfilled <= 555):
        raise SystemExit(
            f"DEVIATION: backfilled count {n_backfilled} outside [500, 555]. "
            "STOP and surface."
        )
    log(f"  pass: {n_backfilled} in [500, 555]")
    return {
        "mte_pre": n_pre,
        "mte_post": int(n_post),
        "n_backfilled": int(n_backfilled),
        "by_source": by_source,
    }


def concordance_check(con, log) -> dict:
    log("\n--- CONCORDANCE CHECK (informational only) ---")
    rows = con.execute(f"""
        WITH derived AS (
          SELECT CAST(research_id AS VARCHAR) AS rid, COUNT(*) AS n_episodes
          FROM {MTE} GROUP BY 1
        ),
        cmp AS (
          SELECT cpm.research_id, cpm.mol_n_tests, COALESCE(d.n_episodes, 0) AS derived_n
          FROM {CPM} cpm
          LEFT JOIN derived d ON d.rid = cpm.research_id
        )
        SELECT
          COUNT(*) FILTER (WHERE mol_n_tests IS NOT NULL) AS n_pinned,
          COUNT(*) FILTER (WHERE mol_n_tests IS NOT NULL AND mol_n_tests = derived_n) AS n_match,
          COUNT(*) FILTER (WHERE mol_n_tests IS NOT NULL AND mol_n_tests <> derived_n) AS n_mismatch,
          COUNT(*) FILTER (WHERE mol_n_tests IS NULL AND derived_n > 0) AS n_pinned_null_but_episodes
        FROM cmp
    """).fetchone()
    pinned, match, mismatch, null_with_eps = rows
    rate = (100.0 * match / pinned) if pinned else 0.0
    log(f"  pinned={pinned} match={match} mismatch={mismatch} "
        f"null_pinned_with_episodes={null_with_eps} match_rate={rate:.2f}%")
    if rate < 99.0:
        log(f"  NOTE: match rate {rate:.2f}% < 99% - documenting (NOT failing). "
            "Pinned feeders intentionally remain authoritative.")
    return {
        "pinned_with_value": int(pinned),
        "match": int(match),
        "mismatch": int(mismatch),
        "pinned_null_with_episodes": int(null_with_eps),
        "match_rate_pct": round(rate, 2),
    }


def write_report(con, log, summary: dict) -> None:
    log("\n--- WRITE BACKFILL REPORT ---")
    by_src = con.execute(f"""
        SELECT source_table, COUNT(*) FROM {MTE}
        WHERE ingestion_source = '{INGEST_TAG}'
        GROUP BY 1 ORDER BY 2 DESC
    """).fetchall()

    overall_by_src = con.execute(f"""
        SELECT source_table, COUNT(*) FROM {MTE}
        GROUP BY 1 ORDER BY 2 DESC LIMIT 20
    """).fetchall()

    md = ["# Script 269 - Molecular Episode Backfill Report",
          f"_Generated {utc_now()}_",
          "",
          "## Backfill summary",
          f"- mte_v2 rows: {summary['post']['mte_pre']} -> "
          f"{summary['post']['mte_post']} (delta +{summary['post']['n_backfilled']})",
          f"- All inserts tagged `ingestion_source='{INGEST_TAG}'`",
          "",
          "## Backfilled rows by source_table",
          "| source_table | n_inserted |",
          "|---|---:|"]
    for src, n in by_src:
        md.append(f"| {src} | {n} |")

    md += ["",
           "## Top 20 source_table values (overall, post-backfill)",
           "| source_table | n_rows |",
           "|---|---:|"]
    for src, n in overall_by_src:
        md.append(f"| {src or 'NULL'} | {n} |")

    md += ["",
           "## Concordance check (informational)",
           f"- Patients with pinned mol_n_tests value: {summary['concordance']['pinned_with_value']}",
           f"- Match (pinned == derived from episodes): {summary['concordance']['match']}",
           f"- Mismatch (pinned != derived): {summary['concordance']['mismatch']}",
           f"- Pinned NULL but has episodes: {summary['concordance']['pinned_null_with_episodes']}",
           f"- Match rate: {summary['concordance']['match_rate_pct']}%",
           "",
           "_NOTE: pinned CPM feeders remain authoritative per Scripts 252-265 architecture._",
           "_Episode table is a drill-down surface; backfill closes F1 audit gap without_",
           "_changing the patient-level rollup that CPM uses._",
           "",
           "## CRITICAL NON-ACTION (NOT performed by this script)",
           "- `mol_has_thyroseq`, `mol_has_afirma`, `mol_platform`, "
           "`molecular_tested_confirmed` remain pinned to "
           "`canonical_molecular_tested_v1`",
           "- `mol_n_tests` remains pinned to `_molecular_patient_rollup_v227`",
           "- These pinned feeders are intentionally NOT re-derived from the "
           "expanded episode table"]

    REPORT.write_text("\n".join(md))
    log(f"  wrote {REPORT}")


def main() -> int:
    log = TeeLogger(RUN_LOG)
    t0 = time.time()
    summaries: dict = {"script": SCRIPT_TAG, "run_date": RUN_DATE}
    try:
        log("=" * 78)
        log(f"=== START {SCRIPT_TAG}")
        log(f"started_at: {utc_now()}")

        con = connect_locked()
        log(f"connected to {PUBLICATION_DB}")

        pre = preflight(con, log)
        summaries["pre"] = pre

        discovery = schema_discovery(con, log)
        summaries["schema_discovery_ingestion_source_present_pre"] = (
            discovery["ingestion_source_present_pre"])

        add_ingestion_source(con, log, discovery["ingestion_source_present_pre"])

        n_ts = backfill_thyroseq(con, log)
        n_braf = backfill_ngs_braf(con, log)
        n_ret = backfill_ret(con, log)

        by_source = {
            "thyroseq_molecular_enrichment": n_ts,
            "extracted_braf_recovery_v1": n_braf,
            "ret_patient_adjudicated_v226": n_ret,
        }

        post = post_count_and_check(con, log, pre["mte_starting_rows"], by_source)
        summaries["post"] = post
        summaries["concordance"] = concordance_check(con, log)
        write_report(con, log, summaries)

        elapsed = time.time() - t0
        summaries["elapsed_seconds"] = round(elapsed, 1)
        DECISION_LOG.write_text(json.dumps(summaries, indent=2, default=str))
        log(f"\nwrote {DECISION_LOG}")
        log(f"=== END {SCRIPT_TAG} elapsed={elapsed:.1f}s")
        return 0

    except Exception as e:
        log(f"\nFATAL: {e!r}")
        import traceback
        log(traceback.format_exc())
        return 1
    finally:
        log.close()


if __name__ == "__main__":
    sys.exit(main())
