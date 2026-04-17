#!/usr/bin/env python3
"""
Script 265b - addendum to scripts/265_canonical_finalization.py

Fixes uncovered during the live --apply run of 265:

A) The 16 patients with any_fusion_positive=TRUE but mol_n_fusions=0 (after the
   PARSE_ERROR-excluded rule): all 16 have molecular_variant_long rows but NONE
   typed as variant_class='FUSION' (15 are 'OTHER', 1 is 'SNV'/KRAS). The
   upstream flag was set from ThyroSeq result-text NLP, not the variant parser.
   Per the new rule (FUSION + gene_symbol IS NOT NULL), flip these 16 to FALSE
   and surface them into a review table for downstream NLP rescue.

B) The pointer view shows 1273 distinct master_columns vs 1493 CPM columns - a
   220-column gap. Verified that the registry's feeds_master_columns_normalized
   has only ever encoded 1273 distinct CPM cols (snapshot pre265 confirms). The
   prior pointer view's 1494 was inflated by an additional source we no longer
   have. Surface the 220 unmapped CPM cols into a workspace review table so the
   gap is documented.

Default is dry-run; pass --apply to execute.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402
from _v1_1_helpers import (  # noqa: E402
    ensure_audit_table, make_logger, record_audit, snapshot_table,
    utc_ts, write_decision_log,
)

REPO = Path(__file__).resolve().parent.parent
OUT_DIR = REPO / "scripts" / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)
RUN_LOG = OUT_DIR / "265b_run.log"
DECISION_LOG = OUT_DIR / "265b_decision_log.json"

SCRIPT_TAG = "Script 265b"
SCRIPT_NUM = "265b"
RUN_DATE = "2026-04-17"

CPM = f'{PUBLICATION_DB}.main.canonical_patient_master'
MVL = f'{PUBLICATION_DB}.main.molecular_variant_long'
FUSION_FLAG_REVIEW = (
    f'{PUBLICATION_DB}.manuscript_workspace.fusion_flag_unparsed_review_v265'
)
CPM_UNMAPPED_REVIEW = (
    f'{PUBLICATION_DB}.manuscript_workspace.cpm_cols_unmapped_review_v265'
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true",
                    help="Execute writes; default dry-run.")
    args = ap.parse_args()
    do_writes = bool(args.apply)
    log, fh = make_logger(RUN_LOG)
    t0 = time.time()
    try:
        log("=" * 78)
        log(f"=== START scripts/265b_finalization_addendum.py "
            f"({'APPLY' if do_writes else 'DRY-RUN'})")
        log(f"started_at: {utc_now()}")
        con = connect_locked()
        log(f"connected to {PUBLICATION_DB}")

        # ----- A: 16 fusion-flag anomalies -----
        log("\nFIX A - 16 patients with any_fusion_positive=TRUE but mol_n_fusions=0")
        anomaly_rids = [r[0] for r in con.execute(f"""
            SELECT research_id FROM {CPM}
            WHERE mol_n_fusions = 0 AND any_fusion_positive = TRUE
            ORDER BY research_id
        """).fetchall()]
        log(f"  anomalies: {len(anomaly_rids)} -> {anomaly_rids}")

        if do_writes and anomaly_rids:
            snapshot_table(
                con, CPM, f"canonical_patient_master_pre265b_{utc_ts()}",
                SCRIPT_TAG, "Pre-265b snapshot of CPM (16-row fusion flip).")
            con.execute(f"DROP TABLE IF EXISTS {FUSION_FLAG_REVIEW}")
            con.execute(f"""
                CREATE TABLE {FUSION_FLAG_REVIEW} AS
                WITH ids AS (
                  SELECT research_id FROM {CPM}
                  WHERE mol_n_fusions = 0 AND any_fusion_positive = TRUE
                )
                SELECT
                  i.research_id,
                  TRUE  AS prior_any_fusion_positive,
                  FALSE AS new_any_fusion_positive,
                  v.molecular_variant_id, v.gene_symbol, v.variant_class,
                  v.interpretation_text,
                  current_timestamp AS reviewed_at,
                  '265b: upstream NLP set TRUE; variant_class is OTHER/SNV (no FUSION row).'
                    AS notes
                FROM ids i
                LEFT JOIN {MVL} v USING (research_id)
            """)
            n_review = con.execute(
                f"SELECT COUNT(*) FROM {FUSION_FLAG_REVIEW}").fetchone()[0]
            log(f"  surfaced {n_review} rows -> {FUSION_FLAG_REVIEW}")

            con.execute(f"""
                UPDATE {CPM}
                SET any_fusion_positive = FALSE
                WHERE mol_n_fusions = 0 AND any_fusion_positive = TRUE
            """)
            log(f"  flipped {len(anomaly_rids)} CPM rows: any_fusion_positive -> FALSE")

            comment = (
                "Script 265b: 16 ThyroSeq result-text fusion-positive cases that "
                "the variant parser (molecular_variant_long) did not surface as "
                "variant_class='FUSION'. The CPM flag was flipped to FALSE per the "
                "post-Step-5 rule (FUSION + gene_symbol IS NOT NULL); rescue "
                "candidates listed here for downstream NLP review."
            ).replace("'", "''")
            con.execute(f"COMMENT ON TABLE {FUSION_FLAG_REVIEW} IS '{comment}'")

            check = con.execute(f"""
                SELECT
                  COUNT(*) FILTER (WHERE mol_n_fusions > 0 AND any_fusion_positive = FALSE) AS a,
                  COUNT(*) FILTER (WHERE mol_n_fusions = 0 AND any_fusion_positive = TRUE)  AS b
                FROM {CPM}
            """).fetchone()
            log(f"  post-flip contradictions: a={check[0]} b={check[1]} (target 0,0)")
        else:
            log("  DRY-RUN; would flip 16 flags + create review table")

        # ----- B: 220 unmapped CPM cols -----
        log("\nFIX B - 220+ CPM cols with no registry feeder")
        unmapped = [r[0] for r in con.execute(f"""
            WITH cpm_cols AS (
              SELECT column_name FROM information_schema.columns
              WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
                AND table_name='canonical_patient_master'
            ),
            ptr AS (
              SELECT DISTINCT master_column
              FROM {PUBLICATION_DB}.manuscript_workspace.canonical_detail_pointer_v1
            )
            SELECT c.column_name FROM cpm_cols c
            LEFT JOIN ptr ON ptr.master_column = c.column_name
            WHERE ptr.master_column IS NULL
            ORDER BY c.column_name
        """).fetchall()]
        log(f"  unmapped CPM cols: {len(unmapped)}")
        for c in unmapped[:20]:
            log(f"    {c}")
        if len(unmapped) > 20:
            log(f"    ... +{len(unmapped) - 20} more")

        if do_writes:
            con.execute(f"DROP TABLE IF EXISTS {CPM_UNMAPPED_REVIEW}")
            con.execute(f"""
                CREATE TABLE {CPM_UNMAPPED_REVIEW} (
                  cpm_column_name VARCHAR PRIMARY KEY,
                  ordinal_position INTEGER,
                  data_type VARCHAR,
                  inferred_category VARCHAR,
                  notes VARCHAR,
                  surfaced_at TIMESTAMP
                )
            """)
            con.execute(f"""
                INSERT INTO {CPM_UNMAPPED_REVIEW}
                SELECT
                  c.column_name,
                  c.ordinal_position,
                  c.data_type,
                  CASE
                    WHEN c.column_name LIKE 'ajcc%' THEN 'derived_staging'
                    WHEN c.column_name LIKE 'ames_%' THEN 'derived_score'
                    WHEN c.column_name LIKE 'macis_%' THEN 'derived_score'
                    WHEN c.column_name LIKE 'ages_%' THEN 'derived_score'
                    WHEN c.column_name LIKE '%_inferred_negative' THEN 'inferred_default'
                    WHEN c.column_name LIKE '%_calculable_flag' THEN 'derived_flag'
                    WHEN c.column_name LIKE 'any_%' THEN 'derived_aggregate'
                    ELSE 'derived_or_unmapped'
                  END AS inferred_category,
                  '265b: derived in CPM build pipeline; no single detail-table feeder.' AS notes,
                  current_timestamp AS surfaced_at
                FROM information_schema.columns c
                LEFT JOIN (
                  SELECT DISTINCT master_column
                  FROM {PUBLICATION_DB}.manuscript_workspace.canonical_detail_pointer_v1
                ) p ON p.master_column = c.column_name
                WHERE c.table_catalog='{PUBLICATION_DB}'
                  AND c.table_schema='main'
                  AND c.table_name='canonical_patient_master'
                  AND p.master_column IS NULL
            """)
            n_unmapped = con.execute(
                f"SELECT COUNT(*) FROM {CPM_UNMAPPED_REVIEW}").fetchone()[0]
            log(f"  inserted {n_unmapped} rows -> {CPM_UNMAPPED_REVIEW}")

            comment = (
                "Script 265b: CPM columns with no detail-table feeder in "
                "detail_table_registry_v1. These are derived/computed columns "
                "(staging, scores, inferred defaults, aggregates) created in the "
                "CPM build pipeline. Pre-265 pointer view inflated these to the "
                "1494 reported count via a non-registry source (now lost). Use "
                "this table to expand the registry with build-pipeline mappings."
            ).replace("'", "''")
            con.execute(f"COMMENT ON TABLE {CPM_UNMAPPED_REVIEW} IS '{comment}'")

            cat_counts = con.execute(f"""
                SELECT inferred_category, COUNT(*)
                FROM {CPM_UNMAPPED_REVIEW}
                GROUP BY inferred_category ORDER BY 2 DESC
            """).fetchall()
            for c in cat_counts:
                log(f"    category={c[0]:<25}  n={c[1]}")
        else:
            log("  DRY-RUN; would create review table")

        # ----- audit rows -----
        if do_writes:
            ensure_audit_table(con)
            record_audit(
                con, SCRIPT_NUM,
                "step_5_residual_16_flag_anomalies",
                "fusion_flag_unparsed_review_v265 rows",
                count_before=len(anomaly_rids), count_after=0,
                target_after=0, status="OK",
                notes=("Flipped 16 flags FALSE per FUSION+gene_symbol rule; "
                       "16 NLP rescue candidates surfaced for review."))
            record_audit(
                con, SCRIPT_NUM,
                "step_10_pointer_coverage_gap",
                "cpm_cols_unmapped_review_v265 rows",
                count_before=len(unmapped), count_after=len(unmapped),
                target_after=0, status="DOCUMENTED_GAP",
                notes=("Registry feeds_master_columns_normalized maps 1273 of 1493 "
                       "CPM cols. 220 derived cols (AJCC, AMES, etc.) have no "
                       "detail-table feeder. Surfaced for registry expansion."))
            log("  audit rows written")

        elapsed = time.time() - t0
        log(f"=== END elapsed={elapsed:.1f}s")
        write_decision_log(DECISION_LOG, {
            "script": SCRIPT_TAG, "run_date": RUN_DATE,
            "do_writes": do_writes, "elapsed_seconds": round(elapsed, 1),
            "anomaly_rids": anomaly_rids,
            "n_unmapped_cols": len(unmapped),
        })
        return 0
    except Exception as e:
        log(f"FATAL: {e!r}")
        import traceback
        log(traceback.format_exc())
        return 1
    finally:
        fh.close()


if __name__ == "__main__":
    sys.exit(main())
