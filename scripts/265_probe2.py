#!/usr/bin/env python3
"""
Probe-2 — additional read-only state needed before scripts/265 is written.

Inspects:
  - detail_table_registry_v1 schema + sample rows
  - nan_string_audit_v1_1 sample to understand the column_name format
  - canonical_detail_pointer_v1 88 colliding columns + their feeders
  - existence of feeder tables referenced in the user's verdicts
  - molecular_variant_long PARSE_ERROR_FUSION_FULLTEXT count
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
LOG_PATH = REPO / "scripts" / "output" / "265_probe2.log"


def main() -> int:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    fh = LOG_PATH.open("w", encoding="utf-8")

    def log(msg: str) -> None:
        line = f"[{datetime.now(timezone.utc).isoformat()}] {msg}"
        print(line, flush=True)
        fh.write(line + "\n")
        fh.flush()

    con = connect_locked()
    PD = PUBLICATION_DB

    log("=== detail_table_registry_v1 schema ===")
    cols = con.execute(f"""
        SELECT column_name, data_type FROM information_schema.columns
        WHERE table_catalog='{PD}' AND table_schema='manuscript_workspace'
          AND table_name='detail_table_registry_v1'
        ORDER BY ordinal_position
    """).fetchall()
    for c in cols:
        log(f"  {c[0]}  {c[1]}")
    has_secondary = any(c[0] == "feeds_master_columns_secondary" for c in cols)
    log(f"feeds_master_columns_secondary present: {has_secondary}")

    log("\n=== detail_table_registry_v1 sample (3 rows) ===")
    rows = con.execute(f"""
        SELECT * FROM {PD}.manuscript_workspace.detail_table_registry_v1 LIMIT 3
    """).fetchall()
    col_names = [c[0] for c in cols]
    for r in rows:
        log("---")
        for k, v in zip(col_names, r):
            sv = (str(v)[:120] + "…") if v is not None and len(str(v)) > 120 else str(v)
            log(f"  {k}: {sv}")

    log("\n=== nan_string_audit_v1_1 first 10 rows ===")
    rows = con.execute(f"""
        SELECT * FROM {PD}.manuscript_workspace.nan_string_audit_v1_1
        ORDER BY n_literal_nan DESC LIMIT 10
    """).fetchall()
    nan_cols = [c[0] for c in con.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_catalog='{PD}' AND table_schema='manuscript_workspace'
          AND table_name='nan_string_audit_v1_1' ORDER BY ordinal_position
    """).fetchall()]
    for r in rows:
        log(" | ".join(f"{k}={v}" for k, v in zip(nan_cols, r)))

    log("\n=== nan_string_audit_v1_1 distinct column_name samples ===")
    ex = con.execute(f"""
        SELECT column_name FROM {PD}.manuscript_workspace.nan_string_audit_v1_1
        ORDER BY column_name LIMIT 20
    """).fetchall()
    for r in ex:
        log(f"  {r[0]}")

    log("\n=== nan_string_audit_v1_1 group by repair_action ===")
    rows = con.execute(f"""
        SELECT repair_action, COUNT(*), SUM(n_literal_nan), SUM(n_true_null)
        FROM {PD}.manuscript_workspace.nan_string_audit_v1_1
        GROUP BY repair_action ORDER BY 2 DESC
    """).fetchall()
    for r in rows:
        log(f"  action={r[0]!r:<30}  n_rows={r[1]}  sum_literal_nan={r[2]}  sum_null={r[3]}")

    log("\n=== 88 colliding master_columns + their feeders ===")
    rows = con.execute(f"""
        WITH c AS (
          SELECT master_column, COUNT(DISTINCT detail_table_name) AS n_feeders,
                 STRING_AGG(DISTINCT detail_table_name, ' || ') AS feeders
          FROM {PD}.manuscript_workspace.canonical_detail_pointer_v1
          GROUP BY master_column
        )
        SELECT master_column, n_feeders, feeders
        FROM c WHERE n_feeders > 1
        ORDER BY n_feeders DESC, master_column
    """).fetchall()
    log(f"  total colliding: {len(rows)}")
    for r in rows:
        log(f"  {r[0]:<55}  n={r[1]}  feeders={r[2]}")

    log("\n=== existence check for verdict feeder tables ===")
    feeders = [
        "canonical_molecular_tested_v1", "_molecular_patient_rollup_v227",
        "molecular_test_episode_v2", "extracted_braf_recovery_v1",
        "tg_timeline_patient_summary_v1", "tg_postop_surveillance_windows_v1",
        "thyroglobulin_lab_canonical_v1", "imaging_patient_summary_v1",
        "canonical_us_nodule_characteristics_v1", "canonical_malignant_diagnosis_v1",
        "tumor_episode_master_v2", "synoptic_tumor_long_v1", "tumor_pathology",
        "patient_tumor_rollup_v1", "extracted_fna_bethesda_v1",
        "fna_episode_master_v2", "complication_phenotype_v1",
        "vc_paralysis_recalibration_v236", "ete_adjudication_v1",
        "canonical_recurrence_v1", "recurrence_event_clean_v1",
        "ret_patient_adjudicated_v226",
        "molecular_variant_long",
    ]
    for t in feeders:
        n = con.execute(f"""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_catalog='{PD}' AND table_schema='main' AND table_name='{t}'
        """).fetchone()[0]
        log(f"  {t:<45}  exists_in_main: {bool(n)}")

    log("\n=== molecular_variant_long PARSE_ERROR_FUSION_FULLTEXT counts ===")
    try:
        rows = con.execute(f"""
            SELECT variant_class, COUNT(*),
                   COUNT(*) FILTER (WHERE gene_symbol IS NULL) AS n_null_gene
            FROM {PD}.main.molecular_variant_long
            WHERE variant_class LIKE '%FUSION%' OR variant_class LIKE '%PARSE%'
            GROUP BY variant_class ORDER BY 2 DESC
        """).fetchall()
        for r in rows:
            log(f"  variant_class={r[0]!r:<40}  n={r[1]}  n_null_gene={r[2]}")
    except Exception as e:
        log(f"  ERROR: {e}")

    log("\n=== CPM mol_n_fusions / any_fusion_positive coverage ===")
    rows = con.execute(f"""
        SELECT
          COUNT(*) AS total,
          COUNT(*) FILTER (WHERE mol_n_fusions IS NULL) AS null_n_fusions,
          COUNT(*) FILTER (WHERE mol_n_fusions = 0) AS zero_n_fusions,
          COUNT(*) FILTER (WHERE mol_n_fusions > 0) AS pos_n_fusions,
          COUNT(*) FILTER (WHERE any_fusion_positive IS NULL) AS null_flag,
          COUNT(*) FILTER (WHERE any_fusion_positive = TRUE) AS true_flag,
          COUNT(*) FILTER (WHERE any_fusion_positive = FALSE) AS false_flag
        FROM {PD}.main.canonical_patient_master
    """).fetchone()
    log(f"  total={rows[0]}  null_n_fusions={rows[1]}  zero={rows[2]}  pos={rows[3]}")
    log(f"  null_flag={rows[4]}  true_flag={rows[5]}  false_flag={rows[6]}")

    log("\n=== mol_n_fusions current derivation pointer (registry hint) ===")
    try:
        rows = con.execute(f"""
            SELECT detail_table_name, schema_name, feeds_master_columns_normalized
            FROM {PD}.manuscript_workspace.detail_table_registry_v1
            WHERE feeds_master_columns_normalized LIKE '%mol_n_fusions%'
               OR feeds_master_columns_normalized LIKE '%any_fusion_positive%'
        """).fetchall()
        for r in rows:
            log(f"  detail={r[0]} schema={r[1]} feeds_includes_fusion=YES")
    except Exception as e:
        log(f"  ERROR: {e}")

    log("\n=== molecular_test_episode_v2 row count + RID 7744 detail ===")
    try:
        n = con.execute(
            f"SELECT COUNT(*) FROM {PD}.main.molecular_test_episode_v2"
        ).fetchone()[0]
        log(f"  total rows: {n}")
        cols = con.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_catalog='{PD}' AND table_schema='main'
              AND table_name='molecular_test_episode_v2'
            ORDER BY ordinal_position
        """).fetchall()
        log(f"  columns: {[c[0] for c in cols]}")
    except Exception as e:
        log(f"  ERROR: {e}")

    log("\n=== Thyroid 2026 UPdated.main candidate stale tables ===")
    try:
        rows = con.execute("""
            SELECT table_name FROM duckdb_tables()
            WHERE database_name='Thyroid 2026 UPdated' AND schema_name='main'
              AND (
                table_name = 'canonical_patient_master_v1'
                OR table_name LIKE '%_v225'
                OR table_name LIKE 'thyroid_ete_fix_%'
                OR table_name LIKE '%_pre_v1_0_%'
              )
            ORDER BY table_name
        """).fetchall()
        log(f"  candidates count: {len(rows)}")
        for r in rows:
            log(f"  {r[0]}")
    except Exception as e:
        log(f"  ERROR: {e}")

    fh.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
