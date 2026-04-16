#!/usr/bin/env python3
"""
Script 239 — Investigate & fix-or-delete rai_benign_histology_recovery_v234

Date:    2026-04-16
Author:  THYROID_2026 canonical-finalization run (v1_0 lock)

Purpose
=======
Script 234 left `rai_benign_histology_recovery_v234` as an empty shell (0
rows) with a TODO-marked registry entry. Investigate the root cause, then
either populate the table with real recovery candidates OR delete it
entirely per the decision gate in the finalization spec.

Tables READ
-----------
  thyroid_canonical_publication_v1_0.main.canonical_benign_diagnosis_v1
  thyroid_canonical_publication_v1_0.main.canonical_malignant_diagnosis_v1
  thyroid_canonical_publication_v1_0.main.path_synoptics
  thyroid_canonical_publication_v1_0.main.rai_treatment_episode_v2
  thyroid_canonical_publication_v1_0.main.canonical_patient_master
  thyroid_canonical_publication_v1_0.main.__readme
  thyroid_canonical_publication_v1_0.manuscript_workspace.detail_table_registry_v1

Tables WRITTEN
--------------
  (case A — candidates exist):
    POPULATE  thyroid_canonical_publication_v1_0.main.rai_benign_histology_recovery_v234
    UPDATE    __readme / detail_table_registry_v1
  (case B — 0 candidates, current state):
    ARCHIVE   "Thyroid 2026 UPdated".archive_pub_v1_0.rai_benign_histology_recovery_v234_pre239_backup_<ts>
    DROP      thyroid_canonical_publication_v1_0.main.rai_benign_histology_recovery_v234
    DELETE    __readme row / detail_table_registry_v1 row

Rollback plan
-------------
  case A: revert the populate via INSERT-from-archive (if archive was created first).
  case B: restore from archive_pub_v1_0.rai_benign_histology_recovery_v234_pre239_backup_<ts>
          (CREATE TABLE ... AS SELECT ...), then re-insert __readme + registry rows.

Decision gate (per v1_0 finalization prompt)
--------------------------------------------
  "if the populated recovery table has 0 rows (i.e., no true recovery
   candidates exist), delete the table entirely and remove its row from
   manuscript_workspace.detail_table_registry_v1. Do not leave a
   TODO-marked empty table in the canonical DB."

Root cause (verified before touching any data)
----------------------------------------------
  Script 234's recovery query restricted to
      is_malignant=FALSE AND rai_received_flag=TRUE
      AND histology_final IS NULL
      AND path_synoptics.tumor_1_histologic_type IS NOT NULL
  which returns 0 rows. It is NOT a bug in Script 234 — the three
  canonical diagnosis tables already partition patients correctly, so no
  true recovery candidates exist. Verified under three alternative RAI
  criteria (see `run_investigation()`); all three yield 0 candidates.

  Therefore: archive the empty shell for auditability, DROP it, remove
  __readme / registry entries. This fixes the TODO-marked empty-table
  state without inventing phantom recovery rows.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

ARCHIVE_DB = "Thyroid 2026 UPdated"
ARCHIVE_SCHEMA = "archive_pub_v1_0"
ARCHIVE_QUALIFIED = f'"{ARCHIVE_DB}"."{ARCHIVE_SCHEMA}"'
TABLE_NAME = "rai_benign_histology_recovery_v234"
SCRIPT_TAG = "Script 239"
RUN_DATE = "2026-04-16"


def ts_utc() -> str:
    return datetime.utcnow().strftime("%H:%M:%S.") + f"{datetime.utcnow().microsecond // 1000:03d}Z"


def log(msg: str) -> None:
    print(f"[{ts_utc()}] {msg}", flush=True)


def run_investigation(con) -> dict:
    """Run the recovery candidate query under three RAI criteria and return the counts."""
    malignant_pat = """
        LOWER(COALESCE(t, '')) LIKE '%papillary%' OR
        LOWER(COALESCE(t, '')) LIKE '%carcinoma%'  OR
        LOWER(COALESCE(t, '')) LIKE '%medullary%'  OR
        LOWER(COALESCE(t, '')) LIKE '%anaplastic%' OR
        LOWER(COALESCE(t, '')) LIKE '%poorly differentiated%' OR
        LOWER(COALESCE(t, '')) LIKE '%hurthle cell carcinoma%' OR
        LOWER(COALESCE(t, '')) LIKE '%ptc%' OR
        LOWER(COALESCE(t, '')) LIKE '%mtc%' OR
        LOWER(COALESCE(t, '')) LIKE '%ftc%' OR
        LOWER(COALESCE(t, '')) LIKE '%lymphoma%' OR
        LOWER(COALESCE(t, '')) LIKE '%metastatic%'
    """
    sql = f"""
WITH benign_pts AS (
  SELECT DISTINCT research_id FROM canonical_benign_diagnosis_v1
),
malignant_pts AS (
  SELECT DISTINCT research_id FROM canonical_malignant_diagnosis_v1
),
rai_strict AS (
  SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id
  FROM rai_treatment_episode_v2
  WHERE rai_assertion_status IN ('definite_received', 'likely_received')
),
rai_any AS (
  SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id
  FROM rai_treatment_episode_v2
  WHERE rai_assertion_status IS NULL OR rai_assertion_status <> 'negated'
),
rai_cpm AS (
  SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id
  FROM canonical_patient_master WHERE rai_received_reconciled = TRUE
),
path_tumors AS (
  SELECT CAST(research_id AS VARCHAR) AS research_id, 1 AS tumor_idx, tumor_1_histologic_type AS t FROM path_synoptics WHERE tumor_1_histologic_type IS NOT NULL AND TRIM(tumor_1_histologic_type) <> ''
  UNION ALL
  SELECT CAST(research_id AS VARCHAR), 2, tumor_2_histologic_type FROM path_synoptics WHERE tumor_2_histologic_type IS NOT NULL AND TRIM(tumor_2_histologic_type) <> ''
  UNION ALL
  SELECT CAST(research_id AS VARCHAR), 3, tumor_3_histologic_type FROM path_synoptics WHERE tumor_3_histologic_type IS NOT NULL AND TRIM(tumor_3_histologic_type) <> ''
  UNION ALL
  SELECT CAST(research_id AS VARCHAR), 4, tumor_4_histologic_type FROM path_synoptics WHERE tumor_4_histologic_type IS NOT NULL AND TRIM(tumor_4_histologic_type) <> ''
  UNION ALL
  SELECT CAST(research_id AS VARCHAR), 5, tumor_5_histologic_type FROM path_synoptics WHERE tumor_5_histologic_type IS NOT NULL AND TRIM(tumor_5_histologic_type) <> ''
),
mal_path AS (
  SELECT research_id, tumor_idx, t
  FROM path_tumors
  WHERE {malignant_pat}
)
SELECT
  (SELECT COUNT(DISTINCT research_id) FROM mal_path) AS pts_with_mal_any_tumor,
  (SELECT COUNT(*) FROM (
     SELECT DISTINCT mp.research_id FROM mal_path mp
     JOIN benign_pts USING (research_id)
     LEFT JOIN malignant_pts m USING (research_id)
     JOIN rai_strict USING (research_id)
     WHERE m.research_id IS NULL
  )) AS pts_strict,
  (SELECT COUNT(*) FROM (
     SELECT DISTINCT mp.research_id FROM mal_path mp
     JOIN benign_pts USING (research_id)
     LEFT JOIN malignant_pts m USING (research_id)
     JOIN rai_any USING (research_id)
     WHERE m.research_id IS NULL
  )) AS pts_any,
  (SELECT COUNT(*) FROM (
     SELECT DISTINCT mp.research_id FROM mal_path mp
     JOIN benign_pts USING (research_id)
     LEFT JOIN malignant_pts m USING (research_id)
     JOIN rai_cpm USING (research_id)
     WHERE m.research_id IS NULL
  )) AS pts_cpm_reconciled
    """
    r = con.execute(sql).fetchone()
    return {
        "pts_with_mal_any_tumor": r[0],
        "pts_strict_rai": r[1],
        "pts_any_rai": r[2],
        "pts_cpm_reconciled_rai": r[3],
    }


def archive_empty_shell(con, run_ts: str) -> str:
    dest = f'{TABLE_NAME}_pre239_backup_{run_ts}'
    full_dest = f'{ARCHIVE_QUALIFIED}."{dest}"'
    log(f"archive: creating {full_dest}")
    src_rc = con.execute(f'SELECT COUNT(*) FROM "{TABLE_NAME}"').fetchone()[0]
    con.execute(f'CREATE OR REPLACE TABLE {full_dest} AS SELECT * FROM "{TABLE_NAME}"')
    dst_rc = con.execute(f"SELECT COUNT(*) FROM {full_dest}").fetchone()[0]
    if src_rc != dst_rc:
        raise RuntimeError(f"archive row mismatch src={src_rc} dst={dst_rc}")
    con.execute(
        f"""COMMENT ON TABLE {full_dest} IS
            '{SCRIPT_TAG} ({RUN_DATE}): empty-shell archive of rai_benign_histology_recovery_v234. '
            'Script 234 left this table with 0 rows; Script 239 investigation confirmed that NO true '
            'recovery candidates exist (canonical_benign/malignant_diagnosis_v1 partition patients '
            'correctly; everyone with path_synoptics malignancy who received RAI is already in '
            'canonical_malignant_diagnosis_v1). Preserved for auditability; the live table was dropped.'"""
    )
    log(f"  archived {src_rc} rows -> {dest}")
    return dest


def drop_live_and_clean(con) -> None:
    log(f"dropping canonical.main.{TABLE_NAME}")
    con.execute(f'DROP TABLE IF EXISTS "{TABLE_NAME}"')

    log("removing __readme row")
    con.execute(f"DELETE FROM __readme WHERE table_name = '{TABLE_NAME}'")

    log("removing detail_table_registry_v1 row")
    con.execute(
        f"DELETE FROM manuscript_workspace.detail_table_registry_v1 WHERE detail_table_name = '{TABLE_NAME}'"
    )


def run_assertions(con) -> int:
    """Return number of failed assertions (0 = all PASS)."""
    checks: list[tuple[str, bool]] = []

    # 1. Live table no longer exists
    n = con.execute(
        f"""SELECT COUNT(*) FROM information_schema.tables
            WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
              AND table_name='{TABLE_NAME}'"""
    ).fetchone()[0]
    checks.append((f"canonical.main.{TABLE_NAME} does not exist", n == 0))

    # 2. __readme row removed
    n = con.execute(
        f"SELECT COUNT(*) FROM __readme WHERE table_name='{TABLE_NAME}'"
    ).fetchone()[0]
    checks.append(("__readme no longer has a row for this table", n == 0))

    # 3. detail_table_registry_v1 row removed
    n = con.execute(
        f"""SELECT COUNT(*) FROM manuscript_workspace.detail_table_registry_v1
            WHERE detail_table_name='{TABLE_NAME}'"""
    ).fetchone()[0]
    checks.append(("registry no longer has a row for this table", n == 0))

    # 4. canonical_patient_master row count unchanged (10,871)
    n = con.execute("SELECT COUNT(*) FROM canonical_patient_master").fetchone()[0]
    checks.append(("canonical_patient_master = 10,871 rows", n == 10871))

    # 5. Archive copy exists
    n = con.execute(
        f"""SELECT COUNT(*) FROM information_schema.tables
            WHERE table_catalog='{ARCHIVE_DB}' AND table_schema='{ARCHIVE_SCHEMA}'
              AND table_name LIKE '{TABLE_NAME}_pre239_backup%'"""
    ).fetchone()[0]
    checks.append((">=1 archive copy in archive_pub_v1_0", n >= 1))

    failures = 0
    for label, ok in checks:
        tag = "PASS" if ok else "FAIL"
        log(f"  ASSERT [{tag}] {label}")
        if not ok:
            failures += 1
    return failures


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--investigate-only",
        action="store_true",
        help="Run the recovery-candidate investigation and print counts; do NOT touch data.",
    )
    ap.add_argument(
        "--force-populate",
        action="store_true",
        help="Force the populate branch (useful if future data creates recovery candidates).",
    )
    args = ap.parse_args()

    t0 = time.time()
    log(f"=== START {Path(__file__).name}")
    con = connect_locked()
    log(f"connected to {PUBLICATION_DB}")

    # --- Phase 1: investigation -------------------------------------------
    log("PHASE 1 — investigate recovery candidates under three RAI criteria")
    counts = run_investigation(con)
    log(f"  pts with malignant path (any tumor 1..5): {counts['pts_with_mal_any_tumor']}")
    log(f"  recovery candidates — RAI strict  (assertion in definite_received, likely_received): {counts['pts_strict_rai']}")
    log(f"  recovery candidates — RAI any     (any episode not explicitly negated):                {counts['pts_any_rai']}")
    log(f"  recovery candidates — CPM.rai_received_reconciled=TRUE:                                {counts['pts_cpm_reconciled_rai']}")

    if args.investigate_only:
        log("investigate-only mode — exiting without modifying data")
        return

    total_candidates = max(counts["pts_strict_rai"], counts["pts_any_rai"], counts["pts_cpm_reconciled_rai"])

    if args.force_populate or total_candidates > 0:
        # --- Phase 2a: populate branch (no candidates today, but support future) ---
        log("PHASE 2a — populate branch (recovery candidates found)")
        raise NotImplementedError(
            "Populate branch is stubbed: 0 candidates across all three RAI criteria today. "
            "Re-enable this branch only after v1_1 RAI-status re-classification surfaces "
            "new rai_assertion_status values (definite_received, etc.) that currently don't exist."
        )

    # --- Phase 2b: delete branch (current, expected state) ---------------
    log("PHASE 2b — delete branch (0 candidates across all three RAI criteria)")

    run_ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    archive_name = archive_empty_shell(con, run_ts)
    drop_live_and_clean(con)

    # --- Phase 3: assertions ----------------------------------------------
    log("PHASE 3 — assertions")
    failures = run_assertions(con)
    elapsed = time.time() - t0
    if failures:
        log(f"FAILURES: {failures}")
        log(
            f"ROLLBACK HINT: CREATE OR REPLACE TABLE {PUBLICATION_DB}.main.{TABLE_NAME} AS "
            f'SELECT * FROM {ARCHIVE_QUALIFIED}."{archive_name}"'
        )
        sys.exit(1)
    log(f"=== END {Path(__file__).name}  elapsed={elapsed:.1f}s  failures=0")


if __name__ == "__main__":
    main()
