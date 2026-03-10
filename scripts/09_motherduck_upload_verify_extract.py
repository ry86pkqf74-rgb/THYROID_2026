#!/usr/bin/env python3
"""
09_motherduck_upload_verify_extract.py — Phase 6 Post-Integration Pipeline

Performs in order:
  1. Git commit & push (new Parquet DVC pointers, data dictionary, reports)
  2. DVC tracking of 9 new Parquet files
  3. MotherDuck upload (raw XLSX → raw_ tables, cleaned Parquet → tables, view)
  4. Verification queries (row counts, coverage, samples)
  5. Optional: improved follow-up date extraction → extracted_clinical_events_v2
  6. Output verify_md.txt summary

Usage:
  python scripts/09_motherduck_upload_verify_extract.py
  python scripts/09_motherduck_upload_verify_extract.py --dry-run
  python scripts/09_motherduck_upload_verify_extract.py --skip-git --skip-dvc
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
import textwrap
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("phase6_upload")

ROOT = Path(__file__).resolve().parent.parent
PROCESSED = ROOT / "processed"
RAW_LOCAL = Path.home() / "Downloads" / "Active Master Files"
VERIFY_FILE = ROOT / "verify_md.txt"

NEW_TABLES = [
    "complications",
    "molecular_testing",
    "operative_details",
    "fna_history",
    "us_nodules_tirads",
    "serial_imaging_us",
    "path_synoptics",
    "clinical_notes",
    "extracted_clinical_events",
]

RAW_XLSX_SOURCES = {
    "raw_complications":      ("Thyroid all_Complications 12_1_25.xlsx",   "Complications"),
    "raw_molecular_testing":  ("THYROSEQ_AFIRMA_12_5.xlsx",               "Thyroseq and AFIRMA"),
    "raw_operative_details":  ("Thyroid OP Sheet data.xlsx",               "Physical OP sheet data"),
    "raw_fna_history":        ("FNAs 12_5_2025.xlsx",                      "FNA Bethesda"),
    "raw_us_nodules_tirads":  ("US Nodules TIRADS 12_1_25.xlsx",           None),
    "raw_serial_imaging":     ("Imaging_12_1_25.xlsx",                     None),
    "raw_path_synoptics":     ("All Diagnoses & synoptic 12_1_2025.xlsx",  "synoptics + Dx merged"),
    "raw_clinical_notes":     ("Notes 12_1_25.xlsx",                       None),
}

MD_DATABASE = "thyroid_research_2026"

ADVANCED_V2_SQL = textwrap.dedent("""\
    CREATE OR REPLACE VIEW advanced_features_v2 AS
    SELECT
        mc.research_id,
        mc.age_at_surgery,
        mc.sex,
        mc.surgery_date,
        tp.histology_1_type,
        tp.variant_standardized,
        tp.surgery_type_normalized,
        tp.histology_1_overall_stage_ajcc8 AS overall_stage_ajcc8,
        TRY_CAST(tp.histology_1_largest_tumor_cm AS DOUBLE) AS largest_tumor_cm,
        TRY_CAST(tp.histology_1_ln_examined AS DOUBLE) AS ln_examined,
        TRY_CAST(tp.histology_1_ln_positive AS DOUBLE) AS ln_positive,
        tp.tumor_1_extrathyroidal_ext,
        tp.tumor_1_gross_ete,
        comp.rln_injury_or_vocal_cord_paralysis_vocal_cord_palsy AS rln_injury_vocal_cord_paralysis,
        comp.vocal_cord_status,
        comp.seroma,
        comp.hematoma,
        comp.hypocalcemia,
        comp.hypoparathyroidism,
        od.ebl,
        od.skin_skin_time_min AS skin_to_skin_time,
        bp.is_mng,
        bp.is_graves,
        bp.is_follicular_adenoma,
        bp.is_hashimoto,
        tp.braf_mutation_mentioned,
        tp.ras_mutation_mentioned,
        tp.ret_mutation_mentioned,
        tp.tert_mutation_mentioned,
        CASE WHEN comp.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_complications,
        CASE WHEN od.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_operative_details,
        CASE WHEN cn.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_clinical_notes,
        CASE WHEN ps.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_path_synoptics,
        CASE WHEN mt_agg.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_molecular_testing,
        CASE WHEN fh_agg.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_fna_history,
        CASE WHEN unt_agg.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_us_tirads,
        mc.has_tumor_pathology,
        mc.has_benign_pathology,
        mc.has_fna_cytology,
        mc.has_ultrasound_reports,
        mc.has_ct_imaging,
        mc.has_mri_imaging,
        mc.has_nuclear_med,
        mc.has_thyroglobulin_labs,
        mc.has_anti_thyroglobulin_labs,
        mc.has_parathyroid
    FROM master_cohort mc
    LEFT JOIN tumor_pathology tp      ON mc.research_id = tp.research_id
    LEFT JOIN benign_pathology bp     ON mc.research_id = bp.research_id
    LEFT JOIN complications comp      ON mc.research_id = CAST(comp.research_id AS VARCHAR)
    LEFT JOIN operative_details od    ON mc.research_id = CAST(od.research_id AS VARCHAR)
    LEFT JOIN clinical_notes cn       ON mc.research_id = CAST(cn.research_id AS VARCHAR)
    LEFT JOIN path_synoptics ps       ON mc.research_id = CAST(ps.research_id AS VARCHAR)
    LEFT JOIN (SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id FROM molecular_testing) mt_agg
        ON mc.research_id = mt_agg.research_id
    LEFT JOIN (SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id FROM fna_history) fh_agg
        ON mc.research_id = fh_agg.research_id
    LEFT JOIN (SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id FROM us_nodules_tirads) unt_agg
        ON mc.research_id = unt_agg.research_id
""")

FOLLOWUP_V2_SQL = textwrap.dedent("""\
    CREATE OR REPLACE TABLE extracted_clinical_events_v2 AS
    WITH base AS (
        SELECT * FROM extracted_clinical_events
    ),
    raw_notes_flat AS (
        SELECT
            CAST("Research ID number" AS VARCHAR) AS research_id,
            col_name,
            col_text
        FROM (
            SELECT * FROM raw_clinical_notes
        )
        UNPIVOT (col_text FOR col_name IN (COLUMNS(* EXCLUDE "Research ID number")))
        WHERE col_text IS NOT NULL
    ),
    new_dates AS (
        SELECT
            research_id,
            'follow_up' AS event_type,
            'follow_up_date' AS event_subtype,
            NULL::DOUBLE AS event_value,
            NULL::VARCHAR AS event_unit,
            CASE
                WHEN try_strptime(
                    regexp_extract(col_text, '(\\d{1,2}/\\d{1,2}/\\d{2,4})', 1),
                    '%m/%d/%Y'
                ) IS NOT NULL
                THEN strftime(
                    try_strptime(
                        regexp_extract(col_text, '(\\d{1,2}/\\d{1,2}/\\d{2,4})', 1),
                        '%m/%d/%Y'
                    ),
                    '%Y-%m-%d'
                )
                WHEN try_strptime(
                    regexp_extract(col_text, '(\\d{1,2}/\\d{1,2}/\\d{2,4})', 1),
                    '%m/%d/%y'
                ) IS NOT NULL
                THEN strftime(
                    try_strptime(
                        regexp_extract(col_text, '(\\d{1,2}/\\d{1,2}/\\d{2,4})', 1),
                        '%m/%d/%y'
                    ),
                    '%Y-%m-%d'
                )
                ELSE NULL
            END AS event_date,
            regexp_extract(col_text,
                '(?:follow[\\s-]*up|f/u|seen\\s+on|visit\\s+on|return\\s+on|next\\s+appointment)'
                '.*?(\\d{1,2}/\\d{1,2}/\\d{2,4})', 0
            ) AS event_text,
            'raw_notes_v2_regex' AS source_column
        FROM raw_notes_flat
        WHERE regexp_matches(col_text,
              '(?:follow[\\s-]*up|f/u|seen\\s+on|visit\\s+on|return\\s+on|next\\s+appointment)'
              '.*?\\d{1,2}/\\d{1,2}/\\d{2,4}')
    )
    SELECT * FROM base
    UNION ALL
    SELECT
        CAST(research_id AS INT) AS research_id,
        event_type, event_subtype, event_value, event_unit,
        event_date, event_text, source_column
    FROM new_dates
    WHERE event_date IS NOT NULL
      AND CAST(research_id AS INT) NOT IN (
          SELECT DISTINCT CAST(research_id AS INT)
          FROM base
          WHERE event_type = 'follow_up'
      )
""")


class VerifyLog:
    """Accumulates verification output for verify_md.txt."""

    def __init__(self):
        self.lines: list[str] = []
        self.pass_count = 0
        self.fail_count = 0

    def section(self, title: str) -> None:
        self.lines.append(f"\n{'='*72}")
        self.lines.append(f"  {title}")
        self.lines.append(f"{'='*72}\n")

    def record(self, label: str, passed: bool, detail: str = "") -> None:
        tag = "PASS" if passed else "FAIL"
        if passed:
            self.pass_count += 1
        else:
            self.fail_count += 1
        msg = f"[{tag}] {label}"
        if detail:
            msg += f"\n       {detail}"
        self.lines.append(msg)
        log.info(msg)

    def text(self, msg: str) -> None:
        self.lines.append(msg)
        log.info(msg)

    def write(self, path: Path) -> None:
        header = (
            f"THYROID_2026 — MotherDuck Verification Report\n"
            f"Generated: {datetime.now().isoformat()}\n"
            f"Results: {self.pass_count} passed, {self.fail_count} failed\n"
        )
        path.write_text(header + "\n".join(self.lines) + "\n")
        log.info(f"Verification report → {path}")


def _run(cmd: list[str], *, dry_run: bool = False, check: bool = True,
         cwd: Path | None = None) -> subprocess.CompletedProcess:
    display = " ".join(cmd)
    if dry_run:
        log.info(f"[DRY RUN] {display}")
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
    log.info(f"$ {display}")
    return subprocess.run(
        cmd, capture_output=True, text=True, cwd=cwd or ROOT, check=check,
    )


# ═══════════════════════════════════════════════════════════════════
#  PHASE 1: GIT COMMIT & PUSH
# ═══════════════════════════════════════════════════════════════════

def phase1_git(dry_run: bool) -> None:
    log.info("\n" + "="*72)
    log.info("  PHASE 1: Git Commit & Push")
    log.info("="*72)

    files_to_add = [
        "data_dictionary.csv",
        "data_dictionary.md",
        "integration_report.csv",
        "integrate_missing_sources.py",
        "scripts/08_integrate_missing_sources.py",
    ]

    dvc_pointers = list(PROCESSED.glob("*.parquet.dvc"))
    for p in dvc_pointers:
        files_to_add.append(str(p.relative_to(ROOT)))

    gitignore_processed = PROCESSED / ".gitignore"
    if gitignore_processed.exists():
        files_to_add.append(str(gitignore_processed.relative_to(ROOT)))

    existing = [f for f in files_to_add if (ROOT / f).exists()]
    if not existing:
        log.info("No files to commit.")
        return

    for f in existing:
        _run(["git", "add", f], dry_run=dry_run)

    status = _run(["git", "status", "--porcelain"], dry_run=False)
    staged = [l for l in status.stdout.splitlines() if l.startswith(("A ", "M ", "R "))]
    if not staged and not dry_run:
        log.info("Nothing new to commit — all files already tracked.")
        return

    commit_msg = (
        "feat: integrate 8 raw Excel sources + NLP extraction "
        "(complications, molecular, FNA, US, synoptics, notes)"
    )
    _run(["git", "commit", "-m", commit_msg], dry_run=dry_run, check=False)

    if dry_run:
        log.info("[DRY RUN] Would prompt for git push confirmation")
        return

    answer = input("\nPush to origin/main? [y/N] ").strip().lower()
    if answer in ("y", "yes"):
        result = _run(["git", "push", "origin", "HEAD"], dry_run=dry_run, check=False)
        if result.returncode == 0:
            log.info("Pushed to remote successfully.")
        else:
            log.warning(f"Push failed: {result.stderr}")
    else:
        log.info("Push skipped by user.")


# ═══════════════════════════════════════════════════════════════════
#  PHASE 2: DVC TRACKING
# ═══════════════════════════════════════════════════════════════════

def phase2_dvc(dry_run: bool) -> None:
    log.info("\n" + "="*72)
    log.info("  PHASE 2: DVC Tracking")
    log.info("="*72)

    new_parquets = [PROCESSED / f"{t}.parquet" for t in NEW_TABLES]
    existing_parquets = [p for p in new_parquets if p.exists()]

    if not existing_parquets:
        log.warning("No new Parquet files found to DVC-track.")
        return

    for pq_path in existing_parquets:
        dvc_file = Path(str(pq_path) + ".dvc")
        if dvc_file.exists():
            log.info(f"  Already DVC-tracked: {pq_path.name}")
            continue
        try:
            result = _run(["dvc", "add", str(pq_path)], dry_run=dry_run, check=False)
            if result.returncode != 0 and not dry_run:
                log.warning(f"  dvc add failed for {pq_path.name}: {result.stderr.strip()}")
            else:
                log.info(f"  DVC-tracked: {pq_path.name}")
        except FileNotFoundError:
            log.warning("DVC not found on PATH. Install with `pip install dvc`.")
            return

    _run(["git", "add", "processed/*.parquet.dvc", "processed/.gitignore"],
         dry_run=dry_run, check=False)
    _run(["git", "commit", "-m", "chore: dvc-track 9 new Phase 6 parquet files"],
         dry_run=dry_run, check=False)

    try:
        remote_check = _run(["dvc", "remote", "list"], dry_run=False, check=False)
        if remote_check.stdout.strip():
            log.info("DVC remote detected — pushing...")
            _run(["dvc", "push"], dry_run=dry_run, check=False)
        else:
            log.warning(
                "No DVC remote configured. Run `dvc remote add -d <name> <url>` "
                "then `dvc push` to store Parquet files externally."
            )
    except FileNotFoundError:
        log.warning("DVC not found on PATH. Install with `pip install dvc` to enable tracking.")


# ═══════════════════════════════════════════════════════════════════
#  PHASE 3: MOTHERDUCK UPLOAD
# ═══════════════════════════════════════════════════════════════════

def _get_md_connection():
    import duckdb

    token = os.getenv("MOTHERDUCK_TOKEN")
    if not token:
        raise RuntimeError(
            "MOTHERDUCK_TOKEN not set. Export it before running this script."
        )
    con = duckdb.connect(f"md:{MD_DATABASE}?motherduck_token={token}")
    return con


def phase3_motherduck_upload(dry_run: bool) -> None:
    log.info("\n" + "="*72)
    log.info("  PHASE 3: MotherDuck Upload")
    log.info("="*72)

    if dry_run:
        log.info("[DRY RUN] Would upload raw XLSX + cleaned Parquet to MotherDuck")
        return

    try:
        con = _get_md_connection()
    except Exception as exc:
        log.error(f"MotherDuck connection failed: {exc}")
        return

    # -- 3a: Install Excel extension ---
    try:
        con.execute("INSTALL excel; LOAD excel;")
        log.info("Excel extension loaded.")
    except Exception as exc:
        log.warning(f"Excel extension issue (may already be loaded): {exc}")
        try:
            con.execute("LOAD excel;")
        except Exception:
            pass

    # -- 3b: Upload raw XLSX as raw_ tables ---
    log.info("\n--- Uploading raw XLSX tables ---")
    for table_name, (xlsx_file, sheet) in RAW_XLSX_SOURCES.items():
        xlsx_path = RAW_LOCAL / xlsx_file
        if not xlsx_path.exists():
            alt = ROOT / "raw" / xlsx_file
            if alt.exists():
                xlsx_path = alt
            else:
                log.warning(f"  Skipping {table_name}: {xlsx_file} not found")
                continue

        try:
            escaped_path = str(xlsx_path).replace("'", "''")
            if sheet:
                sql = (
                    f"CREATE OR REPLACE TABLE {table_name} AS "
                    f"SELECT * FROM read_xlsx('{escaped_path}', "
                    f"sheet='{sheet}', all_varchar=true, ignore_errors=true)"
                )
            else:
                sql = (
                    f"CREATE OR REPLACE TABLE {table_name} AS "
                    f"SELECT * FROM read_xlsx('{escaped_path}', "
                    f"all_varchar=true, ignore_errors=true)"
                )
            con.execute(sql)
            count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            log.info(f"  {table_name}: {count:,} rows uploaded from {xlsx_file}")
        except Exception as exc:
            log.error(f"  FAILED {table_name}: {exc}")

    # -- 3c: Upload cleaned Parquet as permanent tables ---
    log.info("\n--- Uploading cleaned Parquet tables ---")
    for tbl in NEW_TABLES:
        pq_path = PROCESSED / f"{tbl}.parquet"
        if not pq_path.exists():
            log.warning(f"  Skipping {tbl}: Parquet not found")
            continue

        try:
            escaped = str(pq_path).replace("'", "''")
            con.execute(
                f"CREATE OR REPLACE TABLE {tbl} AS "
                f"SELECT * FROM read_parquet('{escaped}')"
            )
            count = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            log.info(f"  {tbl}: {count:,} rows")
        except Exception as exc:
            log.error(f"  FAILED {tbl}: {exc}")

    # -- 3d: Create advanced_features_v2 view ---
    log.info("\n--- Creating advanced_features_v2 view ---")
    try:
        con.execute(ADVANCED_V2_SQL)
        n = con.execute("SELECT COUNT(*) FROM advanced_features_v2").fetchone()[0]
        log.info(f"  advanced_features_v2: {n:,} rows")
    except Exception as exc:
        log.error(f"  View creation failed: {exc}")
        log.info("  (Dependent tables like master_cohort may need to exist first)")

    con.close()
    log.info("MotherDuck upload complete.")


# ═══════════════════════════════════════════════════════════════════
#  PHASE 4: VERIFICATION QUERIES
# ═══════════════════════════════════════════════════════════════════

def _local_parquet_count(tbl: str) -> int | None:
    import duckdb as _ddb

    pq_path = PROCESSED / f"{tbl}.parquet"
    if not pq_path.exists():
        return None
    local = _ddb.connect()
    n = local.execute(
        f"SELECT COUNT(*) FROM read_parquet('{pq_path}')"
    ).fetchone()[0]
    local.close()
    return n


def phase4_verify(dry_run: bool) -> VerifyLog:
    vlog = VerifyLog()
    vlog.section("PHASE 4: Verification Queries")

    if dry_run:
        vlog.text("[DRY RUN] Would run verification queries against MotherDuck")
        return vlog

    try:
        con = _get_md_connection()
    except Exception as exc:
        vlog.record("MotherDuck connection", False, str(exc))
        return vlog

    vlog.record("MotherDuck connection", True)

    # -- 4a: Row count reconciliation ---
    vlog.section("Row Count Reconciliation: Local Parquet vs MotherDuck")
    for tbl in NEW_TABLES:
        local_n = _local_parquet_count(tbl)
        if local_n is None:
            vlog.record(f"{tbl} local", False, "Parquet file not found")
            continue
        try:
            md_n = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            match = local_n == md_n
            vlog.record(
                f"{tbl}",
                match,
                f"local={local_n:,}  md={md_n:,}" + ("" if match else "  *** MISMATCH ***"),
            )
        except Exception as exc:
            vlog.record(f"{tbl} MotherDuck", False, str(exc))

    # -- 4b: Coverage check ---
    vlog.section("Coverage: Distinct research_id vs master_cohort")
    try:
        master_n = con.execute(
            "SELECT COUNT(DISTINCT research_id) FROM master_cohort"
        ).fetchone()[0]
        vlog.text(f"master_cohort distinct patients: {master_n:,}")
    except Exception as exc:
        vlog.record("master_cohort count", False, str(exc))
        master_n = None

    for tbl in NEW_TABLES:
        try:
            tbl_n = con.execute(
                f"SELECT COUNT(DISTINCT CAST(research_id AS INT)) FROM {tbl}"
            ).fetchone()[0]
            coverage = f"{tbl_n:,} patients"
            if master_n:
                pct = tbl_n / master_n * 100
                coverage += f" ({pct:.1f}% of master)"
            vlog.record(f"{tbl} coverage", True, coverage)
        except Exception as exc:
            vlog.record(f"{tbl} coverage", False, str(exc))

    # -- 4c: Sample extraction check ---
    vlog.section("Sample: extracted_clinical_events (first 20 rows)")
    try:
        rows = con.execute(
            "SELECT research_id, event_type, event_subtype, event_value, event_date "
            "FROM extracted_clinical_events "
            "WHERE event_value IS NOT NULL "
            "LIMIT 20"
        ).fetchall()
        cols = ["research_id", "event_type", "event_subtype", "event_value", "event_date"]
        vlog.text(f"  {'  '.join(f'{c:>18s}' for c in cols)}")
        vlog.text("  " + "-" * 92)
        for row in rows:
            vlog.text(f"  {'  '.join(f'{str(v):>18s}' for v in row)}")
        vlog.record("Sample extraction", len(rows) > 0, f"{len(rows)} rows returned")
    except Exception as exc:
        vlog.record("Sample extraction", False, str(exc))

    # -- 4d: Stress test: GROUP BY on raw_clinical_notes ---
    vlog.section("Stress Test: GROUP BY on raw_clinical_notes")
    try:
        import time
        t0 = time.perf_counter()
        result = con.execute("""
            SELECT
                COUNT(*) AS total_rows,
                COUNT(DISTINCT "Research ID number") AS unique_patients
            FROM raw_clinical_notes
        """).fetchone()
        elapsed = time.perf_counter() - t0
        vlog.record(
            "Stress test query",
            True,
            f"rows={result[0]:,}  patients={result[1]:,}  elapsed={elapsed:.2f}s",
        )
    except Exception as exc:
        vlog.record("Stress test query", False, str(exc))

    con.close()
    return vlog


# ═══════════════════════════════════════════════════════════════════
#  PHASE 5: IMPROVED FOLLOW-UP DATE EXTRACTION (BONUS)
# ═══════════════════════════════════════════════════════════════════

def phase5_improved_extraction(dry_run: bool, vlog: VerifyLog) -> None:
    vlog.section("PHASE 5 (Bonus): Improved Follow-Up Date Extraction")

    if dry_run:
        vlog.text("[DRY RUN] Would run improved follow-up date extraction")
        return

    try:
        con = _get_md_connection()
    except Exception as exc:
        vlog.record("MotherDuck connection (phase 5)", False, str(exc))
        return

    try:
        has_raw = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_name = 'raw_clinical_notes'"
        ).fetchone()[0]
        if not has_raw:
            vlog.record("raw_clinical_notes", False, "Table not found — skipping v2")
            con.close()
            return
    except Exception:
        vlog.record("raw_clinical_notes check", False, "Could not verify table")
        con.close()
        return

    try:
        v1_fu = con.execute(
            "SELECT COUNT(*) FROM extracted_clinical_events "
            "WHERE event_type = 'follow_up'"
        ).fetchone()[0]
        vlog.text(f"  v1 follow_up events: {v1_fu:,}")

        con.execute(FOLLOWUP_V2_SQL)

        v2_total = con.execute(
            "SELECT COUNT(*) FROM extracted_clinical_events_v2"
        ).fetchone()[0]
        v2_fu = con.execute(
            "SELECT COUNT(*) FROM extracted_clinical_events_v2 "
            "WHERE event_type = 'follow_up'"
        ).fetchone()[0]

        improvement = v2_fu - v1_fu
        vlog.record(
            "extracted_clinical_events_v2",
            True,
            f"total={v2_total:,}  follow_up_events={v2_fu:,}  "
            f"new_dates_added={improvement:,}",
        )
    except Exception as exc:
        vlog.record("v2 extraction", False, str(exc))

    con.close()


# ═══════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Phase 6 post-integration: git, DVC, MotherDuck upload & verification"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Print commands without executing them")
    parser.add_argument("--skip-git", action="store_true",
                        help="Skip git commit/push phase")
    parser.add_argument("--skip-dvc", action="store_true",
                        help="Skip DVC tracking phase")
    parser.add_argument("--skip-motherduck", action="store_true",
                        help="Skip MotherDuck upload phase")
    parser.add_argument("--skip-verify", action="store_true",
                        help="Skip verification phase")
    parser.add_argument("--skip-bonus", action="store_true",
                        help="Skip improved follow-up extraction")
    args = parser.parse_args()

    log.info("=" * 72)
    log.info("  THYROID LAKEHOUSE — PHASE 6 POST-INTEGRATION PIPELINE")
    log.info("=" * 72)
    if args.dry_run:
        log.info("*** DRY RUN MODE — no changes will be made ***\n")

    # Phase 1
    if not args.skip_git:
        phase1_git(args.dry_run)
    else:
        log.info("Skipping Phase 1 (git) by request.")

    # Phase 2
    if not args.skip_dvc:
        phase2_dvc(args.dry_run)
    else:
        log.info("Skipping Phase 2 (DVC) by request.")

    # Phase 3
    if not args.skip_motherduck:
        phase3_motherduck_upload(args.dry_run)
    else:
        log.info("Skipping Phase 3 (MotherDuck) by request.")

    # Phase 4
    vlog = VerifyLog()
    if not args.skip_verify:
        vlog = phase4_verify(args.dry_run)
    else:
        log.info("Skipping Phase 4 (verification) by request.")

    # Phase 5
    if not args.skip_bonus:
        phase5_improved_extraction(args.dry_run, vlog)
    else:
        log.info("Skipping Phase 5 (bonus extraction) by request.")

    # Phase 6: Write report
    vlog.section("SUMMARY")
    vlog.text(f"Total checks: {vlog.pass_count + vlog.fail_count}")
    vlog.text(f"Passed: {vlog.pass_count}")
    vlog.text(f"Failed: {vlog.fail_count}")
    vlog.write(VERIFY_FILE)

    log.info("\n" + "=" * 72)
    log.info("  PIPELINE COMPLETE")
    log.info("=" * 72)
    log.info(f"  Verification report: {VERIFY_FILE}")
    log.info("")
    log.info("  Next steps:")
    log.info("    1. Open MotherDuck UI → check tables")
    log.info("    2. Run dashboard against new advanced_features_v2 view")
    log.info(f"    3. Review {VERIFY_FILE} for any FAIL results")
    log.info("=" * 72)

    if vlog.fail_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
