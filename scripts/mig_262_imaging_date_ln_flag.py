#!/usr/bin/env python3
"""mig_262 — Imaging date cleanup (YY-typo outliers) + US LN suspicious flag rebuild.

(1) ``raw_imaging_12_slots_v1.exam_date`` — archives affected rows then fixes known
    4-digit OCR/century outliers (dispatch: rids 12048 → 2002; 10511 → 2022).
    Does **not** attempt bulk recovery of legitimately missing dates (tracked as
    CF-mig262-NULL-DATE-RECOVERY).
(2) Rebuilds rollup via Script 366/367 after widening ``n_abnormal_us_ln_on_exam``
    in ``366_canonical_us_exam_master_v2.py`` (LN shell rows with NULL suspicious_flag).

Archive snapshot on \"Thyroid 2026 UPdated\".archive_pub_v1_0 per dispatch.

Usage:
  .venv/bin/python scripts/mig_262_imaging_date_ln_flag.py
  .venv/bin/python scripts/mig_262_imaging_date_ln_flag.py --dry-run
  .venv/bin/python scripts/mig_262_imaging_date_ln_flag.py --apply
  .venv/bin/python scripts/mig_262_imaging_date_ln_flag.py --apply --ln-only
      # If ``main.raw_imaging_12_slots_v1`` is missing (never promoted /
      archived-only), LN VIEW rebuild + heuristic still runs; signoff notes skip.

Closes: CF-mig260e-IMAGING-12SLOTS-DATE-QUALITY, CF-mig260g-US-LN-SUSPICIOUS-FLAG-UNDERFIRE
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = __file__.rsplit("/", 1)[0]
REPO_ROOT = SCRIPT_DIR.rsplit("/", 1)[0]
sys.path.insert(0, SCRIPT_DIR)

ARCHIVE_TBL = """\"Thyroid 2026 UPdated\".archive_pub_v1_0.raw_imaging_12_slots_v1_pre_mig262_20260501"""


def _run(con, sql: str):
    return con.execute(sql).fetchdf()


def _table_exists(con, fq: str) -> bool:
    parts = fq.split(".")
    if len(parts) == 3:
        cat, sch, tbl = parts
        n = con.execute(
            """SELECT COUNT(*) FROM information_schema.tables
               WHERE table_catalog = ? AND table_schema = ?
                 AND table_name = ? AND table_type = 'BASE TABLE'""",
            [cat, sch, tbl],
        ).fetchone()[0]
        return int(n) > 0
    n = con.execute(
        """SELECT COUNT(*) FROM information_schema.tables
           WHERE table_schema = 'main' AND table_name = ? AND table_type='BASE TABLE'""",
        [fq],
    ).fetchone()[0]
    return int(n) > 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--ln-only",
        action="store_true",
        help='Skip imaging DML/signoff imagery lines (use when '
        "`main.raw_imaging_12_slots_v1` absent); still rebuild 366/367.",
    )
    args = parser.parse_args()
    if args.apply and args.dry_run:
        print("Use only one of --apply or --dry-run", file=sys.stderr)
        return 2

    from _md_connect import connect_locked  # noqa: E402

    con = connect_locked()
    log_lines: list[str] = []

    def log(msg: str) -> None:
        print(msg)
        log_lines.append(msg)

    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    log(f"mig_262 started utc {stamp}")

    img_tbl = 'thyroid_canonical_publication_v1_0.main.raw_imaging_12_slots_v1'

    def probe_cupm(label: str) -> None:
        try:
            row = _run(
                con,
                """SELECT
                    COUNT(*) AS n_with_us,
                    COUNT(*) FILTER (WHERE has_us_ln_findings_ever)
                        AS n_ln_findings,
                    COUNT(*) FILTER (WHERE any_suspicious_us_ln_ever)
                        AS n_suspicious_ln
                   FROM main.canonical_us_patient_master_VIEW_v2""",
            )
            log(f"[probe {label}] {row.iloc[0].to_dict()}")
        except Exception as exc:  # noqa: BLE001
            log(f"[probe {label} SKIP — view not queriable ({type(exc).__name__})")

    probe_cupm("cupm_ln_pre")

    has_raw = _table_exists(con, img_tbl)
    log(f"[probe raw_imaging_12_slots_v1 BASE TABLE exists] {has_raw}")

    if has_raw:
        for label, sql in [
            ("outlier_dates", """SELECT COUNT(*) AS n FROM main.raw_imaging_12_slots_v1
                WHERE exam_date IS NOT NULL
                  AND (YEAR(exam_date) < 1990 OR YEAR(exam_date) > 2030)"""),
            ("null_exam_dates", """SELECT COUNT(*) AS n FROM main.raw_imaging_12_slots_v1
                 WHERE exam_date IS NULL"""),
        ]:
            row = _run(con, sql)
            log(f"[probe {label}] {row.iloc[0].to_dict()}")
    else:
        log("[probe imaging] SKIP — table missing from publication canonical")

    n_signed = int(
        _run(
            con,
            "SELECT COUNT(*) AS n FROM main.signoff_migration WHERE mig_id = 'mig_262'",
        )["n"].iloc[0]
    )
    if n_signed > 0 and args.apply:
        log("mig_262 already signed off — refusing --apply (idempotent guard).")
        con.close()
        return 0

    out_dir = f"{REPO_ROOT}/scripts/output"
    pre_path = f"{out_dir}/mig_262_pre_snapshot_log.txt"
    apply_path = f"{out_dir}/mig_262_apply_log.txt"

    if args.dry_run or not args.apply:
        log("Dry-run / no --apply: stopping before DDL+DML (+366/367).")
        try:
            Path(out_dir).mkdir(parents=True, exist_ok=True)
            Path(pre_path).write_text("\n".join(log_lines) + "\n", encoding="utf-8")
        except OSError as e:
            log(f"(warn could not write {pre_path}: {e})")
        else:
            log(f"Wrote {pre_path}")
        con.close()
        return 0

    if args.apply and not has_raw and not args.ln_only:
        log(
            "EXIT 3: main.raw_imaging_12_slots_v1 is absent — imaging leg cannot run. "
            "Re-ingest multimodule/raw layer first, or rerun with `--apply --ln-only` "
            "for LN VIEW rebuild only."
        )
        con.close()
        return 3

    if not has_raw:
        log("NOTICE: Imaging table absent — LN-only apply path.")

    if args.ln_only:
        log("--ln-only set: SKIP imaging archive + DATE UPDATE.")

    imaging_applied = has_raw and not args.ln_only

    img_summary_sentence = ""

    if imaging_applied:
        log(f"CREATE OR REPLACE TABLE {ARCHIVE_TBL} AS selective snapshot...")
        con.execute(
            f"""CREATE OR REPLACE TABLE {ARCHIVE_TBL} AS
            SELECT *
            FROM main.raw_imaging_12_slots_v1 r
            WHERE r.exam_date IS NULL
               OR YEAR(r.exam_date) < 1990
               OR YEAR(r.exam_date) > 2030"""
        )

        arc_n = int(
            _run(con, f"SELECT COUNT(*) AS n FROM {ARCHIVE_TBL}")["n"].iloc[0]
        )
        log(f"Archive row count (NULL or year out-of-range source rows): {arc_n}")

        log("UPDATE rid 12048 pre-1990 outlier exam_date → 2002-08-29...")
        con.execute(
            """UPDATE main.raw_imaging_12_slots_v1
               SET exam_date = DATE '2002-08-29'
               WHERE CAST(research_id AS BIGINT) = 12048
                 AND YEAR(exam_date) < 1990"""
        )

        log("UPDATE rid 10511 post-2030 outlier exam_date → 2022-03-03...")
        con.execute(
            """UPDATE main.raw_imaging_12_slots_v1
               SET exam_date = DATE '2022-03-03'
               WHERE CAST(research_id AS BIGINT) = 10511
                 AND YEAR(exam_date) > 2030"""
        )

        out = _run(
            con,
            """SELECT
                COUNT(*) AS n_total,
                COUNT(*) FILTER (WHERE exam_date IS NULL) AS n_null,
                COUNT(*) FILTER (
                  WHERE YEAR(exam_date) < 1990 OR YEAR(exam_date) > 2030)
                  AS n_out_of_band
               FROM main.raw_imaging_12_slots_v1""",
        )
        log(f"[post-date-fix row stats] {out.iloc[0].to_dict()}")
        n_oob = int(out.iloc[0]["n_out_of_band"])
        if n_oob > 0:
            log(f"ERROR: expected 0 rows with year outside 1990–2030; got {n_oob}")
            con.close()
            return 1
        img_summary_sentence = (
            "Imaging 12-slot: archived pre-change rows + fixed century outliers "
            "rids 12048/10511; NULL bulk recovery still CF-mig262-NULL-DATE-RECOVERY. "
        )
    else:
        if not has_raw:
            img_summary_sentence = (
                "Imaging SKIPPED: main.raw_imaging_12_slots_v1 absent in publication DB "
                "(re-promote ingest / restore before re-running imaging leg). "
            )
        elif args.ln_only:
            img_summary_sentence = "Imaging leg skipped (--ln-only). "
        else:
            img_summary_sentence = ""

    python = f"{REPO_ROOT}/.venv/bin/python"
    for script_name in ("366_canonical_us_exam_master_v2.py", "367_canonical_us_patient_master_v2.py"):
        argv = [python, f"{SCRIPT_DIR}/{script_name}", "--commit"]
        log(f"subprocess: {' '.join(argv)}")
        subprocess.check_call(argv, cwd=REPO_ROOT)

    signoff_blob = (
        img_summary_sentence
        + "LN: widened n_abnormal_us_ln_on_exam heuristic (script 366) + VIEW "
          "rebuild 366/367."
    )
    con.execute(
        """INSERT INTO main.signoff_migration
           (mig_id, signed_off_at, by_actor, summary)
           VALUES (?, CURRENT_TIMESTAMP::TIMESTAMP, ?, ?)""",
        ["mig_262", "cursor_agent", signoff_blob],
    )
    log("INSERT signoff_migration mig_262 OK")

    row_ln = _run(
        con,
        """SELECT
            COUNT(*) AS n_with_us,
            COUNT(*) FILTER (WHERE has_us_ln_findings_ever) AS n_ln_findings,
            COUNT(*) FILTER (WHERE any_suspicious_us_ln_ever) AS n_suspicious_post
           FROM main.canonical_us_patient_master_VIEW_v2""",
    )
    log(f"[post-rebuild LN rollup] {row_ln.iloc[0].to_dict()}")

    n_susp = int(row_ln.iloc[0]["n_suspicious_post"])
    if n_susp < 100:
        log(f"WARN: suspicious LN count unexpectedly low (<100): {n_susp}")

    Path(out_dir).mkdir(parents=True, exist_ok=True)
    Path(apply_path).write_text("\n".join(log_lines) + "\n", encoding="utf-8")
    log(f"Wrote {apply_path}")

    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
