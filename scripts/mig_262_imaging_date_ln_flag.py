#!/usr/bin/env python3
"""mig_262 — Imaging date typo cleanup + US LN suspicious flag rebuild.

**Imaging — two backends**

1. **Publication layer (MotherDuck default):** century/OCR outliers on rid **12048**
   (**``YEAR(exam_date)=202``** → corrected **2002-08-29**) and **10511**
   (**``YEAR(exam_date)=3022``** → **2022-03-03**) in
   ``main.imaging_exam_master_v1`` and ``main.canonical_us_nodule_v2``.
   Invoke with ``--apply-imaging-only`` (separate ``mig_262_imaging`` signoff).
2. **Optional raw ingest table** ``main.raw_imaging_12_slots_v1`` — if present, full
   ``--apply`` can archive + PATCH that table instead (legacy path from multimodule
   script 50). Most publication DBs never materialized #2.

**LN flag:** widen ``n_abnormal_us_ln_on_exam`` in ``366_canonical_us_exam_master_v2.py``;
refresh VIEWs via scripts **366 → 367**; original signoff **mig_262**.

Archive snapshots on ``"Thyroid 2026 UPdated".archive_pub_v1_0``.

Usage::

  .venv/bin/python scripts/mig_262_imaging_date_ln_flag.py
  .venv/bin/python scripts/mig_262_imaging_date_ln_flag.py --dry-run
  .venv/bin/python scripts/mig_262_imaging_date_ln_flag.py --apply --ln-only
  .venv/bin/python scripts/mig_262_imaging_date_ln_flag.py --apply-imaging-only

Closes: CF-mig260e (imaging dates), CF-mig260g (LN rollup).
Open: CF-mig262-NULL-DATE-RECOVERY — see studies/mig_262_null_date_recovery_20260501/.
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

ARCHIVE_RAW_TBL = """\"Thyroid 2026 UPdated\".archive_pub_v1_0.raw_imaging_12_slots_v1_pre_mig262_20260501"""

ARCH_TS = datetime.now(timezone.utc).strftime("%Y%m%d")


def archive_exam() -> str:
    return (
        f"\"Thyroid 2026 UPdated\".archive_pub_v1_0.imaging_exam_master_v1_pre_mig262img_{ARCH_TS}_v1"
    )


def archive_nodule() -> str:
    return (
        f"\"Thyroid 2026 UPdated\".archive_pub_v1_0.canonical_us_nodule_v2_pre_mig262img_{ARCH_TS}_v1"
    )


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


def probe_cupm(con, log) -> None:
    try:
        row = _run(
            con,
            """SELECT
                COUNT(*) AS n_with_us,
                COUNT(*) FILTER (WHERE has_us_ln_findings_ever) AS n_ln_findings,
                COUNT(*) FILTER (WHERE any_suspicious_us_ln_ever)
                    AS n_suspicious_ln
               FROM main.canonical_us_patient_master_VIEW_v2""",
        )
        log(f"[probe cupm] {row.iloc[0].to_dict()}")
    except Exception as exc:  # noqa: BLE001
        log(f"[probe cupm SKIP — ({type(exc).__name__})]")


def probe_publication_imaging_outliers(con, log) -> None:
    for label, tbl in [
        ("imaging_exam_oob", "imaging_exam_master_v1"),
        ("nodule_v2_oob", "canonical_us_nodule_v2"),
    ]:
        try:
            r = _run(
                con,
                f"""SELECT COUNT(*) AS n FROM main.{tbl}
                    WHERE exam_date IS NOT NULL
                      AND (YEAR(exam_date) < 1990 OR YEAR(exam_date) > 2030)""",
            )
            log(f"[probe {label}] {int(r.iloc[0]['n'])}")
            rids = _run(
                con,
                f"""SELECT research_id, exam_date FROM main.{tbl}
                    WHERE CAST(research_id AS BIGINT) IN (12048, 10511)
                      AND exam_date IS NOT NULL
                      AND (YEAR(exam_date) < 1990 OR YEAR(exam_date) > 2030)""",
            )
            if not rids.empty:
                log(f"[probe {label} target_rid rows] {rids.to_dict('records')}")
        except Exception as exc:  # noqa: BLE001
            log(f"[probe {label} ERR ({type(exc).__name__}: {exc})]")


def _rebuild_views(python_exe: str, log) -> None:
    for script_name in ("366_canonical_us_exam_master_v2.py", "367_canonical_us_patient_master_v2.py"):
        argv = [python_exe, f"{SCRIPT_DIR}/{script_name}", "--commit"]
        log(f"subprocess: {' '.join(argv)}")
        subprocess.check_call(argv, cwd=REPO_ROOT)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--apply-imaging-only", action="store_true", dest="apply_imaging_only")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--ln-only",
        action="store_true",
        help="LN VIEW rebuild leg only (writes mig_262 when combined with --apply).",
    )
    args = parser.parse_args()
    if args.dry_run and (args.apply or args.apply_imaging_only):
        print("Use only one of --dry-run OR --apply / --apply-imaging-only", file=sys.stderr)
        return 2
    if args.apply and args.apply_imaging_only:
        print("--apply and --apply-imaging-only are mutually exclusive.", file=sys.stderr)
        return 2

    from _md_connect import connect_locked  # noqa: E402

    con = connect_locked()
    log_lines: list[str] = []

    def log(msg: str) -> None:
        print(msg)
        log_lines.append(msg)

    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    log(f"mig_262 script started utc {stamp}")

    probe_cupm(con, log)
    probe_publication_imaging_outliers(con, log)

    img_tbl = "thyroid_canonical_publication_v1_0.main.raw_imaging_12_slots_v1"
    has_raw = _table_exists(con, img_tbl)
    log(f"[probe raw_imaging_12_slots_v1 exists] {has_raw}")
    if has_raw:
        for label, sql in [
            ("outlier_raw", """SELECT COUNT(*) AS n FROM main.raw_imaging_12_slots_v1
                WHERE exam_date IS NOT NULL
                  AND (YEAR(exam_date) < 1990 OR YEAR(exam_date) > 2030)"""),
        ]:
            row = _run(con, sql)
            log(f"[probe {label}] {row.iloc[0].to_dict()}")

    n_signed_ln = int(
        _run(
            con, "SELECT COUNT(*) AS n FROM main.signoff_migration WHERE mig_id = 'mig_262'"
        )["n"].iloc[0]
    )
    n_signed_img = int(
        _run(
            con,
            "SELECT COUNT(*) AS n FROM main.signoff_migration WHERE mig_id = 'mig_262_imaging'",
        )["n"].iloc[0]
    )

    out_dir = f"{REPO_ROOT}/scripts/output"
    pre_path = f"{out_dir}/mig_262_pre_snapshot_log.txt"
    apply_path_ln = f"{out_dir}/mig_262_apply_log.txt"
    apply_path_img = f"{out_dir}/mig_262_apply_imaging_log.txt"

    if args.dry_run:
        Path(out_dir).mkdir(parents=True, exist_ok=True)
        Path(pre_path).write_text("\n".join(log_lines) + "\n", encoding="utf-8")
        log(f"Wrote {pre_path}")
        con.close()
        return 0

    python_exe = f"{REPO_ROOT}/.venv/bin/python"

    if args.apply_imaging_only:
        if n_signed_img > 0:
            log("mig_262_imaging already signed off — refusing --apply-imaging-only.")
            con.close()
            return 0
        a_exam = archive_exam()
        a_nod = archive_nodule()
        log(f"CREATE OR REPLACE {a_exam} / {a_nod} ...")

        log("Snapshot impacted imaging_exam_master_v1 rows...")
        con.execute(
            f"""CREATE OR REPLACE TABLE {a_exam} AS
            SELECT *
            FROM main.imaging_exam_master_v1
            WHERE (CAST(research_id AS BIGINT) = 12048 AND YEAR(exam_date) < 1990)
               OR (CAST(research_id AS BIGINT) = 10511 AND YEAR(exam_date) > 2030)"""
        )
        log("Snapshot impacted canonical_us_nodule_v2 rows...")
        con.execute(
            f"""CREATE OR REPLACE TABLE {a_nod} AS
            SELECT *
            FROM main.canonical_us_nodule_v2
            WHERE (CAST(research_id AS BIGINT) = 12048 AND YEAR(exam_date) < 1990)
               OR (CAST(research_id AS BIGINT) = 10511 AND YEAR(exam_date) > 2030)"""
        )

        log(
            "UPDATE imaging_exam_master_v1 typo dates (rid 12048 → 2002-08-29; "
            "rid 10511 → 2022-03-03)..."
        )
        con.execute(
            """UPDATE main.imaging_exam_master_v1
               SET exam_date = DATE '2002-08-29'
               WHERE CAST(research_id AS BIGINT) = 12048
                 AND YEAR(exam_date) = 202"""
        )
        con.execute(
            """UPDATE main.imaging_exam_master_v1
               SET exam_date = DATE '2022-03-03'
               WHERE CAST(research_id AS BIGINT) = 10511
                 AND YEAR(exam_date) = 3022"""
        )
        log("UPDATE canonical_us_nodule_v2 (same predicates)...")
        con.execute(
            """UPDATE main.canonical_us_nodule_v2
               SET exam_date = DATE '2002-08-29'
               WHERE CAST(research_id AS BIGINT) = 12048
                 AND YEAR(exam_date) = 202"""
        )
        con.execute(
            """UPDATE main.canonical_us_nodule_v2
               SET exam_date = DATE '2022-03-03'
               WHERE CAST(research_id AS BIGINT) = 10511
                 AND YEAR(exam_date) = 3022"""
        )

        _rebuild_views(python_exe, log)

        chk_ex = int(
            _run(
                con,
                """SELECT COUNT(*) AS n FROM main.imaging_exam_master_v1
                   WHERE CAST(research_id AS BIGINT) IN (12048, 10511)
                     AND YEAR(exam_date) IN (202, 3022)""",
            )["n"].iloc[0]
        )
        chk_nd = int(
            _run(
                con,
                """SELECT COUNT(*) AS n FROM main.canonical_us_nodule_v2
                   WHERE CAST(research_id AS BIGINT) IN (12048, 10511)
                     AND YEAR(exam_date) IN (202, 3022)""",
            )["n"].iloc[0]
        )
        log(f"[verify target typo years remain] exam={chk_ex} nodule={chk_nd}")
        if chk_ex or chk_nd:
            log("ERROR: targeted typo years still present after UPDATE.")
            con.close()
            return 1

        con.execute(
            """INSERT INTO main.signoff_migration
               (mig_id, signed_off_at, by_actor, summary)
               VALUES (?, CURRENT_TIMESTAMP::TIMESTAMP, ?, ?)""",
            [
                "mig_262_imaging",
                "cursor_agent",
                "Publication imaging: archived + fixed exam_date century typos rid "
                "12048→2002-08-29 and rid 10511→2022-03-03 in imaging_exam_master_v1 + "
                "canonical_us_nodule_v2; refreshed 366/367. Raw raw_imaging_12_slots "
                "layer optional/separate.",
            ],
        )
        probe_cupm(con, log)
        probe_publication_imaging_outliers(con, log)
        Path(out_dir).mkdir(parents=True, exist_ok=True)
        Path(apply_path_img).write_text("\n".join(log_lines) + "\n", encoding="utf-8")
        log(f"Wrote {apply_path_img}")
        con.close()
        return 0

    if not args.apply:
        Path(out_dir).mkdir(parents=True, exist_ok=True)
        Path(pre_path).write_text("\n".join(log_lines) + "\n", encoding="utf-8")
        log(f"Wrote {pre_path}")
        log("Pass --apply or --apply-imaging-only to execute DDL/DML.")
        con.close()
        return 0

    # --- LN leg (--apply / --apply --ln-only) ---
    if n_signed_ln > 0:
        log("mig_262 already signed off — refusing --apply LN leg.")
        con.close()
        return 0

    if args.apply and not has_raw and not args.ln_only:
        log(
            "EXIT 3: main.raw_imaging_12_slots_v1 absent AND not --ln-only. "
            "Use `--apply --ln-only`, or `--apply-imaging-only` for publication dates."
        )
        con.close()
        return 3

    imaging_applied = has_raw and not args.ln_only
    img_summary_sentence = ""

    if imaging_applied:
        log(f"CREATE OR REPLACE TABLE {ARCHIVE_RAW_TBL} (raw layer)...")
        con.execute(
            f"""CREATE OR REPLACE TABLE {ARCHIVE_RAW_TBL} AS
            SELECT *
            FROM main.raw_imaging_12_slots_v1 r
            WHERE r.exam_date IS NULL
               OR YEAR(r.exam_date) < 1990
               OR YEAR(r.exam_date) > 2030"""
        )
        arc_n = int(_run(con, f"SELECT COUNT(*) AS n FROM {ARCHIVE_RAW_TBL}")["n"].iloc[0])
        log(f"Archive rowcount: {arc_n}")
        log("UPDATE raw imaging (rid 12048 / 10511)...")
        con.execute(
            """UPDATE main.raw_imaging_12_slots_v1 SET exam_date = DATE '2002-08-29'
               WHERE CAST(research_id AS BIGINT) = 12048 AND YEAR(exam_date) < 1990"""
        )
        con.execute(
            """UPDATE main.raw_imaging_12_slots_v1 SET exam_date = DATE '2022-03-03'
               WHERE CAST(research_id AS BIGINT) = 10511 AND YEAR(exam_date) > 2030"""
        )
        out = _run(
            con,
            """SELECT COUNT(*) FILTER (
                     WHERE YEAR(exam_date) < 1990 OR YEAR(exam_date) > 2030) AS n_oob
               FROM main.raw_imaging_12_slots_v1""",
        )
        if int(out.iloc[0]["n_oob"]) > 0:
            log(f"ERROR raw layer still has year OOB rows: {out.iloc[0].to_dict()}")
            con.close()
            return 1
        img_summary_sentence = "Raw raw_imaging_12_slots patched; "
    elif not has_raw:
        img_summary_sentence = "Raw layer absent — "
        if args.ln_only:
            img_summary_sentence += "LN-only. "
        else:
            img_summary_sentence += "unexpected; "
    else:
        img_summary_sentence = "Raw layer skipped (--ln-only). "

    _rebuild_views(python_exe, log)

    blob = (
        img_summary_sentence
        + "LN: n_abnormal_us_ln_on_exam heuristic (366) + patient VIEW rebuild (367)."
    )
    con.execute(
        """INSERT INTO main.signoff_migration
           (mig_id, signed_off_at, by_actor, summary)
           VALUES (?, CURRENT_TIMESTAMP::TIMESTAMP, ?, ?)""",
        ["mig_262", "cursor_agent", blob],
    )
    probe_cupm(con, log)
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    Path(apply_path_ln).write_text("\n".join(log_lines) + "\n", encoding="utf-8")
    log(f"Wrote {apply_path_ln}")
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
