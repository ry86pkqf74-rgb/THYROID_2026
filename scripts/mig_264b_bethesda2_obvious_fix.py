#!/usr/bin/env python3
"""mig_264b — Bethesda-2 obvious-fix apply (MotherDuck publication DB).

Sub-cohorts (from mig_264 audit):
  (a) histology_final = NIFTP → is_malignant=FALSE (clinical non-malignant)
  (b) histology follicular adenoma → is_malignant=FALSE
  (c) Negative MIN(days_fna_to_surgery) → repoint bethesda_final to latest
      **preoperative** FNA Bethesda (bethesda_final_num), with name remap

Archive: \"Thyroid 2026 UPdated\".archive_pub_v1_0.cpm_pre_mig264b_20260502

Closes CF-mig264-OBVIOUS-FIXES.

Usage:
  .venv/bin/python scripts/mig_264b_bethesda2_obvious_fix.py --dry-run
  .venv/bin/python scripts/mig_264b_bethesda2_obvious_fix.py --apply
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

SCRIPT_DIR = __file__.rsplit("/", 1)[0]
REPO_ROOT = SCRIPT_DIR.rsplit("/", 1)[0]
sys.path.insert(0, SCRIPT_DIR)

ARCHIVE_SCHEMA = '"Thyroid 2026 UPdated".archive_pub_v1_0'
ARCH_TBL = f"{ARCHIVE_SCHEMA}.cpm_pre_mig264b_20260502"

_EXPECTED_PRE = {"niftp": 22, "folio_adeno": 2, "neg_fna_days": 19}

BETHESDA_NAME_FROM_NUM_SQL = """CASE CAST(f.bethesda_final_num AS BIGINT)
      WHEN 1 THEN 'Nondiagnostic'
      WHEN 2 THEN 'Benign'
      WHEN 3 THEN 'AUS/FLUS'
      WHEN 4 THEN 'Follicular Neoplasm'
      WHEN 5 THEN 'Suspicious for Malignancy'
      WHEN 6 THEN 'Malignant'
      ELSE NULL
    END"""


def _rid_join(alias_a: str, alias_b: str) -> str:
    return f"CAST({alias_a}.research_id AS VARCHAR) = CAST({alias_b}.research_id AS VARCHAR)"


NEG_FNA_LIST_SQL = f"""
WITH bethesda2_malig AS (
  SELECT research_id FROM main.canonical_patient_master
  WHERE bethesda_final = 2 AND COALESCE(is_malignant, FALSE)
),
intervals AS (
  SELECT
    cpm.research_id,
    MIN(
      DATE_DIFF(
        'day',
        CAST(f.fna_date_resolved AS DATE),
        CAST(cpm.first_surgery_date AS DATE)
      )
    ) AS days_fna_to_surg
  FROM main.canonical_patient_master cpm
  JOIN main.canonical_fna_events_v1 f ON {_rid_join('cpm', 'f')}
  JOIN bethesda2_malig b ON {_rid_join('cpm', 'b')}
  WHERE f.fna_date_resolved IS NOT NULL
    AND cpm.first_surgery_date IS NOT NULL
  GROUP BY cpm.research_id
)
SELECT CAST(research_id AS VARCHAR) AS rid
FROM intervals
WHERE days_fna_to_surg < 0
ORDER BY 1
"""


def _run(con, sql: str):
    return con.execute(sql).fetchdf()


def _write_out(path: str, lines: list[str]) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="DDL+DML against MotherDuck")
    parser.add_argument("--dry-run", action="store_true", help="Counts + cohort lists only")
    args = parser.parse_args()
    if args.apply == args.dry_run:
        print("Specify exactly one of --apply | --dry-run", file=sys.stderr)
        return 2

    from _md_connect import connect_locked  # noqa: E402

    con = connect_locked()
    log_lines: list[str] = []
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def log(msg: str) -> None:
        print(msg)
        log_lines.append(msg)

    log(f"mig_264b started utc {stamp}")

    sig = int(
        con.execute(
            "SELECT COUNT(*) FROM main.signoff_migration WHERE mig_id = 'mig_264b'"
        ).fetchone()[0]
    )

    cohort_malig = int(
        con.execute("""
SELECT COUNT(*) FROM main.canonical_patient_master
WHERE bethesda_final = 2 AND COALESCE(is_malignant, FALSE)
""").fetchone()[0]
    )
    total_b2 = int(
        con.execute(
            "SELECT COUNT(*) FROM main.canonical_patient_master WHERE bethesda_final = 2"
        ).fetchone()[0]
    )
    log(f"PRE  Bethesda-2 + malignant: {cohort_malig} ; Bethesda-2 all: {total_b2}")

    n_niftp = int(
        con.execute("""
SELECT COUNT(*) FROM main.canonical_patient_master
WHERE bethesda_final = 2 AND COALESCE(is_malignant, FALSE)
  AND histology_final = 'NIFTP'
""").fetchone()[0]
    )
    n_folio = int(
        con.execute("""
SELECT COUNT(*) FROM main.canonical_patient_master
WHERE bethesda_final = 2 AND COALESCE(is_malignant, FALSE)
  AND histology_final ILIKE '%follicular adenoma%'
""").fetchone()[0]
    )
    neg_df = _run(con, NEG_FNA_LIST_SQL)
    n_neg = len(neg_df)
    rid_in = "(" + ",".join("'" + r + "'" for r in neg_df["rid"].astype(str)) + ")" if n_neg else "('')"
    pending_neg_bf2 = int(
        con.execute(
            f"""
SELECT COUNT(*) FROM main.canonical_patient_master cpm
WHERE CAST(cpm.research_id AS VARCHAR) IN {rid_in}
  AND cpm.bethesda_final = 2
  AND COALESCE(cpm.is_malignant, FALSE)
"""
        ).fetchone()[0]
    )

    if sig > 0 and args.apply:
        log("SKIP APPLY: mig_264b already in signoff_migration — post-state probes only:")
        pm = int(
            con.execute("""
SELECT COUNT(*) FROM main.canonical_patient_master
WHERE bethesda_final = 2 AND COALESCE(is_malignant, FALSE)
""").fetchone()[0]
        )
        nm = float(
            con.execute("""
SELECT 100.0 * SUM(CASE WHEN COALESCE(is_malignant, FALSE) THEN 1 ELSE 0 END) / COUNT(*)
FROM main.canonical_patient_master WHERE bethesda_final = 2
""").fetchone()[0]
            or 0.0
        )
        log(f"POST Bethesda-2 + malig n={pm}; ROM among B2(all)≈{nm:.1f}%")
        _write_out(f"{REPO_ROOT}/scripts/output/mig_264b_apply_log.txt", log_lines + ["(skip/no DML)"])
        con.close()
        return 0

    neg_list_sql = ",".join("'" + x + "'" for x in neg_df["rid"].astype(str).tolist())
    log(f"PRE  NIFTP∩B2∩malig: {n_niftp} (expected {_EXPECTED_PRE['niftp']})")
    log(f"PRE  follicular adenoma∩B2∩malig: {n_folio} (expected {_EXPECTED_PRE['folio_adeno']})")
    log(f"PRE  neg-FNA-day cohort: {n_neg} (expected {_EXPECTED_PRE['neg_fna_days']}); "
        f"B2∩malig pending among neg cohort: {pending_neg_bf2}")
    if neg_list_sql:
        log(f"     neg_rid sample (all): {neg_list_sql[:200]}"
            f"{'...' if len(neg_list_sql) > 200 else ''}")

    if sig > 0 and args.dry_run:
        log("Dry-run: signoff present; DB already migrated.")
        con.close()
        return 0

    full_precheck_ok = (
        (n_niftp, n_folio, n_neg)
        == (
            _EXPECTED_PRE["niftp"],
            _EXPECTED_PRE["folio_adeno"],
            _EXPECTED_PRE["neg_fna_days"],
        )
    )
    resume_precheck_ok = (
        full_precheck_ok is False
        and n_neg == _EXPECTED_PRE["neg_fna_days"]
        and (n_niftp, n_folio) == (0, 0)
        and pending_neg_bf2 > 0
        and pending_neg_bf2 <= _EXPECTED_PRE["neg_fna_days"]
    )

    if not full_precheck_ok and not resume_precheck_ok:
        log(
            "FAIL: mig_264b cohort shape unknown. Expected full PRE (22,2,19) or "
            "resume (NIFTP/FA cleared, neg cohort still pending FNA Bethesda repoint)."
        )
        con.close()
        return 1

    if args.dry_run:
        log("Dry-run OK — no writes.")
        _write_out(f"{REPO_ROOT}/scripts/output/mig_264b_pre_snapshot_log.txt", log_lines)
        log(f"Wrote {REPO_ROOT}/scripts/output/mig_264b_pre_snapshot_log.txt")
        con.close()
        return 0

    resume_only = resume_precheck_ok and not full_precheck_ok

    if resume_only:
        log("RESUME: applying neg-FNA-day Bethesda repoint only (histology-tier already applied)")

    if not resume_only:
        snap_sql = f"""
CREATE OR REPLACE TABLE {ARCH_TBL} AS
SELECT
  research_id,
  bethesda_final,
  bethesda_final_name,
  is_malignant,
  histology_final,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig264b_snapshot_ts
FROM main.canonical_patient_master
WHERE bethesda_final = 2 AND COALESCE(is_malignant, FALSE)
  AND (
    histology_final = 'NIFTP'
    OR histology_final ILIKE '%follicular adenoma%'
    OR CAST(research_id AS VARCHAR) IN {rid_in}
  )
"""

        log(f"Archive CREATE OR REPLACE {ARCH_TBL}")
        con.execute(snap_sql)
        snap_n = int(con.execute(f"SELECT COUNT(*) FROM {ARCH_TBL}").fetchone()[0])
        log(f"Snapshot rows: {snap_n} (expect <= 43; overlaps may shrink count)")

        log("UPDATE: NIFTP → is_malignant FALSE")
        con.execute("""
UPDATE main.canonical_patient_master
SET
  is_malignant = FALSE,
  cpm_built_at = CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
WHERE bethesda_final = 2 AND COALESCE(is_malignant, FALSE)
  AND histology_final = 'NIFTP'
""")
        log("UPDATE NIFTP complete")

        log("UPDATE: follicular adenoma → is_malignant FALSE")
        con.execute("""
UPDATE main.canonical_patient_master
SET
  is_malignant = FALSE,
  cpm_built_at = CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
WHERE bethesda_final = 2 AND COALESCE(is_malignant, FALSE)
  AND histology_final ILIKE '%follicular adenoma%'
""")
    else:
        log("(Resume) skip archive rebuild + histology clears — already applied")

    log("UPDATE: neg-FNA-day → latest preop FNA Bethesda")
    con.execute(
        f"""
UPDATE main.canonical_patient_master AS cpm
SET
  bethesda_final = (
    SELECT CAST(f.bethesda_final_num AS BIGINT)
    FROM main.canonical_fna_events_v1 f
    WHERE CAST(f.research_id AS VARCHAR) = CAST(cpm.research_id AS VARCHAR)
      AND CAST(f.fna_date_resolved AS DATE) < CAST(cpm.first_surgery_date AS DATE)
      AND f.bethesda_final_num IS NOT NULL
    ORDER BY CAST(f.fna_date_resolved AS DATE) DESC NULLS LAST,
             f.fna_event_id DESC
    LIMIT 1
  ),
  bethesda_final_name = (
    SELECT {BETHESDA_NAME_FROM_NUM_SQL}
    FROM main.canonical_fna_events_v1 f
    WHERE CAST(f.research_id AS VARCHAR) = CAST(cpm.research_id AS VARCHAR)
      AND CAST(f.fna_date_resolved AS DATE) < CAST(cpm.first_surgery_date AS DATE)
      AND f.bethesda_final_num IS NOT NULL
    ORDER BY CAST(f.fna_date_resolved AS DATE) DESC NULLS LAST,
             f.fna_event_id DESC
    LIMIT 1
  ),
  cpm_built_at = CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
WHERE CAST(cpm.research_id AS VARCHAR) IN {rid_in}
  AND cpm.bethesda_final = 2
"""
    )

    v_niftp = int(
        con.execute("""
SELECT COUNT(*) FROM main.canonical_patient_master
WHERE bethesda_final = 2 AND COALESCE(is_malignant, FALSE)
  AND histology_final = 'NIFTP'
""").fetchone()[0]
    )
    v_folio = int(
        con.execute("""
SELECT COUNT(*) FROM main.canonical_patient_master
WHERE bethesda_final = 2 AND COALESCE(is_malignant, FALSE)
  AND histology_final ILIKE '%follicular adenoma%'
""").fetchone()[0]
    )
    cohort_post = int(
        con.execute("""
SELECT COUNT(*) FROM main.canonical_patient_master
WHERE bethesda_final = 2 AND COALESCE(is_malignant, FALSE)
""").fetchone()[0]
    )
    rom_pct = float(
        con.execute("""
SELECT 100.0 * SUM(CASE WHEN COALESCE(is_malignant, FALSE) THEN 1 ELSE 0 END) / COUNT(*)
FROM main.canonical_patient_master
WHERE bethesda_final = 2
""").fetchone()[0]
        or 0.0
    )
    total_b2_post = int(
        con.execute(
            "SELECT COUNT(*) FROM main.canonical_patient_master WHERE bethesda_final = 2"
        ).fetchone()[0]
    )

    log(f"VERIFY NIFTP residual (expect 0): {v_niftp}")
    log(f"VERIFY follicular adeno residual (expect 0): {v_folio}")
    nominal_b2malig_post = 385 - 43
    log(f"VERIFY Bethesda-2 + malig count: {cohort_post} (audit nominal {nominal_b2malig_post})")

    row = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main.canonical_patient_master"
    ).fetchone()
    log(f"CPM rows/distinct_rid: {row[0]} / {row[1]}")

    if v_niftp != 0 or v_folio != 0:
        log("FAIL: NIFTP/follicular adeno not fully cleared.")
        con.close()
        return 1
    if cohort_post != nominal_b2malig_post:
        log(
            f"WARN: B2+malig count {cohort_post} differs from nominal {nominal_b2malig_post} "
            "(possible overlap among the three mig_264 sub-cohorts)."
        )
    if row[0] != 10871 or row[1] != 10871:
        log("FAIL: CPM row invariant broken")
        con.close()
        return 1

    log(f"ROM among all Bethesda-2 rows: {rom_pct:.1f}% (n_B2={total_b2_post})")

    summary = (
        "mig_264b: 22 NIFTP + 2 follicular adenoma → is_malignant=FALSE; "
        "19 negative-FNA-day patients → bethesda_final/name from latest preop FNA. "
        f"Post B2+malig n={cohort_post}; ROM(B2)≈{rom_pct:.1f}%. "
        "Closes CF-mig264-OBVIOUS-FIXES."
    )

    prov_run_id = "canonical_cleanup_mig264b_20260502"
    try:
        con.execute(
            "DELETE FROM manuscript_workspace.cpm_reconciliation_provenance_v1 "
            "WHERE run_id = ?",
            [prov_run_id],
        )
        con.execute(
            """
INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1 (
  run_id,
  started_at,
  ended_at,
  phases_applied,
  critical_findings_cleared,
  high_findings_cleared,
  med_findings_cleared,
  held_for_adjudication
)
VALUES (
  ?,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  'mig264b bethesda2 obvious fix — NIFTP/FA malignant flag; neg-FNA-day Bethesda repoint',
  'CF-mig264-OBVIOUS-FIXES closed',
  '0',
  '0',
  '0'
)
""",
            [prov_run_id],
        )
        log("INSERT cpm_reconciliation_provenance_v1 mig_264b OK")
    except Exception as exc:
        log(f"WARN provenance insert: {exc}")

    con.execute(
        """
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
VALUES (?, CAST(CURRENT_TIMESTAMP AS TIMESTAMP), ?, ?)
""",
        ["mig_264b", "cursor_composer_mig264b", summary],
    )
    log("INSERT signoff_migration mig_264b OK")

    out_apply = f"{REPO_ROOT}/scripts/output/mig_264b_apply_log.txt"
    _write_out(out_apply, log_lines)
    log(f"Wrote {out_apply}")

    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
