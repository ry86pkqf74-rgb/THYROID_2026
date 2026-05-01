#!/usr/bin/env python3
"""mig_261 — path_synoptics CAP-template label normalization + surg_date DATE retype.

Normalizes focality, tumor_1–5 LVI / ETE / histologic_type (LOWER+TRIM+typo-map);
retypes ``surg_date`` TIMESTAMP→DATE.

Archive: \"Thyroid 2026 UPdated\".archive_pub_v1_0.path_synoptics_pre_mig261_20260501

Closes: CF-mig262b/c/d/e (focality, LVI, ETE drift + surg_date timestamp).

Usage:
  .venv/bin/python scripts/mig_261_path_synoptics_label_norm.py --dry-run
  .venv/bin/python scripts/mig_261_path_synoptics_label_norm.py --apply
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

SCRIPT_DIR = __file__.rsplit("/", 1)[0]
REPO_ROOT = SCRIPT_DIR.rsplit("/", 1)[0]
sys.path.insert(0, SCRIPT_DIR)

ARCHIVE_SNAPSHOT = (
    '"Thyroid 2026 UPdated".archive_pub_v1_0.path_synoptics_pre_mig261_20260501'
)
RN_COL = "_mig261_rn"


def _run(con, sql: str):
    return con.execute(sql).fetchdf()


ASSIGN_RN = f"""
WITH numbered AS (
  SELECT
    row_number() OVER (
      ORDER BY
        CAST(research_id AS VARCHAR),
        COALESCE(CAST(surg_date AS VARCHAR), ''),
        COALESCE(tumor_focality, ''),
        COALESCE(tumor_1_histologic_type, ''),
        COALESCE(tumor_1_lymphatic_invasion, ''),
        COALESCE(tumor_1_extrathyroidal_extension, '')
    ) AS rn,
    CAST(research_id AS VARCHAR) AS rid_v,
    surg_date AS sd,
    tumor_focality AS tf,
    tumor_1_histologic_type AS h1,
    tumor_1_lymphatic_invasion AS l1,
    tumor_1_extrathyroidal_extension AS e1
  FROM main.path_synoptics
)
UPDATE main.path_synoptics AS ps
SET {RN_COL} = n.rn
FROM numbered n
WHERE CAST(ps.research_id AS VARCHAR) = n.rid_v
  AND ps.surg_date IS NOT DISTINCT FROM n.sd
  AND ps.tumor_focality IS NOT DISTINCT FROM n.tf
  AND ps.tumor_1_histologic_type IS NOT DISTINCT FROM n.h1
  AND ps.tumor_1_lymphatic_invasion IS NOT DISTINCT FROM n.l1
  AND ps.tumor_1_extrathyroidal_extension IS NOT DISTINCT FROM n.e1
"""


def _lvi_case(col: str) -> str:
    return f"""CASE LOWER(TRIM({col}))
  WHEN 'preesent' THEN 'present'
  WHEN 'indeeterminate' THEN 'indeterminate'
  WHEN 'indeterminent' THEN 'indeterminate'
  WHEN 'indetermiante' THEN 'indeterminate'
  WHEN 'extensivre' THEN 'extensive'
  WHEN 'extensiver' THEN 'extensive'
  ELSE LOWER(TRIM({col}))
END"""


def _ete_case(col: str) -> str:
    return f"""CASE LOWER(TRIM(REPLACE({col}, ';', '')))
  WHEN 'extesive' THEN 'extensive'
  ELSE LOWER(TRIM(REPLACE({col}, ';', '')))
END"""


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Execute writes")
    parser.add_argument("--dry-run", action="store_true", help="Probes only")
    args = parser.parse_args()
    if args.apply and args.dry_run:
        print("Use only one of --apply or --dry-run", file=sys.stderr)
        return 2

    from _md_connect import connect_locked  # noqa: E402

    con = connect_locked()
    log_lines: list[str] = []
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def log(msg: str) -> None:
        print(msg)
        log_lines.append(msg)

    log(f"mig_261 started utc {stamp}")

    n_signed = int(
        _run(
            con,
            "SELECT COUNT(*) AS n FROM main.signoff_migration WHERE mig_id = 'mig_261'",
        )["n"].iloc[0]
    )
    if n_signed > 0 and args.apply:
        log("mig_261 already signed off — refusing --apply (idempotent guard).")
        con.close()
        return 0

    r = con.execute(
        """SELECT COUNT(*) AS n, COUNT(DISTINCT (research_id, surg_date)) AS nd
           FROM main.path_synoptics"""
    ).fetchone()
    log(f"path_synoptics rows={r[0]} distinct(rid,surg_date)={r[1]}")

    t = con.execute(
        """SELECT data_type FROM information_schema.columns
           WHERE table_catalog = 'thyroid_canonical_publication_v1_0'
             AND table_schema = 'main' AND table_name = 'path_synoptics'
             AND column_name = 'surg_date'"""
    ).fetchone()
    log(f"surg_date type (pre): {t}")

    n_time = int(
        _run(
            con,
            """SELECT COUNT(*) AS n FROM main.path_synoptics WHERE surg_date IS NOT NULL
               AND (EXTRACT(HOUR FROM surg_date) <> 0
                 OR EXTRACT(MINUTE FROM surg_date) <> 0
                 OR EXTRACT(SECOND FROM surg_date) <> 0)""",
        )["n"].iloc[0]
    )
    log(f"surg_date rows with non-midnight time: {n_time}")

    typos = int(
        _run(
            con,
            """SELECT COUNT(*) AS n FROM main.path_synoptics
                WHERE tumor_1_lymphatic_invasion IN (
                  'preesent','indeeterminate','indeterminent','indetermiante',
                  'extensivre','extensiver')""",
        )["n"].iloc[0]
    )
    log(f"tumor_1 LVI typo rows (pre): {typos}")

    pre_path = f"{REPO_ROOT}/scripts/output/mig_261_pre_snapshot_log.txt"
    apply_path = f"{REPO_ROOT}/scripts/output/mig_261_apply_log.txt"

    if args.dry_run or not args.apply:
        log("Dry-run / no --apply: stopping before DDL+DML.")
        with open(pre_path, "w", encoding="utf-8") as fh:
            fh.write("\n".join(log_lines) + "\n")
        log(f"Wrote {pre_path}")
        con.close()
        return 0

    # --- apply ---
    log("Adding aligner column...")
    con.execute(
        f"ALTER TABLE main.path_synoptics ADD COLUMN IF NOT EXISTS {RN_COL} BIGINT"
    )
    con.execute(f"UPDATE main.path_synoptics SET {RN_COL} = NULL")
    con.execute(ASSIGN_RN)
    null_rn = int(
        _run(
            con,
            f"SELECT COUNT(*) AS n FROM main.path_synoptics WHERE {RN_COL} IS NULL",
        )["n"].iloc[0]
    )
    if null_rn:
        log(f"ERROR: {null_rn} rows still NULL {RN_COL} — abort")
        con.close()
        return 1
    dup_rn = int(
        _run(
            con,
            f"""SELECT COUNT(*) - COUNT(DISTINCT {RN_COL}) AS n FROM main.path_synoptics""",
        )["n"].iloc[0]
    )
    if dup_rn:
        log(f"ERROR: duplicate {RN_COL} — abort")
        con.close()
        return 1

    log(f"CREATE OR REPLACE TABLE {ARCHIVE_SNAPSHOT} AS ...")
    con.execute(
        f"""CREATE OR REPLACE TABLE {ARCHIVE_SNAPSHOT} AS
        SELECT {RN_COL}, research_id, surg_date, tumor_focality,
               tumor_1_lymphatic_invasion, tumor_1_extrathyroidal_extension,
               tumor_1_histologic_type,
               tumor_2_lymphatic_invasion, tumor_2_extrathyroidal_extension,
               tumor_2_histologic_type,
               tumor_3_lymphatic_invasion, tumor_3_extrathyroidal_extension,
               tumor_3_histologic_type,
               tumor_4_lymphatic_invasion, tumor_4_extrathyroidal_extension,
               tumor_4_histologic_type,
               tumor_5_lymphatic_invasion, tumor_5_extrathyroidal_extension,
               tumor_5_histologic_type
        FROM main.path_synoptics"""
    )

    log("UPDATE focality...")
    con.execute(
        """UPDATE main.path_synoptics
            SET tumor_focality = LOWER(TRIM(REPLACE(tumor_focality, chr(10), '')))
            WHERE tumor_focality IS NOT NULL"""
    )

    for tid in range(1, 6):
        col = f"tumor_{tid}_lymphatic_invasion"
        log(f"UPDATE {col}...")
        con.execute(
            f"""UPDATE main.path_synoptics
                SET {col} = {_lvi_case(col)}
                WHERE {col} IS NOT NULL"""
        )

    for tid in range(1, 6):
        col = f"tumor_{tid}_extrathyroidal_extension"
        log(f"UPDATE {col}...")
        con.execute(
            f"""UPDATE main.path_synoptics
                SET {col} = {_ete_case(col)}
                WHERE {col} IS NOT NULL"""
        )

    for tid in range(1, 6):
        col = f"tumor_{tid}_histologic_type"
        log(f"UPDATE {col}...")
        con.execute(
            f"""UPDATE main.path_synoptics
                SET {col} = LOWER(TRIM({col}))
                WHERE {col} IS NOT NULL"""
        )

    log("ALTER surg_date -> DATE...")
    con.execute(
        """ALTER TABLE main.path_synoptics ALTER COLUMN surg_date
           SET DATA TYPE DATE USING CAST(surg_date AS DATE)"""
    )

    # Verify RN join + expected transforms
    mismatch_parts: list[str] = [
        """(pre.tumor_focality IS NOT NULL
          AND ps.tumor_focality IS DISTINCT FROM
            LOWER(TRIM(REPLACE(pre.tumor_focality, chr(10), ''))))"""
    ]
    for tid in range(1, 6):
        lvi = f"tumor_{tid}_lymphatic_invasion"
        ete = f"tumor_{tid}_extrathyroidal_extension"
        hist = f"tumor_{tid}_histologic_type"
        mismatch_parts.append(
            f"""(pre.{lvi} IS NOT NULL
              AND ps.{lvi} IS DISTINCT FROM {_lvi_case(f"pre.{lvi}")})"""
        )
        mismatch_parts.append(
            f"""(pre.{ete} IS NOT NULL
              AND ps.{ete} IS DISTINCT FROM {_ete_case(f"pre.{ete}")})"""
        )
        mismatch_parts.append(
            f"""(pre.{hist} IS NOT NULL
              AND ps.{hist} IS DISTINCT FROM LOWER(TRIM(pre.{hist})))"""
        )
    mismatch_sql = "\n                   OR ".join(mismatch_parts)

    log("Post verify (archive vs live on aligner)...")
    bad = int(
        _run(
            con,
            f"""SELECT COUNT(*) AS n
                FROM {ARCHIVE_SNAPSHOT} pre
                JOIN main.path_synoptics ps ON ps.{RN_COL} = pre.{RN_COL}
                WHERE {mismatch_sql}
                """,
        )["n"].iloc[0]
    )
    if bad:
        log(f"ERROR: verification mismatch rows: {bad}")
        con.close()
        return 1

    typo_conds = " OR ".join(
        f"""tumor_{tid}_lymphatic_invasion IN (
                 'preesent','indeeterminate','indeterminent','indetermiante',
                 'extensivre','extensiver')"""
        for tid in range(1, 6)
    )
    typo_left = int(
        _run(
            con,
            f"SELECT COUNT(*) AS n FROM main.path_synoptics WHERE {typo_conds}",
        )["n"].iloc[0]
    )
    log(f"LVI typo rows any slot (post): {typo_left}")
    if typo_left:
        log("ERROR: typos remain")
        con.close()
        return 1

    log(f"DROP aligner column {RN_COL}")
    con.execute(f"ALTER TABLE main.path_synoptics DROP COLUMN {RN_COL}")

    ex = int(
        _run(
            con,
            "SELECT COUNT(*) AS n FROM main.signoff_migration WHERE mig_id = 'mig_261'",
        )["n"].iloc[0]
    )
    if ex == 0:
        con.execute(
            """INSERT INTO main.signoff_migration
               (mig_id, signed_off_at, by_actor, summary)
               VALUES (
                 'mig_261', CURRENT_TIMESTAMP::TIMESTAMP, 'cursor_agent',
                 'path_synoptics CAP labels: focality+LVI+ETE+hist LOWER/TRIM/typo-map tumors 1-5; surg_date TIMESTAMP→DATE'
               )"""
        )
        log("INSERT signoff_migration mig_261 OK")
    else:
        log("signoff_migration mig_261 already present — skip INSERT")

    with open(apply_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(log_lines) + "\n")
    log(f"Wrote {apply_path}")

    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
