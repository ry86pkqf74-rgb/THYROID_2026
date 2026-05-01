#!/usr/bin/env python3
"""mig_267 — canonical_histology_lookup_v1 SSOT for histology_final → coarse groups.

Maps every distinct ``canonical_patient_master.histology_final`` string (MotherDuck
probe: 38 values as of 2026-05-02) to ``histology_group`` used by manuscript /
Snowflake cohorts — replacing duplicated ``CASE WHEN histology_final ILIKE``
patterns.

Ratifications follow cursor_prompts/CURSOR_PROMPT_MIG_267_* (Logan dispatch pass):
metastatic / recurrent qualifiers → orthogonal boolean flags; edge rows (DHGTC,
NUT, mixed composite, CASTLE-like, PDTC-neuroendocrine typo row, etc.) mapped
explicitly.

Archive (distinct histogram): ``Thyroid 2026 UPdated``.archive_pub_v1_0.

Artifacts:
  scripts/output/mig_267_histology_distinct_probe.csv   (refresh probe)
  scripts/output/mig_267_histology_lookup_rows.csv      (lookup snapshot)
  scripts/output/mig_267_canonical_histology_lookup_seed.sql  (Snowflake DDL)
  scripts/output/mig_267_apply_log.txt
  qc_framework_v1/migrations/267_canonical_histology_lookup_v1_20260502.sql

Usage:
  .venv/bin/python scripts/mig_267_canonical_histology_lookup.py --dry-run
  .venv/bin/python scripts/mig_267_canonical_histology_lookup.py --apply
"""

from __future__ import annotations

import argparse
import csv
import sys
from datetime import datetime, timezone

SCRIPT_DIR = __file__.rsplit("/", 1)[0]
REPO_ROOT = SCRIPT_DIR.rsplit("/", 1)[0]
sys.path.insert(0, SCRIPT_DIR)

ARCHIVE_DB = '"Thyroid 2026 UPdated"'
ARCHIVE_SCHEMA = f"{ARCHIVE_DB}.archive_pub_v1_0"

# (histology_final_raw, histology_normalized, histology_group, is_metastatic,
#  is_recurrent, notes)
# histology_group ∈ {PTC, FTC, MTC, ATC, PDTC, Hurthle, Other} — manuscript /
# Snowflake coarse stratification (NIFTP/FTUMP/benign-adenoma-on-malignant-row → Other).
RATIFIED_LOOKUP: list[tuple[str, str, str, bool, bool, str]] = [
    ("PTC", "Papillary thyroid carcinoma", "PTC", False, False, ""),
    ("follicular carcinoma", "Follicular thyroid carcinoma", "FTC", False, False, ""),
    ("MTC", "Medullary thyroid carcinoma", "MTC", False, False, ""),
    ("metastatic PTC", "Papillary thyroid carcinoma", "PTC", True, False, ""),
    ("NIFTP", "Non-invasive follicular thyroid neoplasm with papillary-like features", "Other", False, False, ""),
    ("poorly differentiated thyroid carcinoma", "Poorly differentiated thyroid carcinoma", "PDTC", False, False, ""),
    ("FTUMP", "Follicular tumor of uncertain malignant potential", "Other", False, False, ""),
    ("anaplastic carcinoma", "Anaplastic thyroid carcinoma", "ATC", False, False, ""),
    ("differentiated high grade thyroid carcinoma", "Differentiated high-grade thyroid carcinoma", "PDTC", False, False, "Logan 20260502 §3 DHGTC→PDTC"),
    ("metastatic MTC", "Medullary thyroid carcinoma", "MTC", True, False, ""),
    ("recurrent/metastatic PTC", "Papillary thyroid carcinoma", "PTC", True, True, ""),
    ("follicular adenoma", "Follicular adenoma", "Other", False, False, "Benign label on malignant-flag rows — cohort artifact"),
    ("metastatic PTC tall cell variant", "PTC tall cell variant", "PTC", True, False, ""),
    ("metastatic PTC with tall cell features", "PTC with tall cell features", "PTC", True, False, ""),
    ("metastatic PTC classical", "PTC classical", "PTC", True, False, ""),
    ("metastatic thyroid carcinoma", "Metastatic thyroid carcinoma — lineage unspecified", "Other", True, False, "Ambiguous primary lineage"),
    ("metastatic follicular carcinoma", "Metastatic follicular carcinoma", "FTC", True, False, ""),
    ("recurrent/metastatic follicular carcinoma", "Metastatic follicular carcinoma", "FTC", True, True, ""),
    ("metastatic PTC follicular", "PTC follicular variant", "PTC", True, False, ""),
    ("metastatic PTC classical with focal tall cell features", "PTC classical with focal tall cell features", "PTC", True, False, ""),
    ("metastatic PTC/anaplastic carcinoma", "Dedifferentiated PTC/anaplastic composite", "ATC", True, False, "Worst-grade reporting bucket"),
    ("adenoid cystic carcinoma", "Adenoid cystic carcinoma", "Other", False, False, ""),
    ("differentiated thyroid carcinoma", "Differentiated thyroid carcinoma — papillary lineage default", "PTC", False, False, ""),
    ("angiosarcoma of the thyroid", "Angiosarcoma", "Other", False, False, ""),
    ("Atypical hurthle cell neoplasm", "Atypical Hürthle cell neoplasm", "Hurthle", False, False, ""),
    (
        "metastatic PTC classical with extensive follicular growth pattern & oncocytic & focal tall cell features <5%",
        "PTC composite variant description",
        "PTC",
        True,
        False,
        "",
    ),
    ("recurrent MTC", "Medullary thyroid carcinoma", "MTC", False, True, ""),
    ("infiltrating carcinoma with thymus-like differentiation", "CASTLE / thyroid carcinoma with thymus-like differentiation", "Other", False, False, ""),
    ("high-grade PTC with thymic-like features", "High-grade PTC with thymic-like features", "PTC", False, False, ""),
    ("MTC/PTC mixed composite", "Mixed medullary-papillary composite", "Other", False, False, "Logan 20260502 §3→Other"),
    ("poorly differentiated carcinoma with neuroendocrine differntiation", "Poorly differentiated with neuroendocrine features", "PDTC", False, False, "Synoptic typo differntiation"),
    ("high grade carcinoma with focal squamous features", "High-grade carcinoma with squamous differentiation", "PDTC", False, False, ""),
    ("atypical follicular adenoma", "Atypical follicular adenoma", "Other", False, False, ""),
    ("metastatic thyroid carcinoma with hurthle cell and paillary features", "Metastatic Hürthle-papillary composite", "Hurthle", True, False, ""),
    ("NUT carcinoma", "NUT carcinoma", "Other", False, False, "Logan 20260502 §3→Other malignant bucket"),
    ("poorly differentiated PTC", "Poorly differentiated papillary carcinoma", "PDTC", False, False, ""),
    ("metastatic PTC with focal tall cell features", "PTC with focal tall cell features", "PTC", True, False, ""),
    ("metastatic anaplastic carcinoma", "Metastatic anaplastic carcinoma", "ATC", True, False, ""),
]


def _run(con, sql: str):
    return con.execute(sql).fetchdf()


def _semantic_bucket(hist_group: str) -> str:
    """Align SSOT coarse bucket with cortex_analyst thyroid_2026_semantic_model.yaml."""
    return "Other malignant" if hist_group == "Other" else hist_group


def _emit_snowflake_seed(rows: list[tuple], path: str, stamp_lit: str) -> None:
    """Snowflake-compatible CREATE OR REPLACE TABLE AS SELECT FROM VALUES."""

    import os

    os.makedirs(os.path.dirname(path), exist_ok=True)

    def esc(v: str) -> str:
        return str(v).replace("'", "''")

    val_lines: list[str] = []
    for tup in rows:
        raw, norm, grp, meta, rec, note = tup
        sem = _semantic_bucket(grp)
        val_lines.append(
            ", ".join(
                [
                    f"'{esc(raw)}'",
                    f"'{esc(norm)}'",
                    f"'{esc(grp)}'",
                    f"'{esc(sem)}'",
                    "TRUE" if meta else "FALSE",
                    "TRUE" if rec else "FALSE",
                    "'logan_via_cursor_dispatch_20260502'",
                    f"CAST('{stamp_lit}' AS TIMESTAMP_NTZ)",
                    f"'{esc(note)}'",
                ]
            )
        )
    joined_vals = ",\n    ".join(f"({line})" for line in val_lines)
    sql = f"""-- AUTO-GENERATED by scripts/mig_267_canonical_histology_lookup.py
-- Run in Snowflake THYROID_VALIDATION.PUBLIC before cohort manuscript views.

CREATE OR REPLACE TABLE THYROID_VALIDATION.PUBLIC.CANONICAL_HISTOLOGY_LOOKUP_V1 AS
SELECT
  column1 AS histology_final_raw,
  column2 AS histology_normalized,
  column3 AS histology_group,
  column4 AS histology_group_semantic,
  column5 AS is_metastatic,
  column6 AS is_recurrent,
  column7 AS ratified_by,
  column8 AS ratified_at,
  column9 AS notes
FROM VALUES
    {joined_vals}
  AS v(column1, column2, column3, column4, column5, column6, column7, column8, column9);
"""
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(sql)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if args.apply and args.dry_run:
        print("Use only one of --apply or --dry-run", file=sys.stderr)
        return 2
    if not args.apply and not args.dry_run:
        print("Specify --dry-run or --apply", file=sys.stderr)
        return 2

    from _md_connect import connect_locked  # noqa: E402

    con = connect_locked()
    log_lines: list[str] = []
    utc_stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def log(msg: str) -> None:
        print(msg)
        log_lines.append(msg)

    log(f"mig_267 started utc {utc_stamp}")

    probe_path = f"{REPO_ROOT}/scripts/output/mig_267_histology_distinct_probe.csv"
    probe = _run(
        con,
        """
SELECT histology_final, COUNT(*) AS n_patients
FROM main.canonical_patient_master
WHERE histology_final IS NOT NULL
GROUP BY 1 ORDER BY 2 DESC
""",
    )
    probe.to_csv(probe_path, index=False)
    log(f"Wrote probe {probe_path} (n_distinct={len(probe)})")

    probe_set = set(probe["histology_final"].astype(str))
    ratified_set = {r[0] for r in RATIFIED_LOOKUP}
    missing_in_ratified = sorted(probe_set - ratified_set)
    extra_in_ratified = sorted(ratified_set - probe_set)
    if missing_in_ratified:
        log(f"FAIL: probe histology_final values missing from RATIFIED_LOOKUP: {missing_in_ratified}")
        con.close()
        return 1
    if extra_in_ratified:
        log(f"WARN: RATIFIED_LOOKUP has extras not in current probe (safe): {extra_in_ratified}")

    ratified_by = "logan_via_cursor_dispatch_20260502"
    stamp_lit = "2026-05-02 00:00:00"
    seed_path = f"{REPO_ROOT}/snowflake_trial/sql/canonical_histology_lookup_v1_seed.sql"
    _emit_snowflake_seed(RATIFIED_LOOKUP, seed_path, stamp_lit)
    log(f"Wrote Snowflake seed {seed_path}")

    rows_csv = f"{REPO_ROOT}/scripts/output/mig_267_histology_lookup_rows.csv"
    with open(rows_csv, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(
            [
                "histology_final_raw",
                "histology_normalized",
                "histology_group",
                "histology_group_semantic",
                "is_metastatic",
                "is_recurrent",
                "ratified_by",
                "notes",
            ]
        )
        for tup in RATIFIED_LOOKUP:
            raw, norm, grp, meta, rec, note = tup
            w.writerow([raw, norm, grp, _semantic_bucket(grp), meta, rec, ratified_by, note])
    log(f"Wrote {rows_csv}")

    signed_n = int(
        con.execute(
            "SELECT COUNT(*) FROM main.signoff_migration WHERE mig_id = 'mig_267'"
        ).fetchone()[0]
    )

    if args.dry_run:
        log("Dry-run: skipping archive DDL + lookup table writes + signoff.")
        log("(RATIFIED_LOOKUP rows match probe_set — full coverage implied.)")
        out_log = f"{REPO_ROOT}/scripts/output/mig_267_apply_log.txt"
        with open(out_log, "w", encoding="utf-8") as fh:
            fh.write("\n".join(log_lines) + "\n")
        log(f"Wrote {out_log}")
        con.close()
        return 0

    if signed_n > 0:
        log("signoff_migration mig_267 already present — refusing duplicate --apply")
        log("(Re-run with manual archive review if rebuild needed.)")
        con.close()
        return 3

    archive_tbl = "histology_distinct_pre_mig267_20260502"
    log(f"Creating archive {ARCHIVE_SCHEMA}.{archive_tbl}")
    con.execute(
        f"""
CREATE OR REPLACE TABLE {ARCHIVE_SCHEMA}.{archive_tbl} AS
SELECT histology_final, COUNT(*) AS n_patients
FROM main.canonical_patient_master
WHERE histology_final IS NOT NULL
GROUP BY 1
"""
    )

    log("CREATE OR REPLACE TABLE main.canonical_histology_lookup_v1")
    con.execute(
        """
DROP TABLE IF EXISTS main.canonical_histology_lookup_v1;
CREATE TABLE main.canonical_histology_lookup_v1 (
  histology_final_raw VARCHAR PRIMARY KEY,
  histology_normalized VARCHAR,
  histology_group VARCHAR,
  histology_group_semantic VARCHAR,
  is_metastatic BOOLEAN,
  is_recurrent BOOLEAN,
  ratified_by VARCHAR,
  ratified_at TIMESTAMP,
  notes VARCHAR
);
"""
    )

    import pandas as pd  # noqa: E402

    rat_ts = datetime.now(timezone.utc).replace(tzinfo=None)
    df_rows = []
    for tup in RATIFIED_LOOKUP:
        raw, norm, grp, meta, rec, note = tup
        df_rows.append(
            {
                "histology_final_raw": raw,
                "histology_normalized": norm,
                "histology_group": grp,
                "histology_group_semantic": _semantic_bucket(grp),
                "is_metastatic": meta,
                "is_recurrent": rec,
                "ratified_by": ratified_by,
                "ratified_at": rat_ts,
                "notes": note,
            }
        )
    df = pd.DataFrame(df_rows)
    con.register("_m267_ins", df)
    con.execute(
        """
INSERT INTO main.canonical_histology_lookup_v1
SELECT * FROM _m267_ins
"""
    )
    con.unregister("_m267_ins")
    log(f"Inserted {len(df_rows)} lookup rows")

    verify = _run(
        con,
        """
WITH d AS (
  SELECT DISTINCT histology_final FROM main.canonical_patient_master
  WHERE histology_final IS NOT NULL
)
SELECT
  COUNT(*) AS n_distinct_in_cpm,
  COUNT(l.histology_final_raw) AS n_joined,
  COUNT(*) - COUNT(l.histology_final_raw) AS n_uncovered
FROM d
LEFT JOIN main.canonical_histology_lookup_v1 l
  ON d.histology_final = l.histology_final_raw
"""
    )
    log(str(verify.to_dict("records")[0]))
    n_unc = int(verify["n_uncovered"].iloc[0])
    if n_unc != 0:
        log(f"FAIL coverage n_uncovered={n_unc}")
        con.close()
        return 1

    summary = (
        "Built canonical_histology_lookup_v1 SSOT (38 histology_final strings → "
        "PTC/FTC/MTC/ATC/PDTC/Hurthle/Other) with is_metastatic/is_recurrent flags; "
        "ratified Logan dispatch 20260502; replaces ILIKE drift across Snowflake scripts."
    )
    con.execute(
        """
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
VALUES ('mig_267', CAST(CURRENT_TIMESTAMP AS TIMESTAMP), ?, ?)
""",
        ["cursor_composer_mig267", summary],
    )
    log("INSERT signoff_migration mig_267 OK")
    log("mig_267 PASS")

    out_apply = f"{REPO_ROOT}/scripts/output/mig_267_apply_log.txt"
    with open(out_apply, "w", encoding="utf-8") as fh:
        fh.write("\n".join(log_lines) + "\n")
    log(f"Wrote {out_apply}")
    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
