#!/usr/bin/env python3
"""Script 374 — TIRADS column rename + recompute on canonical_us_nodule_v2 (Phase 5).

Probe finding (2026-04-21):
  * tirads_reported (4,303 both-populated rows) and tirads_score_2017 are
    NOT equivalent — 1,861 rows disagree. tirads_reported is what was
    extracted/reported in the radiologist's text; tirads_score_2017 is the
    sum of ACR feature points. They have different semantics and BOTH must
    be preserved.
  * 18,684 rows have all 5 ACR features populated; only 508 have all 5 ACR
    point values. Recompute should close that gap by deriving points from
    the feature labels. (We cannot derive points from labels here because
    the feature-string → point-value mapping is not in this script's scope;
    therefore we recompute total + category from the existing per-feature
    point columns wherever all 5 are present.)

Renames applied via ALTER TABLE ... RENAME COLUMN:
  tirads_reported                       → tirads_reported_in_text
  tirads_score_2017                     → acr2017_tirads_points
  tirads_level_2017                     → acr2017_tirads_category
  tirads_category_v2                    → updated_tirads_category
  tirads_band_ambiguous                 → acr2017_band_ambiguous
  tirads_concordant_flag                → acr2017_vs_updated_concordant
  tirads_score_component_complete       → acr2017_feature_points_complete

Dropped (legacy encodings already superseded):
  tirads_category_code_legacy_v1
  tirads_category_modified_legacy_v1

Recomputed where all 5 ACR feature point columns are non-null:
  acr2017_tirads_points    = SUM of 5 *_pts columns
  acr2017_tirads_category  = TR1/TR2/TR3/TR4/TR5 banding (ACR 2017)

Concordance computed where both categories present:
  acr2017_vs_updated_concordant = (acr2017_tirads_category = updated_tirads_category)
"""
from __future__ import annotations

import argparse
import datetime
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

PUB = PUBLICATION_DB
TARGET = f"{PUB}.main.canonical_us_nodule_v2"
SCRIPT_TAG = "Script 374"

OUT_DIR = HERE / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)
RUN_TS = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")
DECISION_LOG = OUT_DIR / f"374_us_nodule_v2_tirads_{RUN_TS}.json"

RENAMES: list[tuple[str, str]] = [
    ("tirads_reported",                  "tirads_reported_in_text"),
    ("tirads_score_2017",                "acr2017_tirads_points"),
    ("tirads_level_2017",                "acr2017_tirads_category"),
    ("tirads_category_v2",               "updated_tirads_category"),
    ("tirads_band_ambiguous",            "acr2017_band_ambiguous"),
    ("tirads_concordant_flag",           "acr2017_vs_updated_concordant"),
    ("tirads_score_component_complete",  "acr2017_feature_points_complete"),
]
DROP_COLS: list[str] = [
    "tirads_category_code_legacy_v1",
    "tirads_category_modified_legacy_v1",
]


def log(msg: str) -> None:
    now = datetime.datetime.now(datetime.UTC).strftime("%H:%M:%S.%f")[:-3]
    print(f"[{now}Z] {msg}", flush=True)


def existing_columns(con) -> set[str]:
    return {
        r[0] for r in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_catalog=? AND table_schema='main' "
            "AND table_name='canonical_us_nodule_v2'", [PUB],
        ).fetchall()
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    log(f"{SCRIPT_TAG} start  commit={args.commit}")
    con = connect_locked()

    cols = existing_columns(con)
    log(f"  pre-rename column count: {len(cols)}")

    # Pre-checks
    eq = con.execute(
        f"""SELECT
            COUNT(*) AS both_present,
            SUM(CASE WHEN tirads_reported = tirads_score_2017 THEN 1 ELSE 0 END) AS equal_rows
           FROM {TARGET}
           WHERE tirads_reported IS NOT NULL AND tirads_score_2017 IS NOT NULL"""
    ).fetchone()
    log(f"  tirads_reported vs tirads_score_2017: both={eq[0]} equal={eq[1]} "
        f"unequal={eq[0] - eq[1]} → keep BOTH under disambiguating names")

    if not args.commit:
        log("dry-run only.")
        return 0

    # 1) Drop the two legacy code columns first (idempotent)
    log("DROP legacy code columns")
    for c in DROP_COLS:
        if c in cols:
            log(f"  ALTER TABLE ... DROP COLUMN {c}")
            con.execute(f"ALTER TABLE {TARGET} DROP COLUMN {c}")
        else:
            log(f"  {c} already absent")

    # 2) Renames (idempotent)
    log("RENAME columns")
    cols = existing_columns(con)
    for old, new in RENAMES:
        if old in cols and new not in cols:
            log(f"  RENAME {old} → {new}")
            con.execute(f"ALTER TABLE {TARGET} RENAME COLUMN {old} TO {new}")
        elif new in cols:
            log(f"  {new} already present (skip)")
        else:
            log(f"  WARN: {old} missing; cannot rename to {new}")

    # 3) Recompute acr2017_tirads_points + category from per-feature points
    log("RECOMPUTE acr2017_tirads_points + acr2017_tirads_category from "
        "5 per-feature point columns where all are present")
    # acr2017_tirads_points: only OVERWRITE when computable AND existing
    # value is null OR was originally tirads_score_2017 (which we just
    # renamed). To preserve provenance, we conservatively only fill where
    # the column is currently NULL.
    con.execute(f"""
UPDATE {TARGET} SET
    acr2017_tirads_points = COALESCE(
        acr2017_tirads_points,
        CASE
            WHEN composition_pts IS NOT NULL
             AND echogenicity_pts IS NOT NULL
             AND shape_pts IS NOT NULL
             AND margin_pts IS NOT NULL
             AND foci_pts IS NOT NULL
            THEN composition_pts + echogenicity_pts + shape_pts
                 + margin_pts + foci_pts
        END
    )
""")

    con.execute(f"""
UPDATE {TARGET} SET
    acr2017_tirads_category = COALESCE(
        acr2017_tirads_category,
        CASE
            WHEN acr2017_tirads_points IS NULL THEN NULL
            WHEN acr2017_tirads_points = 0  THEN 'TR1'
            WHEN acr2017_tirads_points = 2  THEN 'TR2'
            WHEN acr2017_tirads_points = 3  THEN 'TR3'
            WHEN acr2017_tirads_points BETWEEN 4 AND 6 THEN 'TR4'
            WHEN acr2017_tirads_points >= 7 THEN 'TR5'
            ELSE NULL
        END
    )
""")

    # 4) Recompute concordance flag whenever both categories present
    log("RECOMPUTE acr2017_vs_updated_concordant")
    con.execute(f"""
UPDATE {TARGET} SET
    acr2017_vs_updated_concordant = CASE
        WHEN acr2017_tirads_category IS NOT NULL
         AND updated_tirads_category IS NOT NULL
        THEN (acr2017_tirads_category = updated_tirads_category)
        ELSE NULL
    END
""")

    # 5) Verification reportable
    metrics = con.execute(
        f"""SELECT
            COUNT(*) AS total_rows,
            COUNT(acr2017_tirads_points) AS has_acr2017_points,
            COUNT(acr2017_tirads_category) AS has_acr2017_category,
            COUNT(updated_tirads_category) AS has_updated_category,
            COUNT(CASE WHEN acr2017_tirads_category IS NOT NULL
                       AND updated_tirads_category IS NOT NULL THEN 1 END)
                AS both_populated,
            SUM(CASE WHEN acr2017_vs_updated_concordant = FALSE THEN 1 ELSE 0 END)
                AS disagreeing_rows,
            COUNT(tirads_reported_in_text) AS has_text_extracted_tirads
           FROM {TARGET}"""
    ).fetchone()
    metric_keys = [
        "total_rows", "has_acr2017_points", "has_acr2017_category",
        "has_updated_category", "both_populated", "disagreeing_rows",
        "has_text_extracted_tirads",
    ]
    metric_dict = dict(zip(metric_keys, metrics))
    log(f"  metrics: {metric_dict}")

    # COMMENT update
    comment = (
        "US v2 master per-nodule. Grain: (research_id, us_exam_id, "
        "nodule_index_within_exam). Built 2026-04-21 by Script 362; TIRADS "
        f"columns disambiguated by Script 374 ({RUN_TS}). Three TIRADS "
        "systems coexist: acr2017_tirads_points/category (computed from 5 "
        "ACR features), updated_tirads_category (Emory updated system), "
        "tirads_reported_in_text (radiologist text). "
        "acr2017_vs_updated_concordant flags agreement when both available."
    ).replace("'", "''")
    con.execute(f"COMMENT ON TABLE {TARGET} IS '{comment}';")

    DECISION_LOG.write_text(json.dumps({
        "script": SCRIPT_TAG, "run_ts_utc": RUN_TS,
        "renames": [{"old": o, "new": n} for o, n in RENAMES],
        "dropped_columns": DROP_COLS,
        "post_recompute_metrics": metric_dict,
    }, indent=2, default=str))
    log(f"decision log: {DECISION_LOG}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
