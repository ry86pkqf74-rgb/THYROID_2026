#!/usr/bin/env python3
"""Script 376 — feature-string normalization + ACR 2017 points recompute.

Closes three gaps discovered in the 2026-04-21 post-consolidation audit on
``thyroid_canonical_publication_v1_0.main.canonical_us_nodule_v2``:

1. Free-text and underscore variants of the same feature value
   (e.g. ``wider than tall`` vs ``wider_than_tall``) blocked the existing
   points-mapping CASE expressions.
2. Per-feature ``*_pts`` columns were populated on only 591–3,784 rows each
   against 21,458–29,852 rows where the underlying feature is present, so
   ``acr2017_tirads_points`` covered a tiny fraction of the cohort.
3. ``acr2017_vs_updated_concordant`` returned NULL for rows where the two
   category columns genuinely disagree (4 TR2/TR3 rows observed) instead of
   FALSE.

Plus one schema tightening:

4. Adds ``source_modality`` to ``canonical_us_nodule_v2`` to match the
   ``v2`` gland and lymph-node tables. Per ALTER-TABLE limitations in
   MotherDuck/DuckDB (no ``ADD CONSTRAINT`` and no inline ``CHECK`` on
   ``ADD COLUMN``), the column-level ``CHECK (source_modality = 'US')``
   enforcement is logged and skipped at the schema level; the script
   instead asserts the invariant explicitly after backfill (and the
   default value pins it to ``'US'``).

PHI: structured features and synthetic IDs only — no clinical_note_text.

Decision: OVERWRITE existing per-feature ``*_pts`` columns with newly
computed values. Aggregate ``acr2017_tirads_points`` /
``acr2017_tirads_category`` only overwrite when the new computation
resolves all 5 features (otherwise the prior value is preserved).
``acr2017_vs_updated_concordant`` is fully recomputed (TRUE/FALSE/NULL).
A row-level snapshot of the pre-change feature/points/category columns is
written to ``manuscript_workspace.canonical_us_nodule_v2_pre_s376_snapshot``
before any UPDATE.

Usage::

    .venv/bin/python scripts/Script_376_feature_string_to_pts_recompute.py
    .venv/bin/python scripts/Script_376_feature_string_to_pts_recompute.py --commit
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
SNAPSHOT = f"{PUB}.manuscript_workspace.canonical_us_nodule_v2_pre_s376_snapshot"
SCRIPT_TAG = "Script 376"

OUT_DIR = HERE / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)
RUN_TS = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")
DECISION_LOG = OUT_DIR / f"376_feature_string_to_pts_{RUN_TS}.json"

# ---------------------------------------------------------------------------
# ACR 2017 point maps (anchored on 2026-04-21 audit of canonical_us_nodule_v2)
# ---------------------------------------------------------------------------
COMPOSITION_PTS: dict[str, int] = {
    "solid": 2,
    "predominantly_solid": 2,        # Emory ">95% solid" label
    "mixed cystic and solid": 1,
    "mixed": 1,
    "predominantly_cystic": 1,       # Emory "<5% solid" label, conservative
    "cystic": 0,
    "anechoic": 0,
    "spongiform": 0,
    # NB: composition='hypoechoic' (1 row) is a data error -> coerced to NULL
    # below before lookup, so deliberately absent from this map.
}
ECHOGENICITY_PTS: dict[str, int] = {
    "anechoic": 0,
    "hyperechoic": 1,
    "isoechoic": 1,
    "hypoechoic": 2,
    "very_hypoechoic": 3,
}
SHAPE_PTS: dict[str, int] = {
    "wider_than_tall": 0,
    "taller_than_wide": 3,
}
MARGIN_PTS: dict[str, int] = {
    "smooth": 0,
    "ill_defined": 0,
    "lobulated": 2,
    "irregular": 2,
    "microlobulated": 2,
    "extrathyroidal_extension": 3,
}
FOCI_PTS: dict[str, int] = {
    "none": 0,
    "": 0,
    "large_comet_tail": 0,
    "large_comet_tail_artifacts": 0,
    "macrocalcifications": 1,
    "peripheral_calcifications": 2,
    "peripheral_rim_calcifications": 2,
    "punctate_echogenic_foci": 3,
}


# ---------------------------------------------------------------------------
# Normalizers
# ---------------------------------------------------------------------------
def norm_space_to_underscore(s: str | None) -> str | None:
    if s is None:
        return None
    out = s.strip().lower().replace("-", "_").replace(" ", "_")
    return out or None


def norm_foci_tokens(raw: str | None) -> list[str]:
    """Parse python-list-repr (or bare token) into a clean lowercase token list.

    Returns an empty list when the value represents "no foci" — i.e. None,
    empty, ``[]``, ``[none]``, bare ``none``.
    """
    if raw is None:
        return []
    s = raw.strip()
    if s in ("", "[]", "none", "[none]", "[None]"):
        return []
    s = s.strip("[]")
    tokens: list[str] = []
    for t in s.split(","):
        t = t.strip().strip('"').strip("'").lower().replace("-", "_").replace(" ", "_")
        if t and t != "none":
            tokens.append(t)
    return tokens


def foci_pts(raw: str | None) -> int | None:
    """Return MAX ACR 2017 points across the parsed foci token set.

    Empty / no-foci -> 0 points (the column was populated but with the
    explicit "no echogenic foci" sentinel, which is meaningful evidence).
    Tokens with no point mapping are skipped; if every token is unmapped
    we return None to surface the gap in the unmapped-values audit.
    """
    tokens = norm_foci_tokens(raw)
    if not tokens:
        return 0
    pts = [FOCI_PTS.get(t) for t in tokens]
    pts = [p for p in pts if p is not None]
    return max(pts) if pts else None


def pts_to_category(total: int | None) -> str | None:
    """ACR 2017 banding. Per-band cut points: 0 / 2 / 3 / 4-6 / >=7.

    1 point falls outside the published bands (the system was designed so the
    minimum non-zero score is 2); we return None there to avoid implying a
    spurious category.
    """
    if total is None:
        return None
    if total == 0:
        return "TR1"
    if total == 2:
        return "TR2"
    if total == 3:
        return "TR3"
    if 4 <= total <= 6:
        return "TR4"
    if total >= 7:
        return "TR5"
    return None  # total == 1 -> undefined band


# ---------------------------------------------------------------------------
# Driver helpers
# ---------------------------------------------------------------------------
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


def add_source_modality(con, args) -> dict:
    """Phase 1: idempotent ALTER for source_modality + backfill + verify.

    Logs (and continues past) the absence of MotherDuck-side
    ``CHECK (source_modality = 'US')`` enforcement. The DEFAULT value plus
    an explicit NOT-NULL/uniformity assertion below provide the same
    runtime guarantee.
    """
    cols = existing_columns(con)
    if "source_modality" in cols:
        log("Phase 1: source_modality column already present (skip ADD)")
    else:
        log("Phase 1: ADD COLUMN source_modality VARCHAR DEFAULT 'US'")
        if args.commit:
            con.execute(
                f"ALTER TABLE {TARGET} "
                "ADD COLUMN source_modality VARCHAR DEFAULT 'US'"
            )

    # Defensive backfill (DEFAULT only fills new rows; existing rows
    # might be NULL on systems where ADD COLUMN doesn't backfill).
    if args.commit:
        con.execute(
            f"UPDATE {TARGET} SET source_modality = 'US' "
            "WHERE source_modality IS NULL"
        )

    # CHECK constraint via ALTER TABLE is not supported in DuckDB/MotherDuck
    # ("Not implemented Error: No support for that ALTER TABLE option yet!"
    # for ADD CONSTRAINT, "Adding columns with constraints not yet
    # supported" for inline). Document the limitation and rely on the
    # DEFAULT + explicit invariant assertion instead.
    log("Phase 1: CHECK (source_modality='US') skipped — DuckDB/MotherDuck "
        "does not support ALTER TABLE ADD CONSTRAINT or inline CHECK on "
        "ADD COLUMN. DEFAULT 'US' + post-write assertion enforce uniformity.")

    if args.commit:
        rows = con.execute(
            f"SELECT source_modality, COUNT(*) FROM {TARGET} GROUP BY 1"
        ).fetchall()
        log(f"Phase 1 verify: source_modality groups = {rows}")
        if not (len(rows) == 1 and rows[0][0] == "US"):
            raise SystemExit(
                f"source_modality invariant violated: {rows!r}"
            )
        return {"source_modality_groups": rows}
    return {"source_modality_groups": None}


def take_snapshot(con, args) -> int:
    """Phase 2-pre: row-level snapshot before any UPDATE."""
    if not args.commit:
        log("snapshot: dry-run, skipping")
        return 0
    log(f"Phase 2-pre: writing snapshot -> {SNAPSHOT}")
    con.execute(f"DROP TABLE IF EXISTS {SNAPSHOT}")
    con.execute(f"""
        CREATE TABLE {SNAPSHOT} AS
        SELECT nodule_id, research_id, us_exam_id, nodule_index_within_exam,
               composition, echogenicity, shape, margins, echogenic_foci,
               composition_pts, echogenicity_pts, shape_pts, margin_pts, foci_pts,
               acr2017_tirads_points, acr2017_tirads_category,
               updated_tirads_category, acr2017_vs_updated_concordant
        FROM {TARGET}
    """)
    n = con.execute(f"SELECT COUNT(*) FROM {SNAPSHOT}").fetchone()[0]
    log(f"  snapshot rows: {n}")
    snap_comment = (
        "Pre-Script-376 snapshot of canonical_us_nodule_v2 feature, "
        "per-feature points, ACR 2017 aggregate, and concordance flag "
        f"columns. Captured {RUN_TS} before normalize+recompute."
    ).replace("'", "''")
    con.execute(f"COMMENT ON TABLE {SNAPSHOT} IS '{snap_comment}';")
    return n


def fetch_feature_rows(con) -> list[tuple]:
    log("Phase 2: fetching feature-bearing rows")
    rows = con.execute(f"""
        SELECT nodule_id, composition, echogenicity, shape, margins,
               echogenic_foci, acr2017_tirads_category, updated_tirads_category
        FROM {TARGET}
        WHERE composition    IS NOT NULL
           OR echogenicity   IS NOT NULL
           OR shape          IS NOT NULL
           OR margins        IS NOT NULL
           OR echogenic_foci IS NOT NULL
    """).fetchall()
    log(f"  fetched {len(rows)} rows")
    return rows


def compute_updates(rows: list[tuple]) -> list[tuple]:
    log("Phase 2: computing per-row updates")
    updates: list[tuple] = []
    for (nid, comp, ech, shp, mrg, foci, _acr_cat, upd_cat) in rows:
        # Composition: lowercase + strip; coerce data-error 'hypoechoic' to NULL.
        comp_norm = comp.strip().lower() if comp else None
        if comp_norm == "hypoechoic":
            comp_norm = None
        # Echogenicity values are already canonical underscore form.
        ech_norm = ech.strip().lower() if ech else None
        # Shape and margins have space/hyphen variants -> underscore form.
        shp_norm = norm_space_to_underscore(shp)
        mrg_norm = norm_space_to_underscore(mrg)

        cp = COMPOSITION_PTS.get(comp_norm) if comp_norm else None
        ep = ECHOGENICITY_PTS.get(ech_norm) if ech_norm else None
        sp = SHAPE_PTS.get(shp_norm) if shp_norm else None
        mp = MARGIN_PTS.get(mrg_norm) if mrg_norm else None
        fp = foci_pts(foci)

        total: int | None = None
        if None not in (cp, ep, sp, mp, fp):
            total = cp + ep + sp + mp + fp  # type: ignore[operator]
        new_cat = pts_to_category(total)

        if new_cat is not None and upd_cat is not None:
            concordant: bool | None = (new_cat == upd_cat)
        else:
            concordant = None

        updates.append((
            nid, comp_norm, ech_norm, shp_norm, mrg_norm,
            cp, ep, sp, mp, fp,
            total, new_cat, concordant,
        ))
    log(f"  computed {len(updates)} updates")
    # Quick in-memory tallies for the dry-run preview.
    n_full = sum(1 for u in updates if u[10] is not None)
    n_new_cat = sum(1 for u in updates if u[11] is not None)
    n_disagree = sum(1 for u in updates if u[12] is False)
    log(f"  (preview) all-5-resolved: {n_full} | new_cat: {n_new_cat} | "
        f"disagreeing: {n_disagree}")
    return updates


def write_back(con, updates: list[tuple]) -> None:
    log(f"Phase 2: write-back via temp staging ({len(updates)} rows)")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _s376_stage (
          nodule_id VARCHAR PRIMARY KEY,
          composition VARCHAR,
          echogenicity VARCHAR,
          shape VARCHAR,
          margins VARCHAR,
          composition_pts INT,
          echogenicity_pts INT,
          shape_pts INT,
          margin_pts INT,
          foci_pts INT,
          acr2017_tirads_points INT,
          acr2017_tirads_category VARCHAR,
          acr2017_vs_updated_concordant BOOLEAN
        )
    """)
    con.executemany(
        "INSERT INTO _s376_stage VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        updates,
    )

    # OVERWRITE strategy:
    #   * feature strings + per-feature *_pts: unconditional overwrite
    #     (the new normalized form is strictly more usable).
    #   * acr2017_tirads_points / category: only overwrite when the new
    #     computation resolves all 5 features (preserves provenance for
    #     rows where one or more features remain unmappable).
    #   * acr2017_vs_updated_concordant: full overwrite (this is the bug
    #     fix — old logic returned NULL on genuine disagreements).
    con.execute(f"""
        UPDATE {TARGET} AS v
        SET composition  = s.composition,
            echogenicity = s.echogenicity,
            shape        = s.shape,
            margins      = s.margins,
            composition_pts  = s.composition_pts,
            echogenicity_pts = s.echogenicity_pts,
            shape_pts        = s.shape_pts,
            margin_pts       = s.margin_pts,
            foci_pts         = s.foci_pts,
            acr2017_tirads_points = COALESCE(
                s.acr2017_tirads_points, v.acr2017_tirads_points
            ),
            acr2017_tirads_category = COALESCE(
                s.acr2017_tirads_category, v.acr2017_tirads_category
            ),
            acr2017_vs_updated_concordant = s.acr2017_vs_updated_concordant
        FROM _s376_stage s
        WHERE v.nodule_id = s.nodule_id
    """)
    log("  write-back complete")

    # The Python-side flag is derived from the FRESHLY computed category;
    # when COALESCE preserved a prior acr2017_tirads_category for a row
    # whose 5 features didn't fully resolve, the flag was left NULL even
    # though the *stored* acr/updated categories agreed or disagreed. Fix
    # by recomputing the flag in SQL against the final persisted values
    # (this is the canonical semantics of "do the two categories agree?").
    log("  recomputing acr2017_vs_updated_concordant from final stored values")
    con.execute(f"""
        UPDATE {TARGET} SET
            acr2017_vs_updated_concordant = CASE
                WHEN acr2017_tirads_category IS NOT NULL
                 AND updated_tirads_category IS NOT NULL
                THEN (acr2017_tirads_category = updated_tirads_category)
                ELSE NULL
            END
    """)


# ---------------------------------------------------------------------------
# Phase 3 — verification queries
# ---------------------------------------------------------------------------
def phase3_verify(con) -> dict:
    log("Phase 3: verification")
    coverage = con.execute(f"""
        SELECT
          COUNT(*) FILTER (WHERE composition_pts IS NOT NULL)         AS comp_pts_n,
          COUNT(*) FILTER (WHERE echogenicity_pts IS NOT NULL)        AS ech_pts_n,
          COUNT(*) FILTER (WHERE shape_pts IS NOT NULL)               AS shape_pts_n,
          COUNT(*) FILTER (WHERE margin_pts IS NOT NULL)              AS margin_pts_n,
          COUNT(*) FILTER (WHERE foci_pts IS NOT NULL)                AS foci_pts_n,
          COUNT(*) FILTER (WHERE acr2017_tirads_points IS NOT NULL)   AS acr_pts_total_n,
          COUNT(*) FILTER (WHERE acr2017_tirads_category IS NOT NULL) AS acr_cat_n
        FROM {TARGET}
    """).fetchone()
    cov_keys = ["comp_pts_n", "ech_pts_n", "shape_pts_n", "margin_pts_n",
                "foci_pts_n", "acr_pts_total_n", "acr_cat_n"]
    cov = dict(zip(cov_keys, coverage))
    log(f"  coverage: {cov}")

    flag_rows = con.execute(f"""
        SELECT acr2017_vs_updated_concordant,
               (acr2017_tirads_category = updated_tirads_category) AS raw_equal,
               COUNT(*) AS n
        FROM {TARGET}
        WHERE acr2017_tirads_category IS NOT NULL
          AND updated_tirads_category IS NOT NULL
        GROUP BY 1, 2
        ORDER BY 1, 2
    """).fetchall()
    log("  concordance flag truth-table:")
    for r in flag_rows:
        log(f"    {r}")

    matrix = con.execute(f"""
        SELECT acr2017_tirads_category, updated_tirads_category, COUNT(*)
        FROM {TARGET}
        WHERE acr2017_tirads_category IS NOT NULL
          AND updated_tirads_category IS NOT NULL
        GROUP BY 1, 2
        ORDER BY 1, 2
    """).fetchall()
    log("  concordance matrix (acr_cat, upd_cat, n):")
    for r in matrix:
        log(f"    {r}")

    unmapped = con.execute(f"""
        SELECT 'composition' AS col, composition AS val, COUNT(*) AS n
        FROM {TARGET}
        WHERE composition IS NOT NULL AND composition_pts IS NULL
        GROUP BY 1, 2
        UNION ALL
        SELECT 'echogenicity', echogenicity, COUNT(*) FROM {TARGET}
        WHERE echogenicity IS NOT NULL AND echogenicity_pts IS NULL GROUP BY 1, 2
        UNION ALL
        SELECT 'shape', shape, COUNT(*) FROM {TARGET}
        WHERE shape IS NOT NULL AND shape_pts IS NULL GROUP BY 1, 2
        UNION ALL
        SELECT 'margins', margins, COUNT(*) FROM {TARGET}
        WHERE margins IS NOT NULL AND margin_pts IS NULL GROUP BY 1, 2
        UNION ALL
        SELECT 'echogenic_foci', echogenic_foci, COUNT(*) FROM {TARGET}
        WHERE echogenic_foci IS NOT NULL AND foci_pts IS NULL GROUP BY 1, 2
        ORDER BY col, n DESC
    """).fetchall()
    log(f"  unmapped feature strings ({len(unmapped)} groups):")
    for r in unmapped:
        log(f"    {r}")

    modality = con.execute(
        f"SELECT source_modality, COUNT(*) FROM {TARGET} GROUP BY 1"
    ).fetchall()
    log(f"  source_modality: {modality}")

    return {
        "coverage": cov,
        "concordance_flag_truth_table": [
            {"flag": r[0], "raw_equal": r[1], "n": r[2]} for r in flag_rows
        ],
        "concordance_matrix": [
            {"acr": r[0], "updated": r[1], "n": r[2]} for r in matrix
        ],
        "unmapped_feature_strings": [
            {"col": r[0], "val": r[1], "n": r[2]} for r in unmapped
        ],
        "source_modality_groups": [
            {"source_modality": r[0], "n": r[1]} for r in modality
        ],
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--commit", action="store_true",
                    help="Apply changes (default: dry-run preview).")
    args = ap.parse_args()
    log(f"{SCRIPT_TAG} start  commit={args.commit}")

    con = connect_locked()

    phase1 = add_source_modality(con, args)
    snap_n = take_snapshot(con, args)

    rows = fetch_feature_rows(con)
    updates = compute_updates(rows)

    if args.commit:
        write_back(con, updates)

    phase3 = phase3_verify(con) if args.commit else {"note": "dry-run, skipped"}

    DECISION_LOG.write_text(json.dumps({
        "script": SCRIPT_TAG,
        "run_ts_utc": RUN_TS,
        "commit": args.commit,
        "target": TARGET,
        "snapshot_table": SNAPSHOT if args.commit else None,
        "snapshot_rows": snap_n,
        "feature_rows_processed": len(rows),
        "phase1_source_modality": phase1,
        "phase3_verify": phase3,
    }, indent=2, default=str))
    log(f"decision log: {DECISION_LOG}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
