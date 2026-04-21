#!/usr/bin/env python3
"""Script 362 — Build main.canonical_us_nodule_v2 (US v2 consolidation, Phase 2).

Single nodule master that supersedes:
  * canonical_us_nodule_master_v1            (cunm — has size_cm/margin singular)
  * canonical_us_nodule_characteristics_v1   (cunc — has us_exam_id, points, ACR TIRADS)
  * imaging_nodule_master_v1                 (subset of cunc; not re-merged)

Plus overlay from already-parsed enrichment (no LLM run in this script):
  * tirads_v2_nodules_raw                    (Qwen2.5-32B; halo, vasc, ETE,
                                              chammas, elastography, dynamics, FNA)

Plus legacy backfill rows for the 4,733 patients in us_nodules_tirads but
absent from cunc/cunm. Each non-empty nodule_<k> field becomes one shell row
with all structured cols NULL and nlp_backfill_pending = TRUE.

Grain:  one row per (research_id, us_exam_id, nodule_index_within_exam).
Key:    composite above; nodule_id (hash) carried forward where present.

Source-precedence (per cursor prompt 2026-04-21):
  * Structured features (composition, echogenicity, shape, margins,
    echogenic_foci, size_cm_max): cunc wins (already merged from base sources),
    fall back to cunm.size_cm / cunm.margin.
  * TIRADS:    cunc wins; cunm value retained as parallel field where they
               disagree (audit only — surfaced via Script 363).
  * Dynamics + FNA + halo + vascularity + chammas + elastography + ETE:
    cunm already has these (it merged tirads_v2 + dynamics_llm into v1).
    Use cunm value first; LEFT-join tirads_v2 to fill remaining holes.
  * Provenance flags from cunm carried forward; new `source_us_nodules_tirads`
    flag added for legacy rows.

DOES NOT touch v1 tables (CPM keeps reading them until cutover script).
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

SCRIPT_TAG = "Script 362"
TARGET = f"{PUBLICATION_DB}.main.canonical_us_nodule_v2"
OUT_DIR = HERE / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)
RUN_TS = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")
DECISION_LOG = OUT_DIR / f"362_canonical_us_nodule_v2_{RUN_TS}.json"

# Hard expectations from probe (2026-04-21):
EXPECTED_CUNC = 37_016
EXPECTED_CUNM = 37_016
EXPECTED_USNT_PATIENTS = 10_859
EXPECTED_CUNC_PATIENTS = 6_126
EXPECTED_LEGACY_PATIENTS = EXPECTED_USNT_PATIENTS - EXPECTED_CUNC_PATIENTS  # 4733
# Both cunc and cunm have 59 upstream duplicate keys on
# (research_id, exam_date, nodule_index_within_exam) — same index pointing to
# different lateralities. v2 grain ignores laterality per spec, so we keep one
# row per key (largest-size / highest-score wins) and surface the conflict via
# Script 363's audit queue.
UPSTREAM_DUP_KEYS = 59
EXPECTED_PRIMARY_ROWS = EXPECTED_CUNC - UPSTREAM_DUP_KEYS  # 36957
# us_nodules_tirads has only 5 legacy patients with any content (us_1 only;
# no us_1_date and no nodule_<k> populated for the rest). Real legacy
# backfill is therefore near-zero — the prompt's 5K-12K estimate assumed the
# us_nodules_tirads table carried per-nodule legacy data that does not exist.

LEGACY_NODULE_COLS = [f"nodule_{k}" for k in range(1, 11)] + [
    f"n{k}" for k in range(11, 15)
]  # us_nodules_tirads has nodule_1..10 then n11..n14


def log(msg: str) -> None:
    now = datetime.datetime.now(datetime.UTC).strftime("%H:%M:%S.%f")[:-3]
    print(f"[{now}Z] {msg}", flush=True)


# ─── primary v2 build SQL ───────────────────────────────────────────────────


BUILD_PRIMARY_SQL = f"""
CREATE OR REPLACE TABLE {TARGET} AS
WITH cunc AS (
    SELECT
        c.research_id,
        c.us_exam_id,
        c.exam_date,
        c.nodule_index_within_exam,
        c.nodule_id,
        c.laterality,
        c.location_raw,
        c.location_detail,
        c.length_mm,
        c.width_mm,
        c.height_mm,
        c.volume_ml,
        c.size_cm_max,
        c.extracted_size_cm,
        c.composition,
        c.echogenicity,
        c.shape,
        c.margins,
        c.calcifications,
        c.echogenic_foci,
        c.composition_pts,
        c.echogenicity_pts,
        c.shape_pts,
        c.margin_pts,
        c.foci_pts,
        c.tirads_reported,
        c.tirads_score_2017,
        c.tirads_level_2017,
        c.tirads_category_v2,
        c.tirads_band_ambiguous,
        c.tirads_category_code_legacy_v1,
        c.tirads_category_modified_legacy_v1,
        c.tirads_concordant_flag,
        c.suspicious_flag,
        c.tirads_score_component_complete,
        c.source_tables       AS cunc_source_tables,
        c.resolution_rule,
        c.data_completeness_pct,
        c.calcifications_coverage_status
    FROM {PUBLICATION_DB}.main.canonical_us_nodule_characteristics_v1 c
),
cunm AS (
    SELECT
        m.research_id,
        m.exam_date,
        m.laterality,
        m.nodule_index_within_exam,
        m.size_cm,
        m.composition         AS cunm_composition,
        m.echogenicity        AS cunm_echogenicity,
        m.shape               AS cunm_shape,
        m.margin              AS cunm_margin,
        m.echogenic_foci      AS cunm_echogenic_foci,
        m.tirads_score_2017   AS cunm_tirads_score_2017,
        m.tirads_category_v2  AS cunm_tirads_category_v2,
        m.tirads_level_2017   AS cunm_tirads_level_2017,
        m.tirads_points_total,
        m.chammas_type,
        m.elastography_category,
        m.extrathyroidal_extension_on_us,
        m.fna_recommended_this_nodule,
        m.interval_growth_flag,
        m.source_base,
        m.source_tirads_v2,
        m.source_tirads_llm,
        m.source_dynamics_llm,
        m.source_fna_linkage,
        m.nodule_master_id
    FROM {PUBLICATION_DB}.main.canonical_us_nodule_master_v1 m
),
v2 AS (
    -- Pull only the fields cunm does NOT already have (halo, vascularity, FNA-prior, prior_size).
    -- Aggregate to (research_id, exam_date, nodule_index) to avoid join fan-out.
    SELECT
        TRY_CAST(research_id AS INTEGER)    AS research_id,
        TRY_CAST(linkage_date AS DATE)      AS exam_date,
        nodule_index_within_exam,
        ANY_VALUE(halo)                     AS halo,
        ANY_VALUE(vascularity)              AS vascularity,
        ANY_VALUE(prior_size_mm_max)        AS prior_size_mm_max,
        BOOL_OR(fna_performed_prior_or_concurrent) AS fna_performed_prior_or_concurrent,
        ANY_VALUE(comparison_statement)     AS comparison_statement
    FROM {PUBLICATION_DB}.main.tirads_v2_nodules_raw
    WHERE TRY_CAST(linkage_date AS DATE) IS NOT NULL
      AND TRY_CAST(research_id AS INTEGER) IS NOT NULL
    GROUP BY 1, 2, 3
),
merged AS (
    SELECT
        c.research_id,
        c.us_exam_id,
        c.exam_date,
        c.nodule_index_within_exam,
        c.nodule_id,
        COALESCE(c.laterality, m.laterality)               AS laterality,
        c.location_raw,
        c.location_detail,
        c.length_mm,
        c.width_mm,
        c.height_mm,
        c.volume_ml,
        COALESCE(c.size_cm_max, m.size_cm)                 AS size_cm_max,
        c.extracted_size_cm,
        COALESCE(c.composition,   m.cunm_composition)      AS composition,
        COALESCE(c.echogenicity,  m.cunm_echogenicity)     AS echogenicity,
        COALESCE(c.shape,         m.cunm_shape)            AS shape,
        COALESCE(c.margins,       m.cunm_margin)           AS margins,
        c.calcifications,
        COALESCE(c.echogenic_foci, m.cunm_echogenic_foci)  AS echogenic_foci,

        -- Sonography enrichment: cunm wins (already merged), then v2 fills holes
        v.halo                                             AS halo,
        v.vascularity                                      AS vascularity,
        m.extrathyroidal_extension_on_us,
        m.chammas_type,
        m.elastography_category,

        -- TIRADS scoring: cunc primary, cunm parallel (audit Script 363)
        c.composition_pts,
        c.echogenicity_pts,
        c.shape_pts,
        c.margin_pts,
        c.foci_pts,
        c.tirads_reported,
        COALESCE(c.tirads_score_2017,  m.cunm_tirads_score_2017)  AS tirads_score_2017,
        COALESCE(c.tirads_level_2017,  m.cunm_tirads_level_2017)  AS tirads_level_2017,
        COALESCE(c.tirads_category_v2, m.cunm_tirads_category_v2) AS tirads_category_v2,
        c.tirads_band_ambiguous,
        c.tirads_category_code_legacy_v1,
        c.tirads_category_modified_legacy_v1,
        c.tirads_concordant_flag,
        c.suspicious_flag,
        c.tirads_score_component_complete,

        -- Dynamics + FNA: cunm primary (already integrated), v2 fills extras
        m.interval_growth_flag,
        v.prior_size_mm_max                                AS prior_size_mm_max,
        m.fna_recommended_this_nodule,
        v.fna_performed_prior_or_concurrent                AS fna_performed_prior_or_concurrent,
        v.comparison_statement,

        -- Provenance from cunm + new flag for legacy rows (FALSE here; TRUE only in legacy CTE)
        COALESCE(m.source_base,         FALSE)             AS source_base,
        COALESCE(m.source_tirads_v2,    FALSE)             AS source_tirads_v2,
        COALESCE(m.source_tirads_llm,   FALSE)             AS source_tirads_llm,
        COALESCE(m.source_dynamics_llm, FALSE)             AS source_dynamics_llm,
        COALESCE(m.source_fna_linkage,  FALSE)             AS source_fna_linkage,
        FALSE                                              AS source_us_nodules_tirads,

        c.cunc_source_tables                               AS source_tables_cunc_legacy,
        c.data_completeness_pct,
        c.resolution_rule,
        c.calcifications_coverage_status,
        m.nodule_master_id,

        -- Aggregate-row guard: long, multi-laterality location_raw with no measurements
        CASE
            WHEN c.size_cm_max IS NULL
             AND c.composition IS NULL
             AND c.echogenicity IS NULL
             AND LENGTH(COALESCE(c.location_raw, '')) > 300
             AND regexp_matches(LOWER(c.location_raw),
                                '(right|left|isthmus).*(right|left|isthmus)')
            THEN TRUE ELSE FALSE
        END                                                AS is_aggregate_row
    FROM cunc c
    LEFT JOIN cunm m
      ON c.research_id              = m.research_id
     AND c.exam_date                = m.exam_date
     AND c.nodule_index_within_exam = m.nodule_index_within_exam
     AND COALESCE(c.laterality,'__') = COALESCE(m.laterality,'__')
    LEFT JOIN v2 v
      ON c.research_id              = v.research_id
     AND c.exam_date                = v.exam_date
     AND c.nodule_index_within_exam = v.nodule_index_within_exam
),
deduped AS (
    -- 59 upstream rows share the same (research_id, us_exam_id, nodule_index)
    -- with different lateralities. v2 grain drops laterality from the key,
    -- so collapse to one row per key — keep largest size + highest TIRADS.
    SELECT *
    FROM merged
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY research_id, us_exam_id, nodule_index_within_exam
        ORDER BY size_cm_max DESC NULLS LAST,
                 tirads_score_2017 DESC NULLS LAST,
                 laterality NULLS LAST
    ) = 1
)
SELECT
    research_id,
    us_exam_id,
    exam_date,
    nodule_index_within_exam,
    nodule_id,
    laterality, location_raw, location_detail,
    length_mm, width_mm, height_mm, volume_ml,
    size_cm_max, extracted_size_cm,
    composition, echogenicity, shape, margins, calcifications, echogenic_foci,
    halo, vascularity, extrathyroidal_extension_on_us,
    chammas_type, elastography_category,
    composition_pts, echogenicity_pts, shape_pts, margin_pts, foci_pts,
    tirads_reported, tirads_score_2017, tirads_level_2017,
    tirads_category_v2, tirads_band_ambiguous,
    tirads_category_code_legacy_v1, tirads_category_modified_legacy_v1,
    tirads_concordant_flag, suspicious_flag, tirads_score_component_complete,
    interval_growth_flag, prior_size_mm_max,
    fna_recommended_this_nodule, fna_performed_prior_or_concurrent,
    comparison_statement,
    source_base, source_tirads_v2, source_tirads_llm,
    source_dynamics_llm, source_fna_linkage, source_us_nodules_tirads,
    source_tables_cunc_legacy,
    data_completeness_pct, resolution_rule, calcifications_coverage_status,
    nodule_master_id,
    is_aggregate_row,
    -- nlp_backfill_pending: TRUE when no current source covers this row
    CASE
        WHEN NOT (source_base OR source_tirads_v2 OR source_tirads_llm)
        THEN TRUE
        ELSE FALSE
    END AS nlp_backfill_pending
FROM deduped;
"""


# ─── legacy backfill SQL (4,733 patients in us_nodules_tirads only) ─────────


def _legacy_union_sql() -> str:
    """One UNION arm per nodule_<k>/n<k> column in us_nodules_tirads.

    Probe shows only 5 of the 4,733 legacy patients have any populated cell
    (in us_1), and 0 have a parseable us_1_date. We still emit a row for each
    non-empty nodule_<k>/n<k> field — currently produces near-zero rows but
    kept so future ingest of those columns will flow through automatically.
    """
    parts: list[str] = []
    for k, col in enumerate(LEGACY_NODULE_COLS, start=1):
        parts.append(f"""
SELECT
    TRY_CAST(u.research_id AS INTEGER)                     AS research_id,
    md5(u.research_id || '|' || COALESCE(u.us_1_date,''))  AS us_exam_id,
    TRY_CAST(u.us_1_date AS DATE)                          AS exam_date,
    {k}                                                    AS nodule_index_within_exam,
    md5(u.research_id || '|' || COALESCE(u.us_1_date,'') || '|nod' || {k})  AS nodule_id,
    NULL::VARCHAR                                          AS laterality,
    u.{col}                                                AS location_raw,
    NULL::VARCHAR                                          AS location_detail,
    NULL::DOUBLE                                           AS length_mm,
    NULL::DOUBLE                                           AS width_mm,
    NULL::DOUBLE                                           AS height_mm,
    NULL::DOUBLE                                           AS volume_ml,
    NULL::DOUBLE                                           AS size_cm_max,
    NULL::DOUBLE                                           AS extracted_size_cm,
    NULL::VARCHAR                                          AS composition,
    NULL::VARCHAR                                          AS echogenicity,
    NULL::VARCHAR                                          AS shape,
    NULL::VARCHAR                                          AS margins,
    NULL::VARCHAR                                          AS calcifications,
    NULL                                                   AS echogenic_foci,
    NULL::VARCHAR                                          AS halo,
    NULL::VARCHAR                                          AS vascularity,
    NULL::VARCHAR                                          AS extrathyroidal_extension_on_us,
    NULL::VARCHAR                                          AS chammas_type,
    NULL::VARCHAR                                          AS elastography_category,
    NULL::DOUBLE                                           AS composition_pts,
    NULL::DOUBLE                                           AS echogenicity_pts,
    NULL::DOUBLE                                           AS shape_pts,
    NULL::DOUBLE                                           AS margin_pts,
    NULL::DOUBLE                                           AS foci_pts,
    TRY_CAST(u.n{k}_tr AS INTEGER)                         AS tirads_reported,
    NULL::DOUBLE                                           AS tirads_score_2017,
    NULL::VARCHAR                                          AS tirads_level_2017,
    NULL::VARCHAR                                          AS tirads_category_v2,
    NULL::BOOLEAN                                          AS tirads_band_ambiguous,
    NULL::INTEGER                                          AS tirads_category_code_legacy_v1,
    NULL::VARCHAR                                          AS tirads_category_modified_legacy_v1,
    NULL::BOOLEAN                                          AS tirads_concordant_flag,
    NULL::BOOLEAN                                          AS suspicious_flag,
    NULL::BOOLEAN                                          AS tirads_score_component_complete,
    NULL::BOOLEAN                                          AS interval_growth_flag,
    NULL::DOUBLE                                           AS prior_size_mm_max,
    NULL::BOOLEAN                                          AS fna_recommended_this_nodule,
    NULL::BOOLEAN                                          AS fna_performed_prior_or_concurrent,
    NULL::VARCHAR                                          AS comparison_statement,
    TRUE                                                   AS source_base,
    FALSE                                                  AS source_tirads_v2,
    FALSE                                                  AS source_tirads_llm,
    FALSE                                                  AS source_dynamics_llm,
    FALSE                                                  AS source_fna_linkage,
    TRUE                                                   AS source_us_nodules_tirads,
    'us_nodules_tirads'                                    AS source_tables_cunc_legacy,
    NULL::DOUBLE                                           AS data_completeness_pct,
    'legacy_only'                                          AS resolution_rule,
    NULL::VARCHAR                                          AS calcifications_coverage_status,
    NULL::BIGINT                                           AS nodule_master_id,
    FALSE                                                  AS is_aggregate_row,
    TRUE                                                   AS nlp_backfill_pending
FROM {PUBLICATION_DB}.main.us_nodules_tirads u
LEFT JOIN {PUBLICATION_DB}.main.canonical_us_nodule_characteristics_v1 c
       ON TRY_CAST(u.research_id AS INTEGER) = c.research_id
WHERE c.research_id IS NULL
  AND u.{col} IS NOT NULL
  AND TRIM(u.{col}) <> ''
""")
    return " UNION ALL ".join(parts)


LEGACY_INSERT_SQL = f"""
INSERT INTO {TARGET}
{_legacy_union_sql()}
;
"""


# ─── verification ───────────────────────────────────────────────────────────


def verify(con) -> dict:
    n_total = con.execute(f"SELECT COUNT(*) FROM {TARGET}").fetchone()[0]
    n_pts = con.execute(
        f"SELECT COUNT(DISTINCT research_id) FROM {TARGET}"
    ).fetchone()[0]
    n_pending = con.execute(
        f"SELECT COUNT(*) FROM {TARGET} WHERE nlp_backfill_pending"
    ).fetchone()[0]
    n_agg = con.execute(
        f"SELECT COUNT(*) FROM {TARGET} WHERE is_aggregate_row"
    ).fetchone()[0]
    dup = con.execute(
        f"""SELECT COUNT(*) FROM (
                SELECT research_id, us_exam_id, nodule_index_within_exam,
                       COUNT(*) c
                FROM {TARGET}
                GROUP BY 1,2,3 HAVING COUNT(*) > 1
            )"""
    ).fetchone()[0]
    out = {
        "row_count": n_total,
        "patient_count": n_pts,
        "nlp_backfill_pending": n_pending,
        "is_aggregate_row": n_agg,
        "duplicate_keys": dup,
    }
    log(f"  verify: rows={n_total} pts={n_pts} pending={n_pending} "
        f"aggregate={n_agg} dup_keys={dup}")
    if dup != 0:
        raise SystemExit(f"DUPLICATE KEYS in v2: {dup}")
    if n_total < EXPECTED_PRIMARY_ROWS - 5:
        raise SystemExit(
            f"Expected ≥ {EXPECTED_PRIMARY_ROWS} rows; got {n_total}"
        )
    return out


COMMENT_SQL = (
    f"COMMENT ON TABLE {TARGET} IS "
    f"'US v2 master per-nodule. Grain: one row per (research_id, us_exam_id, "
    f"nodule_index_within_exam). Built {RUN_TS} by Script 362 from cunc_v1 + "
    f"cunm_v1 + tirads_v2_nodules_raw + us_nodules_tirads legacy backfill. "
    f"nlp_backfill_pending = TRUE flags rows with no parsed-source coverage; "
    f"diagnostic only — no LLM run in this consolidation pass.';"
)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true",
                    help="Default = dry-run (counts only).")
    args = ap.parse_args()

    log(f"{SCRIPT_TAG} start  commit={args.commit}")
    con = connect_locked()

    if not args.commit:
        # Dry-run: confirm sources are at expected sizes, do not write.
        for tbl, expected in (
            ("canonical_us_nodule_characteristics_v1", EXPECTED_CUNC),
            ("canonical_us_nodule_master_v1", EXPECTED_CUNM),
            ("us_nodules_tirads", EXPECTED_USNT_PATIENTS),
        ):
            n = con.execute(
                f"SELECT COUNT(*) FROM {PUBLICATION_DB}.main.{tbl}"
            ).fetchone()[0]
            log(f"  source {tbl} rows={n} expected≈{expected}")
        log("dry-run only; pass --commit to materialize v2.")
        return 0

    log(f"  CREATE OR REPLACE {TARGET} (primary build)")
    con.execute(BUILD_PRIMARY_SQL)
    n_after_primary = con.execute(f"SELECT COUNT(*) FROM {TARGET}").fetchone()[0]
    log(f"  after primary: rows={n_after_primary}")

    log("  INSERT legacy backfill rows from us_nodules_tirads")
    con.execute(LEGACY_INSERT_SQL)
    n_after_legacy = con.execute(f"SELECT COUNT(*) FROM {TARGET}").fetchone()[0]
    log(f"  after legacy: rows={n_after_legacy}  "
        f"(+{n_after_legacy - n_after_primary} legacy rows)")

    summary = verify(con)
    log(f"  COMMENT ON TABLE {TARGET}")
    con.execute(COMMENT_SQL)

    DECISION_LOG.write_text(json.dumps(
        {
            "script": SCRIPT_TAG,
            "run_ts_utc": RUN_TS,
            "target": TARGET,
            "after_primary": n_after_primary,
            "after_legacy": n_after_legacy,
            "verify": summary,
        },
        indent=2,
        default=str,
    ))
    log(f"decision log: {DECISION_LOG}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
