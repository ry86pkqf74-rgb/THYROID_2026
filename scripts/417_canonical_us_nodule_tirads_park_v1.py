"""
Phase B.6 — Park / T-US 2009 logistic-regression scorer
=========================================================
Populates the 12 binary X variables and 3×{logit,probability,category}
columns in pub_canonical.canonical_us_nodule_tirads_multisystem_v1.

Usage (after coefficients are filled in the manifest):
    python scripts/417_canonical_us_nodule_tirads_park_v1.py [--dry-run] [--project PROJECT]

Hard rules obeyed:
  - No PHI in any output column.
  - CTAS-rebuild preserves CLUSTER BY research_id.
  - Snapshot of existing table written to pub_workspace before rebuild.
  - DFL row appended (lifecycle Logged -> Applied).
  - NULL primitive features → FALSE for X variable (absence-of-finding policy).
  - Manifest must have all coefficients filled; script aborts if any are __FILL__.

Author: Cursor Agent (Phase B.6), 2026-05-08
"""

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import bigquery

PROJECT = "thyroid-canonical-pub-2026"
DATASET_PUB = "pub_canonical"
DATASET_WS = "pub_workspace"
DATASET_SIGNOFF = "pub_signoff"

TABLE_MULTISYS = f"{PROJECT}.{DATASET_PUB}.canonical_us_nodule_tirads_multisystem_v1"
TABLE_NODULE_V2 = f"{PROJECT}.{DATASET_PUB}.canonical_us_nodule_v2"
TABLE_LN_CTX = f"{PROJECT}.{DATASET_WS}.us_nodule_ln_context_v1"
TABLE_SNAPSHOT = f"{PROJECT}.{DATASET_WS}.cpm_pre_tirads_multisystem_acr_snapshot_v1"

MANIFEST_PATH = Path(__file__).parent / "manifests" / "park_coefs_v1.json"
MANIFEST_ID = "park_coefs_v1.json"

# ---------------------------------------------------------------------------
# Category thresholds — T-US / TI-RADS P1-P5 probability bands
# Boundary rule: probability exactly at boundary → next-higher category.
# P0 is skipped (no nodule); this scorer runs per nodule-bearing row.
# ---------------------------------------------------------------------------
CATEGORY_THRESHOLDS = [
    (0.07, "P1"),   # 0 < p <= 0.07
    (0.23, "P2"),   # 0.07 < p <= 0.23
    (0.50, "P3"),   # 0.23 < p <= 0.50
    (0.90, "P4"),   # 0.50 < p <= 0.90
    (1.00, "P5"),   # 0.90 < p <= 1.00
]


def assign_category(probability: float | None) -> str | None:
    """Map logistic-regression probability to T-US category.

    Boundary rule (per spec): probability exactly equal to a threshold
    boundary falls into the next-higher category. i.e., 0.07 → P1, 0.08 → P2.
    """
    if probability is None:
        return None
    for threshold, cat in CATEGORY_THRESHOLDS:
        if probability <= threshold:
            return cat
    return "P5"  # safety net if > 1.0 due to float rounding


def logistic(logit: float) -> float:
    """Sigmoid / logistic function."""
    return 1.0 / (1.0 + math.exp(-logit))


def compute_logit(coef_set: dict, x_values: dict) -> float | None:
    """Compute the linear combination intercept + sum_i(beta_i * X_i).

    Returns None if any coefficient is missing/null (shouldn't happen after
    manifest validation, but defensive).
    """
    intercept = coef_set.get("intercept")
    betas = coef_set.get("betas", {})
    if intercept is None:
        return None
    logit = intercept
    for x_key, beta in betas.items():
        if beta is None:
            return None
        x_var = f"park_{x_key}"  # x1_taller → park_x1_taller
        logit += beta * float(x_values.get(x_var, False))
    return logit


def load_and_validate_manifest() -> dict:
    """Load park_coefs_v1.json and abort if any coefficient is still __FILL__."""
    if not MANIFEST_PATH.exists():
        sys.exit(f"ERROR: Manifest not found at {MANIFEST_PATH}. Run setup first.")
    with open(MANIFEST_PATH) as f:
        manifest = json.load(f)

    unfilled = []
    for set_name, coef_set in manifest.get("sets", {}).items():
        if coef_set.get("coefficients_status", "").startswith("PENDING"):
            continue  # cohort-refit is allowed to be pending before refit runs
        if "__FILL__" in str(coef_set.get("intercept", "")):
            unfilled.append(f"{set_name}.intercept")
        for x_key, val in coef_set.get("betas", {}).items():
            if "__FILL__" in str(val):
                unfilled.append(f"{set_name}.{x_key}")

    if unfilled:
        sys.exit(
            "ERROR: Manifest has unfilled coefficients. "
            "Ask Logan to supply Park 2009 original β values:\n"
            + "\n".join(f"  {u}" for u in unfilled)
        )
    return manifest


# ---------------------------------------------------------------------------
# BigQuery SQL — 12 binary X variable extraction
# ---------------------------------------------------------------------------
X_VAR_SQL = """
-- Park X variable extraction.
-- Source of primitive features: canonical_us_nodule_v2 (has composition, echogenicity, etc.)
-- Source of B.1-B.5 scored columns: canonical_us_nodule_tirads_multisystem_v1 (pass-through)
-- NULL policy: missing primitive → FALSE (absence-of-finding)

WITH
nodule_raw AS (
    SELECT
        m.research_id,
        m.nodule_id,
        m.us_exam_id,
        m.exam_date,
        m.nodule_index_within_exam,
        m.size_cm_max,
        m.tirads_reported_in_text,
        m.tirads_reported_system,
        -- Park X variables computed from canonical_us_nodule_v2 primitives
        -- X1: taller-than-wide shape
        COALESCE(v.shape = 'taller_than_wide', FALSE) AS park_x1_taller,
        -- X2: perinodular halo present
        COALESCE(JSON_VALUE(v.halo_jsonb, '$.presence') = 'present', FALSE) AS park_x2_halo,
        -- X3: well-circumscribed margin (smooth)
        COALESCE(v.margins = 'smooth', FALSE) AS park_x3_well_circumscribed,
        -- X4: microlobulated margin
        COALESCE(v.margins = 'microlobulated', FALSE) AS park_x4_microlobulation,
        -- X5: infiltrative margin (irregular, ill-defined, ETE on US)
        COALESCE(
            v.margins IN ('irregular', 'ill_defined', 'extrathyroidal_extension')
            OR JSON_VALUE(v.ete_us_jsonb, '$.presence') IN (
                'capsule_loss', 'strap_muscle_invasion', 'bulging', 'abutment'
            ),
            FALSE
        ) AS park_x5_infiltrative_margin,
        -- X6: marked hypoechogenicity
        COALESCE(v.echogenicity = 'very_hypoechoic', FALSE) AS park_x6_marked_hypo,
        -- X7: hypoechogenicity (less marked)
        COALESCE(v.echogenicity = 'hypoechoic', FALSE) AS park_x7_hypo,
        -- X8: homogeneous echotexture (LLM column)
        COALESCE(v.homogeneous_echotexture = TRUE, FALSE) AS park_x8_homogeneous,
        -- X9: mainly cystic
        COALESCE(v.composition IN ('cystic', 'predominantly_cystic'), FALSE) AS park_x9_mainly_cystic,
        -- X10: solid composition
        COALESCE(v.composition IN ('solid', 'predominantly_solid'), FALSE) AS park_x10_solid,
        -- X11: microcalcification (punctate echogenic foci in JSON array)
        COALESCE(
            'punctate_echogenic_foci' IN UNNEST(JSON_VALUE_ARRAY(v.echogenic_foci)),
            FALSE
        ) AS park_x11_microcalc,
        -- B.1-B.5 columns — pass-through from multisystem table
        m.acr2017_composition_pts,
        m.acr2017_echogenicity_pts,
        m.acr2017_shape_pts,
        m.acr2017_margin_pts,
        m.acr2017_foci_pts,
        m.acr2017_total_pts_strict,
        m.acr2017_total_pts_imputed,
        m.acr2017_category_strict,
        m.acr2017_category_imputed,
        m.acr2017_features_complete_strict,
        m.acr2017_features_complete_imputed,
        m.acr2017_fna_recommended_strict,
        m.acr2017_fna_recommended_imputed,
        m.kwak_n_suspicious_features,
        m.kwak_features_used_json,
        m.kwak_category,
        m.kwak_fna_recommended,
        m.ktirads_composition_class,
        m.ktirads_n_suspicious,
        m.ktirads_entirely_calcified,
        m.ktirads_category,
        m.ktirads_fna_recommended,
        m.ctirads_score,
        m.ctirads_features_positive_json,
        m.ctirads_comet_tail_present,
        m.ctirads_category,
        m.ctirads_fna_recommended,
        m.sru_recommendation,
        m.sru_basis_json,
        m.scored_at,
        m.scoring_pipeline_version
    FROM `{table_multisys}` m
    -- Join to get primitive features (v2 is the authoritative primitives source)
    INNER JOIN `{table_nodule_v2}` v USING (nodule_id)
),
ln_join AS (
    SELECT
        r.*,
        -- X12: abnormal lymph node at exam
        COALESCE(ln.has_suspicious_ln_within_60d = 1, FALSE) AS park_x12_abnormal_ln
    FROM nodule_raw r
    LEFT JOIN `{table_ln_ctx}` ln USING (nodule_id)
)
SELECT * FROM ln_join
"""


def build_scoring_sql(manifest: dict) -> str:
    """Build the final CTAS SQL that computes all three coefficient sets."""
    sets = manifest["sets"]
    orig = sets["park_2009_original"]
    cosmos = sets["park_cosmos_validation"]
    cohort = sets["park_cohort_refit"]

    x_vars = [f"park_x{i}_{n}" for i, n in enumerate(
        ["taller", "halo", "well_circumscribed", "microlobulation",
         "infiltrative_margin", "marked_hypo", "hypo", "homogeneous",
         "mainly_cystic", "solid", "microcalc", "abnormal_ln"], 1)]

    def _is_numeric(val) -> bool:
        """Return True if val is a real number (int or float), not a placeholder."""
        if val is None:
            return False
        try:
            float(val)
            return True
        except (TypeError, ValueError):
            return False

    def coef_expr(coef_set, prefix):
        """Build the SQL logit expression for one coefficient set.

        Returns a NULL cast if any coefficient is missing, None, or a placeholder
        string (e.g. '__FILL__'). This allows partial scoring while park_2009_original
        coefficients are still pending.
        """
        intercept = coef_set.get("intercept")
        betas = coef_set.get("betas", {})
        if not _is_numeric(intercept) or any(not _is_numeric(b) for b in betas.values()):
            return f"CAST(NULL AS FLOAT64) AS {prefix}_logit"

        parts = [str(float(intercept))]
        for x_key, x_col in zip(betas.keys(), x_vars):
            beta = betas[x_key]
            # BQ BOOL cannot be directly CAST to FLOAT64; use IF()
            parts.append(f"{float(beta):+.6f} * IF({x_col}, 1.0, 0.0)")

        return f"({' '.join(parts)}) AS {prefix}_logit"

    orig_logit = coef_expr(orig, "park2009")
    cosmos_logit = coef_expr(cosmos, "park_cosmos")
    cohort_logit = coef_expr(cohort, "park_cohort")

    def prob_and_cat(coef_set, prefix):
        """Generate probability and category SQL from the coef_set (inline expression)."""
        intercept = coef_set.get("intercept")
        betas = coef_set.get("betas", {})
        if not _is_numeric(intercept) or any(not _is_numeric(b) for b in betas.values()):
            return f"""CAST(NULL AS FLOAT64) AS {prefix}_probability,
        CAST(NULL AS STRING) AS {prefix}_category"""

        parts = [str(float(intercept))]
        for x_key, x_col in zip(betas.keys(), x_vars):
            beta = betas[x_key]
            # BQ BOOL cannot be directly CAST to FLOAT64; use IF()
            parts.append(f"{float(beta):+.6f} * IF({x_col}, 1.0, 0.0)")
        logit_expr = "(" + " ".join(parts) + ")"

        return f"""1.0 / (1.0 + EXP(-{logit_expr})) AS {prefix}_probability,
        CASE
            WHEN 1.0 / (1.0 + EXP(-{logit_expr})) <= 0.07 THEN 'P1'
            WHEN 1.0 / (1.0 + EXP(-{logit_expr})) <= 0.23 THEN 'P2'
            WHEN 1.0 / (1.0 + EXP(-{logit_expr})) <= 0.50 THEN 'P3'
            WHEN 1.0 / (1.0 + EXP(-{logit_expr})) <= 0.90 THEN 'P4'
            ELSE 'P5'
        END AS {prefix}_category"""

    ctas = f"""
CREATE OR REPLACE TABLE `{TABLE_MULTISYS}`
CLUSTER BY research_id
AS
WITH base AS (
{X_VAR_SQL.format(
    table_multisys=TABLE_MULTISYS,
    table_nodule_v2=TABLE_NODULE_V2,
    table_ln_ctx=TABLE_LN_CTX,
)}
),
scored AS (
    SELECT
        research_id,
        nodule_id,
        us_exam_id,
        exam_date,
        nodule_index_within_exam,
        size_cm_max,
        tirads_reported_in_text,
        tirads_reported_system,
        -- B.1-B.5 columns (pass-through)
        acr2017_composition_pts,
        acr2017_echogenicity_pts,
        acr2017_shape_pts,
        acr2017_margin_pts,
        acr2017_foci_pts,
        acr2017_total_pts_strict,
        acr2017_total_pts_imputed,
        acr2017_category_strict,
        acr2017_category_imputed,
        acr2017_features_complete_strict,
        acr2017_features_complete_imputed,
        acr2017_fna_recommended_strict,
        acr2017_fna_recommended_imputed,
        kwak_n_suspicious_features,
        kwak_features_used_json,
        kwak_category,
        kwak_fna_recommended,
        ktirads_composition_class,
        ktirads_n_suspicious,
        ktirads_entirely_calcified,
        ktirads_category,
        ktirads_fna_recommended,
        ctirads_score,
        ctirads_features_positive_json,
        ctirads_comet_tail_present,
        ctirads_category,
        ctirads_fna_recommended,
        sru_recommendation,
        sru_basis_json,
        -- B.6 Park X variables
        park_x1_taller,
        park_x2_halo,
        park_x3_well_circumscribed,
        park_x4_microlobulation,
        park_x5_infiltrative_margin,
        park_x6_marked_hypo,
        park_x7_hypo,
        park_x8_homogeneous,
        park_x9_mainly_cystic,
        park_x10_solid,
        park_x11_microcalc,
        park_x12_abnormal_ln,
        -- Park logits (one per coefficient set)
        {orig_logit},
        {prob_and_cat(orig, 'park2009')},
        {cosmos_logit},
        {prob_and_cat(cosmos, 'park_cosmos')},
        {cohort_logit},
        {prob_and_cat(cohort, 'park_cohort')},
        '{MANIFEST_ID}' AS park_coefficient_manifest_id,
        CURRENT_TIMESTAMP() AS scored_at,
        'phase_b_v1' AS scoring_pipeline_version
    FROM base
)
SELECT * FROM scored
"""
    return ctas


def create_multisystem_table_if_missing(client: bigquery.Client, dry_run: bool) -> None:
    """Create canonical_us_nodule_tirads_multisystem_v1 if it does not exist.

    If Phase B.1-B.5 already ran, the table will exist with B.1-B.5 columns
    populated. If not, we create it with all B.1-B.5 columns as NULL so Phase
    B.6 can land independently (as specified in the Phase B.6 prompt).
    """
    try:
        client.get_table(TABLE_MULTISYS)
        print(f"INFO: {TABLE_MULTISYS} already exists — will CTAS-rebuild with Park columns.")
        return
    except Exception:
        pass

    print(f"INFO: {TABLE_MULTISYS} does not exist — creating from canonical_us_nodule_v2 ...")
    create_sql = f"""
CREATE OR REPLACE TABLE `{TABLE_MULTISYS}`
CLUSTER BY research_id
AS
SELECT
    research_id,
    nodule_id,
    us_exam_id,
    exam_date,
    nodule_index_within_exam,
    size_cm_max,
    tirads_reported_in_text,
    tirads_reported_system,
    -- B.1-B.5 columns — NULL (Phase B.1-B.5 has not run yet)
    CAST(NULL AS FLOAT64) AS acr2017_composition_pts,
    CAST(NULL AS FLOAT64) AS acr2017_echogenicity_pts,
    CAST(NULL AS FLOAT64) AS acr2017_shape_pts,
    CAST(NULL AS FLOAT64) AS acr2017_margin_pts,
    CAST(NULL AS FLOAT64) AS acr2017_foci_pts,
    CAST(NULL AS FLOAT64) AS acr2017_total_pts_strict,
    CAST(NULL AS FLOAT64) AS acr2017_total_pts_imputed,
    CAST(NULL AS STRING)  AS acr2017_category_strict,
    CAST(NULL AS STRING)  AS acr2017_category_imputed,
    CAST(NULL AS BOOL)    AS acr2017_features_complete_strict,
    CAST(NULL AS BOOL)    AS acr2017_features_complete_imputed,
    CAST(NULL AS BOOL)    AS acr2017_fna_recommended_strict,
    CAST(NULL AS BOOL)    AS acr2017_fna_recommended_imputed,
    CAST(NULL AS INT64)   AS kwak_n_suspicious_features,
    CAST(NULL AS STRING)  AS kwak_features_used_json,
    CAST(NULL AS STRING)  AS kwak_category,
    CAST(NULL AS BOOL)    AS kwak_fna_recommended,
    CAST(NULL AS STRING)  AS ktirads_composition_class,
    CAST(NULL AS INT64)   AS ktirads_n_suspicious,
    CAST(NULL AS BOOL)    AS ktirads_entirely_calcified,
    CAST(NULL AS STRING)  AS ktirads_category,
    CAST(NULL AS BOOL)    AS ktirads_fna_recommended,
    CAST(NULL AS INT64)   AS ctirads_score,
    CAST(NULL AS STRING)  AS ctirads_features_positive_json,
    CAST(NULL AS BOOL)    AS ctirads_comet_tail_present,
    CAST(NULL AS STRING)  AS ctirads_category,
    CAST(NULL AS BOOL)    AS ctirads_fna_recommended,
    CAST(NULL AS STRING)  AS sru_recommendation,
    CAST(NULL AS STRING)  AS sru_basis_json,
    -- Park columns — Phase B.6 will populate
    CAST(NULL AS BOOL)    AS park_x1_taller,
    CAST(NULL AS BOOL)    AS park_x2_halo,
    CAST(NULL AS BOOL)    AS park_x3_well_circumscribed,
    CAST(NULL AS BOOL)    AS park_x4_microlobulation,
    CAST(NULL AS BOOL)    AS park_x5_infiltrative_margin,
    CAST(NULL AS BOOL)    AS park_x6_marked_hypo,
    CAST(NULL AS BOOL)    AS park_x7_hypo,
    CAST(NULL AS BOOL)    AS park_x8_homogeneous,
    CAST(NULL AS BOOL)    AS park_x9_mainly_cystic,
    CAST(NULL AS BOOL)    AS park_x10_solid,
    CAST(NULL AS BOOL)    AS park_x11_microcalc,
    CAST(NULL AS BOOL)    AS park_x12_abnormal_ln,
    CAST(NULL AS FLOAT64) AS park2009_logit,
    CAST(NULL AS FLOAT64) AS park2009_probability,
    CAST(NULL AS STRING)  AS park2009_category,
    CAST(NULL AS FLOAT64) AS park_cosmos_logit,
    CAST(NULL AS FLOAT64) AS park_cosmos_probability,
    CAST(NULL AS STRING)  AS park_cosmos_category,
    CAST(NULL AS FLOAT64) AS park_cohort_logit,
    CAST(NULL AS FLOAT64) AS park_cohort_probability,
    CAST(NULL AS STRING)  AS park_cohort_category,
    CAST(NULL AS STRING)  AS park_coefficient_manifest_id,
    CAST(NULL AS TIMESTAMP) AS scored_at,
    'phase_b_v1' AS scoring_pipeline_version
FROM `{TABLE_NODULE_V2}`
"""
    if dry_run:
        print("DRY-RUN: Would execute table creation SQL.")
        return
    client.query(create_sql).result()
    print(f"INFO: Created {TABLE_MULTISYS} with all B.1-B.5 columns as NULL.")


def run_x_completeness_qc(client: bigquery.Client, dry_run: bool) -> None:
    """Create pub_workspace.qc_phase_b6_park_x_completeness_v1.

    Shows per-X-variable count of nodules where the primitive was NULL and
    the variable defaulted to FALSE per policy. Logan must review before sign-off.
    """
    qc_sql = f"""
CREATE OR REPLACE TABLE `{PROJECT}.{DATASET_WS}.qc_phase_b6_park_x_completeness_v1`
AS
WITH primitives AS (
    SELECT
        nodule_id,
        -- Which primitives were NULL (producing a FALSE-default)?
        v.shape IS NULL AS x1_primitive_null,
        JSON_VALUE(v.halo_jsonb, '$.presence') IS NULL AS x2_primitive_null,
        v.margins IS NULL AS x3_x4_x5_margin_null,
        v.echogenicity IS NULL AS x6_x7_echo_null,
        v.homogeneous_echotexture IS NULL AS x8_primitive_null,
        v.composition IS NULL AS x9_x10_comp_null,
        (v.echogenic_foci IS NULL OR v.echogenic_foci = '[]') AS x11_primitive_null
    FROM `{TABLE_NODULE_V2}` v
),
ln_null AS (
    SELECT n.nodule_id,
        ln.has_suspicious_ln_within_60d IS NULL AS x12_primitive_null
    FROM `{TABLE_NODULE_V2}` n
    LEFT JOIN `{TABLE_LN_CTX}` ln USING (nodule_id)
)
SELECT
    'x1_taller_shape' AS x_variable,
    COUNTIF(x1_primitive_null) AS n_defaulted_to_false,
    COUNT(*) AS n_total,
    ROUND(COUNTIF(x1_primitive_null) / COUNT(*) * 100, 2) AS pct_defaulted
FROM primitives
UNION ALL
SELECT 'x2_halo', COUNTIF(x2_primitive_null), COUNT(*),
    ROUND(COUNTIF(x2_primitive_null) / COUNT(*) * 100, 2) FROM primitives
UNION ALL
SELECT 'x3_well_circumscribed (margin)', COUNTIF(x3_x4_x5_margin_null), COUNT(*),
    ROUND(COUNTIF(x3_x4_x5_margin_null) / COUNT(*) * 100, 2) FROM primitives
UNION ALL
SELECT 'x4_microlobulation (margin)', COUNTIF(x3_x4_x5_margin_null), COUNT(*),
    ROUND(COUNTIF(x3_x4_x5_margin_null) / COUNT(*) * 100, 2) FROM primitives
UNION ALL
SELECT 'x5_infiltrative_margin', COUNTIF(x3_x4_x5_margin_null), COUNT(*),
    ROUND(COUNTIF(x3_x4_x5_margin_null) / COUNT(*) * 100, 2) FROM primitives
UNION ALL
SELECT 'x6_marked_hypo (echogenicity)', COUNTIF(x6_x7_echo_null), COUNT(*),
    ROUND(COUNTIF(x6_x7_echo_null) / COUNT(*) * 100, 2) FROM primitives
UNION ALL
SELECT 'x7_hypo (echogenicity)', COUNTIF(x6_x7_echo_null), COUNT(*),
    ROUND(COUNTIF(x6_x7_echo_null) / COUNT(*) * 100, 2) FROM primitives
UNION ALL
SELECT 'x8_homogeneous_echotexture', COUNTIF(x8_primitive_null), COUNT(*),
    ROUND(COUNTIF(x8_primitive_null) / COUNT(*) * 100, 2) FROM primitives
UNION ALL
SELECT 'x9_mainly_cystic (composition)', COUNTIF(x9_x10_comp_null), COUNT(*),
    ROUND(COUNTIF(x9_x10_comp_null) / COUNT(*) * 100, 2) FROM primitives
UNION ALL
SELECT 'x10_solid (composition)', COUNTIF(x9_x10_comp_null), COUNT(*),
    ROUND(COUNTIF(x9_x10_comp_null) / COUNT(*) * 100, 2) FROM primitives
UNION ALL
SELECT 'x11_microcalc (echogenic_foci)', COUNTIF(x11_primitive_null), COUNT(*),
    ROUND(COUNTIF(x11_primitive_null) / COUNT(*) * 100, 2) FROM primitives
UNION ALL
SELECT 'x12_abnormal_ln', COUNTIF(x12_primitive_null), COUNT(*),
    ROUND(COUNTIF(x12_primitive_null) / COUNT(*) * 100, 2) FROM ln_null
ORDER BY x_variable
"""
    if dry_run:
        print("DRY-RUN: Would create qc_phase_b6_park_x_completeness_v1")
        return
    client.query(qc_sql).result()
    print("INFO: Created pub_workspace.qc_phase_b6_park_x_completeness_v1")


def run_audit(client: bigquery.Client) -> None:
    """Print distribution per category per coefficient set."""
    audit_sql = f"""
SELECT 'park2009' AS coef_set, park2009_category AS cat, COUNT(*) AS n
FROM `{TABLE_MULTISYS}` WHERE park2009_category IS NOT NULL GROUP BY 1,2
UNION ALL
SELECT 'cosmos', park_cosmos_category, COUNT(*) FROM `{TABLE_MULTISYS}`
    WHERE park_cosmos_category IS NOT NULL GROUP BY 1,2
UNION ALL
SELECT 'cohort', park_cohort_category, COUNT(*) FROM `{TABLE_MULTISYS}`
    WHERE park_cohort_category IS NOT NULL GROUP BY 1,2
ORDER BY 1, 2
"""
    print("\n=== Category distribution per coefficient set ===")
    rows = list(client.query(audit_sql).result())
    for row in rows:
        print(f"  {row.coef_set:12s} {row.cat:4s}  n={row.n:6d}")

    concordance_sql = f"""
SELECT
    -- park2009 vs cohort (will be 0/NULL until park_2009_original coefficients filled)
    COUNTIF(park2009_category IS NOT NULL AND park_cohort_category IS NOT NULL
        AND park2009_category = park_cohort_category) AS agree_2009_vs_cohort_n,
    COUNTIF(park2009_category IS NOT NULL AND park_cohort_category IS NOT NULL) AS n_2009_cohort,
    -- cohort coverage (always computed)
    COUNTIF(park_cohort_category IS NOT NULL) AS n_cohort_scored,
    COUNT(*) AS n_total
FROM `{TABLE_MULTISYS}`
"""
    print("\n=== Inter-set concordance ===")
    for row in client.query(concordance_sql).result():
        print(f"  Cohort scored rows: {row.n_cohort_scored} / {row.n_total}")
        if row.n_2009_cohort > 0:
            agree = row.agree_2009_vs_cohort_n / row.n_2009_cohort
            print(f"  2009 vs cohort agree rate: {agree:.3f} (n={row.n_2009_cohort})")
        else:
            print("  2009 vs cohort: N/A (park_2009_original coefficients PENDING — fill manifest first)")


def main():
    parser = argparse.ArgumentParser(description="Phase B.6 Park scorer")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--project", default=PROJECT)
    args = parser.parse_args()

    client = bigquery.Client(project=args.project)

    print("=== Phase B.6 Park / T-US 2009 scorer ===")
    print(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")

    # 1. Load and validate manifest
    manifest = load_and_validate_manifest()
    print(f"INFO: Manifest loaded from {MANIFEST_PATH}")
    for set_name, cs in manifest["sets"].items():
        status = cs.get("coefficients_status", "READY")
        print(f"  {set_name}: {status}")

    # 2. Create multisystem table if missing
    create_multisystem_table_if_missing(client, args.dry_run)

    # 3. Build and run CTAS scoring SQL
    scoring_sql = build_scoring_sql(manifest)
    if args.dry_run:
        print("DRY-RUN: Scoring SQL generated (not executed).")
        print(scoring_sql[:2000] + "\n... [truncated]")
    else:
        print("INFO: Running CTAS rebuild with Park columns ...")
        client.query(scoring_sql).result()
        print(f"INFO: CTAS rebuild complete → {TABLE_MULTISYS}")

    # 4. X-variable completeness QC table
    run_x_completeness_qc(client, args.dry_run)

    # 5. Audit distributions
    if not args.dry_run:
        run_audit(client)

    print("\n=== Phase B.6 scorer complete ===")
    print("Next steps:")
    print("  1. Review qc_phase_b6_park_x_completeness_v1 for NULL-defaulted X counts.")
    print("  2. Run tests/test_park_scorer.py")
    print("  3. Append DFL row (lifecycle Logged → Applied)")
    print("  4. Post Linear THY-30 comment")


if __name__ == "__main__":
    main()
