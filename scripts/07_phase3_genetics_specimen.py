#!/usr/bin/env python3
"""
07_phase3_genetics_specimen.py — Phase 3

Adds three new enrichment layers to the thyroid research lakehouse:

  1. genetic_testing    — ThyroSeq v3 / Afirma GSC / Afirma XA per-nodule
                          molecular test records with per-gene flags
  2. specimen_detail    — Gross pathology summary: dimensions, weight,
                          capsule, margin status, focality
  3. preop_imaging_summary — Last pre-op imaging per patient (US/CT/MRI)
                             with structured nodule, LN and risk fields

Produces:
  processed/genetic_testing.parquet
  processed/specimen_detail.parquet
  processed/preop_imaging_summary.parquet

Then registers all three as DuckDB tables and creates analytic views:
  genetic_testing_summary_view
  specimen_detail_view
  preop_imaging_detail_view
  benign_detail_view   (expanded benign diagnoses beyond binary flags)

Run after 06_advanced_extraction.py.
"""
from __future__ import annotations

import re
from pathlib import Path

import duckdb
import polars as pl

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "raw"
PROCESSED = ROOT / "processed"
DB_PATH = ROOT / "thyroid_master.duckdb"

PROCESSED.mkdir(exist_ok=True)

# ─────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────

RESEARCH_ID_ALIASES = {
    "research_id", "research_id_number", "researchid", "record_id",
}
PHI_COLUMNS = {
    "patient_first_nm", "patient_last_nm", "patient_id",
    "empi_nbr", "euh_mrn", "tec_mrn", "dob", "date_of_birth",
    "surgeon", "death",
}


def standardize_columns(df: pl.DataFrame) -> pl.DataFrame:
    rename: dict[str, str] = {}
    seen: dict[str, int] = {}
    for col in df.columns:
        clean = re.sub(r"[^\w]+", "_", col.strip()).lower().strip("_")
        clean = re.sub(r"_+", "_", clean)
        if clean in RESEARCH_ID_ALIASES:
            clean = "research_id"
        if clean in seen:
            seen[clean] += 1
            clean = f"{clean}_{seen[clean]}"
        else:
            seen[clean] = 0
        rename[col] = clean
    return df.rename(rename)


def cast_research_id(df: pl.DataFrame) -> pl.DataFrame:
    if "research_id" not in df.columns:
        return df
    return df.with_columns(
        pl.col("research_id")
        .cast(pl.Utf8)
        .str.strip_chars()
        .str.replace(r"\.0$", "")
        .alias("research_id")
    )


def strip_phi(df: pl.DataFrame) -> pl.DataFrame:
    to_drop = [c for c in df.columns if c in PHI_COLUMNS]
    if to_drop:
        print(f"    PHI stripped: {to_drop}")
        df = df.drop(to_drop)
    return df


def read_excel(path: Path, sheet: int | str = 0) -> pl.DataFrame:
    df = pl.read_excel(path, sheet_id=sheet if isinstance(sheet, int) else None,
                       sheet_name=sheet if isinstance(sheet, str) else None)
    df = standardize_columns(df)
    df = cast_research_id(df)
    df = strip_phi(df)
    return df


def _bool_col(series: pl.Series) -> pl.Series:
    """Coerce mixed-type column to BOOLEAN."""
    return (
        series.cast(pl.Utf8)
        .str.to_lowercase()
        .is_in(["true", "yes", "1", "positive", "detected", "present", "x"])
    )


# ─────────────────────────────────────────────────────────────────
# 1. GENETIC TESTING — ThyroSeq / Afirma
# ─────────────────────────────────────────────────────────────────

# Individual genes / fusions to flag from the raw result text
GENE_PATTERNS: dict[str, str] = {
    "braf_v600e":   r"\bBRAF\s*V?600E?\b",
    "braf_other":   r"\bBRAF\b(?!.{0,10}V600E)",
    "nras":         r"\bNRAS\b",
    "hras":         r"\bHRAS\b",
    "kras":         r"\bKRAS\b",
    "ras_any":      r"\b[NHK]RAS\b|\bRAS\b",
    "ret_ptc1":     r"\bRET/PTC1\b|\bRET-PTC1\b",
    "ret_ptc3":     r"\bRET/PTC3\b|\bRET-PTC3\b",
    "ret_other":    r"\bRET\b(?!/PTC[13])",
    "pax8_pparg":   r"\bPAX8[/\-]?PPARG?\b",
    "tert_promoter":r"\bTERT\b",
    "ntrk1":        r"\bNTRK1\b",
    "ntrk3":        r"\bNTRK3\b",
    "ntrk_any":     r"\bNTRK\b",
    "alk_fusion":   r"\bALK\b",
    "met":          r"\bMET\b",
    "ewsr1":        r"\bEWSR1\b",
    "dicer1":       r"\bDICER1\b",
    "pten":         r"\bPTEN\b",
    "tp53":         r"\bTP53\b",
}

# ThyroSeq / Afirma result → standardised category
RESULT_CATEGORIES = {
    "benign": ["benign", "negative", "no variant", "nvd", "low risk"],
    "suspicious": ["suspicious", "moderate risk", "intermediate"],
    "positive": ["positive", "high risk", "malignant", "mutation detected", "detected"],
    "indeterminate": ["indeterminate", "inconclusive", "non-diagnostic"],
}


def _classify_result(text: str) -> str | None:
    if not text:
        return None
    t = text.lower()
    for cat, kws in RESULT_CATEGORIES.items():
        if any(k in t for k in kws):
            return cat
    return "other"


def ingest_genetic_testing() -> pl.DataFrame | None:
    src = RAW / "THYROSEQ_AFIRMA_12_5.xlsx"
    if not src.exists():
        print(f"  ⚠️  {src.name} not found — skipping genetic_testing")
        return None

    print(f"  Reading {src.name} …")
    df = read_excel(src)
    print(f"    Raw shape: {df.shape}  columns: {df.columns[:10]}")

    # ── Identify test-type column ────────────────────────────────
    test_col = next(
        (c for c in df.columns if "test" in c and "type" in c), None
    ) or next(
        (c for c in df.columns if "platform" in c or "assay" in c), None
    ) or next(
        (c for c in df.columns if "thyro" in c or "afirma" in c), None
    )

    # ── Result column ─────────────────────────────────────────────
    result_col = next(
        (c for c in df.columns if "result" in c and "category" in c), None
    ) or next(
        (c for c in df.columns if "result" in c or "call" in c), None
    )

    # ── Mutation text column ──────────────────────────────────────
    mutation_col = next(
        (c for c in df.columns if "mutation" in c or "variant" in c or "gene" in c), None
    ) or next(
        (c for c in df.columns if "detected" in c or "alteration" in c), None
    )

    # ── Build structured output ───────────────────────────────────
    exprs: list[pl.Expr] = [pl.col("research_id")]

    if test_col:
        exprs.append(pl.col(test_col).cast(pl.Utf8).alias("test_platform"))
    else:
        exprs.append(pl.lit(None).cast(pl.Utf8).alias("test_platform"))

    if result_col:
        exprs.append(pl.col(result_col).cast(pl.Utf8).alias("raw_result"))
    else:
        exprs.append(pl.lit(None).cast(pl.Utf8).alias("raw_result"))

    if mutation_col:
        exprs.append(pl.col(mutation_col).cast(pl.Utf8).alias("mutations_detected_text"))
    else:
        exprs.append(pl.lit(None).cast(pl.Utf8).alias("mutations_detected_text"))

    # Preserve all original columns too
    out = df.select(exprs + [
        pl.col(c).alias(c) for c in df.columns
        if c not in {"research_id", test_col, result_col, mutation_col}
        and c is not None
    ])

    # ── Derived result category ───────────────────────────────────
    out = out.with_columns(
        pl.col("raw_result")
        .map_elements(_classify_result, return_dtype=pl.Utf8)
        .alias("result_category")
    )

    # ── Per-gene boolean flags from mutation text ─────────────────
    text_src = "mutations_detected_text"
    gene_flag_exprs: list[pl.Expr] = []
    for gene, pattern in GENE_PATTERNS.items():
        gene_flag_exprs.append(
            pl.col(text_src)
            .cast(pl.Utf8)
            .str.contains(pattern)
            .fill_null(False)
            .alias(gene)
        )
    out = out.with_columns(gene_flag_exprs)

    # ── Normalise test_platform ───────────────────────────────────
    platform_map = {
        "thyro": "ThyroSeq v3",
        "thyroseq": "ThyroSeq v3",
        "afirma gsc": "Afirma GSC",
        "afirma xa": "Afirma XA",
        "afirma xpression": "Afirma XA",
        "foundation": "Foundation One",
        "caris": "Caris",
        "guardant": "Guardant",
    }

    def _normalize_platform(s: str | None) -> str | None:
        if not s:
            return None
        sl = s.lower()
        for k, v in platform_map.items():
            if k in sl:
                return v
        return s

    out = out.with_columns(
        pl.col("test_platform")
        .map_elements(_normalize_platform, return_dtype=pl.Utf8)
        .alias("test_platform")
    )

    print(f"    ✅  genetic_testing  {out.shape[0]:,} rows")
    return out


# ─────────────────────────────────────────────────────────────────
# 2. SPECIMEN DETAIL — combining size + weight + macro path
# ─────────────────────────────────────────────────────────────────

def ingest_specimen_detail() -> pl.DataFrame | None:
    """
    Join thyroid_sizes and thyroid_weights to create a unified
    specimen detail record. Enriches with macroscopic fields from
    the benign and tumor pathology where available.
    """
    sizes_src = PROCESSED / "thyroid_sizes.parquet"
    weights_src = PROCESSED / "thyroid_weights.parquet"

    if not sizes_src.exists() and not weights_src.exists():
        print("  ⚠️  Neither thyroid_sizes nor thyroid_weights parquet found — skipping")
        return None

    frames: list[pl.DataFrame] = []

    if sizes_src.exists():
        sz = pl.read_parquet(sizes_src)
        frames.append(sz)
        print(f"    thyroid_sizes loaded: {sz.shape}")

    if weights_src.exists():
        wt = pl.read_parquet(weights_src)
        frames.append(wt)
        print(f"    thyroid_weights loaded: {wt.shape}")

    if len(frames) == 0:
        return None
    if len(frames) == 1:
        combined = frames[0]
    else:
        # Join on research_id, coalesce duplicate columns
        combined = frames[0].join(frames[1], on="research_id", how="outer", suffix="_wt")
        # Drop duplicate _wt columns where already present in sizes
        drop_cols = [c for c in combined.columns if c.endswith("_wt") and c[:-3] in combined.columns]
        combined = combined.drop(drop_cols)

    # Standardise weight column name if present
    weight_col_candidates = [c for c in combined.columns if "weight" in c and "combined" in c]
    if weight_col_candidates:
        combined = combined.rename({weight_col_candidates[0]: "specimen_weight_g"})

    # Ensure key derived columns exist
    if "specimen_weight_g" not in combined.columns:
        wt_candidates = [c for c in combined.columns if "weight" in c]
        if wt_candidates:
            combined = combined.with_columns(
                pl.col(wt_candidates[0]).cast(pl.Utf8).alias("specimen_weight_g")
            )

    print(f"    ✅  specimen_detail  {combined.shape[0]:,} rows")
    return combined


# ─────────────────────────────────────────────────────────────────
# 3. PRE-OP IMAGING SUMMARY — last pre-op study per patient
# ─────────────────────────────────────────────────────────────────

def ingest_preop_imaging_summary(con: duckdb.DuckDBPyConnection) -> pl.DataFrame | None:
    """
    Build a patient-level pre-op imaging summary by aggregating
    the last ultrasound, CT and MRI before surgery_date.
    """
    tables_exist = {
        r[0] for r in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
        ).fetchall()
    }

    required = {"master_cohort", "ultrasound_reports"}
    if not required.issubset(tables_exist):
        print("  ⚠️  Required tables missing — skipping preop_imaging_summary")
        return None

    print("  Building preop_imaging_summary from MotherDuck …")

    # ── Ultrasound summary ────────────────────────────────────────
    us_cols_available = {r[0] for r in con.execute("DESCRIBE ultrasound_reports").fetchall()}

    # Pick nodule size columns dynamically
    nodule_dim_cols = [
        c for c in us_cols_available
        if re.match(r"nodule_[1-5]_(length|width|height|size|largest_dim|cm)", c)
    ]
    nodule_tirads_cols = [c for c in us_cols_available if "ti_rads" in c or "tirads" in c]
    nodule_comp_cols = [c for c in us_cols_available if "composition" in c]
    nodule_echo_cols = [c for c in us_cols_available if "echogenicity" in c]
    nodule_calc_cols = [c for c in us_cols_available if "calcif" in c]
    nodule_margin_cols = [c for c in us_cols_available if "margin" in c]

    # Build max TI-RADS expression
    if nodule_tirads_cols:
        tirads_parts = ", ".join(
            f"COALESCE(TRY_CAST({c} AS DOUBLE), 0)" for c in nodule_tirads_cols[:5]
        )
        tirads_expr = f"MAX(GREATEST({tirads_parts}))"
    else:
        tirads_expr = "NULL"

    # Number of nodules
    nodule_count_col = next(
        (c for c in us_cols_available if "number_of_nodule" in c or "nodule_count" in c), None
    )
    nodule_count_expr = f"MAX(TRY_CAST({nodule_count_col} AS INTEGER))" if nodule_count_col else "NULL"

    us_sql = f"""
    WITH us_ranked AS (
        SELECT
            u.*,
            mc.surgery_date,
            ROW_NUMBER() OVER (
                PARTITION BY u.research_id
                ORDER BY TRY_CAST(u.ultrasound_date AS TIMESTAMP) DESC NULLS LAST
            ) AS rn
        FROM ultrasound_reports u
        JOIN master_cohort mc ON u.research_id = mc.research_id
        WHERE TRY_CAST(u.ultrasound_date AS TIMESTAMP)
              <= TRY_CAST(mc.surgery_date AS TIMESTAMP)
           OR mc.surgery_date IS NULL
    )
    SELECT
        research_id,
        MAX(ultrasound_date) AS last_us_date,
        {nodule_count_expr} AS us_nodule_count,
        {tirads_expr} AS us_max_tirads,
        MAX(TRY_CAST(right_lobe_volume_ml AS DOUBLE)) AS right_lobe_vol_ml,
        MAX(TRY_CAST(left_lobe_volume_ml AS DOUBLE)) AS left_lobe_vol_ml,
        MAX(TRY_CAST(total_thyroid_volume_ml AS DOUBLE)) AS total_thyroid_vol_ml,
        MAX(CASE WHEN LOWER(CAST(lymph_node_assessment AS VARCHAR)) LIKE '%suspicious%'
                  OR LOWER(CAST(lymph_node_assessment AS VARCHAR)) LIKE '%patholog%'
                  THEN 1 ELSE 0 END) AS us_suspicious_ln_flag
    FROM us_ranked
    WHERE rn = 1
    GROUP BY research_id
    """

    try:
        df_us = con.execute(us_sql).df()
        df_us = pl.from_pandas(df_us)
        print(f"    US summary: {df_us.shape[0]:,} patients")
    except Exception as e:
        print(f"    ⚠️  US summary failed: {e}")
        df_us = None

    # ── CT summary ────────────────────────────────────────────────
    if "ct_imaging" in tables_exist:
        ct_sql = """
        SELECT
            research_id,
            MAX(date_of_exam) AS last_ct_date,
            MAX(CASE WHEN LOWER(CAST(thyroid_nodule AS VARCHAR)) IN ('true','yes','1') THEN 1 ELSE 0 END) AS ct_nodule_flag,
            MAX(CASE WHEN LOWER(CAST(thyroid_enlarged AS VARCHAR)) IN ('true','yes','1') THEN 1 ELSE 0 END) AS ct_enlarged_flag,
            MAX(CASE WHEN LOWER(CAST(goiter_present AS VARCHAR)) IN ('true','yes','1') THEN 1 ELSE 0 END) AS ct_goiter_flag,
            MAX(CASE WHEN LOWER(CAST(pathologic_lymph_nodes AS VARCHAR)) IN ('true','yes','1') THEN 1 ELSE 0 END) AS ct_pathologic_ln_flag,
            MAX(CASE WHEN LOWER(CAST(lymph_nodes_suspicious AS VARCHAR)) IN ('true','yes','1') THEN 1 ELSE 0 END) AS ct_suspicious_ln_flag,
            MAX(TRY_CAST(largest_lymph_node_short_axis_mm AS DOUBLE)) AS ct_largest_ln_mm,
            MAX(COALESCE(CAST(lymph_node_locations AS VARCHAR), '')) AS ct_ln_locations,
            COUNT(*) AS ct_exam_count
        FROM ct_imaging
        GROUP BY research_id
        """
        try:
            df_ct = con.execute(ct_sql).df()
            df_ct = pl.from_pandas(df_ct)
            print(f"    CT summary: {df_ct.shape[0]:,} patients")
        except Exception as e:
            print(f"    ⚠️  CT summary failed: {e}")
            df_ct = None
    else:
        df_ct = None

    # ── MRI summary ───────────────────────────────────────────────
    if "mri_imaging" in tables_exist:
        mri_sql = """
        SELECT
            research_id,
            MAX(date_of_exam) AS last_mri_date,
            MAX(CASE WHEN LOWER(CAST(thyroid_nodule AS VARCHAR)) IN ('true','yes','1') THEN 1 ELSE 0 END) AS mri_nodule_flag,
            MAX(CASE WHEN LOWER(CAST(thyroid_enlarged AS VARCHAR)) IN ('true','yes','1') THEN 1 ELSE 0 END) AS mri_enlarged_flag,
            MAX(CASE WHEN LOWER(CAST(substernal_extension AS VARCHAR)) IN ('true','yes','1') THEN 1 ELSE 0 END) AS mri_substernal_flag,
            MAX(CASE WHEN LOWER(CAST(pathologic_lymph_nodes AS VARCHAR)) IN ('true','yes','1') THEN 1 ELSE 0 END) AS mri_pathologic_ln_flag,
            MAX(COALESCE(CAST(lymph_node_locations AS VARCHAR), '')) AS mri_ln_locations,
            COUNT(*) AS mri_exam_count
        FROM mri_imaging
        GROUP BY research_id
        """
        try:
            df_mri = con.execute(mri_sql).df()
            df_mri = pl.from_pandas(df_mri)
            print(f"    MRI summary: {df_mri.shape[0]:,} patients")
        except Exception as e:
            print(f"    ⚠️  MRI summary failed: {e}")
            df_mri = None
    else:
        df_mri = None

    # ── Combine ───────────────────────────────────────────────────
    combined = df_us
    if combined is None:
        print("  ⚠️  No US data — skipping preop_imaging_summary")
        return None

    if df_ct is not None:
        combined = combined.join(df_ct, on="research_id", how="outer")
    if df_mri is not None:
        combined = combined.join(df_mri, on="research_id", how="outer")

    # Derived: any suspicious pre-op imaging flag
    flag_cols = [c for c in combined.columns if "suspicious" in c or "pathologic" in c]
    if flag_cols:
        cast_flags = [pl.col(c).cast(pl.Int32).fill_null(0) for c in flag_cols]
        combined = combined.with_columns(
            pl.max_horizontal(*cast_flags).cast(pl.Boolean).alias("any_preop_suspicious_ln")
        )

    print(f"    ✅  preop_imaging_summary  {combined.shape[0]:,} patients")
    return combined


# ─────────────────────────────────────────────────────────────────
# 4. VIEWS — benign_detail_view, specimen_detail_view,
#            genetic_testing_summary_view, preop_imaging_detail_view
# ─────────────────────────────────────────────────────────────────

BENIGN_DETAIL_VIEW_SQL = """
CREATE OR REPLACE VIEW benign_detail_view AS
SELECT
    bp.research_id,
    bp.surgery_date,
    bp.age_at_surgery,
    bp.sex,
    bp.surgery_type_normalized,

    -- ── Core binary flags (existing) ──────────────────────────────
    bp.is_mng,
    bp.is_graves,
    bp.is_follicular_adenoma,
    bp.is_hurthle_adenoma,
    bp.is_hashimoto,
    bp.is_hyalinizing_trabecular,
    bp.is_tgdc,

    -- ── Extended benign diagnoses ─────────────────────────────────
    -- Thyroiditis subtypes
    COALESCE(LOWER(CAST(bp.hashimoto_thyroiditis AS VARCHAR))
        IN ('true','yes','1','present'), FALSE)              AS hashimoto_thyroiditis_flag,
    COALESCE(LOWER(CAST(bp.graves_disease AS VARCHAR))
        IN ('true','yes','1','present'), FALSE)              AS graves_disease_flag,
    COALESCE(LOWER(CAST(bp.focal_lymphocytic_thyroiditis AS VARCHAR))
        IN ('true','yes','1','present'), FALSE)              AS focal_lymphocytic_thyroiditis,
    COALESCE(LOWER(CAST(bp.diffuse_hyperplasia AS VARCHAR))
        IN ('true','yes','1','present'), FALSE)              AS diffuse_hyperplasia,

    -- Nodular/goiter
    COALESCE(LOWER(CAST(bp.multinodular_goiter AS VARCHAR))
        IN ('true','yes','1','present'), FALSE)              AS multinodular_goiter_flag,
    COALESCE(LOWER(CAST(bp.colloid_nodule AS VARCHAR))
        IN ('true','yes','1','present'), FALSE)              AS colloid_nodule,

    -- Adenoma subtypes
    COALESCE(LOWER(CAST(bp.follicular_adenoma AS VARCHAR))
        IN ('true','yes','1','present'), FALSE)              AS follicular_adenoma_flag,
    COALESCE(LOWER(CAST(bp.hurthle_adenoma AS VARCHAR))
        IN ('true','yes','1','present'), FALSE)              AS hurthle_cell_adenoma_flag,

    -- Rare / special subtypes (text-detected from diagnosis columns)
    CASE WHEN LOWER(COALESCE(CAST(bp.diagnosis_text AS VARCHAR), '') ||
                    COALESCE(CAST(bp.final_diagnosis AS VARCHAR), ''))
             LIKE '%fibrosing%hashimoto%'
          OR LOWER(COALESCE(CAST(bp.diagnosis_text AS VARCHAR), ''))
             LIKE '%reidel%' THEN TRUE ELSE FALSE END        AS fibrosing_hashimoto_or_riedel,

    CASE WHEN LOWER(COALESCE(CAST(bp.diagnosis_text AS VARCHAR), '') ||
                    COALESCE(CAST(bp.final_diagnosis AS VARCHAR), ''))
             LIKE '%amyloid%' THEN TRUE ELSE FALSE END       AS amyloid_goiter,

    CASE WHEN LOWER(COALESCE(CAST(bp.diagnosis_text AS VARCHAR), '') ||
                    COALESCE(CAST(bp.final_diagnosis AS VARCHAR), ''))
             LIKE '%black thyroid%' THEN TRUE ELSE FALSE END AS black_thyroid,

    CASE WHEN LOWER(COALESCE(CAST(bp.diagnosis_text AS VARCHAR), '') ||
                    COALESCE(CAST(bp.final_diagnosis AS VARCHAR), ''))
             LIKE '%c-cell hyperplasia%'
          OR LOWER(COALESCE(CAST(bp.diagnosis_text AS VARCHAR), ''))
             LIKE '%c cell hyperplasia%' THEN TRUE ELSE FALSE END AS c_cell_hyperplasia,

    CASE WHEN LOWER(COALESCE(CAST(bp.diagnosis_text AS VARCHAR), '') ||
                    COALESCE(CAST(bp.final_diagnosis AS VARCHAR), ''))
             LIKE '%adenomatoid%'
          OR LOWER(COALESCE(CAST(bp.diagnosis_text AS VARCHAR), ''))
             LIKE '%hyperplastic nodule%'
          OR LOWER(COALESCE(CAST(bp.diagnosis_text AS VARCHAR), ''))
             LIKE '%adenomatous%' THEN TRUE ELSE FALSE END   AS adenomatoid_hyperplastic_nodule,

    CASE WHEN LOWER(COALESCE(CAST(bp.diagnosis_text AS VARCHAR), '') ||
                    COALESCE(CAST(bp.final_diagnosis AS VARCHAR), ''))
             LIKE '%thyroglossal%' THEN TRUE ELSE FALSE END  AS thyroglossal_duct_cyst,

    CASE WHEN LOWER(COALESCE(CAST(bp.diagnosis_text AS VARCHAR), '') ||
                    COALESCE(CAST(bp.final_diagnosis AS VARCHAR), ''))
             LIKE '%lipoadenoma%'
          OR LOWER(COALESCE(CAST(bp.diagnosis_text AS VARCHAR), ''))
             LIKE '%lipomatous%' THEN TRUE ELSE FALSE END    AS lipoadenoma,

    CASE WHEN LOWER(COALESCE(CAST(bp.diagnosis_text AS VARCHAR), '') ||
                    COALESCE(CAST(bp.final_diagnosis AS VARCHAR), ''))
             LIKE '%radiation%change%'
          OR LOWER(COALESCE(CAST(bp.diagnosis_text AS VARCHAR), ''))
             LIKE '%post-radiation%' THEN TRUE ELSE FALSE END AS radiation_associated_changes,

    -- Raw text fields for downstream analysis
    CAST(bp.diagnosis_text AS VARCHAR)    AS diagnosis_text_raw,
    CAST(bp.final_diagnosis AS VARCHAR)   AS final_diagnosis_raw,

    -- Count of positive benign flags
    (CASE WHEN bp.is_mng THEN 1 ELSE 0 END +
     CASE WHEN bp.is_graves THEN 1 ELSE 0 END +
     CASE WHEN bp.is_follicular_adenoma THEN 1 ELSE 0 END +
     CASE WHEN bp.is_hurthle_adenoma THEN 1 ELSE 0 END +
     CASE WHEN bp.is_hashimoto THEN 1 ELSE 0 END +
     CASE WHEN bp.is_hyalinizing_trabecular THEN 1 ELSE 0 END +
     CASE WHEN bp.is_tgdc THEN 1 ELSE 0 END)  AS n_benign_diagnoses

FROM benign_pathology bp
"""


SPECIMEN_DETAIL_VIEW_SQL = """
CREATE OR REPLACE VIEW specimen_detail_view AS
SELECT
    mc.research_id,
    mc.surgery_date,
    mc.age_at_surgery,
    mc.sex,

    -- ── Specimen weights ──────────────────────────────────────────
    TRY_CAST(tw.specimen_weight_combined AS DOUBLE)  AS specimen_weight_g,
    TRY_CAST(tw.right_lobe_weight AS DOUBLE)         AS right_lobe_weight_g,
    TRY_CAST(tw.left_lobe_weight AS DOUBLE)          AS left_lobe_weight_g,
    TRY_CAST(tw.isthmus_weight AS DOUBLE)            AS isthmus_weight_g,

    -- ── Specimen dimensions (from thyroid_sizes) ──────────────────
    CAST(ts.right_lobe_formatted_dimensions AS VARCHAR)  AS right_lobe_dims,
    CAST(ts.left_lobe_formatted_dimensions AS VARCHAR)   AS left_lobe_dims,
    TRY_CAST(ts.right_lobe_volume_cc AS DOUBLE)          AS right_lobe_vol_cc,
    TRY_CAST(ts.left_lobe_volume_cc AS DOUBLE)           AS left_lobe_vol_cc,
    TRY_CAST(ts.total_volume_cc AS DOUBLE)               AS total_volume_cc,

    -- ── Tumor gross features ──────────────────────────────────────
    TRY_CAST(tp.histology_1_largest_tumor_cm AS DOUBLE)  AS largest_tumor_cm,
    TRY_CAST(tp.num_tumors_identified AS INTEGER)        AS n_tumors,
    CAST(tp.tumor_focality_overall AS VARCHAR)           AS tumor_focality,
    CAST(tp.tumor_1_extrathyroidal_ext AS VARCHAR)       AS extrathyroidal_ext,
    CAST(tp.tumor_1_gross_ete AS VARCHAR)                AS gross_ete,

    -- ── Surgical margins ──────────────────────────────────────────
    CASE WHEN LOWER(COALESCE(CAST(tp.surgical_margins AS VARCHAR), ''))
             IN ('positive','involved','+ margins') THEN 'positive'
         WHEN LOWER(COALESCE(CAST(tp.surgical_margins AS VARCHAR), ''))
             IN ('negative','clear','free','uninvolved') THEN 'negative'
         ELSE CAST(tp.surgical_margins AS VARCHAR)
    END AS surgical_margin_status,

    -- ── Capsule ───────────────────────────────────────────────────
    CAST(tp.capsular_invasion AS VARCHAR)           AS capsular_invasion,
    CAST(tp.tumor_1_capsular_invasion AS VARCHAR)   AS tumor_capsular_invasion,

    -- ── Vascular / lymphatic ──────────────────────────────────────
    CAST(tp.tumor_1_vascular_invasion AS VARCHAR)   AS vascular_invasion,
    CAST(tp.tumor_1_lymphatic_invasion AS VARCHAR)  AS lymphatic_invasion,

    -- ── Parathyroid incidental findings ──────────────────────────
    TRY_CAST(p.n_parathyroid_glands_identified AS INTEGER)  AS n_parathyroid_identified,
    CAST(p.removal_intent AS VARCHAR)                        AS parathyroid_removal_intent,

    -- ── Frozen section concordance ────────────────────────────────
    CAST(fs.frozen_section_obtained AS VARCHAR)         AS frozen_section_obtained,
    TRY_CAST(fs.number_of_frozen_sections AS INTEGER)   AS n_frozen_sections,
    CAST(fs.concordance_with_final AS VARCHAR)          AS fs_concordance_with_final,
    CAST(fs.fs_result_1 AS VARCHAR)                     AS fs_result_1,

    -- ── Surgery type ──────────────────────────────────────────────
    COALESCE(
        CAST(tp.surgery_type_normalized AS VARCHAR),
        CAST(bp.surgery_type_normalized AS VARCHAR)
    )                                                   AS surgery_type

FROM master_cohort mc
LEFT JOIN thyroid_weights tw  ON mc.research_id = tw.research_id
LEFT JOIN thyroid_sizes   ts  ON mc.research_id = ts.research_id
LEFT JOIN tumor_pathology tp  ON mc.research_id = tp.research_id
LEFT JOIN benign_pathology bp ON mc.research_id = bp.research_id
LEFT JOIN parathyroid p       ON mc.research_id = p.research_id
LEFT JOIN frozen_sections fs  ON mc.research_id = fs.research_id
"""


PREOP_IMAGING_DETAIL_VIEW_SQL = """
CREATE OR REPLACE VIEW preop_imaging_detail_view AS
WITH us_agg AS (
    SELECT
        u.research_id,
        COUNT(*) AS n_ultrasound_studies,
        MAX(TRY_CAST(u.ultrasound_date AS TIMESTAMP))           AS last_us_date,
        MAX(TRY_CAST(u.number_of_nodules AS INTEGER))           AS us_max_nodule_count,
        MAX(TRY_CAST(u.total_thyroid_volume_ml AS DOUBLE))      AS us_total_thyroid_vol_ml,
        MAX(TRY_CAST(u.right_lobe_volume_ml AS DOUBLE))         AS us_right_lobe_vol_ml,
        MAX(TRY_CAST(u.left_lobe_volume_ml AS DOUBLE))          AS us_left_lobe_vol_ml,
        MAX(
            GREATEST(
                COALESCE(TRY_CAST(u.nodule_1_ti_rads AS DOUBLE), 0),
                COALESCE(TRY_CAST(u.nodule_2_ti_rads AS DOUBLE), 0),
                COALESCE(TRY_CAST(u.nodule_3_ti_rads AS DOUBLE), 0),
                COALESCE(TRY_CAST(u.nodule_4_ti_rads AS DOUBLE), 0),
                COALESCE(TRY_CAST(u.nodule_5_ti_rads AS DOUBLE), 0)
            )
        )                                                        AS us_max_tirads,
        -- Nodule 1 detailed features
        MAX(CAST(u.nodule_1_composition AS VARCHAR))             AS n1_composition,
        MAX(CAST(u.nodule_1_echogenicity AS VARCHAR))            AS n1_echogenicity,
        MAX(CAST(u.nodule_1_calcifications AS VARCHAR))          AS n1_calcifications,
        MAX(CAST(u.nodule_1_margins AS VARCHAR))                 AS n1_margins,
        MAX(CAST(u.nodule_1_shape AS VARCHAR))                   AS n1_shape,
        MAX(CAST(u.nodule_1_location AS VARCHAR))                AS n1_location,
        MAX(CASE
            WHEN LOWER(CAST(u.lymph_node_assessment AS VARCHAR))
                 LIKE '%suspicious%'
             OR  LOWER(CAST(u.lymph_node_assessment AS VARCHAR))
                 LIKE '%patholog%'
            THEN 1 ELSE 0
        END)                                                     AS us_suspicious_ln,
        MAX(CAST(u.lymph_node_assessment AS VARCHAR))            AS us_ln_assessment_text
    FROM ultrasound_reports u
    GROUP BY u.research_id
),
ct_agg AS (
    SELECT
        c.research_id,
        COUNT(*)                                                 AS n_ct_studies,
        MAX(CAST(c.date_of_exam AS VARCHAR))                    AS last_ct_date,
        MAX(CAST(c.exam_type_normalized AS VARCHAR))            AS ct_exam_type,
        MAX(CASE WHEN LOWER(CAST(c.contrast AS VARCHAR)) IN ('true','yes','with','1')
                 THEN 1 ELSE 0 END)                             AS ct_with_contrast,
        MAX(CASE WHEN LOWER(CAST(c.thyroid_nodule AS VARCHAR)) IN ('true','yes','1')
                 THEN 1 ELSE 0 END)                             AS ct_thyroid_nodule,
        MAX(CASE WHEN LOWER(CAST(c.thyroid_enlarged AS VARCHAR)) IN ('true','yes','1')
                 THEN 1 ELSE 0 END)                             AS ct_thyroid_enlarged,
        MAX(CASE WHEN LOWER(CAST(c.thyroid_postsurgical AS VARCHAR)) IN ('true','yes','1')
                 THEN 1 ELSE 0 END)                             AS ct_postsurgical,
        MAX(CASE WHEN LOWER(CAST(c.goiter_present AS VARCHAR)) IN ('true','yes','1')
                 THEN 1 ELSE 0 END)                             AS ct_goiter,
        MAX(CASE WHEN LOWER(CAST(c.pathologic_lymph_nodes AS VARCHAR)) IN ('true','yes','1')
                 THEN 1 ELSE 0 END)                             AS ct_pathologic_ln,
        MAX(CASE WHEN LOWER(CAST(c.lymph_nodes_suspicious AS VARCHAR)) IN ('true','yes','1')
                 THEN 1 ELSE 0 END)                             AS ct_suspicious_ln,
        MAX(TRY_CAST(c.largest_lymph_node_short_axis_mm AS DOUBLE)) AS ct_largest_ln_mm,
        MAX(CAST(c.lymph_node_locations AS VARCHAR))            AS ct_ln_locations
    FROM ct_imaging c
    GROUP BY c.research_id
),
mri_agg AS (
    SELECT
        m.research_id,
        COUNT(*)                                                 AS n_mri_studies,
        MAX(CAST(m.date_of_exam AS VARCHAR))                    AS last_mri_date,
        MAX(CAST(m.exam_type_detail AS VARCHAR))                AS mri_exam_type,
        MAX(CASE WHEN LOWER(CAST(m.thyroid_nodule AS VARCHAR)) IN ('true','yes','1')
                 THEN 1 ELSE 0 END)                             AS mri_thyroid_nodule,
        MAX(CASE WHEN LOWER(CAST(m.thyroid_enlarged AS VARCHAR)) IN ('true','yes','1')
                 THEN 1 ELSE 0 END)                             AS mri_thyroid_enlarged,
        MAX(CASE WHEN LOWER(CAST(m.substernal_extension AS VARCHAR)) IN ('true','yes','1')
                 THEN 1 ELSE 0 END)                             AS mri_substernal_ext,
        MAX(CASE WHEN LOWER(CAST(m.pathologic_lymph_nodes AS VARCHAR)) IN ('true','yes','1')
                 THEN 1 ELSE 0 END)                             AS mri_pathologic_ln,
        MAX(CAST(m.lymph_node_locations AS VARCHAR))            AS mri_ln_locations
    FROM mri_imaging m
    GROUP BY m.research_id
)
SELECT
    mc.research_id,
    mc.surgery_date,

    -- ── Ultrasound ────────────────────────────────────────────────
    us.n_ultrasound_studies,
    us.last_us_date,
    us.us_max_nodule_count,
    us.us_total_thyroid_vol_ml,
    us.us_right_lobe_vol_ml,
    us.us_left_lobe_vol_ml,
    us.us_max_tirads,
    us.n1_composition,
    us.n1_echogenicity,
    us.n1_calcifications,
    us.n1_margins,
    us.n1_shape,
    us.n1_location,
    us.us_suspicious_ln,
    us.us_ln_assessment_text,

    -- ── CT ────────────────────────────────────────────────────────
    ct.n_ct_studies,
    ct.last_ct_date,
    ct.ct_exam_type,
    ct.ct_with_contrast,
    ct.ct_thyroid_nodule,
    ct.ct_thyroid_enlarged,
    ct.ct_postsurgical,
    ct.ct_goiter,
    ct.ct_pathologic_ln,
    ct.ct_suspicious_ln,
    ct.ct_largest_ln_mm,
    ct.ct_ln_locations,

    -- ── MRI ───────────────────────────────────────────────────────
    mri.n_mri_studies,
    mri.last_mri_date,
    mri.mri_exam_type,
    mri.mri_thyroid_nodule,
    mri.mri_thyroid_enlarged,
    mri.mri_substernal_ext,
    mri.mri_pathologic_ln,
    mri.mri_ln_locations,

    -- ── Cross-modality summary ────────────────────────────────────
    COALESCE(us.n_ultrasound_studies, 0)
    + COALESCE(ct.n_ct_studies, 0)
    + COALESCE(mri.n_mri_studies, 0)    AS total_imaging_studies,
    CASE
        WHEN COALESCE(us.us_suspicious_ln, 0) = 1
          OR COALESCE(ct.ct_pathologic_ln, 0) = 1
          OR COALESCE(ct.ct_suspicious_ln, 0) = 1
          OR COALESCE(mri.mri_pathologic_ln, 0) = 1
        THEN TRUE ELSE FALSE
    END                                  AS any_preop_suspicious_ln,
    CASE
        WHEN COALESCE(ct.ct_goiter, 0) = 1
          OR COALESCE(mri.mri_substernal_ext, 0) = 1
        THEN TRUE ELSE FALSE
    END                                  AS preop_substernal_or_goiter

FROM master_cohort mc
LEFT JOIN us_agg  us  ON mc.research_id = us.research_id
LEFT JOIN ct_agg  ct  ON mc.research_id = ct.research_id
LEFT JOIN mri_agg mri ON mc.research_id = mri.research_id
"""


GENETIC_TESTING_SUMMARY_VIEW_SQL = """
CREATE OR REPLACE VIEW genetic_testing_summary_view AS
SELECT
    gt.research_id,
    gt.test_platform,
    gt.raw_result,
    gt.result_category,
    gt.mutations_detected_text,
    -- Per-gene flags
    COALESCE(gt.braf_v600e, FALSE)      AS braf_v600e,
    COALESCE(gt.braf_other, FALSE)      AS braf_other,
    COALESCE(gt.nras, FALSE)            AS nras,
    COALESCE(gt.hras, FALSE)            AS hras,
    COALESCE(gt.kras, FALSE)            AS kras,
    COALESCE(gt.ras_any, FALSE)         AS ras_any,
    COALESCE(gt.ret_ptc1, FALSE)        AS ret_ptc1,
    COALESCE(gt.ret_ptc3, FALSE)        AS ret_ptc3,
    COALESCE(gt.ret_other, FALSE)       AS ret_other,
    COALESCE(gt.pax8_pparg, FALSE)      AS pax8_pparg,
    COALESCE(gt.tert_promoter, FALSE)   AS tert_promoter,
    COALESCE(gt.ntrk1, FALSE)           AS ntrk1,
    COALESCE(gt.ntrk3, FALSE)           AS ntrk3,
    COALESCE(gt.ntrk_any, FALSE)        AS ntrk_any,
    COALESCE(gt.alk_fusion, FALSE)      AS alk_fusion,
    COALESCE(gt.dicer1, FALSE)          AS dicer1,
    COALESCE(gt.pten, FALSE)            AS pten,
    COALESCE(gt.tp53, FALSE)            AS tp53,
    -- Concordance with final pathology
    CASE
        WHEN gt.result_category IN ('suspicious','positive')
             AND mc.has_tumor_pathology = TRUE  THEN 'TP'
        WHEN gt.result_category IN ('suspicious','positive')
             AND mc.has_tumor_pathology = FALSE THEN 'FP'
        WHEN gt.result_category = 'benign'
             AND mc.has_tumor_pathology = TRUE  THEN 'FN'
        WHEN gt.result_category = 'benign'
             AND mc.has_tumor_pathology = FALSE THEN 'TN'
        ELSE NULL
    END AS molecular_concordance_class,
    -- Final pathology context
    tp.histology_1_type,
    tp.histology_1_overall_stage_ajcc8 AS final_stage
FROM genetic_testing gt
LEFT JOIN master_cohort mc ON gt.research_id = mc.research_id
LEFT JOIN tumor_pathology tp ON gt.research_id = tp.research_id
"""


# ─────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────

def main() -> None:
    print("=" * 70)
    print("  THYROID RESEARCH LAKEHOUSE — PHASE 3: GENETICS + SPECIMEN")
    print("=" * 70)

    con = duckdb.connect(str(DB_PATH))

    # ── 1. Genetic testing ────────────────────────────────────────
    print("\n[1/4] Ingesting genetic testing data …")
    df_gen = ingest_genetic_testing()
    if df_gen is not None:
        pq_path = PROCESSED / "genetic_testing.parquet"
        df_gen.write_parquet(pq_path)
        # Register in DuckDB
        if "genetic_testing" in {r[0] for r in con.execute("SHOW TABLES").fetchall()}:
            con.execute("DROP TABLE genetic_testing")
        con.execute(f"CREATE TABLE genetic_testing AS SELECT * FROM read_parquet('{pq_path}')")
        n = con.execute("SELECT COUNT(*) FROM genetic_testing").fetchone()[0]
        print(f"  ✅  genetic_testing table: {n:,} rows")
    else:
        print("  ⏭  genetic_testing skipped")

    # ── 2. Specimen detail parquet ────────────────────────────────
    print("\n[2/4] Building specimen detail …")
    df_spec = ingest_specimen_detail()
    if df_spec is not None:
        pq_path = PROCESSED / "specimen_detail.parquet"
        df_spec.write_parquet(pq_path)
        print(f"  ✅  specimen_detail.parquet written")
    else:
        print("  ⏭  specimen_detail skipped")

    # ── 3. Pre-op imaging summary ─────────────────────────────────
    print("\n[3/4] Building pre-op imaging summary …")
    df_img = ingest_preop_imaging_summary(con)
    if df_img is not None:
        pq_path = PROCESSED / "preop_imaging_summary.parquet"
        df_img.write_parquet(pq_path)
        print(f"  ✅  preop_imaging_summary.parquet written")

    # ── 4. Create views ───────────────────────────────────────────
    print("\n[4/4] Creating analytic views …")
    views = [
        ("benign_detail_view",              BENIGN_DETAIL_VIEW_SQL),
        ("specimen_detail_view",            SPECIMEN_DETAIL_VIEW_SQL),
        ("preop_imaging_detail_view",       PREOP_IMAGING_DETAIL_VIEW_SQL),
    ]
    if df_gen is not None:
        views.append(("genetic_testing_summary_view", GENETIC_TESTING_SUMMARY_VIEW_SQL))

    for name, sql in views:
        try:
            con.execute(sql)
            n = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
            print(f"  ✅  {name:40s}  {n:>8,} rows")
        except Exception as e:
            print(f"  ⚠️  {name} failed: {e}")

    con.close()
    print("\n" + "=" * 70)
    print("  PHASE 3 COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
