"""
Script 51b: Thyroid scoring systems — Python-based implementation.
Replaces the slow multi-CTE SQL in script 51 with pandas-based score computation.

Computes: AJCC8 T/N/M/stage_group, ATA 2015 initial risk, MACIS, AGES, AMES,
LN burden, molecular risk composite.

Usage:
    .venv/bin/python scripts/51b_thyroid_scoring_python.py --md
    .venv/bin/python scripts/51b_thyroid_scoring_python.py --local
"""
from __future__ import annotations
import argparse, os, sys, time
import pandas as pd
import numpy as np

# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------

def get_connection(md: bool):
    import duckdb
    if md:
        token = os.environ.get("MOTHERDUCK_TOKEN") or ""
        if not token:
            try:
                import toml
                token = toml.load(".streamlit/secrets.toml").get("MOTHERDUCK_TOKEN", "")
            except Exception:
                pass
        if not token:
            sys.exit("MOTHERDUCK_TOKEN not found")
        con = duckdb.connect(f"md:thyroid_research_2026?motherduck_token={token}")
    else:
        db = os.environ.get("LOCAL_DUCKDB_PATH", "thyroid_master.duckdb")
        con = duckdb.connect(db)
    return con


def table_exists(con, name: str) -> bool:
    try:
        r = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema='main' AND table_name=?", [name]
        ).fetchone()[0]
        return r > 0
    except Exception:
        return False


def safe_pull(con, sql: str, fallback_cols: list[str]) -> pd.DataFrame:
    try:
        return con.execute(sql).fetchdf()
    except Exception as e:
        print(f"  [WARN] Query failed: {e}")
        return pd.DataFrame(columns=fallback_cols)

# ---------------------------------------------------------------------------
# AJCC 8th Edition staging for DTC
# ---------------------------------------------------------------------------

def compute_t_stage(row) -> str | None:
    size = row.get("tumor_size_cm")
    ete = str(row.get("ete_grade", "") or "").lower()
    gross_ete = row.get("gross_ete_flag") is True

    if pd.isna(size) or size is None:
        # ETE alone can still help determine T3b
        if gross_ete or "gross" in ete:
            return "T3b"
        return None

    size = float(size)
    if gross_ete or "gross" in ete:
        return "T3b"
    # Microscopic ETE does NOT upstage per AJCC8
    if size <= 1.0:
        return "T1a"
    elif size <= 2.0:
        return "T1b"
    elif size <= 4.0:
        return "T2"
    elif size > 4.0:
        return "T3a"
    return None


def compute_n_stage(row) -> str | None:
    ln_pos = row.get("ln_positive")
    central = row.get("central_dissected") is True
    lateral = row.get("lateral_dissected") is True
    ln_level_raw = str(row.get("ln_level_raw", "") or "").lower()
    ln_loc_raw = str(row.get("ln_loc_raw", "") or "").lower()

    if pd.isna(ln_pos) or ln_pos is None:
        return None
    ln_pos = int(ln_pos) if not pd.isna(ln_pos) else 0

    if ln_pos == 0:
        return "N0"

    # N1b: lateral neck or level II-V involvement
    if lateral or any(x in ln_level_raw + ln_loc_raw for x in
                      ["level ii", "level iii", "level iv", "level v",
                       "jugular", "lateral", "posterior triangle", "n1b"]):
        return "N1b"
    # N1a: central (level VI/VII)
    if central or any(x in ln_level_raw + ln_loc_raw for x in
                      ["level vi", "level vii", "central", "paratracheal",
                       "pretracheal", "perithyroidal", "n1a"]):
        return "N1a"
    # Default: any positive node → N1a if no other info
    if ln_pos > 0:
        return "N1a"
    return "N0"


def compute_stage_group(row) -> str | None:
    age = row.get("age_at_surgery")
    t = row.get("ajcc8_t_stage")
    n = row.get("ajcc8_n_stage")
    m = row.get("ajcc8_m_stage", "M0")

    if pd.isna(age) or age is None or t is None:
        return None
    age = float(age)
    m = str(m or "M0")

    if age < 55:
        if m == "M1":
            return "II"
        return "I"
    else:
        # ≥55
        if m == "M1":
            return "IVB"
        if t == "T4b":
            return "IVA"
        if t in ("T4a",) or n == "N1b":
            return "III"
        if t in ("T1a", "T1b", "T2") and n in ("N0", None, "Nx"):
            return "I"
        if t in ("T1a", "T1b", "T2") and n in ("N1a", "N1b"):
            return "II"
        if t == "T3a" or t == "T3b":
            return "II"
        return None


# ---------------------------------------------------------------------------
# ATA 2015 Initial Risk
# ---------------------------------------------------------------------------

def compute_ata_risk(row) -> str | None:
    histology = str(row.get("histology", "") or "").lower()
    ete = str(row.get("ete_grade", "") or "").lower()
    gross_ete = row.get("gross_ete_flag") is True
    margin = str(row.get("margin_r_class", "") or "").lower()
    distant_mets = row.get("distant_mets_proxy") is True
    ln_pos = row.get("ln_positive")
    vasc = str(row.get("vasc_grade", "") or "").lower()
    aggressive_variant = row.get("aggressive_variant_flag") is True
    age = row.get("age_at_surgery")
    ln_max_cm = row.get("ln_max_deposit_cm")

    if histology in ("", "unknown", "nan", None):
        return None
    if histology not in ("ptc", "ptc_classic", "ptc_follicular_variant", "ptc_tall_cell",
                         "ptc_hobnail", "ptc_columnar", "ptc_diffuse_sclerosing",
                         "ftc", "hcc", "hcc_oncocytic", "pdtc", "niftp"):
        # Non-PTC/FTC histology — not subject to ATA DTC risk stratification
        return None

    # HIGH risk criteria
    if gross_ete or "gross" in ete:
        return "high"
    if margin and "r2" in margin:
        return "high"
    if distant_mets:
        return "high"
    if ln_max_cm is not None and not pd.isna(ln_max_cm) and float(ln_max_cm) > 3.0:
        return "high"
    # FTC + extensive vascular invasion
    if histology in ("ftc",) and "extensive" in vasc:
        return "high"

    # INTERMEDIATE risk criteria
    if aggressive_variant:
        return "intermediate"
    if "vascular" in vasc and vasc not in ("absent",):
        return "intermediate"
    if "microscopic" in ete and not gross_ete:
        return "intermediate"
    # >5 positive LN OR any node 0.2-3cm
    if ln_pos is not None and not pd.isna(ln_pos) and int(ln_pos) > 5:
        return "intermediate"
    if ln_max_cm is not None and not pd.isna(ln_max_cm):
        val = float(ln_max_cm)
        if 0.2 <= val <= 3.0:
            return "intermediate"

    # LOW risk — PTC/NIFTP with none of the above
    return "low"


# ---------------------------------------------------------------------------
# MACIS Score
# ---------------------------------------------------------------------------

def compute_macis(row) -> float | None:
    age = row.get("age_at_surgery")
    size = row.get("tumor_size_cm")
    margin = str(row.get("margin_r_class", "") or "").lower()
    gross_ete = row.get("gross_ete_flag") is True
    distant_mets = row.get("distant_mets_proxy") is True

    if pd.isna(age) or pd.isna(size) or age is None or size is None:
        return None

    age = float(age)
    size = float(size)

    age_factor = 0.08 * age if age < 40 else 0.22 * age
    incomplete_resection = 1.0 if ("r1" in margin or "r2" in margin) else 0.0
    local_invasion = 1.0 if gross_ete else 0.0
    distant = 3.0 if distant_mets else 0.0

    return round(3.1 * age_factor + 0.3 * size + incomplete_resection + local_invasion + distant, 3)


def macis_risk_group(score: float | None) -> str | None:
    if score is None or pd.isna(score):
        return None
    if score < 6.0:
        return "low"
    elif score < 7.0:
        return "intermediate"
    elif score < 8.0:
        return "high"
    else:
        return "very_high"


# ---------------------------------------------------------------------------
# AGES Score
# ---------------------------------------------------------------------------

def compute_ages(row) -> float | None:
    age = row.get("age_at_surgery")
    histologic_grade = str(row.get("histologic_grade_raw", "") or "").lower()
    distant_mets = row.get("distant_mets_proxy") is True
    size = row.get("tumor_size_cm")
    gross_ete = row.get("gross_ete_flag") is True

    if pd.isna(age) or age is None:
        return None

    age_score = 0 if float(age) < 40 else float(age)
    grade_score = 3 if any(x in histologic_grade for x in ["3", "high", "poor", "undiff"]) else 0
    mets_score = 1 if distant_mets else 0
    size_score = min(float(size) if size and not pd.isna(size) else 0.0, 3.0)
    ete_score = 1 if (gross_ete or distant_mets) else 0

    return round(age_score * 0.1 + grade_score + mets_score + size_score + ete_score, 2)


# ---------------------------------------------------------------------------
# AMES Risk
# ---------------------------------------------------------------------------

def compute_ames(row) -> str | None:
    age = row.get("age_at_surgery")
    sex = str(row.get("sex", "") or "").lower()
    gross_ete = row.get("gross_ete_flag") is True
    distant_mets = row.get("distant_mets_proxy") is True
    size = row.get("tumor_size_cm")
    histology = str(row.get("histology", "") or "").lower()

    if pd.isna(age) or age is None:
        return None

    age = float(age)

    # Low risk: young patients without high-risk features
    young = (sex in ("female", "f") and age < 50) or (sex in ("male", "m") and age < 40)
    if young and not gross_ete and not distant_mets:
        if size is None or pd.isna(size) or float(size) <= 5.0:
            return "low"

    # High risk: distant mets, gross ETE, or large FTC
    if distant_mets or gross_ete:
        return "high"
    if histology in ("ftc",) and size is not None and not pd.isna(size) and float(size) > 5.0:
        return "high"

    # Older patients or large tumors → high risk
    if not young:
        if size is not None and not pd.isna(size) and float(size) > 5.0:
            return "high"
        return "high"

    return "low"


# ---------------------------------------------------------------------------
# LN burden
# ---------------------------------------------------------------------------

def compute_ln_burden(row) -> dict:
    ln_pos = row.get("ln_positive")
    ln_exam = row.get("ln_examined")
    ln_pos = 0 if pd.isna(ln_pos) or ln_pos is None else int(ln_pos)
    ln_exam = 0 if pd.isna(ln_exam) or ln_exam is None else int(ln_exam)

    ratio = None
    band = None
    if ln_exam > 0:
        ratio = round(ln_pos / ln_exam, 4)
        if ratio == 0:
            band = "N0"
        elif ratio < 0.2:
            band = "low"
        elif ratio < 0.5:
            band = "intermediate"
        else:
            band = "high"

    return {
        "ln_ratio": ratio,
        "ln_burden_band": band,
        "ln_burden_n_positive": ln_pos if ln_pos > 0 else None,
        "ln_burden_n_examined": ln_exam if ln_exam > 0 else None,
    }


# ---------------------------------------------------------------------------
# Molecular risk composite
# ---------------------------------------------------------------------------

def _safe_bool_flag(val) -> bool:
    """Convert potentially NA/None/string boolean to Python bool safely."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return False
    try:
        return str(val).lower() in ("true", "1", "yes")
    except Exception:
        return False


def compute_mol_risk(row) -> str | None:
    braf = _safe_bool_flag(row.get("braf_positive_final"))
    tert = _safe_bool_flag(row.get("tert_positive"))
    ras = _safe_bool_flag(row.get("ras_positive"))
    histology = str(row.get("histology", "") or "").lower()

    if not any([braf, tert, ras]):
        return "wild_type" if histology not in ("", "unknown") else None

    if tert and braf:
        return "high"  # TERT + BRAF co-mutation
    if tert:
        return "high"
    if braf:
        return "intermediate"
    if ras:
        return "low_intermediate"
    return None


# ---------------------------------------------------------------------------
# ATA response (provisional — using Tg proxy)
# ---------------------------------------------------------------------------

def compute_ata_response(row) -> str | None:
    tg_nadir = row.get("tg_nadir")
    tg_rising = row.get("tg_rising_flag") is True
    distant_mets = row.get("distant_mets_proxy") is True
    rai = row.get("rai_received") is True

    if not rai:
        return None  # Only applicable if RAI was given
    if tg_nadir is None or pd.isna(tg_nadir):
        return "insufficient_data"

    tg_nadir = float(tg_nadir)
    if distant_mets:
        return "structural_incomplete"
    if tg_nadir < 0.2 and not tg_rising:
        return "excellent"
    elif 0.2 <= tg_nadir <= 1.0:
        return "indeterminate"
    elif tg_rising or tg_nadir > 1.0:
        return "biochemical_incomplete"
    return "indeterminate"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def build_scoring_table(con, dry_run: bool = False) -> None:
    print("\n" + "="*70)
    print("  Building thyroid_scoring_systems_v1 (Python-based)")
    print("="*70)

    if not table_exists(con, "tumor_episode_master_v2"):
        print("  [SKIP] tumor_episode_master_v2 not available")
        return

    # 1. Pull base tumor data
    print("  Pulling tumor_episode_master_v2...")
    pt = safe_pull(con, """
        SELECT DISTINCT ON (research_id)
            research_id,
            surgery_date,
            primary_histology,
            histology_variant,
            tumor_size_cm,
            LOWER(CAST(COALESCE(extrathyroidal_extension, '') AS VARCHAR)) AS ete_grade_raw,
            COALESCE(gross_ete, FALSE) AS gross_ete_flag,
            LOWER(CAST(COALESCE(margin_status, '') AS VARCHAR)) AS margin_raw,
            COALESCE(multifocality_flag, FALSE) AS multifocal_flag,
            nodal_disease_positive_count AS ln_positive_raw,
            nodal_disease_total_count AS ln_examined_raw,
            t_stage AS ln_level_raw,
            histology_variant AS ln_loc_raw
        FROM tumor_episode_master_v2
        ORDER BY research_id, surgery_date ASC NULLS LAST
    """, ["research_id"])

    # 2. Demographics
    print("  Pulling demographics...")
    demo = safe_pull(con, """
        SELECT DISTINCT ON (research_id)
            research_id, age_at_surgery, sex
        FROM demographics_harmonized_v2
        ORDER BY research_id
    """, ["research_id", "age_at_surgery", "sex"])

    # 3. Master clinical v12 (with deduplication)
    print("  Pulling patient_refined_master_clinical_v12...")
    mcv = safe_pull(con, """
        SELECT research_id,
            LOWER(CAST(COALESCE(ete_grade_v9, '') AS VARCHAR)) AS ete_grade,
            LOWER(CAST(COALESCE(vasc_grade_final_v13, vascular_who_2022_grade, '') AS VARCHAR)) AS vasc_grade,
            LOWER(CAST(COALESCE(margin_r_class_v10, margin_r_classification, '') AS VARCHAR)) AS margin_r_class,
            COALESCE(ln_total_positive, ln_positive_v6) AS ln_positive,
            ln_total_examined AS ln_examined,
            ln_central_dissected AS central_dissected,
            lateral_neck_dissected_v10 AS lateral_dissected,
            braf_positive_final,
            tert_positive_v9 AS tert_positive,
            ras_positive_final,
            max_dose_mci AS max_rai_dose,
            first_rai_date,
            ata_response_category AS ata_response_mcv
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY research_id) AS rn
            FROM patient_refined_master_clinical_v12
        ) t WHERE rn=1
    """, ["research_id"])

    # 4. Bethesda
    print("  Pulling FNA Bethesda...")
    beth = safe_pull(con, """
        SELECT research_id,
               bethesda_final AS bethesda_num,
               bethesda_final_name AS bethesda_category,
               best_source_reliability AS bethesda_confidence,
               source_tables AS bethesda_source
        FROM extracted_fna_bethesda_v1
    """, ["research_id"])

    # 5. RAI
    print("  Pulling RAI...")
    rai = safe_pull(con, """
        SELECT DISTINCT research_id, TRUE AS rai_received
        FROM rai_treatment_episode_v2
        WHERE rai_assertion_status IN ('definite_received','likely_received')
    """, ["research_id"])

    # 6. Recurrence
    print("  Pulling recurrence...")
    rec = safe_pull(con, """
        SELECT research_id,
               COALESCE(recurrence_flag_structured, recurrence_any, FALSE) AS recurrence_flag,
               first_recurrence_date
        FROM extracted_recurrence_refined_v1
    """, ["research_id"])

    # 7. Tg labs
    print("  Pulling Tg labs...")
    tg_labs = safe_pull(con, """
        SELECT research_id,
            MIN(TRY_CAST(REPLACE(TRIM(CAST(result AS VARCHAR)), '<','') AS DOUBLE)) AS tg_nadir,
            MAX(TRY_CAST(REPLACE(TRIM(CAST(result AS VARCHAR)), '<','') AS DOUBLE)) AS tg_max,
            LAST(TRY_CAST(REPLACE(TRIM(CAST(result AS VARCHAR)), '<','') AS DOUBLE)
                ORDER BY TRY_CAST(specimen_collect_dt AS TIMESTAMP) NULLS LAST) AS tg_last
        FROM thyroglobulin_labs
        WHERE result IS NOT NULL AND TRIM(CAST(result AS VARCHAR)) != ''
        GROUP BY research_id
    """, ["research_id", "tg_nadir", "tg_max", "tg_last"])

    # 8. Rising Tg flag from recurrence refined
    tg_rising = safe_pull(con, "SELECT research_id, tg_rising_flag FROM extracted_recurrence_refined_v1",
                          ["research_id", "tg_rising_flag"])

    print("  Merging datasets...")
    # Ensure research_id is consistently int64 across all frames
    for frame in [pt, demo, mcv, beth, rai, rec, tg_labs, tg_rising]:
        if "research_id" in frame.columns:
            frame["research_id"] = pd.to_numeric(frame["research_id"], errors="coerce").astype("Int64")

    df = pt.merge(demo, on="research_id", how="left")
    df = df.merge(mcv, on="research_id", how="left")
    df = df.merge(beth, on="research_id", how="left")
    df = df.merge(rai, on="research_id", how="left")
    df = df.merge(rec, on="research_id", how="left")
    df = df.merge(tg_labs, on="research_id", how="left")
    df = df.merge(tg_rising, on="research_id", how="left")

    # Consolidate ete_grade
    df["ete_grade"] = df["ete_grade"].fillna(df["ete_grade_raw"])
    df["gross_ete_flag"] = (
        df["gross_ete_flag"].fillna(False) |
        df["ete_grade"].str.contains("gross", na=False)
    )

    # Consolidate ln columns
    df["ln_positive"] = df["ln_positive"].fillna(df["ln_positive_raw"])
    df["ln_examined"] = df["ln_examined"].fillna(df["ln_examined_raw"])

    # Histology
    df["histology"] = df["primary_histology"].fillna("unknown").str.lower()

    # Distant mets proxy
    df["distant_mets_proxy"] = df["recurrence_flag"].fillna(False)
    df["rai_received"] = df["rai_received"].fillna(False)

    # Aggressive variant flag
    agg_variants = ["tall_cell", "hobnail", "columnar", "diffuse_sclerosing", "solid", "pdtc"]
    hist_text = (df["primary_histology"].fillna("") + " " + df["histology_variant"].fillna("")).str.lower()
    df["aggressive_variant_flag"] = hist_text.apply(lambda x: any(v in x for v in agg_variants))

    print("  Computing AJCC8 T stage...")
    df["ajcc8_t_stage"] = df.apply(compute_t_stage, axis=1)
    df["ajcc8_t_stage_calculable_flag"] = df["ajcc8_t_stage"].notna()

    print("  Computing AJCC8 N stage...")
    df["ajcc8_n_stage"] = df.apply(compute_n_stage, axis=1)

    print("  Computing AJCC8 M stage (recurrence proxy)...")
    df["ajcc8_m_stage"] = df["distant_mets_proxy"].apply(lambda x: "M1" if x else "M0")

    print("  Computing AJCC8 stage group...")
    df["ajcc8_stage_group"] = df.apply(compute_stage_group, axis=1)
    df["ajcc8_stage_calculable_flag"] = df["ajcc8_stage_group"].notna()

    print("  Computing ATA initial risk...")
    df["ata_initial_risk"] = df.apply(compute_ata_risk, axis=1)
    df["ata_risk_calculable_flag"] = df["ata_initial_risk"].notna()

    print("  Computing ATA response (provisional)...")
    df["ata_response_provisional"] = df.apply(compute_ata_response, axis=1)
    df["ata_response_is_provisional"] = True  # All proxy-based

    print("  Computing MACIS...")
    df["macis_score"] = df.apply(compute_macis, axis=1)
    df["macis_risk_group"] = df["macis_score"].apply(macis_risk_group)
    df["macis_calculable_flag"] = df["macis_score"].notna()
    df["macis_missing_components"] = df.apply(
        lambda r: ",".join(
            [c for c, v in [("age", r.get("age_at_surgery")), ("size", r.get("tumor_size_cm"))]
             if v is None or (isinstance(v, float) and np.isnan(v))]
        ), axis=1
    ).replace("", None)

    print("  Computing AGES...")
    df["ages_score"] = df.apply(compute_ages, axis=1)
    df["ages_calculable_flag"] = df["ages_score"].notna()

    print("  Computing AMES risk...")
    df["ames_risk"] = df.apply(compute_ames, axis=1)
    df["ames_calculable_flag"] = df["ames_risk"].notna()

    print("  Computing LN burden...")
    ln_burden_data = df.apply(compute_ln_burden, axis=1, result_type="expand")
    df = pd.concat([df, ln_burden_data], axis=1)

    print("  Computing molecular risk composite...")
    df["ras_positive"] = df["ras_positive_final"].fillna("false")
    df["molecular_risk_tier"] = df.apply(compute_mol_risk, axis=1)
    df["molecular_risk_calculable_flag"] = df["molecular_risk_tier"].notna()

    # --- Final output columns ---
    out_cols = [
        "research_id",
        # AJCC8
        "ajcc8_t_stage", "ajcc8_n_stage", "ajcc8_m_stage", "ajcc8_stage_group",
        "ajcc8_t_stage_calculable_flag", "ajcc8_stage_calculable_flag",
        # ATA
        "ata_initial_risk", "ata_risk_calculable_flag",
        "ata_response_provisional", "ata_response_is_provisional",
        # MACIS
        "macis_score", "macis_risk_group", "macis_calculable_flag", "macis_missing_components",
        # AGES
        "ages_score", "ages_calculable_flag",
        # AMES
        "ames_risk", "ames_calculable_flag",
        # LN burden
        "ln_ratio", "ln_burden_band", "ln_burden_n_positive", "ln_burden_n_examined",
        # Molecular
        "molecular_risk_tier", "molecular_risk_calculable_flag",
        "braf_positive_final", "tert_positive", "ras_positive_final",
        # Bethesda
        "bethesda_num", "bethesda_category", "bethesda_confidence", "bethesda_source",
        # Clinical inputs (for audit)
        "tumor_size_cm", "age_at_surgery", "sex", "histology", "ete_grade",
        "gross_ete_flag", "vasc_grade", "margin_r_class", "aggressive_variant_flag",
        "multifocal_flag", "ln_positive", "ln_examined",
        "rai_received", "max_rai_dose", "tg_nadir", "tg_max",
        "distant_mets_proxy", "recurrence_flag", "first_recurrence_date",
    ]
    # Only keep columns that exist
    out_cols = [c for c in out_cols if c in df.columns]
    out = df[out_cols].copy()

    print(f"  Output shape: {out.shape}")
    if dry_run:
        print("  [DRY-RUN] Would create thyroid_scoring_systems_v1")
        print(out.head(3).to_string())
        return

    # Write to DuckDB via parquet — use a versioned staging name to avoid lock conflicts
    import tempfile, pathlib, time as _time
    print("  Writing scoring data to MotherDuck...")
    tmp = pathlib.Path(tempfile.mktemp(suffix=".parquet"))
    out.to_parquet(tmp, index=False)
    # Try canonical name; fall back to staging name if locked
    target_table = "thyroid_scoring_systems_v1"
    alt_table = "thyroid_scoring_py_v1"  # used if canonical is locked
    written_table = None
    for tname in [target_table, alt_table]:
        try:
            con.execute(f"DROP TABLE IF EXISTS {tname}")
            con.execute(f"CREATE TABLE {tname} AS SELECT * FROM read_parquet('{tmp}')")
            written_table = tname
            break
        except Exception as e:
            print(f"  Could not write to {tname}: {e}")
    tmp.unlink(missing_ok=True)
    if written_table is None:
        raise RuntimeError("Could not write scoring table under any name")
    if written_table != target_table:
        print(f"  [NOTE] Wrote to {written_table} (canonical name {target_table} is locked; "
              f"re-run after MotherDuck clears the transaction)")
        # Create a VIEW alias so downstream scripts find the data
        try:
            con.execute(f"CREATE OR REPLACE VIEW {target_table} AS SELECT * FROM {written_table}")
            print(f"  Created VIEW {target_table} -> {written_table}")
        except Exception as e:
            print(f"  Could not create view alias: {e}")
    n = con.execute(f"SELECT COUNT(*) FROM {written_table}").fetchone()[0]
    print(f"  {written_table}: {n:,} rows written")

    # Validation summary
    print("\n  === Calculability Summary ===")
    for col in ["ajcc8_t_stage_calculable_flag", "ajcc8_stage_calculable_flag",
                "ata_risk_calculable_flag", "macis_calculable_flag",
                "ages_calculable_flag", "ames_calculable_flag",
                "molecular_risk_calculable_flag"]:
        if col in out.columns:
            pct = out[col].fillna(False).sum() / len(out) * 100
            print(f"    {col}: {pct:.1f}%")

    # Write val_scoring_systems
    val_sql = f"""
    CREATE OR REPLACE TABLE val_scoring_systems AS
    SELECT
        {n} AS n_patients,
        SUM(CASE WHEN ajcc8_stage_calculable_flag THEN 1 ELSE 0 END) AS ajcc8_calculable_n,
        ROUND(100.0*SUM(CASE WHEN ajcc8_stage_calculable_flag THEN 1 ELSE 0 END)/{n}, 2)
            AS ajcc8_calculable_pct,
        SUM(CASE WHEN ata_risk_calculable_flag THEN 1 ELSE 0 END) AS ata_calculable_n,
        ROUND(100.0*SUM(CASE WHEN ata_risk_calculable_flag THEN 1 ELSE 0 END)/{n}, 2)
            AS ata_calculable_pct,
        SUM(CASE WHEN macis_calculable_flag THEN 1 ELSE 0 END) AS macis_calculable_n,
        ROUND(100.0*SUM(CASE WHEN macis_calculable_flag THEN 1 ELSE 0 END)/{n}, 2)
            AS macis_calculable_pct,
        CURRENT_TIMESTAMP AS validated_at
    FROM {written_table}
    """
    con.execute(val_sql)
    val = con.execute("SELECT * FROM val_scoring_systems").fetchdf()
    print("\n  === val_scoring_systems ===")
    print(val.to_string(index=False))
    print("\n  [DONE] thyroid_scoring_systems_v1 complete")


def main():
    p = argparse.ArgumentParser()
    g = p.add_mutually_exclusive_group()
    g.add_argument("--md", action="store_true", help="Use MotherDuck")
    g.add_argument("--local", action="store_true", help="Use local DuckDB")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    print("="*70)
    print("  Connecting...")
    con = get_connection(md=args.md)
    print("  Connected")
    build_scoring_table(con, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
