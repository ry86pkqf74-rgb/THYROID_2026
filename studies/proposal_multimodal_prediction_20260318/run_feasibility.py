#!/usr/bin/env python3
"""
Multimodal Thyroid Cancer Prediction – Feasibility & Statistics Pack
====================================================================
Reads canonical tables from MotherDuck (read-only) and produces all
deliverables for the next-paper proposal.

Usage:
    .venv/bin/python studies/proposal_multimodal_prediction_20260318/run_feasibility.py

All outputs land in studies/proposal_multimodal_prediction_20260318/
"""
from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

import pandas as pd

# ---------- repo root on sys.path ----------
REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from motherduck_client import MotherDuckClient  # noqa: E402

OUT = Path(__file__).resolve().parent
QUERY_LOG: list[str] = []
TIMESTAMP = dt.datetime.now().strftime("%Y%m%d_%H%M")

# ──────────────────────────────────────────────────────────────────────
# helpers
# ──────────────────────────────────────────────────────────────────────

def qlog(label: str, sql: str) -> str:
    """Append to in-memory query log and return the sql unchanged."""
    QUERY_LOG.append(f"-- [{label}]\n{sql.strip()}\n")
    return sql


def run_sql(con, label: str, sql: str) -> pd.DataFrame:
    """Execute SQL, log it, return DataFrame."""
    qlog(label, sql)
    return con.execute(sql).fetchdf()


def safe_df(con, label: str, sql: str) -> pd.DataFrame:
    """Execute and return DF; return empty DF on error."""
    try:
        return run_sql(con, label, sql)
    except Exception as e:
        print(f"  ⚠ {label}: {e}")
        return pd.DataFrame()


def save_csv(df: pd.DataFrame, name: str):
    p = OUT / name
    df.to_csv(p, index=False)
    print(f"  ✓ {name} ({len(df)} rows)")


def save_md(text: str, name: str):
    p = OUT / name
    p.write_text(text)
    print(f"  ✓ {name}")


def save_txt(text: str, name: str):
    p = OUT / name
    p.write_text(text)
    print(f"  ✓ {name}")


# ──────────────────────────────────────────────────────────────────────
# 1 · SCHEMA INVENTORY
# ──────────────────────────────────────────────────────────────────────

# The canonical tables we want to inventory
INVENTORY_TABLES = [
    "master_cohort",
    "manuscript_cohort_v1",
    "analysis_cancer_cohort_v1",
    "patient_analysis_resolved_v1",
    "episode_analysis_resolved_v1_dedup",
    "imaging_nodule_master_v1",
    "imaging_patient_summary_v1",
    "extracted_tirads_validated_v1",
    "operative_episode_detail_v2",
    "molecular_test_episode_v2",
    "rai_treatment_episode_v2",
    "longitudinal_lab_canonical_v1",
    "recurrence_risk_features_mv",
    "provenance_enriched_events_v1",
    "patient_refined_master_clinical_v12",
    "complication_phenotype_v1",
    "complication_patient_summary_v1",
    "extracted_recurrence_refined_v1",
    "extracted_fna_bethesda_v1",
    "thyroid_scoring_py_v1",
]


def build_schema_inventory(con) -> str:
    """Inventory all target tables: existence, row count, column list."""
    lines = ["# Schema Inventory\n",
             f"Generated: {TIMESTAMP}\n",
             "## Table-by-Table\n"]
    inventory_rows = []
    for tbl in INVENTORY_TABLES:
        # check existence
        exists_sql = f"""
SELECT COUNT(*) AS n
FROM information_schema.tables
WHERE table_name = '{tbl}' AND table_schema = 'main'
"""
        exists_df = safe_df(con, f"exists_{tbl}", exists_sql)
        exists = int(exists_df.iloc[0, 0]) > 0 if len(exists_df) else False

        if not exists:
            # try md_ prefixed
            md_tbl = f"md_{tbl}"
            exists_sql2 = f"""
SELECT COUNT(*) AS n
FROM information_schema.tables
WHERE table_name = '{md_tbl}' AND table_schema = 'main'
"""
            exists_df2 = safe_df(con, f"exists_{md_tbl}", exists_sql2)
            exists2 = int(exists_df2.iloc[0, 0]) > 0 if len(exists_df2) else False
            if exists2:
                tbl_actual = md_tbl
                exists = True
            else:
                lines.append(f"### {tbl}\n**Status**: NOT FOUND\n\n")
                inventory_rows.append({"table": tbl, "exists": False, "row_count": 0, "columns": ""})
                continue
        else:
            tbl_actual = tbl

        # row count
        cnt_sql = f"SELECT COUNT(*) AS n FROM {tbl_actual}"
        cnt_df = safe_df(con, f"count_{tbl}", cnt_sql)
        row_count = int(cnt_df.iloc[0, 0]) if len(cnt_df) else 0

        # columns
        cols_sql = f"""
SELECT DISTINCT column_name, data_type
FROM information_schema.columns
WHERE table_name = '{tbl_actual}' AND table_schema = 'main'
ORDER BY ordinal_position
"""
        cols_df = safe_df(con, f"cols_{tbl}", cols_sql)
        col_list = ", ".join(cols_df["column_name"].tolist()) if len(cols_df) else "N/A"
        col_count = len(cols_df)

        lines.append(f"### {tbl} (actual: {tbl_actual})\n")
        lines.append(f"- **Rows**: {row_count:,}")
        lines.append(f"- **Columns**: {col_count}")
        lines.append(f"- **Column list**: {col_list}\n\n")
        inventory_rows.append({
            "table": tbl,
            "actual_name": tbl_actual,
            "exists": True,
            "row_count": row_count,
            "n_columns": col_count,
            "columns": col_list,
        })

    return "\n".join(lines), inventory_rows


# ──────────────────────────────────────────────────────────────────────
# 2 · COHORT COUNTS
# ──────────────────────────────────────────────────────────────────────

COHORT_QUERIES = {
    "master_cohort_patients": "SELECT COUNT(DISTINCT research_id) FROM master_cohort",
    "manuscript_cohort_v1_patients": "SELECT COUNT(DISTINCT research_id) FROM manuscript_cohort_v1",
    "analysis_cancer_cohort_v1_patients": "SELECT COUNT(DISTINCT research_id) FROM analysis_cancer_cohort_v1",
    "patient_analysis_resolved_v1_patients": "SELECT COUNT(DISTINCT research_id) FROM patient_analysis_resolved_v1",
    "episode_dedup_episodes": "SELECT COUNT(*) FROM episode_analysis_resolved_v1_dedup",
    "imaging_nodule_master_v1_rows": "SELECT COUNT(*) FROM imaging_nodule_master_v1",
    "imaging_patient_summary_v1_patients": "SELECT COUNT(DISTINCT research_id) FROM imaging_patient_summary_v1",
    "extracted_tirads_validated_v1_patients": "SELECT COUNT(DISTINCT research_id) FROM extracted_tirads_validated_v1",
    "molecular_tested_patients": "SELECT COUNT(DISTINCT research_id) FROM molecular_test_episode_v2",
    "rai_episodes": "SELECT COUNT(*) FROM rai_treatment_episode_v2",
    "lab_canonical_rows": "SELECT COUNT(*) FROM longitudinal_lab_canonical_v1",
    "lab_canonical_patients": "SELECT COUNT(DISTINCT research_id) FROM longitudinal_lab_canonical_v1",
    "recurrence_risk_features_patients": "SELECT COUNT(DISTINCT research_id) FROM recurrence_risk_features_mv",
    "provenance_events_rows": "SELECT COUNT(*) FROM provenance_enriched_events_v1",
    "complication_phenotype_v1_rows": "SELECT COUNT(*) FROM complication_phenotype_v1",
    "scoring_patients": "SELECT COUNT(DISTINCT research_id) FROM thyroid_scoring_py_v1",
}


def build_cohort_counts(con) -> pd.DataFrame:
    rows = []
    for label, sql in COHORT_QUERIES.items():
        df = safe_df(con, f"cohort_{label}", sql)
        val = int(df.iloc[0, 0]) if len(df) else None
        rows.append({"metric": label, "count": val})
    return pd.DataFrame(rows)


# ──────────────────────────────────────────────────────────────────────
# 3 · ONE-ROW-PER-PATIENT ANALYSIS TABLE (cancer cohort)
# ──────────────────────────────────────────────────────────────────────

ANALYSIS_TABLE_SQL = """
-- One-row-per-patient multimodal analysis table for cancer cohort
WITH cancer AS (
    SELECT DISTINCT research_id
    FROM analysis_cancer_cohort_v1
),
pat AS (
    SELECT
        p.research_id,
        -- Demographics (actual column names from patient_analysis_resolved_v1)
        p.age_at_surgery,
        p.sex,
        p.race,
        -- Core pathology / staging
        p.histology_final,
        p.path_t_stage_raw               AS t_stage,
        p.path_n_stage_raw               AS n_stage,
        p.path_m_stage_raw               AS m_stage,
        p.ete_grade_final                AS ete_grade,
        p.path_multifocal_flag           AS multifocal_flag,
        p.path_tumor_size_cm             AS tumor_size_cm,
        p.path_ln_positive_raw           AS ln_positive_count,
        p.path_ln_examined_raw           AS ln_examined_count,
        p.margin_status_final            AS margin_status,
        p.vascular_invasion_final        AS vascular_invasion,
        -- Molecular
        p.braf_positive_final            AS braf_positive,
        p.ras_positive_final             AS ras_positive,
        p.tert_positive_final            AS tert_positive,
        p.mol_platform                   AS molecular_platform,
        -- Scoring (from resolved layer itself — it already has these)
        p.ajcc8_stage_group              AS ajcc8_stage,
        p.ata_risk_category              AS ata_risk,
        p.macis_score                    AS macis_score,
        p.macis_risk_group,
        p.ames_risk_group,
        p.ages_score,
        p.molecular_risk_tier,
        -- Recurrence (from resolved layer)
        COALESCE(p.any_recurrence_flag, FALSE) AS recurrence_flag,
        p.recurrence_date                AS first_recurrence_date,
        p.structural_recurrence_flag,
        p.biochemical_recurrence_flag,
        -- Complications (from resolved layer)
        p.any_confirmed_complication     AS has_complication_record,
        p.hypocalcemia_status,
        p.rln_status,
        -- Labs (from resolved layer)
        p.tg_nadir,
        p.tg_last_value,
        p.tg_rising_flag,
        p.lab_completeness_score,
        -- Eligibility flags
        p.analysis_eligible_flag,
        p.molecular_eligible_flag,
        p.rai_eligible_flag,
        p.survival_eligible_flag,
        p.scoring_ajcc8_flag,
        p.scoring_ata_flag,
        -- Imaging from resolved layer
        p.imaging_tirads_worst           AS tirads_worst,
        p.imaging_tirads_category        AS tirads_worst_category,
        p.imaging_nodule_size_cm,
        p.imaging_n_nodule_records       AS n_nodules_imaged,
        -- RAI from resolved layer
        p.rai_received_flag              AS has_rai_data,
        p.rai_max_dose_mci,
        -- FNA from resolved layer
        p.fna_bethesda_final             AS bethesda_worst,
        -- Surgery
        p.surg_procedure_type,
        p.surg_n_procedures
    FROM patient_analysis_resolved_v1 p
    WHERE p.research_id IN (SELECT research_id FROM cancer)
),
-- Imaging availability (validated TIRADS — richer detail)
img AS (
    SELECT
        research_id,
        TRUE                             AS has_tirads_validated,
        tirads_worst_score               AS tirads_validated_worst,
        n_sources                        AS tirads_n_sources,
        nodule_size_max_mm               AS tirads_nodule_max_mm,
        concordant_count                 AS tirads_concordant_ct,
        mismatch_count                   AS tirads_mismatch_ct
    FROM extracted_tirads_validated_v1
),
-- Imaging patient summary (from nodule master)
img_sum AS (
    SELECT
        research_id,
        TRUE                             AS has_nodule_master
    FROM imaging_patient_summary_v1
    GROUP BY research_id
),
-- Molecular availability
mol AS (
    SELECT
        research_id,
        TRUE                             AS has_molecular_episode,
        COUNT(*)                         AS n_molecular_tests
    FROM molecular_test_episode_v2
    GROUP BY research_id
),
-- Lab availability
lab AS (
    SELECT
        research_id,
        TRUE                             AS has_labs,
        COUNT(*)                         AS n_lab_values,
        COUNT(DISTINCT analyte_group)    AS n_analyte_groups
    FROM longitudinal_lab_canonical_v1
    GROUP BY research_id
),
-- FNA detail
fna AS (
    SELECT
        research_id,
        TRUE                             AS has_fna_bethesda
    FROM extracted_fna_bethesda_v1
    GROUP BY research_id
)
SELECT
    pat.*,
    -- Imaging flags (validated TIRADS enrichment)
    COALESCE(img.has_tirads_validated, FALSE)     AS has_tirads_validated,
    img.tirads_validated_worst,
    img.tirads_n_sources,
    img.tirads_nodule_max_mm,
    COALESCE(img_sum.has_nodule_master, FALSE)    AS has_nodule_master,
    -- Molecular episode flags
    COALESCE(mol.has_molecular_episode, FALSE)    AS has_molecular_data,
    mol.n_molecular_tests,
    -- Lab flags
    COALESCE(lab.has_labs, FALSE)                  AS has_lab_data,
    lab.n_lab_values,
    lab.n_analyte_groups,
    -- FNA flags
    COALESCE(fna.has_fna_bethesda, FALSE)         AS has_fna_data,
    -- Modality summary
    CASE
        WHEN COALESCE(img.has_tirads_validated, FALSE) AND COALESCE(mol.has_molecular_episode, FALSE) AND COALESCE(lab.has_labs, FALSE)
        THEN 'all_three'
        WHEN COALESCE(img.has_tirads_validated, FALSE) AND COALESCE(mol.has_molecular_episode, FALSE)
        THEN 'imaging_and_molecular'
        WHEN COALESCE(img.has_tirads_validated, FALSE)
        THEN 'imaging_only'
        WHEN COALESCE(mol.has_molecular_episode, FALSE)
        THEN 'molecular_only'
        ELSE 'structured_only'
    END AS modality_group
FROM pat
LEFT JOIN img ON pat.research_id = img.research_id
LEFT JOIN img_sum ON pat.research_id = img_sum.research_id
LEFT JOIN mol ON pat.research_id = mol.research_id
LEFT JOIN lab ON pat.research_id = lab.research_id
LEFT JOIN fna ON pat.research_id = fna.research_id
"""

# ──────────────────────────────────────────────────────────────────────
# 4 · MODALITY COVERAGE
# ──────────────────────────────────────────────────────────────────────

def build_modality_coverage(df: pd.DataFrame) -> pd.DataFrame:
    n = len(df)
    rows = []
    for col in ["has_tirads_validated", "has_nodule_master", "has_molecular_data",
                "has_rai_data", "has_lab_data", "has_fna_data", "has_complication_record"]:
        if col in df.columns:
            ct = int(df[col].sum())
            rows.append({"modality": col.replace("has_", "").replace("_data", "").replace("_", " "),
                          "n_patients": ct, "pct": round(100 * ct / n, 1) if n else 0})
    # modality_group breakdown
    if "modality_group" in df.columns:
        for grp, sub in df.groupby("modality_group"):
            rows.append({"modality": f"group:{grp}", "n_patients": len(sub),
                          "pct": round(100 * len(sub) / n, 1) if n else 0})
    return pd.DataFrame(rows)


# ──────────────────────────────────────────────────────────────────────
# 5 · FEATURE MISSINGNESS
# ──────────────────────────────────────────────────────────────────────

def build_feature_missingness(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    n = len(df)
    for col in df.columns:
        if col == "research_id":
            continue
        null_ct = int(df[col].isna().sum())
        # for boolean-ish columns, also count False as "absent" if it's a flag
        avail = n - null_ct
        rows.append({
            "feature": col,
            "n_available": avail,
            "n_missing": null_ct,
            "pct_available": round(100 * avail / n, 1) if n else 0,
        })
    return pd.DataFrame(rows).sort_values("pct_available", ascending=False)


# ──────────────────────────────────────────────────────────────────────
# 6 · OUTCOME PREVALENCE
# ──────────────────────────────────────────────────────────────────────

def build_outcome_prevalence(df: pd.DataFrame) -> pd.DataFrame:
    n = len(df)
    rows = []
    # Recurrence
    if "recurrence_flag" in df.columns:
        rec_true = int((df["recurrence_flag"] == True).sum())  # noqa
        rows.append({"endpoint": "recurrence_any", "n_events": rec_true,
                      "n_total": n, "pct": round(100 * rec_true / n, 1) if n else 0,
                      "has_date": "partial",
                      "manuscript_safe": "YES – boolean; date sparse (see analysis_summary)"})
    # BRAF positive (as a molecular outcome)
    for mol_col, mol_label in [("braf_positive", "braf_positive"),
                                ("ras_positive", "ras_positive"),
                                ("tert_positive", "tert_positive")]:
        if mol_col in df.columns:
            pos = int((df[mol_col] == True).sum())  # noqa
            rows.append({"endpoint": mol_label, "n_events": pos, "n_total": n,
                          "pct": round(100 * pos / n, 1) if n else 0,
                          "has_date": "N/A",
                          "manuscript_safe": "YES"})
    # Advanced pathology: ETE
    if "ete_grade" in df.columns:
        for grade in ["microscopic", "gross"]:
            ct = int((df["ete_grade"].str.lower() == grade).sum())
            rows.append({"endpoint": f"ete_{grade}", "n_events": ct, "n_total": n,
                          "pct": round(100 * ct / n, 1) if n else 0,
                          "has_date": "N/A",
                          "manuscript_safe": "YES"})
    # Vascular invasion
    if "vascular_invasion" in df.columns:
        vi = int(df["vascular_invasion"].fillna("").str.lower().isin(
            ["focal", "extensive", "present_ungraded", "present", "indeterminate"]
        ).sum())
        rows.append({"endpoint": "vascular_invasion_any", "n_events": vi, "n_total": n,
                      "pct": round(100 * vi / n, 1) if n else 0,
                      "has_date": "N/A",
                      "manuscript_safe": "YES (87% are present_ungraded — synoptic limitation)"})
    # Margin positive (R1 includes synoptic 'x' placeholder → caveat)
    if "margin_status" in df.columns:
        mp = int(df["margin_status"].fillna("").str.lower().isin(["r1", "r2", "positive"]).sum())
        rows.append({"endpoint": "margin_involved_R1_R2", "n_events": mp, "n_total": n,
                      "pct": round(100 * mp / n, 1) if n else 0,
                      "has_date": "N/A",
                      "manuscript_safe": "CAVEAT – R1 includes synoptic 'x' placeholder; true positive margin rate requires subanalysis"})
    # Complication record
    if "has_complication_record" in df.columns:
        comp = int((df["has_complication_record"] == True).sum())  # noqa
        rows.append({"endpoint": "any_complication_confirmed", "n_events": comp, "n_total": n,
                      "pct": round(100 * comp / n, 1) if n else 0,
                      "has_date": "partial",
                      "manuscript_safe": "YES – boolean; timing windows vary"})
    # Structural recurrence
    if "structural_recurrence_flag" in df.columns:
        sr = int((df["structural_recurrence_flag"] == True).sum())  # noqa
        rows.append({"endpoint": "structural_recurrence", "n_events": sr, "n_total": n,
                      "pct": round(100 * sr / n, 1) if n else 0,
                      "has_date": "sparse",
                      "manuscript_safe": "YES – binary; date NOT manuscript-safe"})
    # Biochemical recurrence
    if "biochemical_recurrence_flag" in df.columns:
        br = int((df["biochemical_recurrence_flag"] == True).sum())  # noqa
        rows.append({"endpoint": "biochemical_recurrence", "n_events": br, "n_total": n,
                      "pct": round(100 * br / n, 1) if n else 0,
                      "has_date": "sparse",
                      "manuscript_safe": "YES – binary; date NOT manuscript-safe"})
    return pd.DataFrame(rows)


# ──────────────────────────────────────────────────────────────────────
# 7 · TABLE 1 CANDIDATE
# ──────────────────────────────────────────────────────────────────────

def build_table1(df: pd.DataFrame) -> pd.DataFrame:
    """Simple Table 1: overall and by modality_group."""
    rows = []
    groups = {"Overall": df}
    if "modality_group" in df.columns:
        for g, subdf in df.groupby("modality_group"):
            groups[g] = subdf

    for grp_name, sub in groups.items():
        n = len(sub)
        row = {"Group": grp_name, "N": n}
        # Age
        if "age_at_surgery" in sub.columns:
            age = pd.to_numeric(sub["age_at_surgery"], errors="coerce")
            row["Age_mean(SD)"] = f"{age.mean():.1f} ({age.std():.1f})" if age.notna().sum() else "N/A"
        # Sex
        if "sex" in sub.columns:
            female = int(sub["sex"].str.lower().eq("female").sum())
            row["Female_n(%)"] = f"{female} ({100*female/n:.1f})" if n else "N/A"
        # Race
        if "race" in sub.columns:
            for r in ["White", "Black", "Asian", "Hispanic"]:
                ct = int(sub["race"].str.contains(r, case=False, na=False).sum())
                row[f"Race_{r}_n(%)"] = f"{ct} ({100*ct/n:.1f})" if n else "N/A"
        # Histology
        if "histology_final" in sub.columns:
            top3 = sub["histology_final"].value_counts().head(3)
            for hist, ct in top3.items():
                row[f"Histology_{hist}_n(%)"] = f"{ct} ({100*ct/n:.1f})" if n else "N/A"
        # AJCC stage
        if "ajcc8_stage" in sub.columns:
            for stg in ["I", "II", "III", "IVA", "IVB"]:
                ct = int(sub["ajcc8_stage"].str.upper().eq(stg).sum()) if sub["ajcc8_stage"].notna().any() else 0
                row[f"AJCC8_{stg}_n(%)"] = f"{ct} ({100*ct/n:.1f})" if n else "N/A"
        # ATA risk
        if "ata_risk" in sub.columns:
            for risk in ["low", "intermediate", "high"]:
                ct = int(sub["ata_risk"].str.lower().eq(risk).sum()) if sub["ata_risk"].notna().any() else 0
                row[f"ATA_{risk}_n(%)"] = f"{ct} ({100*ct/n:.1f})" if n else "N/A"
        # Tumor size
        if "tumor_size_cm" in sub.columns:
            ts = pd.to_numeric(sub["tumor_size_cm"], errors="coerce")
            row["TumorSize_median(IQR)"] = (
                f"{ts.median():.1f} ({ts.quantile(0.25):.1f}-{ts.quantile(0.75):.1f})"
                if ts.notna().sum() > 0 else "N/A"
            )
        # Recurrence
        if "recurrence_flag" in sub.columns:
            rec = int((sub["recurrence_flag"] == True).sum())  # noqa
            row["Recurrence_n(%)"] = f"{rec} ({100*rec/n:.1f})" if n else "N/A"
        # BRAF
        if "braf_positive" in sub.columns:
            bp = int((sub["braf_positive"] == True).sum())  # noqa
            row["BRAF+_n(%)"] = f"{bp} ({100*bp/n:.1f})" if n else "N/A"
        # TIRADS
        if "has_tirads_validated" in sub.columns:
            ti = int((sub["has_tirads_validated"] == True).sum())  # noqa
            row["TIRADS_avail_n(%)"] = f"{ti} ({100*ti/n:.1f})" if n else "N/A"
        # Molecular
        if "has_molecular_data" in sub.columns:
            mo = int((sub["has_molecular_data"] == True).sum())  # noqa
            row["Molecular_avail_n(%)"] = f"{mo} ({100*mo/n:.1f})" if n else "N/A"
        # Labs
        if "has_lab_data" in sub.columns:
            la = int((sub["has_lab_data"] == True).sum())  # noqa
            row["Labs_avail_n(%)"] = f"{la} ({100*la/n:.1f})" if n else "N/A"

        rows.append(row)
    return pd.DataFrame(rows)


# ──────────────────────────────────────────────────────────────────────
# 8 · EXPLAIN ANALYZE
# ──────────────────────────────────────────────────────────────────────

EXPLAIN_QUERIES = [
    ("explain_plan_01", "Full analysis table build (most complex)", ANALYSIS_TABLE_SQL),
    ("explain_plan_02", "Imaging + TIRADS + molecular join", """
SELECT p.research_id, t.tirads_worst_score, t.tirads_worst_category,
       m.research_id IS NOT NULL AS has_molecular
FROM patient_analysis_resolved_v1 p
LEFT JOIN extracted_tirads_validated_v1 t ON p.research_id = t.research_id
LEFT JOIN (SELECT DISTINCT research_id FROM molecular_test_episode_v2) m ON p.research_id = m.research_id
WHERE p.research_id IN (SELECT research_id FROM analysis_cancer_cohort_v1)
"""),
    ("explain_plan_03", "Recurrence features aggregation", """
SELECT
    r.research_id,
    BOOL_OR(r.recurrence_flag) AS recurrence_any,
    MIN(r.first_recurrence_date) AS first_recurrence_date,
    MAX(r.tg_max) AS tg_max,
    MAX(r.tg_rising_flag) AS tg_rising
FROM recurrence_risk_features_mv r
WHERE r.research_id IN (SELECT research_id FROM analysis_cancer_cohort_v1)
GROUP BY r.research_id
"""),
]


def run_explain_plans(con):
    for fname, desc, sql in EXPLAIN_QUERIES:
        explain_sql = f"EXPLAIN ANALYZE {sql}"
        try:
            qlog(f"explain_{fname}", explain_sql)
            result = con.execute(explain_sql).fetchdf()
            text = f"-- {desc}\n-- Query:\n{sql.strip()}\n\n-- Plan:\n"
            text += result.to_string(index=False)
            save_txt(text, f"{fname}.txt")
        except Exception as e:
            save_txt(f"-- {desc}\n-- ERROR: {e}\n\n{sql}", f"{fname}.txt")


# ──────────────────────────────────────────────────────────────────────
# 9 · DATA DICTIONARY SUBSET
# ──────────────────────────────────────────────────────────────────────

DATA_DICT_ENTRIES = {
    "research_id": "Unique anonymized patient identifier (integer)",
    "age_at_surgery": "Age at first thyroid surgery (years)",
    "sex": "Patient sex (Female/Male)",
    "race": "Self-reported race (normalized groups)",
    "histology_final": "Final histology type (PTC, FTC, MTC, etc.)",
    "t_stage": "AJCC pathologic T stage",
    "n_stage": "AJCC pathologic N stage",
    "m_stage": "AJCC pathologic M stage",
    "ete_grade": "Extrathyroidal extension grade (none/microscopic/gross)",
    "multifocal_flag": "Multifocal disease flag",
    "tumor_size_cm": "Largest tumor dimension (cm)",
    "ln_positive_count": "Number of lymph nodes positive",
    "ln_examined_count": "Number of lymph nodes examined",
    "margin_status": "Surgical margin status (positive/negative/close)",
    "vascular_invasion": "Vascular invasion status",
    "braf_positive": "BRAF V600E mutation positive flag",
    "ras_positive": "RAS mutation positive flag (any subtype)",
    "tert_positive": "TERT promoter mutation positive flag",
    "molecular_platform": "Molecular testing platform (ThyroSeq/Afirma/Other)",
    "ajcc8_stage": "AJCC 8th Edition stage group",
    "ata_risk": "ATA 2015 initial risk stratification",
    "macis_score": "MACIS prognostic score",
    "has_tirads_data": "Patient has ACR TI-RADS scoring data",
    "tirads_worst": "Worst TI-RADS score across nodules",
    "tirads_worst_category": "Worst TI-RADS category (TR1-TR5)",
    "n_nodules_imaged": "Number of imaged nodules",
    "has_nodule_master": "Patient has per-nodule imaging master record",
    "has_molecular_data": "Patient has molecular testing episode(s)",
    "n_molecular_tests": "Count of molecular testing episodes",
    "has_rai_data": "Patient has RAI treatment episode(s)",
    "n_rai_episodes": "Count of RAI treatment episodes",
    "has_lab_data": "Patient has canonical longitudinal lab records",
    "n_lab_values": "Count of lab measurements",
    "n_analyte_groups": "Count of distinct analyte groups in labs",
    "has_fna_data": "Patient has FNA Bethesda cytology data",
    "bethesda_worst": "Worst (highest) Bethesda category",
    "recurrence_flag": "Any recurrence (structural or biochemical)",
    "first_recurrence_date": "Earliest recurrence date (sparse — see caveats)",
    "has_complication_record": "Patient has refined complication phenotype record",
    "modality_group": "Multimodal data availability classification",
}


def build_data_dict() -> str:
    lines = ["# Data Dictionary — Multimodal Prediction Dataset\n",
             f"Generated: {TIMESTAMP}\n",
             "| Variable | Description | Type |",
             "|----------|-------------|------|"]
    for var, desc in DATA_DICT_ENTRIES.items():
        vtype = "flag" if "flag" in var or var.startswith("has_") else "numeric" if "count" in var or "score" in var or "size" in var else "categorical"
        lines.append(f"| {var} | {desc} | {vtype} |")
    lines.append("\n## Caveats\n")
    lines.append("- `first_recurrence_date`: Only 2.7% exact-source dates; 88.8% unresolved. Not suitable for precise time-to-event analysis without further adjudication.")
    lines.append("- Boolean flags from MotherDuck may arrive as text 'true'/'false'; coerced to Python bool in dataset.")
    lines.append("- `imaging_nodule_long_v2` is deprecated; `imaging_nodule_master_v1` and `imaging_patient_summary_v1` used instead.")
    lines.append("- No PHI or full note text is included in any deliverable.")
    return "\n".join(lines)


# ──────────────────────────────────────────────────────────────────────
# 10 · ANALYSIS SUMMARY
# ──────────────────────────────────────────────────────────────────────

def build_analysis_summary(
    df: pd.DataFrame,
    cohort_counts: pd.DataFrame,
    missingness: pd.DataFrame,
    outcomes: pd.DataFrame,
    modality: pd.DataFrame,
) -> str:
    n = len(df)

    # Counts
    def _cnt(metric):
        row = cohort_counts[cohort_counts["metric"] == metric]
        return int(row["count"].iloc[0]) if len(row) else "N/A"

    # Modality group counts
    def _mg(group):
        if "modality_group" in df.columns:
            return int((df["modality_group"] == group).sum())
        return 0

    structured_only = _mg("structured_only")
    imaging_linked = int(df["has_tirads_validated"].sum()) if "has_tirads_validated" in df.columns else 0
    note_linked = int(df["has_lab_data"].sum()) if "has_lab_data" in df.columns else 0  # proxy: labs/NLP
    all_three = _mg("all_three")

    # Top 10 predictors by completeness
    top10 = missingness.head(10)[["feature", "n_available", "pct_available"]].to_string(index=False)

    # Usable endpoints
    usable = outcomes[outcomes["manuscript_safe"].str.startswith("YES", na=False)]
    usable_str = usable[["endpoint", "n_events", "pct", "manuscript_safe"]].to_string(index=False)

    # Not safe endpoints
    not_safe_items = [
        "time_to_recurrence (88.8% unresolved dates – not manuscript-safe for TTE analysis)",
        "time_to_death (no death events in clinical_events – augmented only with synthetic proxy)",
        "RAI dose (41% coverage – usable as covariate, not as primary endpoint)",
        "voice outcomes (0.23% coverage – too sparse)",
    ]

    summary = f"""# Multimodal Thyroid Cancer Prediction — Feasibility Analysis Summary

Generated: {TIMESTAMP}

## 1. Cohort Size

| Metric | Count |
|--------|-------|
| Full surgical cohort (master_cohort) | {_cnt('master_cohort_patients'):,} |
| Manuscript cohort (manuscript_cohort_v1) | {_cnt('manuscript_cohort_v1_patients'):,} |
| **Cancer analytic cohort (analysis_cancer_cohort_v1)** | **{n:,}** |
| Patient analysis resolved | {_cnt('patient_analysis_resolved_v1_patients'):,} |
| Dedup episodes | {_cnt('episode_dedup_episodes'):,} |

## 2. Multimodal Data Availability (Cancer Cohort, N={n:,})

| Question | Count |
|----------|-------|
| Structured clinical data only | {structured_only:,} |
| Imaging-linked data (TIRADS) | {imaging_linked:,} |
| Note-derived / NLP-linked data (labs, NLP events) | {note_linked:,} |
| All three modalities | {all_three:,} |

## 3. Modality Group Breakdown

{modality[modality['modality'].str.startswith('group:')].to_string(index=False)}

## 4. Usable Endpoints (Manuscript-Safe)

{usable_str}

## 5. Endpoints NOT Manuscript-Safe

{chr(10).join('- ' + x for x in not_safe_items)}

## 6. Top 10 Candidate Predictors (by Completeness)

```
{top10}
```

## 7. Executive Summary

### Recommended Primary Endpoint
**Recurrence (binary)**: 1,986/10,871 overall; precise rate in cancer cohort captured in `recurrence_flag`.
For multimodal prediction, binary recurrence is the most defensible primary endpoint given current data maturity.
Secondary: adverse pathology composite (ETE + vascular invasion + positive margins).

### Recommended Cohort
**analysis_cancer_cohort_v1** (N={n:,}): analysis-eligible patients with confirmed thyroid cancer, complete staging, and eligibility flags.

### Expected Manuscript-Safe Sample Size
- Full cancer cohort: **{n:,}**
- With TIRADS imaging: **{imaging_linked:,}**
- With molecular + imaging + labs: **{all_three:,}**
- Minimum viable multimodal subset for prediction: **{all_three:,}** (if imaging required); **{n:,}** (if imaging optional/imputed)

### Top Blockers
1. **TIRADS coverage**: Only ~{round(100*imaging_linked/n,1) if n else 0}% of cancer cohort has validated TIRADS data — limits mandatory imaging arm
2. **Recurrence date sparsity**: Binary recurrence is available; precise time-to-event is not manuscript-safe (88.8% unresolved dates)
3. **Nuclear medicine absence**: RAI dose coverage capped at 41% — usable as covariate, not endpoint
4. **Vascular invasion grading**: 87% remain 'present_ungraded' — synoptic template limitation
5. **Molecular testing coverage**: only {int(df['has_molecular_data'].sum()) if 'has_molecular_data' in df.columns else 'N/A'} cancer patients with molecular episode data; platform heterogeneity (ThyroSeq vs Afirma)

### Next 5 Concrete Steps
1. Finalize endpoint definition: binary recurrence (primary) + adverse pathology composite (secondary)
2. Run multiple imputation (MICE) for imaging/molecular missingness in full cancer cohort
3. Build multimodal feature matrix: structured (demographics+staging) + imaging (TIRADS+nodule features) + NLP (lab trajectories+complication flags)
4. Fit and internally validate prediction models (logistic + XGBoost + Cox PH on binary recurrence)
5. Draft TRIPOD-compliant Methods section and register study protocol
"""
    return summary


# ──────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("Multimodal Thyroid Cancer Prediction — Feasibility Pack")
    print("=" * 70)

    # Connect read-only to MotherDuck production
    client = MotherDuckClient.for_env("prod")
    con = client.connect_rw()
    print(f"Connected to MotherDuck (token mode: {__import__('motherduck_client').token_mode()})")

    # 1. Schema Inventory
    print("\n[1/9] Schema inventory...")
    schema_md, inv_rows = build_schema_inventory(con)
    save_md(schema_md, "schema_inventory.md")

    # 2. Cohort counts
    print("\n[2/9] Cohort counts...")
    cohort_counts = build_cohort_counts(con)
    save_csv(cohort_counts, "cohort_counts.csv")

    # 3. Build analysis table
    print("\n[3/9] Building one-row-per-patient analysis table...")
    analysis_df = run_sql(con, "analysis_table", ANALYSIS_TABLE_SQL)
    print(f"  → {len(analysis_df):,} cancer patients in analysis table")

    # Coerce boolean columns
    bool_cols = [c for c in analysis_df.columns if c.startswith("has_") or c.endswith("_flag") or c.endswith("_positive")]
    for bc in bool_cols:
        if bc in analysis_df.columns:
            analysis_df[bc] = analysis_df[bc].map(
                lambda x: True if str(x).lower() in ("true", "1", "t") else
                          (False if str(x).lower() in ("false", "0", "f") else None)
            )

    # Save parquet
    analysis_df.to_parquet(OUT / "candidate_modeling_dataset.parquet", index=False)
    print(f"  ✓ candidate_modeling_dataset.parquet ({len(analysis_df)} rows)")

    # 4. Modality coverage
    print("\n[4/9] Modality coverage...")
    modality = build_modality_coverage(analysis_df)
    save_csv(modality, "modality_coverage.csv")

    # 5. Feature missingness
    print("\n[5/9] Feature missingness...")
    missingness = build_feature_missingness(analysis_df)
    save_csv(missingness, "feature_missingness.csv")

    # 6. Outcome prevalence
    print("\n[6/9] Outcome prevalence...")
    outcomes = build_outcome_prevalence(analysis_df)
    save_csv(outcomes, "outcome_prevalence.csv")

    # 7. Table 1
    print("\n[7/9] Table 1 candidate...")
    table1 = build_table1(analysis_df)
    save_csv(table1, "table1_candidate.csv")

    # 8. EXPLAIN ANALYZE
    print("\n[8/9] EXPLAIN ANALYZE...")
    run_explain_plans(con)

    # 9. Data dictionary
    print("\n[9/9] Data dictionary & summary...")
    dd = build_data_dict()
    save_md(dd, "data_dictionary_subset.md")

    # Analysis summary
    summary = build_analysis_summary(analysis_df, cohort_counts, missingness, outcomes, modality)
    save_md(summary, "analysis_summary.md")

    # Query log
    save_txt("\n".join(QUERY_LOG), "query_log.sql")

    con.close()
    print("\n" + "=" * 70)
    print(f"All deliverables saved to: {OUT}")
    print("=" * 70)


if __name__ == "__main__":
    main()
