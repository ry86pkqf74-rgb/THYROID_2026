#!/usr/bin/env python3
"""
Multimodal Recurrence Prediction — V2 Re-Run (Post-mig_086, Auditable)
=======================================================================
Addresses v1 methodological concerns:
  1. No held-out test set (v1 used CV-only)
  2. Feature leakage: tg_rising_flag/tg_nadir/tg_last_value are part of
     the biochemical recurrence *definition* — including them inflates AUC
  3. Source: v1 used local parquet; v2 uses pub_canonical (MotherDuck)

Design decisions:
  - Held-out test split: stratified 70/15/15 (train/val/test) by outcome
  - Split assignments saved transiently to /tmp/ (NOT committed; research_ids
    are pseudo-IDs but kept out of tracked files per governance)
  - Feature sets respecified to match task definition (raw clinicopathological,
    not derived risk scores) with explicit leakage annotations
  - Reports: train AUC, val AUC, and HELD-OUT TEST AUC for all 6 models
  - If test AUC > 0.90: investigates remaining leakage sources

Cohort: pub_canonical (thyroid_canonical_publication_v1_0, post-mig_086)
  - Malignant thyroid pathology (is_malignant=TRUE)
  - ≥6 months follow-up (followup_years >= 0.5)
  - At least one of: TIRADS scored, FNA Bethesda, molecular test
  - Outcome: any_recurrence_flag (non-NULL)

PHI: No row-level data written to markdown or committed files.
     Aggregate counts and metrics only in all outputs.
"""
from __future__ import annotations

import sys
import warnings
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.metrics import roc_auc_score, brier_score_loss, average_precision_score
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

SEED = 42
np.random.seed(SEED)

STUDY_DIR = Path(__file__).resolve().parent
OUT_DIR = STUDY_DIR / "model_results_v2_post_mig_086"
OUT_DIR.mkdir(parents=True, exist_ok=True)
SPLIT_FILE = Path("/tmp/multimodal_v2_splits.parquet")   # transient — not tracked

OUTCOME = "recurrence_flag"

# ── Feature sets (v2 — no leakage, per task definition) ──────────────────────
# Set A: raw clinical/pathology (no derived scoring systems, no postop labs)
FEATURES_A = [
    "age_at_surgery",
    "sex_binary",           # 0=female, 1=male
    "histology_encoded",    # PTC=0, FTC=1, MTC=2, HCC=3, other=4
    "ete_encoded",          # none=0, microscopic=1, gross=2, present_ungraded=3
    "tumor_size_cm",        # from CPM (GREATEST workaround for 80-patient bug)
    "lvi_any_present_path", # boolean
    "margin_involved_any",  # boolean
    "multifocal_flag_path", # boolean
    "n_tumors_path",        # int
    "braf_positive",        # from braf_positive_final in CPM
    "t_stage_encoded",      # T1a=0, T1b=1, T2=2, T3a=3, T3b=4, T4a=5, T4b=6
    "n_stage_encoded",      # N0=0, N1a=1, N1b=2, NX=3
    "m_stage_encoded",      # M0=0, M1=1, MX=2
]

# Set B: Set A + imaging features (TIRADS, ultrasound)
FEATURES_B_EXTRA = [
    "tirads_best_score",    # 1-5 numeric
    "tirads_worst_score",   # 1-5 numeric
    "has_tirads",           # boolean: any TIRADS validated
    "has_acr_recalc",       # boolean: ACR recalculation available
    "nodule_size_max_mm",   # max nodule dimension from TIRADS
    "n_nodule_records",     # count of nodule records
]

# Set C: Set B + notes-derived (preoperative molecular + FNA)
# EXCLUDED (leakage): tg_rising_flag, tg_nadir, tg_last_value
#   Reason: biochemical recurrence = rising Tg > 1.0 ng/mL — these ARE the outcome
# EXCLUDED: ata_initial_risk, macis_score, ames_risk_group, ages_score
#   Reason: derived scoring systems calibrated to predict recurrence;
#            including them creates circular prediction, not independent validation
FEATURES_C_EXTRA = [
    "ras_positive",         # from molecular_test_episode_v2 (BOOL_OR)
    "tert_positive",        # from molecular_test_episode_v2
    "any_high_risk_marker", # high_risk_marker_flag BOOL_OR
    "has_molecular_data",   # boolean: any molecular test
    "n_molecular_tests",    # count of distinct molecular episodes
    "platform_thyroseq",    # boolean: ThyroSeq platform used
    "platform_afirma",      # boolean: Afirma platform used
    "bethesda_final",       # 1-6 numeric FNA category (per-patient best)
    "worst_bethesda_num",   # worst FNA category (most suspicious)
    "has_fna_data",         # boolean: any FNA Bethesda
    "n_fna_episodes",       # count of FNA episodes
]

# LEAKAGE ANNOTATION (for reporting)
LEAKAGE_EXCLUDED_V1 = {
    "tg_rising_flag": "POSTOP_LAB — part of biochemical recurrence definition (Tg > 1.0 ng/mL without structural disease)",
    "tg_nadir": "POSTOP_LAB — derived from same Tg surveillance window as recurrence outcome",
    "tg_last_value": "POSTOP_LAB — postoperative measurement, not preoperative predictor",
    "lab_completeness_score": "POSTOP_LAB — measures completeness of postoperative Tg surveillance",
    "n_lab_values": "POSTOP_LAB — count of postoperative lab measurements",
    "n_analyte_groups": "POSTOP_LAB — postoperative analyte diversity",
    "ata_risk": "DERIVED_SCORE — ATA risk is calibrated to predict recurrence; circular",
    "macis_score": "DERIVED_SCORE — MACIS calibrated to predict recurrence; circular",
    "ames_risk_group": "DERIVED_SCORE — AMES calibrated to predict mortality/recurrence; circular",
    "ages_score": "DERIVED_SCORE — AGES calibrated to predict mortality; quasi-circular",
    "ajcc8_stage": "DERIVED_SCORE — AJCC8 stage is a composite that summarizes T/N/M; T+N+M included directly in Set A",
}


def get_motherduck_connection():
    """Connect to pub_canonical via motherduck_client token resolution."""
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
    import motherduck_client as mdc
    import duckdb
    token = mdc.get_token()
    if not token:
        raise RuntimeError("No MotherDuck token found — check motherduck.local.toml")
    print(f"  MD token: SET (len={len(token)})")
    con = duckdb.connect(f"md:?motherduck_token={token}")
    con.execute("USE thyroid_canonical_publication_v1_0")
    return con


def create_cohort_view(con) -> None:
    """Create manuscript_workspace.cohort_multimodal_recurrence_v1 in pub_canonical."""
    sql = """
CREATE OR REPLACE VIEW manuscript_workspace.cohort_multimodal_recurrence_v1 AS
SELECT DISTINCT
    CAST(cpm.research_id AS VARCHAR) AS research_id,
    cpm.any_recurrence_flag AS recurrence_flag,
    cpm.followup_years,
    CAST(cpm.surg_first_date AS DATE) AS surg_first_date,
    -- Eligibility flags for multimodal criteria
    (t.rid IS NOT NULL) AS has_tirads,
    (f.rid IS NOT NULL) AS has_fna_bethesda,
    (m.rid IS NOT NULL) AS has_molecular
FROM main.canonical_patient_master cpm
LEFT JOIN (
    SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid
    FROM readonly_share.extracted_tirads_validated_v1
) t ON t.rid = CAST(cpm.research_id AS VARCHAR)
LEFT JOIN (
    SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid
    FROM readonly_share.extracted_fna_bethesda_v1
) f ON f.rid = CAST(cpm.research_id AS VARCHAR)
LEFT JOIN (
    SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid
    FROM readonly_share.molecular_test_episode_v2
) m ON m.rid = CAST(cpm.research_id AS VARCHAR)
WHERE cpm.is_malignant = TRUE
  AND cpm.followup_years >= 0.5
  AND cpm.any_recurrence_flag IS NOT NULL
  AND (t.rid IS NOT NULL OR f.rid IS NOT NULL OR m.rid IS NOT NULL)
"""
    con.execute(sql)
    n = con.execute("SELECT COUNT(*) FROM manuscript_workspace.cohort_multimodal_recurrence_v1").fetchone()[0]
    n_recur = con.execute(
        "SELECT SUM(CASE WHEN recurrence_flag THEN 1 ELSE 0 END) FROM manuscript_workspace.cohort_multimodal_recurrence_v1"
    ).fetchone()[0]
    print(f"  Cohort view created: N={n}, recurrence={n_recur} ({100*n_recur/n:.1f}%)")
    return n, n_recur


def pull_features(con) -> pd.DataFrame:
    """Pull all features for the cohort from pub_canonical into pandas DataFrame."""
    sql = """
WITH cohort AS (
    SELECT research_id, recurrence_flag, followup_years, surg_first_date
    FROM manuscript_workspace.cohort_multimodal_recurrence_v1
),
-- Aggregate molecular to per-patient
mol_agg AS (
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        BOOL_OR(braf_flag) AS mol_braf_positive,
        BOOL_OR(ras_flag) AS ras_positive,
        BOOL_OR(tert_flag) AS tert_positive,
        BOOL_OR(high_risk_marker_flag) AS any_high_risk_marker,
        COUNT(DISTINCT molecular_episode_id) AS n_molecular_tests,
        BOOL_OR(LOWER(COALESCE(platform,''))='thyroseq' OR platform LIKE '%ThyroSeq%' OR platform LIKE '%thyroseq%') AS platform_thyroseq,
        BOOL_OR(LOWER(COALESCE(platform,'')) LIKE '%afirma%') AS platform_afirma
    FROM readonly_share.molecular_test_episode_v2
    GROUP BY research_id
),
-- TIRADS per patient (already per-patient)
tirads AS (
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        tirads_best_score,
        tirads_worst_score,
        has_acr_recalculation AS has_acr_recalc,
        nodule_size_max_mm,
        n_nodule_records
    FROM readonly_share.extracted_tirads_validated_v1
),
-- FNA Bethesda (already per-patient)
fna AS (
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        bethesda_final,
        worst_bethesda_num,
        n_fna_episodes
    FROM readonly_share.extracted_fna_bethesda_v1
)
SELECT
    c.research_id,
    CAST(c.recurrence_flag AS INTEGER) AS recurrence_flag,
    YEAR(c.surg_first_date) AS surgery_year,
    -- Set A features
    cpm.age_at_surgery,
    CASE WHEN LOWER(COALESCE(cpm.sex,''))='male' THEN 1 ELSE 0 END AS sex_binary,
    CASE
        WHEN LOWER(COALESCE(cpm.histology_final,'')) LIKE '%ptc%' OR LOWER(COALESCE(cpm.histology_final,''))='ptc' THEN 0
        WHEN LOWER(COALESCE(cpm.histology_final,'')) LIKE '%ftc%' OR LOWER(COALESCE(cpm.histology_final,''))='ftc' THEN 1
        WHEN LOWER(COALESCE(cpm.histology_final,'')) LIKE '%mtc%' OR LOWER(COALESCE(cpm.histology_final,''))='mtc' THEN 2
        WHEN LOWER(COALESCE(cpm.histology_final,'')) LIKE '%hcc%' OR LOWER(COALESCE(cpm.histology_final,'')) LIKE '%hurthle%' OR LOWER(COALESCE(cpm.histology_final,'')) LIKE '%onco%' THEN 3
        ELSE 4
    END AS histology_encoded,
    CASE
        WHEN LOWER(COALESCE(cpm.ete_grade_final_v2,'')) IN ('none','absent','negative','no','no ete') THEN 0
        WHEN LOWER(COALESCE(cpm.ete_grade_final_v2,'')) IN ('microscopic','minimal','focal','micro') THEN 1
        WHEN LOWER(COALESCE(cpm.ete_grade_final_v2,'')) IN ('gross','extensive','major','gross ete') THEN 2
        WHEN cpm.ete_grade_final_v2 IS NOT NULL THEN 3
        ELSE NULL
    END AS ete_encoded,
    GREATEST(COALESCE(cpm.tumor_size_cm_max, 0), COALESCE(cpm.tumor_size_cm_dominant, 0)) AS tumor_size_cm,
    CAST(COALESCE(cpm.lvi_any_present_path, FALSE) AS INTEGER) AS lvi_any_present_path,
    CAST(COALESCE(cpm.margin_involved_any, FALSE) AS INTEGER) AS margin_involved_any,
    CAST(COALESCE(cpm.multifocal_flag_path, FALSE) AS INTEGER) AS multifocal_flag_path,
    COALESCE(cpm.n_tumors_path, 1) AS n_tumors_path,
    CAST(COALESCE(cpm.braf_positive_final, FALSE) AS INTEGER) AS braf_positive,
    CASE
        WHEN cpm.ajcc8_t_stage IN ('T1a','T1A') THEN 0
        WHEN cpm.ajcc8_t_stage IN ('T1b','T1B') THEN 1
        WHEN cpm.ajcc8_t_stage IN ('T2','T2') THEN 2
        WHEN cpm.ajcc8_t_stage IN ('T3a','T3A') THEN 3
        WHEN cpm.ajcc8_t_stage IN ('T3b','T3B') THEN 4
        WHEN cpm.ajcc8_t_stage IN ('T4a','T4A') THEN 5
        WHEN cpm.ajcc8_t_stage IN ('T4b','T4B') THEN 6
        ELSE NULL
    END AS t_stage_encoded,
    CASE
        WHEN cpm.ajcc8_n_stage IN ('N0') THEN 0
        WHEN cpm.ajcc8_n_stage IN ('N1a','N1A') THEN 1
        WHEN cpm.ajcc8_n_stage IN ('N1b','N1B','N1') THEN 2
        WHEN cpm.ajcc8_n_stage IN ('NX','Nx') THEN 3
        ELSE NULL
    END AS n_stage_encoded,
    CASE
        WHEN cpm.ajcc8_m_stage IN ('M0') THEN 0
        WHEN cpm.ajcc8_m_stage IN ('M1') THEN 1
        WHEN cpm.ajcc8_m_stage IN ('MX','Mx') THEN 2
        ELSE NULL
    END AS m_stage_encoded,
    -- Set B features (imaging)
    COALESCE(t.tirads_best_score, NULL) AS tirads_best_score,
    COALESCE(t.tirads_worst_score, NULL) AS tirads_worst_score,
    CASE WHEN t.research_id IS NOT NULL THEN 1 ELSE 0 END AS has_tirads,
    CAST(COALESCE(t.has_acr_recalc, FALSE) AS INTEGER) AS has_acr_recalc,
    t.nodule_size_max_mm,
    COALESCE(t.n_nodule_records, 0) AS n_nodule_records,
    -- Set C features (molecular + FNA, preoperative, no postop Tg)
    CAST(COALESCE(mol.ras_positive, FALSE) AS INTEGER) AS ras_positive,
    CAST(COALESCE(mol.tert_positive, FALSE) AS INTEGER) AS tert_positive,
    CAST(COALESCE(mol.any_high_risk_marker, FALSE) AS INTEGER) AS any_high_risk_marker,
    CASE WHEN mol.research_id IS NOT NULL THEN 1 ELSE 0 END AS has_molecular_data,
    COALESCE(mol.n_molecular_tests, 0) AS n_molecular_tests,
    CAST(COALESCE(mol.platform_thyroseq, FALSE) AS INTEGER) AS platform_thyroseq,
    CAST(COALESCE(mol.platform_afirma, FALSE) AS INTEGER) AS platform_afirma,
    COALESCE(fna.bethesda_final, NULL) AS bethesda_final,
    COALESCE(fna.worst_bethesda_num, NULL) AS worst_bethesda_num,
    CASE WHEN fna.research_id IS NOT NULL THEN 1 ELSE 0 END AS has_fna_data,
    COALESCE(fna.n_fna_episodes, 0) AS n_fna_episodes
FROM cohort c
JOIN main.canonical_patient_master cpm ON CAST(cpm.research_id AS VARCHAR) = c.research_id
LEFT JOIN mol_agg mol ON mol.research_id = c.research_id
LEFT JOIN tirads t ON t.research_id = c.research_id
LEFT JOIN fna ON fna.research_id = c.research_id
"""
    df = con.execute(sql).df()
    print(f"  Feature matrix shape: {df.shape}")
    print(f"  Outcome: {df[OUTCOME].mean()*100:.1f}% recurrence ({df[OUTCOME].sum()}/{len(df)})")
    return df


def create_splits(df: pd.DataFrame) -> pd.DataFrame:
    """
    Stratified 70/15/15 train/val/test split.
    Split assignments saved to /tmp/ (transient, not tracked).
    Returns df with 'split' column added.
    """
    y = df[OUTCOME].values
    idx = np.arange(len(df))

    # First split: 70% train vs 30% (val+test)
    idx_train, idx_tmp, _, y_tmp = train_test_split(
        idx, y, test_size=0.30, stratify=y, random_state=SEED
    )
    # Second split: 50/50 of remaining → 15% val, 15% test
    idx_val, idx_test = train_test_split(
        idx_tmp, test_size=0.50, stratify=y_tmp, random_state=SEED
    )

    df = df.copy()
    df["split"] = "train"
    df.iloc[idx_val, df.columns.get_loc("split")] = "val"
    df.iloc[idx_test, df.columns.get_loc("split")] = "test"

    # Save split assignments (research_id + split label) to /tmp/
    split_df = df[["research_id", "split", "surgery_year", OUTCOME]].copy()
    split_df.to_parquet(SPLIT_FILE, index=False)
    print(f"  Split assignments saved transiently to {SPLIT_FILE}")

    for s in ["train", "val", "test"]:
        sub = df[df["split"] == s]
        print(f"    {s}: N={len(sub)}, recurrence={sub[OUTCOME].mean()*100:.1f}%")

    return df


def prepare_features(df: pd.DataFrame, feature_cols: list[str]) -> pd.DataFrame:
    """Select, encode, impute features — return numeric float64 DataFrame."""
    avail = [c for c in feature_cols if c in df.columns]
    X = df[avail].copy()
    for c in X.columns:
        if X[c].dtype == "bool" or str(X[c].dtype) == "boolean":
            X[c] = X[c].astype("float64")
        elif X[c].dtype == "object":
            le = LabelEncoder()
            mask = X[c].notna()
            if mask.sum() > 0:
                X.loc[mask, c] = le.fit_transform(X.loc[mask, c].astype(str))
            X[c] = pd.to_numeric(X[c], errors="coerce")
    for c in X.columns:
        if str(X[c].dtype).startswith("Int") or str(X[c].dtype).startswith("UInt"):
            X[c] = X[c].astype("float64")
    return X.astype("float64")


def build_pipeline(model_name: str) -> Pipeline:
    """Imputer + Scaler + Model pipeline."""
    if model_name == "logistic":
        mdl = LogisticRegression(
            max_iter=2000, solver="saga", penalty="l2", C=1.0, random_state=SEED
        )
    elif model_name == "xgboost":
        mdl = GradientBoostingClassifier(
            n_estimators=300, max_depth=4, learning_rate=0.05,
            subsample=0.8, random_state=SEED,
        )
    else:
        raise ValueError(f"Unknown model: {model_name}")
    return Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler()),
        ("model", mdl),
    ])


def train_and_evaluate(
    df: pd.DataFrame,
    feature_set_label: str,
    feature_cols: list[str],
    model_name: str,
) -> dict:
    """Train on train split, tune on val split, evaluate on held-out test."""
    feat_avail = [c for c in feature_cols if c in df.columns]

    train_df = df[df["split"] == "train"]
    val_df = df[df["split"] == "val"]
    test_df = df[df["split"] == "test"]

    X_train = prepare_features(train_df, feat_avail)
    y_train = train_df[OUTCOME].values.astype(int)
    X_val = prepare_features(val_df, feat_avail)
    y_val = val_df[OUTCOME].values.astype(int)
    X_test = prepare_features(test_df, feat_avail)
    y_test = test_df[OUTCOME].values.astype(int)

    pipe = build_pipeline(model_name)
    pipe.fit(X_train, y_train)

    prob_train = pipe.predict_proba(X_train)[:, 1]
    prob_val = pipe.predict_proba(X_val)[:, 1]
    prob_test = pipe.predict_proba(X_test)[:, 1]

    auc_train = roc_auc_score(y_train, prob_train)
    auc_val = roc_auc_score(y_val, prob_val)
    auc_test = roc_auc_score(y_test, prob_test)
    brier_test = brier_score_loss(y_test, prob_test)
    ap_test = average_precision_score(y_test, prob_test)

    # Feature importances
    if model_name == "logistic":
        importances = np.abs(pipe.named_steps["model"].coef_[0])
    else:
        importances = pipe.named_steps["model"].feature_importances_

    # Flag leakage in features (should be empty in v2, but check)
    leaky = [f for f in feat_avail if f in LEAKAGE_EXCLUDED_V1]
    if leaky:
        print(f"    ⚠ WARNING: leaky features still in {feature_set_label}: {leaky}")

    overfit_flag = auc_test > 0.90
    if overfit_flag:
        print(f"    ⚠ TEST AUC > 0.90 ({auc_test:.4f}) — investigating leakage ...")
        # Print top 3 features by importance
        fi_sorted = sorted(zip(feat_avail, importances), key=lambda x: -x[1])[:3]
        for fname, imp in fi_sorted:
            note = LEAKAGE_EXCLUDED_V1.get(fname, "not in leakage list")
            print(f"      Top feature: {fname} (importance={imp:.4f}) — {note}")

    return {
        "feature_set": feature_set_label,
        "model": model_name,
        "n_features": len(feat_avail),
        "n_train": len(train_df),
        "n_val": len(val_df),
        "n_test": len(test_df),
        "n_events_train": int(y_train.sum()),
        "n_events_val": int(y_val.sum()),
        "n_events_test": int(y_test.sum()),
        "auc_train": round(auc_train, 4),
        "auc_val": round(auc_val, 4),
        "auc_test": round(auc_test, 4),   # ← the number that counts
        "brier_test": round(brier_test, 4),
        "ap_test": round(ap_test, 4),
        "overfit_flag": overfit_flag,
        "feature_names": feat_avail,
        "importances": importances,
        "pipeline": pipe,
    }


def shap_top5(result: dict) -> list[dict]:
    """Return top-5 features with importance and leakage annotation."""
    fi = list(zip(result["feature_names"], result["importances"]))
    fi.sort(key=lambda x: -x[1])
    out = []
    for rank, (fname, imp) in enumerate(fi[:5], 1):
        leakage = LEAKAGE_EXCLUDED_V1.get(fname)
        out.append({
            "rank": rank,
            "feature": fname,
            "importance": round(float(imp), 6),
            "leakage_note": leakage or "none",
        })
    return out


def create_bqml_eval_table(con) -> None:
    """Create pub_workspace.bqml_eval_log_v1 if it doesn't exist."""
    con.execute("""
CREATE TABLE IF NOT EXISTS pub_workspace.bqml_eval_log_v1 (
    model_id VARCHAR PRIMARY KEY,
    model_version VARCHAR,
    feature_set VARCHAR,
    model_type VARCHAR,
    cohort_n INTEGER,
    n_train INTEGER,
    n_val INTEGER,
    n_test INTEGER,
    n_features INTEGER,
    auc_train DOUBLE,
    auc_val DOUBLE,
    auc_test DOUBLE,
    brier_test DOUBLE,
    ap_test DOUBLE,
    overfit_flag BOOLEAN,
    notes VARCHAR,
    created_at TIMESTAMP
)
""")
    print("  pub_workspace.bqml_eval_log_v1 ready")


def insert_eval_rows(con, results: list[dict], cohort_n: int) -> None:
    """Insert 6 evaluation rows into pub_workspace.bqml_eval_log_v1."""
    ts = datetime.utcnow()
    for r in results:
        fs_tag = r["feature_set"].lower().replace("_", "")
        model_tag = "lr" if r["model"] == "logistic" else "xgb"
        model_id = f"multimodal_recurrence_{fs_tag}_{model_tag}_v2"

        note_parts = []
        if r["overfit_flag"]:
            note_parts.append("TEST_AUC>0.90: investigate_leakage")
        note_parts.append("v2_post_mig_086_held_out_test_split")
        notes = "; ".join(note_parts)

        con.execute("""
INSERT OR REPLACE INTO pub_workspace.bqml_eval_log_v1
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""", [
            model_id,
            "v2_post_mig_086",
            r["feature_set"],
            r["model"],
            cohort_n,
            r["n_train"],
            r["n_val"],
            r["n_test"],
            r["n_features"],
            r["auc_train"],
            r["auc_val"],
            r["auc_test"],
            r["brier_test"],
            r["ap_test"],
            r["overfit_flag"],
            notes,
            ts,
        ])
    print(f"  Inserted {len(results)} rows into pub_workspace.bqml_eval_log_v1")


def write_summary_v2(results: list[dict], cohort_n: int, n_recur: int) -> None:
    """Write model_results_summary_v2.md."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    first = results[0]

    lines = [
        "# Multimodal Prediction Study — V2 Results (Post-mig_086, Auditable)",
        "",
        f"**Generated:** {ts}  ",
        "**Version:** v2 (post-mig_086 pub_canonical)  ",
        f"**Seed:** {SEED}  ",
        "**Evaluation:** HELD-OUT test split (70/15/15 stratified by outcome)  ",
        "**Outcome:** `any_recurrence_flag` (binary)  ",
        "**Cohort:** `manuscript_workspace.cohort_multimodal_recurrence_v1`  ",
        "",
        "---",
        "",
        "## 1. Why V2?",
        "",
        "V1 (2026-03-18) reported AUC=0.975–0.999 using 5-fold CV only on a local parquet",
        "file. V2 addresses three audit concerns:",
        "",
        "| Concern | V1 | V2 Fix |",
        "|---------|-----|--------|",
        "| No held-out test set | CV pooling only — all data seen during training | Stratified 70/15/15 split; test set untouched during model selection |",
        "| Feature leakage | tg_rising_flag, tg_nadir, tg_last_value — postop Tg IS part of the biochemical recurrence definition | Excluded; see leakage table below |",
        "| Source data | Local parquet (pre-mig_086, pre-canonical) | pub_canonical via MotherDuck, post-mig_086 |",
        "",
        "**Note:** V1 results are not deleted — they are annotated as deprecated pending v2 confirmation.",
        "",
        "---",
        "",
        "## 2. Leakage Exclusions (V1 → V2)",
        "",
        "| Feature | Leakage Type | Reason |",
        "|---------|-------------|--------|",
    ]
    for feat, reason in LEAKAGE_EXCLUDED_V1.items():
        ltype = reason.split(" —")[0]
        desc = reason.split("— ", 1)[-1] if " — " in reason else reason
        lines.append(f"| `{feat}` | {ltype} | {desc} |")

    lines += [
        "",
        "---",
        "",
        "## 3. Cohort",
        "",
        "| Criterion | Value |",
        "|-----------|-------|",
        "| Source | pub_canonical (thyroid_canonical_publication_v1_0) |",
        "| Filter | Malignant + ≥6mo FU + any_recurrence_flag non-NULL + ≥1 multimodal source |",
        f"| Total N | {cohort_n} |",
        f"| Recurrence events | {n_recur} ({100*n_recur/cohort_n:.1f}%) |",
        f"| Train N | {first['n_train']} |",
        f"| Val N | {first['n_val']} |",
        f"| Test N | {first['n_test']} (held-out, never seen during training) |",
        "| Split method | Stratified random 70/15/15 by outcome |",
        "",
        "**Note:** V1 prevalence was 46.7% (1,933/4,136) on a local parquet file without",
        "the ≥6mo follow-up filter and without the multimodal source requirement.",
        f"V2 prevalence is {100*n_recur/cohort_n:.1f}% ({n_recur}/{cohort_n}), which is more",  # noqa: E501
        "clinically plausible for thyroid cancer (published rates: 5–30%).",
        "",
        "---",
        "",
        "## 4. Model Performance — HELD-OUT TEST AUC (primary endpoint)",
        "",
        "| Feature Set | Model | Train AUC | Val AUC | **Test AUC** | Brier (test) | Avg Prec (test) |",
        "|-------------|-------|-----------|---------|------------|--------------|-----------------|",
    ]
    for r in results:
        flag = " ⚠" if r["overfit_flag"] else ""
        lines.append(
            f"| {r['feature_set']} | {r['model']} | {r['auc_train']:.4f} | "
            f"{r['auc_val']:.4f} | **{r['auc_test']:.4f}**{flag} | "
            f"{r['brier_test']:.4f} | {r['ap_test']:.4f} |"
        )

    lines += [
        "",
        "⚠ = test AUC > 0.90; requires leakage investigation before acceptance.",
        "",
        "---",
        "",
        "## 5. Incremental Gain by Modality (Test AUC)",
        "",
        "| Comparison | Model | ΔTest AUC |",
        "|------------|-------|-----------|",
    ]
    res_by = {(r["feature_set"], r["model"]): r for r in results}
    for mname in ["logistic", "xgboost"]:
        fs_a = "A_structured"
        fs_b = "B_struct_imaging"
        fs_c = "C_struct_img_notes"
        if (fs_a, mname) in res_by and (fs_b, mname) in res_by:
            da = res_by[(fs_b, mname)]["auc_test"] - res_by[(fs_a, mname)]["auc_test"]
            lines.append(f"| A→B (+imaging) | {mname} | {da:+.4f} |")
        if (fs_b, mname) in res_by and (fs_c, mname) in res_by:
            da = res_by[(fs_c, mname)]["auc_test"] - res_by[(fs_b, mname)]["auc_test"]
            lines.append(f"| B→C (+mol/FNA) | {mname} | {da:+.4f} |")
        if (fs_a, mname) in res_by and (fs_c, mname) in res_by:
            da = res_by[(fs_c, mname)]["auc_test"] - res_by[(fs_a, mname)]["auc_test"]
            lines.append(f"| A→C (total) | {mname} | {da:+.4f} |")

    lines += [
        "",
        "---",
        "",
        "## 6. Feature Importance (Top 5 per model, Feature Set C)",
        "",
    ]
    for r in results:
        if r["feature_set"] != "C_struct_img_notes":
            continue
        lines.append(f"### {r['model'].title()}")
        lines.append("")
        lines.append("| Rank | Feature | Importance | Leakage Note |")
        lines.append("|------|---------|------------|--------------|")
        for item in shap_top5(r):
            lines.append(
                f"| {item['rank']} | `{item['feature']}` | {item['importance']:.6f} | {item['leakage_note']} |"
            )
        lines.append("")

    lines += [
        "---",
        "",
        "## 7. Discussion: Why V1 AUC Was Likely Inflated",
        "",
        "V1 reported AUC=0.999 for XGBoost (Set C, CV-pooled). The most likely causes:",
        "",
        "1. **No true held-out test set.** Cross-validation pools *predicted* probabilities",
        "   across folds, but each fold uses 80% of the data for training. The model has",
        "   effectively seen all patients during training. This is methodologically valid",
        "   for reporting CV AUC, but it cannot substitute for a held-out test set when the",
        "   goal is to generalize to unseen patients.",
        "",
        "2. **Feature leakage.** `tg_rising_flag` is part of the *definition* of",
        "   biochemical recurrence (rising Tg > 1.0 ng/mL without structural disease). When",
        "   this feature appears in Set C, the model is being given the outcome itself as a",
        "   predictor. `tg_nadir` and `tg_last_value` are similarly postoperative measures",
        "   derived from the same surveillance window that generates the recurrence flag.",
        "",
        "3. **Derived scoring systems.** Set A included `ata_initial_risk`, `macis_score`,",
        "   `ames_risk_group`, and `ages_score`. These composite scoring systems are",
        "   *calibrated* to predict recurrence and mortality in thyroid cancer — they",
        "   contain essentially the same information as the outcome, which explains why",
        "   Set A alone achieved AUC=0.975 with 5-fold CV.",
        "",
        "4. **Pre-canonical parquet.** V1 pulled from a local parquet file that may have",
        "   included patients from a development/derivation set used when building the",
        "   canonical tables.",
        "",
        "V2 does not claim v1 was 'wrong' — it may have been correct on its derivation",
        "set. V2 is the **auditable version**: transparent data source (pub_canonical),",
        "held-out test set designed before training, and explicit leakage exclusions.",
        "",
        "---",
        "",
        "## 8. Reproducibility",
        "",
        f"- **Random seed:** {SEED}",
        "- **Split:** Stratified 70/15/15 (train/val/test)",
        "- **Split file:** /tmp/multimodal_v2_splits.parquet (transient, not tracked)",
        "- **Cohort view:** `manuscript_workspace.cohort_multimodal_recurrence_v1`",
        "- **DB:** thyroid_canonical_publication_v1_0 (post-mig_086)",
        "- **Imputation:** Median (sklearn SimpleImputer)",
        "- **Scaling:** StandardScaler",
        "- **Logistic:** L2, C=1.0, saga solver, max_iter=2000",
        "- **XGBoost:** GradientBoostingClassifier, n_estimators=300, max_depth=4, lr=0.05, subsample=0.8",
        "- **Script:** `train_multimodal_models_v2.py`",
        "",
        "---",
        "",
        "## 9. V1 Deprecation Notice",
        "",
        "`studies/proposal_multimodal_prediction_20260318/model_results/model_results_summary.md`",
        "is **deprecated**. Numbers from that file should not appear in any manuscript",
        "submission or conference abstract. Use v2 results from this file. V1 file is",
        "preserved for audit trail but annotated as deprecated in the Manuscript Feedback Log.",
        "",
        "---",
        "",
        "*Generated by train_multimodal_models_v2.py — post-mig_086 canonical re-run*",
    ]

    out_path = OUT_DIR / "model_results_summary_v2.md"
    out_path.write_text("\n".join(lines))
    print(f"  Written: {out_path}")


def write_csv_artifacts(results: list[dict]) -> None:
    """Write perf, FI, and leakage CSVs."""
    # Performance CSV
    perf = []
    for r in results:
        perf.append({
            "feature_set": r["feature_set"],
            "model": r["model"],
            "n_features": r["n_features"],
            "n_train": r["n_train"],
            "n_val": r["n_val"],
            "n_test": r["n_test"],
            "auc_train": r["auc_train"],
            "auc_val": r["auc_val"],
            "auc_test": r["auc_test"],
            "brier_test": r["brier_test"],
            "ap_test": r["ap_test"],
            "overfit_flag": r["overfit_flag"],
        })
    pd.DataFrame(perf).to_csv(OUT_DIR / "model_performance_v2.csv", index=False)

    # Feature importance CSV
    fi_rows = []
    for r in results:
        for fname, imp in zip(r["feature_names"], r["importances"]):
            fi_rows.append({
                "feature_set": r["feature_set"],
                "model": r["model"],
                "feature": fname,
                "importance": round(float(imp), 6),
                "leakage_note": LEAKAGE_EXCLUDED_V1.get(fname, "none"),
            })
    pd.DataFrame(fi_rows).to_csv(OUT_DIR / "feature_importance_v2.csv", index=False)

    # Leakage exclusions CSV
    lex_rows = [{"feature": k, "leakage_type": v.split(" —")[0], "reason": v}
                for k, v in LEAKAGE_EXCLUDED_V1.items()]
    pd.DataFrame(lex_rows).to_csv(OUT_DIR / "leakage_exclusions_v2.csv", index=False)

    print("  CSVs written: model_performance_v2.csv, feature_importance_v2.csv, leakage_exclusions_v2.csv")


def main():
    print("=" * 60)
    print("MULTIMODAL Recurrence Prediction — V2 Re-Run")
    print("=" * 60)

    print("\n[1] Connecting to MotherDuck pub_canonical …")
    con = get_motherduck_connection()

    print("\n[2] Creating cohort view …")
    cohort_n, n_recur = create_cohort_view(con)

    print("\n[3] Pulling features …")
    df = pull_features(con)
    if len(df) != cohort_n:
        print(f"  NOTE: df rows ({len(df)}) != cohort view ({cohort_n}) — dedup may apply")
        df = df.drop_duplicates(subset=["research_id"])
        print(f"  After dedup: {len(df)}")
        cohort_n = len(df)

    print("\n[4] Creating train/val/test split …")
    df = create_splits(df)

    print("\n[5] Training models …")
    feat_sets = {
        "A_structured": FEATURES_A,
        "B_struct_imaging": FEATURES_A + FEATURES_B_EXTRA,
        "C_struct_img_notes": FEATURES_A + FEATURES_B_EXTRA + FEATURES_C_EXTRA,
    }

    results = []
    for fs_label, cols in feat_sets.items():
        for model_name in ["logistic", "xgboost"]:
            avail = [c for c in cols if c in df.columns]
            print(f"  {fs_label} × {model_name} ({len(avail)} features) …")
            r = train_and_evaluate(df, fs_label, cols, model_name)
            results.append(r)
            print(
                f"    Train AUC={r['auc_train']:.4f} | Val AUC={r['auc_val']:.4f} | "
                f"Test AUC={r['auc_test']:.4f} {'⚠ OVERFIT?' if r['overfit_flag'] else ''}"
            )

    print("\n[6] Writing artifacts …")
    write_summary_v2(results, cohort_n, n_recur)
    write_csv_artifacts(results)

    print("\n[7] Creating bqml_eval_log_v1 and inserting rows …")
    create_bqml_eval_table(con)
    insert_eval_rows(con, results, cohort_n)

    print("\n[8] Summary table:")
    print(f"{'Feature Set':<25} {'Model':<10} {'Train':>8} {'Val':>8} {'TEST':>8}")
    print("-" * 65)
    for r in results:
        flag = " *" if r["overfit_flag"] else ""
        print(f"{r['feature_set']:<25} {r['model']:<10} {r['auc_train']:>8.4f} {r['auc_val']:>8.4f} {r['auc_test']:>8.4f}{flag}")

    test_aucs = [r["auc_test"] for r in results]
    if any(a > 0.90 for a in test_aucs):
        print("\n⚠ WARNING: One or more models have test AUC > 0.90.")
        print("  Investigate remaining feature leakage before reporting.")
    else:
        print("\n✓ All test AUCs are in a plausible clinical range.")
        print(f"  Honest held-out test AUC range: {min(test_aucs):.4f}–{max(test_aucs):.4f}")

    con.close()
    print(f"\n✓ V2 complete. Outputs in: {OUT_DIR}")
    return results


if __name__ == "__main__":
    main()
