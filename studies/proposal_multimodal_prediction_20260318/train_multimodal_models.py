#!/usr/bin/env python3
"""
Multimodal Prediction Study — Model Training & Evaluation
==========================================================
Trains logistic regression and XGBoost models across three feature sets:
  A: Structured-only
  B: Structured + Imaging
  C: Structured + Imaging + Notes-derived (molecular/lab/FNA)

Outcome: recurrence_flag (binary, 0% missing, 46.7% prevalence)
Seed: 42, stratified 80/20 split, no PHI.

Outputs (to model_results/):
  - model_performance.csv
  - auc_comparison.csv
  - calibration_metrics.csv
  - feature_importance.csv
  - figures/roc_curves.png
  - figures/calibration_plot.png
  - model_results_summary.md
"""
from __future__ import annotations

import os
import sys
import json
import warnings
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd
from sklearn.model_selection import StratifiedKFold, cross_val_predict
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.metrics import (
    roc_auc_score,
    brier_score_loss,
    roc_curve,
    average_precision_score,
    classification_report,
)
from sklearn.calibration import calibration_curve
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

warnings.filterwarnings("ignore", category=FutureWarning)

# ────────────────────────────────────────────
# CONFIG
# ────────────────────────────────────────────
SEED = 42
np.random.seed(SEED)

STUDY_DIR = Path(__file__).resolve().parent
DATA_PATH = STUDY_DIR / "candidate_modeling_dataset.parquet"
OUT_DIR = STUDY_DIR / "model_results"
FIG_DIR = OUT_DIR / "figures"

OUTCOME = "recurrence_flag"

# ── Feature set definitions ──────────────────
# A: Structured clinical/pathology only
FEATURES_A = [
    "age_at_surgery",
    "sex",
    "race",
    "histology_final",
    "t_stage",
    "n_stage",
    "m_stage",
    "ete_grade",
    "tumor_size_cm",
    "ln_examined_count",
    "margin_status",
    "vascular_invasion",
    "ajcc8_stage",
    "ata_risk",
    "macis_score",
    "ames_risk_group",
    "ages_score",
    "surg_procedure_type",
]

# B: A + Imaging
FEATURES_B_EXTRA = [
    "tirads_worst",
    "tirads_worst_category",
    "imaging_nodule_size_cm",
    "n_nodules_imaged",
    "has_tirads_validated",
    "tirads_nodule_max_mm",
]

# C: B + Notes-derived (molecular, lab, FNA)
FEATURES_C_EXTRA = [
    "braf_positive",
    "ras_positive",
    "tert_positive",
    "molecular_platform",
    "molecular_risk_tier",
    "bethesda_worst",
    "tg_nadir",
    "tg_last_value",
    "tg_rising_flag",
    "lab_completeness_score",
    "n_lab_values",
    "n_analyte_groups",
    "has_fna_data",
    "has_molecular_data",
    "n_molecular_tests",
]


# ────────────────────────────────────────────
# HELPERS
# ────────────────────────────────────────────
def load_data() -> pd.DataFrame:
    """Load parquet, drop research_id, encode outcome."""
    df = pd.read_parquet(DATA_PATH)
    # Ensure outcome is int 0/1
    df[OUTCOME] = df[OUTCOME].astype(int)
    return df


def prepare_features(df: pd.DataFrame, feature_cols: list[str]) -> pd.DataFrame:
    """Select features, encode categoricals, impute, return numeric DataFrame."""
    X = df[feature_cols].copy()

    # Encode categoricals
    cat_cols = X.select_dtypes(include=["object", "category", "bool"]).columns.tolist()
    for c in cat_cols:
        X[c] = X[c].astype(str).replace({"None": np.nan, "nan": np.nan, "<NA>": np.nan})
        le = LabelEncoder()
        mask = X[c].notna()
        if mask.sum() > 0:
            X.loc[mask, c] = le.fit_transform(X.loc[mask, c])
        X[c] = pd.to_numeric(X[c], errors="coerce")

    # Nullable integer → float
    for c in X.columns:
        if pd.api.types.is_integer_dtype(X[c]) or str(X[c].dtype).startswith("Int"):
            X[c] = X[c].astype("float64")

    return X.astype("float64")


def build_pipeline(model_name: str):
    """Return (imputer + scaler + model) pipeline."""
    if model_name == "logistic":
        mdl = LogisticRegression(
            max_iter=2000,
            solver="saga",
            penalty="l2",
            C=1.0,
            random_state=SEED,
        )
    elif model_name == "xgboost":
        mdl = GradientBoostingClassifier(
            n_estimators=300,
            max_depth=4,
            learning_rate=0.05,
            subsample=0.8,
            random_state=SEED,
        )
    else:
        raise ValueError(f"Unknown model: {model_name}")

    return Pipeline(
        [
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
            ("model", mdl),
        ]
    )


def evaluate_cv(
    X: pd.DataFrame,
    y: np.ndarray,
    model_name: str,
    feature_set_label: str,
    n_splits: int = 5,
) -> dict:
    """Stratified k-fold CV, returning metrics + per-fold AUCs."""
    cv = StratifiedKFold(n_splits=n_splits, shuffle=True, random_state=SEED)
    pipe = build_pipeline(model_name)

    # Cross-val predicted probabilities (for full-sample ROC + calibration)
    y_prob = cross_val_predict(pipe, X, y, cv=cv, method="predict_proba")[:, 1]

    # Per-fold AUCs
    fold_aucs = []
    for train_idx, test_idx in cv.split(X, y):
        p = build_pipeline(model_name)
        p.fit(X.iloc[train_idx], y[train_idx])
        y_test_prob = p.predict_proba(X.iloc[test_idx])[:, 1]
        fold_aucs.append(roc_auc_score(y[test_idx], y_test_prob))

    auc_mean = np.mean(fold_aucs)
    auc_std = np.std(fold_aucs)
    auc_full = roc_auc_score(y, y_prob)
    brier = brier_score_loss(y, y_prob)
    ap = average_precision_score(y, y_prob)

    # Fit final model on full data for feature importance
    final_pipe = build_pipeline(model_name)
    final_pipe.fit(X, y)

    if model_name == "logistic":
        coefs = final_pipe.named_steps["model"].coef_[0]
        importances = np.abs(coefs)
    else:
        importances = final_pipe.named_steps["model"].feature_importances_

    return {
        "feature_set": feature_set_label,
        "model": model_name,
        "auc_cv_mean": round(auc_mean, 4),
        "auc_cv_std": round(auc_std, 4),
        "auc_pooled": round(auc_full, 4),
        "brier_score": round(brier, 4),
        "avg_precision": round(ap, 4),
        "n_features": X.shape[1],
        "n_samples": X.shape[0],
        "n_events": int(y.sum()),
        "fold_aucs": [round(a, 4) for a in fold_aucs],
        "y_prob": y_prob,
        "y_true": y,
        "importances": importances,
        "feature_names": list(X.columns),
        "final_pipeline": final_pipe,
    }


# ────────────────────────────────────────────
# MAIN
# ────────────────────────────────────────────
def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    FIG_DIR.mkdir(parents=True, exist_ok=True)

    print("Loading data …")
    df = load_data()
    y = df[OUTCOME].values
    print(f"  N={len(df)}, events={y.sum()} ({100*y.mean():.1f}%)")

    # ── Build feature matrices ──
    feat_sets = {
        "A_structured": FEATURES_A,
        "B_struct_imaging": FEATURES_A + FEATURES_B_EXTRA,
        "C_struct_img_notes": FEATURES_A + FEATURES_B_EXTRA + FEATURES_C_EXTRA,
    }

    results = []
    for fs_label, cols in feat_sets.items():
        available = [c for c in cols if c in df.columns]
        X = prepare_features(df, available)
        print(f"\nFeature set {fs_label}: {len(available)} features, shape={X.shape}")

        for model_name in ["logistic", "xgboost"]:
            print(f"  Training {model_name} …")
            res = evaluate_cv(X, y, model_name, fs_label)
            results.append(res)
            print(
                f"    AUC={res['auc_cv_mean']:.4f}±{res['auc_cv_std']:.4f}  "
                f"Brier={res['brier_score']:.4f}"
            )

    # ── Save performance CSV ──
    perf_rows = []
    for r in results:
        perf_rows.append({
            "feature_set": r["feature_set"],
            "model": r["model"],
            "auc_cv_mean": r["auc_cv_mean"],
            "auc_cv_std": r["auc_cv_std"],
            "auc_pooled": r["auc_pooled"],
            "brier_score": r["brier_score"],
            "avg_precision": r["avg_precision"],
            "n_features": r["n_features"],
            "n_samples": r["n_samples"],
            "n_events": r["n_events"],
        })
    perf_df = pd.DataFrame(perf_rows)
    perf_df.to_csv(OUT_DIR / "model_performance.csv", index=False)
    print(f"\n✓ model_performance.csv ({len(perf_df)} rows)")

    # ── AUC comparison ──
    auc_rows = []
    for r in results:
        for i, a in enumerate(r["fold_aucs"]):
            auc_rows.append({
                "feature_set": r["feature_set"],
                "model": r["model"],
                "fold": i + 1,
                "auc": a,
            })
    auc_df = pd.DataFrame(auc_rows)
    auc_df.to_csv(OUT_DIR / "auc_comparison.csv", index=False)
    print(f"✓ auc_comparison.csv ({len(auc_df)} rows)")

    # ── Calibration metrics ──
    cal_rows = []
    for r in results:
        fop, mpv = calibration_curve(r["y_true"], r["y_prob"], n_bins=10, strategy="uniform")
        ece = float(np.mean(np.abs(fop - mpv)))
        cal_rows.append({
            "feature_set": r["feature_set"],
            "model": r["model"],
            "brier_score": r["brier_score"],
            "expected_calibration_error": round(ece, 4),
        })
    cal_df = pd.DataFrame(cal_rows)
    cal_df.to_csv(OUT_DIR / "calibration_metrics.csv", index=False)
    print(f"✓ calibration_metrics.csv ({len(cal_df)} rows)")

    # ── Feature importance ──
    fi_rows = []
    for r in results:
        for fname, imp in zip(r["feature_names"], r["importances"]):
            fi_rows.append({
                "feature_set": r["feature_set"],
                "model": r["model"],
                "feature": fname,
                "importance": round(float(imp), 6),
            })
    fi_df = pd.DataFrame(fi_rows)
    fi_df.to_csv(OUT_DIR / "feature_importance.csv", index=False)
    print(f"✓ feature_importance.csv ({len(fi_df)} rows)")

    # ── Figure 1: ROC curves ──
    fig, axes = plt.subplots(1, 3, figsize=(18, 5.5), dpi=150)
    colors = {"logistic": "#0072B2", "xgboost": "#D55E00"}
    for ax, fs_label in zip(axes, feat_sets.keys()):
        for r in results:
            if r["feature_set"] != fs_label:
                continue
            fpr, tpr, _ = roc_curve(r["y_true"], r["y_prob"])
            label = f"{r['model']} (AUC={r['auc_cv_mean']:.3f}±{r['auc_cv_std']:.3f})"
            ax.plot(fpr, tpr, color=colors[r["model"]], lw=2, label=label)
        ax.plot([0, 1], [0, 1], "k--", lw=0.8, alpha=0.5)
        ax.set_xlabel("False Positive Rate", fontsize=11)
        ax.set_ylabel("True Positive Rate", fontsize=11)
        ax.set_title(fs_label.replace("_", " ").title(), fontsize=12, fontweight="bold")
        ax.legend(loc="lower right", fontsize=9)
        ax.set_xlim(-0.02, 1.02)
        ax.set_ylim(-0.02, 1.02)
    fig.suptitle("ROC Curves by Feature Set", fontsize=14, fontweight="bold", y=1.02)
    fig.tight_layout()
    fig.savefig(FIG_DIR / "roc_curves.png", bbox_inches="tight", dpi=300)
    plt.close(fig)
    print("✓ figures/roc_curves.png")

    # ── Figure 2: Calibration plots ──
    fig, axes = plt.subplots(1, 3, figsize=(18, 5.5), dpi=150)
    for ax, fs_label in zip(axes, feat_sets.keys()):
        for r in results:
            if r["feature_set"] != fs_label:
                continue
            fop, mpv = calibration_curve(r["y_true"], r["y_prob"], n_bins=10, strategy="uniform")
            label = f"{r['model']} (Brier={r['brier_score']:.3f})"
            ax.plot(mpv, fop, "o-", color=colors[r["model"]], lw=2, label=label, markersize=5)
        ax.plot([0, 1], [0, 1], "k--", lw=0.8, alpha=0.5)
        ax.set_xlabel("Mean Predicted Probability", fontsize=11)
        ax.set_ylabel("Fraction of Positives", fontsize=11)
        ax.set_title(fs_label.replace("_", " ").title(), fontsize=12, fontweight="bold")
        ax.legend(loc="upper left", fontsize=9)
        ax.set_xlim(-0.02, 1.02)
        ax.set_ylim(-0.02, 1.02)
    fig.suptitle("Calibration Plots by Feature Set", fontsize=14, fontweight="bold", y=1.02)
    fig.tight_layout()
    fig.savefig(FIG_DIR / "calibration_plot.png", bbox_inches="tight", dpi=300)
    plt.close(fig)
    print("✓ figures/calibration_plot.png")

    # ── Generate summary report ──
    generate_summary(perf_df, cal_df, fi_df, results, feat_sets)
    print(f"\n✓ All outputs saved to: {OUT_DIR}")


def generate_summary(perf_df, cal_df, fi_df, results, feat_sets):
    """Write model_results_summary.md."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")

    lines = [
        "# Multimodal Prediction Study — Model Results Summary",
        f"\n**Generated:** {ts}  ",
        f"**Seed:** {SEED}  ",
        f"**Evaluation:** 5-fold stratified cross-validation  ",
        f"**Outcome:** `recurrence_flag` (any recurrence, binary)  ",
        "",
        "## 1. Outcome Justification",
        "",
        "| Criterion | Value |",
        "|-----------|-------|",
        f"| Outcome | `recurrence_flag` |",
        f"| Prevalence | 46.7% (1,933 / 4,136) |",
        f"| Missingness | 0% |",
        f"| Type | Binary (0/1) |",
        f"| Manuscript-safe | YES |",
        "",
        "**Rationale:** `recurrence_flag` was selected as the primary endpoint because it is:",
        "- Fully available (0% missing) across the entire analysis-eligible cancer cohort",
        "- Has adequate event prevalence (46.7%) for stable model training",
        "- Clinically meaningful — recurrence is the primary outcome of interest for thyroid cancer prognosis",
        "- Manuscript-safe with documented provenance (see outcome_prevalence.csv)",
        "- Alternative endpoints either had excessive missingness (`structural_recurrence_flag`: 53% missing),",
        "  extreme class imbalance (`has_complication_record`: 1.0%), or were molecular markers rather than outcomes",
        "",
        "## 2. Feature Set Definitions",
        "",
        "### Set A — Structured Clinical Only (baseline)",
        "",
    ]
    lines.append("| # | Feature |")
    lines.append("|---|---------|")
    for i, f in enumerate(FEATURES_A, 1):
        lines.append(f"| {i} | `{f}` |")
    lines.append("")
    lines.append("### Set B — Structured + Imaging")
    lines.append("")
    lines.append("Set A plus:")
    lines.append("")
    for f in FEATURES_B_EXTRA:
        lines.append(f"- `{f}`")
    lines.append("")
    lines.append("### Set C — Structured + Imaging + Notes-Derived")
    lines.append("")
    lines.append("Set B plus:")
    lines.append("")
    for f in FEATURES_C_EXTRA:
        lines.append(f"- `{f}`")

    # Performance table
    lines.append("")
    lines.append("## 3. Model Performance Comparison")
    lines.append("")
    lines.append("| Feature Set | Model | AUC (CV mean±SD) | AUC (pooled) | Brier Score | Avg Precision | N Features |")
    lines.append("|-------------|-------|-------------------|--------------|-------------|---------------|------------|")
    for _, row in perf_df.iterrows():
        lines.append(
            f"| {row['feature_set']} | {row['model']} | "
            f"{row['auc_cv_mean']:.4f}±{row['auc_cv_std']:.4f} | "
            f"{row['auc_pooled']:.4f} | "
            f"{row['brier_score']:.4f} | "
            f"{row['avg_precision']:.4f} | "
            f"{row['n_features']} |"
        )

    # Incremental gain
    lines.append("")
    lines.append("## 4. Incremental Gain by Modality")
    lines.append("")
    lines.append("| Comparison | Model | AUC Δ | Brier Δ |")
    lines.append("|------------|-------|-------|---------|")
    for model_name in ["logistic", "xgboost"]:
        aucs = {}
        briers = {}
        for _, row in perf_df.iterrows():
            if row["model"] == model_name:
                aucs[row["feature_set"]] = row["auc_cv_mean"]
                briers[row["feature_set"]] = row["brier_score"]

        if "A_structured" in aucs and "B_struct_imaging" in aucs:
            da = aucs["B_struct_imaging"] - aucs["A_structured"]
            db = briers["B_struct_imaging"] - briers["A_structured"]
            lines.append(f"| A→B (+ imaging) | {model_name} | {da:+.4f} | {db:+.4f} |")
        if "B_struct_imaging" in aucs and "C_struct_img_notes" in aucs:
            da = aucs["C_struct_img_notes"] - aucs["B_struct_imaging"]
            db = briers["C_struct_img_notes"] - briers["B_struct_imaging"]
            lines.append(f"| B→C (+ notes) | {model_name} | {da:+.4f} | {db:+.4f} |")
        if "A_structured" in aucs and "C_struct_img_notes" in aucs:
            da = aucs["C_struct_img_notes"] - aucs["A_structured"]
            db = briers["C_struct_img_notes"] - briers["A_structured"]
            lines.append(f"| A→C (total) | {model_name} | {da:+.4f} | {db:+.4f} |")

    # Calibration
    lines.append("")
    lines.append("## 5. Calibration Metrics")
    lines.append("")
    lines.append("| Feature Set | Model | Brier Score | ECE |")
    lines.append("|-------------|-------|-------------|-----|")
    for _, row in cal_df.iterrows():
        lines.append(
            f"| {row['feature_set']} | {row['model']} | "
            f"{row['brier_score']:.4f} | "
            f"{row['expected_calibration_error']:.4f} |"
        )

    # Top predictors per model
    lines.append("")
    lines.append("## 6. Top Predictors (Feature Set C, Full Model)")
    lines.append("")
    for model_name in ["logistic", "xgboost"]:
        lines.append(f"### {model_name.title()}")
        lines.append("")
        sub = fi_df[
            (fi_df["feature_set"] == "C_struct_img_notes") & (fi_df["model"] == model_name)
        ].sort_values("importance", ascending=False).head(15)
        lines.append("| Rank | Feature | Importance |")
        lines.append("|------|---------|------------|")
        for rank, (_, row) in enumerate(sub.iterrows(), 1):
            lines.append(f"| {rank} | `{row['feature']}` | {row['importance']:.4f} |")
        lines.append("")

    # Figures
    lines.append("## 7. Figures")
    lines.append("")
    lines.append("### ROC Curves")
    lines.append("![ROC Curves](model_results/figures/roc_curves.png)")
    lines.append("")
    lines.append("### Calibration Plots")
    lines.append("![Calibration](model_results/figures/calibration_plot.png)")
    lines.append("")

    # Reproducibility
    lines.append("## 8. Reproducibility")
    lines.append("")
    lines.append(f"- **Random seed:** {SEED}")
    lines.append("- **CV:** 5-fold stratified")
    lines.append("- **Imputation:** Median (via `SimpleImputer`)")
    lines.append("- **Scaling:** StandardScaler")
    lines.append("- **Logistic:** L2, C=1.0, saga solver, max_iter=2000")
    lines.append("- **XGBoost:** GradientBoostingClassifier, n_estimators=300, max_depth=4, lr=0.05, subsample=0.8")
    lines.append("- **Script:** `train_multimodal_models.py`")
    lines.append(f"- **Input:** `candidate_modeling_dataset.parquet` (N={4136})")
    lines.append("")

    md_path = OUT_DIR / "model_results_summary.md"
    md_path.write_text("\n".join(lines))
    print(f"✓ model_results_summary.md")


if __name__ == "__main__":
    main()
