"""
Phase B.6 — Park / T-US 2009 cohort refit job
==============================================
Trains a logistic regression on this cohort's nodules that have all 12 Park X
variables AND a final-pathology malignancy label within ±90d of the US exam.

70/30 train/test split, random_state=42. sklearn LogisticRegression L2 C=1.0.
Binary inputs: no standardization.

Acceptance gate: train AUC >= 0.75, test AUC >= 0.70. If below, confidence='low'
is stamped in the manifest and surfaced in the THY-30 comment.

Outputs:
  - scripts/manifests/park_coefs_v1.json  (park_cohort_refit section updated)
  - pub_workspace.park_cohort_refit_split_v1  (train/test nodule_id split)

Usage:
    python scripts/417b_park_cohort_refit.py [--dry-run] [--project PROJECT]

Author: Cursor Agent (Phase B.6), 2026-05-08
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import bigquery

try:
    import numpy as np
    import pandas as pd
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import roc_auc_score
    from sklearn.model_selection import train_test_split
except ImportError as e:
    sys.exit(f"ERROR: Missing dependency: {e}. pip install scikit-learn pandas numpy")

PROJECT = "thyroid-canonical-pub-2026"
DATASET_PUB = "pub_canonical"
DATASET_WS = "pub_workspace"

TABLE_NODULE_V2 = f"{PROJECT}.{DATASET_PUB}.canonical_us_nodule_v2"
TABLE_LN_CTX = f"{PROJECT}.{DATASET_WS}.us_nodule_ln_context_v1"
TABLE_PATH_MALIGNANT = f"{PROJECT}.{DATASET_PUB}.canonical_path_malignant_events_v1"
TABLE_PATH_BENIGN = f"{PROJECT}.{DATASET_PUB}.canonical_path_benign_events_v1"
TABLE_SPLIT = f"{PROJECT}.{DATASET_WS}.park_cohort_refit_split_v1"

MANIFEST_PATH = Path(__file__).parent / "manifests" / "park_coefs_v1.json"

X_COLS = [
    "park_x1_taller", "park_x2_halo", "park_x3_well_circumscribed",
    "park_x4_microlobulation", "park_x5_infiltrative_margin",
    "park_x6_marked_hypo", "park_x7_hypo", "park_x8_homogeneous",
    "park_x9_mainly_cystic", "park_x10_solid", "park_x11_microcalc",
    "park_x12_abnormal_ln",
]

# Fetch labelled corpus SQL — joins nodule to final-path malignancy within ±90d
CORPUS_SQL = f"""
WITH
nodule_raw AS (
    SELECT
        n.nodule_id,
        n.research_id,
        n.us_exam_id,
        n.exam_date,
        -- X1-X11 from nodule table
        COALESCE(n.shape = 'taller_than_wide', FALSE) AS park_x1_taller,
        COALESCE(JSON_VALUE(n.halo_jsonb, '$.presence') = 'present', FALSE) AS park_x2_halo,
        COALESCE(n.margins = 'smooth', FALSE) AS park_x3_well_circumscribed,
        COALESCE(n.margins = 'microlobulated', FALSE) AS park_x4_microlobulation,
        COALESCE(
            n.margins IN ('irregular', 'ill_defined', 'extrathyroidal_extension')
            OR JSON_VALUE(n.ete_us_jsonb, '$.presence') IN (
                'capsule_loss', 'strap_muscle_invasion', 'bulging', 'abutment'
            ),
            FALSE
        ) AS park_x5_infiltrative_margin,
        COALESCE(n.echogenicity = 'very_hypoechoic', FALSE) AS park_x6_marked_hypo,
        COALESCE(n.echogenicity = 'hypoechoic', FALSE) AS park_x7_hypo,
        COALESCE(n.homogeneous_echotexture = TRUE, FALSE) AS park_x8_homogeneous,
        COALESCE(n.composition IN ('cystic', 'predominantly_cystic'), FALSE) AS park_x9_mainly_cystic,
        COALESCE(n.composition IN ('solid', 'predominantly_solid'), FALSE) AS park_x10_solid,
        COALESCE(
            'punctate_echogenic_foci' IN UNNEST(JSON_VALUE_ARRAY(n.echogenic_foci)),
            FALSE
        ) AS park_x11_microcalc
    FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_us_nodule_v2` n
),
ln_join AS (
    SELECT r.*,
        COALESCE(ln.has_suspicious_ln_within_60d = 1, FALSE) AS park_x12_abnormal_ln
    FROM nodule_raw r
    LEFT JOIN `thyroid-canonical-pub-2026.pub_workspace.us_nodule_ln_context_v1` ln USING (nodule_id)
),
-- Malignant label: research_id with a surgery_date within ±90d of US exam.
-- All rows in canonical_path_malignant_events_v1 are confirmed malignant.
malignant_match AS (
    SELECT DISTINCT
        CAST(p.research_id AS STRING) AS research_id,
        p.surgery_date
    FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_path_malignant_events_v1` p
    WHERE p.surgery_date IS NOT NULL
),
-- Benign label: research_id with path_date within ±90d of US exam.
benign_match AS (
    SELECT DISTINCT
        CAST(b.research_id AS STRING) AS research_id,
        b.path_date
    FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_path_benign_events_v1` b
    WHERE b.path_date IS NOT NULL
),
-- Per-nodule label via temporal proximity.
-- A nodule exam matches a path event if the path date is within ±90d of exam_date.
-- Priority: any malignant match within window → 1; benign-only within window → 0.
path_join AS (
    SELECT
        n.*,
        CASE
            WHEN EXISTS (
                SELECT 1 FROM malignant_match m
                WHERE m.research_id = CAST(n.research_id AS STRING)
                  AND ABS(DATE_DIFF(m.surgery_date, n.exam_date, DAY)) <= 90
            ) THEN 1
            WHEN EXISTS (
                SELECT 1 FROM benign_match b
                WHERE b.research_id = CAST(n.research_id AS STRING)
                  AND ABS(DATE_DIFF(CAST(b.path_date AS DATE), n.exam_date, DAY)) <= 90
            ) THEN 0
            ELSE NULL
        END AS malignant_label
    FROM ln_join n
)
SELECT *
FROM path_join
WHERE malignant_label IS NOT NULL
"""


def main():
    parser = argparse.ArgumentParser(description="Park cohort refit")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--project", default=PROJECT)
    args = parser.parse_args()

    client = bigquery.Client(project=args.project)

    print("=== Phase B.6 Park cohort refit ===")
    print(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")

    # 1. Load corpus
    print("INFO: Fetching labelled corpus ...")
    df = client.query(CORPUS_SQL).to_dataframe()
    print(f"INFO: Corpus size: {len(df)} nodules with path label.")

    if len(df) < 100:
        sys.exit(
            f"ERROR: Corpus too small ({len(df)} nodules). "
            "Minimum 100 required for a stable refit. "
            "Check canonical_path_malignant_events_v1 path-linkage logic."
        )

    X = df[X_COLS].astype(float).values
    y = df["malignant_label"].astype(int).values

    # 2. 70/30 train/test split
    X_train, X_test, y_train, y_test, idx_train, idx_test = train_test_split(
        X, y, np.arange(len(df)), test_size=0.30, random_state=42, stratify=y
    )
    print(f"INFO: Train n={len(y_train)}, test n={len(y_test)}")

    # 3. Logistic regression L2 C=1.0 (no standardization — binary inputs)
    clf = LogisticRegression(
        penalty="l2", C=1.0, solver="lbfgs", max_iter=1000, random_state=42
    )
    clf.fit(X_train, y_train)

    # 4. AUC
    train_auc = roc_auc_score(y_train, clf.predict_proba(X_train)[:, 1])
    test_auc = roc_auc_score(y_test, clf.predict_proba(X_test)[:, 1])
    print(f"INFO: Train AUC = {train_auc:.4f},  Test AUC = {test_auc:.4f}")

    confidence = "high" if test_auc >= 0.70 and train_auc >= 0.75 else "low"
    if confidence == "low":
        print(
            "WARNING: AUC below acceptance gate "
            f"(train={train_auc:.4f} need ≥0.75, test={test_auc:.4f} need ≥0.70). "
            "Manifest will be stamped confidence='low'. Surface to Logan before sign-off."
        )

    # 5. Extract coefficients
    intercept = float(clf.intercept_[0])
    coef_dict = {
        x_col.replace("park_", "").replace("x", "x"): float(c)
        for x_col, c in zip(X_COLS, clf.coef_[0])
    }
    # Map to manifest key names
    key_map = {
        f"x{i+1}_{name.split('_', 1)[1]}": name.split('_', 1)[1]
        for i, name in enumerate(X_COLS)
    }
    beta_dict = {}
    for i, x_col in enumerate(X_COLS):
        key = f"x{i+1}_{x_col.replace('park_x', '').split('_', 1)[1]}"
        beta_dict[key] = float(clf.coef_[0][i])

    # Rebuild as the exact key format used in the manifest
    beta_manifest = {}
    x_names = [
        "x1_taller", "x2_halo", "x3_well_circumscribed", "x4_microlobulation",
        "x5_infiltrative_margin", "x6_marked_hypo", "x7_hypo", "x8_homogeneous",
        "x9_mainly_cystic", "x10_solid", "x11_microcalc", "x12_abnormal_ln",
    ]
    for x_name, coef in zip(x_names, clf.coef_[0]):
        beta_manifest[x_name] = float(coef)

    # 6. Update manifest
    if not args.dry_run:
        with open(MANIFEST_PATH) as f:
            manifest = json.load(f)
        refit_set = manifest["sets"]["park_cohort_refit"]
        refit_set["intercept"] = intercept
        refit_set["betas"] = beta_manifest
        refit_set["refit_at"] = datetime.now(timezone.utc).isoformat()
        refit_set["refit_n_train"] = int(len(y_train))
        refit_set["refit_n_test"] = int(len(y_test))
        refit_set["refit_auc_train"] = round(train_auc, 4)
        refit_set["refit_auc_test"] = round(test_auc, 4)
        refit_set["confidence"] = confidence
        refit_set["coefficients_status"] = f"READY (confidence={confidence})"
        refit_set["source"] = (
            f"This cohort (thyroid-canonical-pub-2026), "
            f"70/30 train/holdout, random_state=42, "
            f"sklearn LogisticRegression(penalty='l2', C=1.0, solver='lbfgs', max_iter=1000). "
            f"Train n={len(y_train)}, test n={len(y_test)}. "
            f"Train AUC={train_auc:.4f}, Test AUC={test_auc:.4f}."
        )
        with open(MANIFEST_PATH, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"INFO: Updated manifest at {MANIFEST_PATH}")

    # 7. Persist train/test split to BQ
    split_df = df[["nodule_id", "research_id"]].copy()
    split_df["split"] = "test"
    split_df.loc[idx_train, "split"] = "train"
    split_df["malignant_label"] = df["malignant_label"].values
    split_df["refit_run_at"] = datetime.now(timezone.utc).isoformat()

    if args.dry_run:
        print(f"DRY-RUN: Would write {len(split_df)} rows to {TABLE_SPLIT}")
    else:
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = client.load_table_from_dataframe(split_df, TABLE_SPLIT, job_config=job_config)
        job.result()
        print(f"INFO: Wrote {len(split_df)} rows to {TABLE_SPLIT}")

    print("\n=== Cohort refit complete ===")
    print(f"  Intercept: {intercept:.4f}")
    for k, v in beta_manifest.items():
        print(f"  β {k:30s}: {v:+.4f}")
    print(f"  Train AUC: {train_auc:.4f}")
    print(f"  Test  AUC: {test_auc:.4f}")
    print(f"  Confidence: {confidence}")

    if confidence == "low":
        print(
            "\nACTION REQUIRED: AUC below gate. Do not proceed to sign-off. "
            "Review path-linkage logic or feature coverage with Logan."
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
