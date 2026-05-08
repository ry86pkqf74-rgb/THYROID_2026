"""
Phase B.6 v2 — Park / T-US 2009 cohort refit (nodule-level path linkage)
==========================================================================
Supersedes 417b_park_cohort_refit.py. The v1 version used patient-level path
linkage (canonical_path_malignant_events_v1 joined on research_id) which
mislabeled multinodular-goiter patients (one malignant nodule among five) as
all-malignant, dragging test AUC to 0.661 (below the 0.70 acceptance gate).

This v2 script joins to pub_workspace.us_nodule_path_outcome_v1, which uses
laterality-aware per-nodule path labels. See:
  - scripts/_phase_b6_step3b_us_nodule_path_outcome_v1.sql
  - exports/phase_b_deterministic_scorers_20260507/README.md (B.6 finalization)

Usage:
    python scripts/417b_v2_park_cohort_refit.py [--dry-run] [--project PROJECT]

Outputs:
    - scripts/manifests/park_coefs_v1.json (park_cohort_refit section updated +
      linkage_strategy field stamped)
    - pub_workspace.park_cohort_refit_split_v2 (train/test nodule_id split,
      nodule-level labels)
    - pub_workspace.qc_phase_b6_park_label_flip_v1 (per-nodule label diff vs v1
      patient-level split)

Author: Cursor Agent (Phase B.6 finalization), 2026-05-07
"""

from __future__ import annotations

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
TABLE_OUTCOME_V1 = f"{PROJECT}.{DATASET_WS}.us_nodule_path_outcome_v1"
TABLE_SPLIT_V1 = f"{PROJECT}.{DATASET_WS}.park_cohort_refit_split_v1"
TABLE_SPLIT_V2 = f"{PROJECT}.{DATASET_WS}.park_cohort_refit_split_v2"
TABLE_LABEL_FLIP = f"{PROJECT}.{DATASET_WS}.qc_phase_b6_park_label_flip_v1"

MANIFEST_PATH = Path(__file__).parent / "manifests" / "park_coefs_v1.json"

X_COLS = [
    "park_x1_taller", "park_x2_halo", "park_x3_well_circumscribed",
    "park_x4_microlobulation", "park_x5_infiltrative_margin",
    "park_x6_marked_hypo", "park_x7_hypo", "park_x8_homogeneous",
    "park_x9_mainly_cystic", "park_x10_solid", "park_x11_microcalc",
    "park_x12_abnormal_ln",
]

# Fetch labelled corpus SQL — joins canonical_us_nodule_v2 to the new
# us_nodule_path_outcome_v1 view, which has per-nodule labels.
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
    FROM `{TABLE_NODULE_V2}` n
),
ln_join AS (
    SELECT r.*,
        COALESCE(ln.has_suspicious_ln_within_60d = 1, FALSE) AS park_x12_abnormal_ln
    FROM nodule_raw r
    LEFT JOIN `{TABLE_LN_CTX}` ln USING (nodule_id)
)
SELECT
    j.nodule_id, j.research_id, j.us_exam_id, j.exam_date,
    j.park_x1_taller, j.park_x2_halo, j.park_x3_well_circumscribed,
    j.park_x4_microlobulation, j.park_x5_infiltrative_margin,
    j.park_x6_marked_hypo, j.park_x7_hypo, j.park_x8_homogeneous,
    j.park_x9_mainly_cystic, j.park_x10_solid, j.park_x11_microcalc,
    j.park_x12_abnormal_ln,
    o.nodule_path_malignant AS malignant_label,
    o.linkage_method,
    o.lat_norm
FROM ln_join j
INNER JOIN `{TABLE_OUTCOME_V1}` o USING (nodule_id)
WHERE o.path_label_present = TRUE
  AND o.nodule_path_malignant IS NOT NULL
"""


def _persist_label_flip(client: bigquery.Client, dry_run: bool) -> None:
    """Compare v1 (patient-level) labels to v2 (nodule-level) per-nodule.

    Writes pub_workspace.qc_phase_b6_park_label_flip_v1 with the per-nodule diff
    so we can quantify how many nodules changed labels.
    """
    flip_sql = f"""
    CREATE OR REPLACE TABLE `{TABLE_LABEL_FLIP}` AS
    WITH v1 AS (
      SELECT nodule_id, malignant_label AS v1_label, split AS v1_split
      FROM `{TABLE_SPLIT_V1}`
    ),
    v2 AS (
      SELECT nodule_id, malignant_label AS v2_label, split AS v2_split, linkage_method
      FROM `{TABLE_SPLIT_V2}`
    )
    SELECT
      COALESCE(v1.nodule_id, v2.nodule_id) AS nodule_id,
      v1.v1_label, v2.v2_label,
      v1.v1_split, v2.v2_split,
      v2.linkage_method,
      CASE
        WHEN v1.v1_label IS NULL AND v2.v2_label IS NOT NULL THEN 'added_in_v2'
        WHEN v1.v1_label IS NOT NULL AND v2.v2_label IS NULL THEN 'dropped_in_v2'
        WHEN v1.v1_label = v2.v2_label THEN 'unchanged'
        WHEN v1.v1_label = 1 AND v2.v2_label = 0 THEN 'mal_to_ben'
        WHEN v1.v1_label = 0 AND v2.v2_label = 1 THEN 'ben_to_mal'
        ELSE 'other'
      END AS flip_class
    FROM v1
    FULL OUTER JOIN v2 USING (nodule_id)
    """
    if dry_run:
        print("DRY-RUN: Would create qc_phase_b6_park_label_flip_v1")
        return
    client.query(flip_sql).result()
    summary = client.query(
        f"SELECT flip_class, COUNT(*) n FROM `{TABLE_LABEL_FLIP}` GROUP BY 1 ORDER BY n DESC"
    ).result()
    print("\n=== Label flip summary (v1 patient-level vs v2 nodule-level) ===")
    for r in summary:
        print(f"  {r.flip_class:25s} n={r.n}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Park cohort refit v2 (nodule-level)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--project", default=PROJECT)
    args = parser.parse_args()

    client = bigquery.Client(project=args.project)

    print("=== Phase B.6 v2 Park cohort refit (nodule-level path linkage) ===")
    print(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")

    # 1. Load corpus
    print("INFO: Fetching labelled corpus from us_nodule_path_outcome_v1 ...")
    df = client.query(CORPUS_SQL).to_dataframe()
    print(f"INFO: Corpus size: {len(df)} nodules with nodule-level path label.")
    print(f"      malignant: {(df['malignant_label']==1).sum()}")
    print(f"      benign:    {(df['malignant_label']==0).sum()}")

    if len(df) < 100:
        sys.exit(
            f"ERROR: Corpus too small ({len(df)} nodules). "
            "Check us_nodule_path_outcome_v1 and acceptance gates."
        )

    X = df[X_COLS].astype(float).values
    y = df["malignant_label"].astype(int).values

    # 2. 70/30 stratified train/test split (same as v1 for reproducibility)
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

    # 5. AUC gate decision tree (per prompt §3d)
    if train_auc >= 0.78 and test_auc >= 0.75:
        confidence = "high"
        gate_msg = "PASS (high)"
    elif train_auc >= 0.75 and test_auc >= 0.70:
        confidence = "medium"
        gate_msg = "PASS (medium)"
    elif train_auc >= 0.72 or test_auc >= 0.68:
        confidence = "low"
        gate_msg = "MARGINAL — surgical-cohort signal compression flag"
    else:
        confidence = "halt"
        gate_msg = "HALT — train AUC < 0.72 AND test AUC < 0.68"
    print(f"INFO: AUC gate -> {gate_msg}")

    # 6. Extract coefficients
    intercept = float(clf.intercept_[0])
    x_names = [
        "x1_taller", "x2_halo", "x3_well_circumscribed", "x4_microlobulation",
        "x5_infiltrative_margin", "x6_marked_hypo", "x7_hypo", "x8_homogeneous",
        "x9_mainly_cystic", "x10_solid", "x11_microcalc", "x12_abnormal_ln",
    ]
    beta_manifest = {n: float(c) for n, c in zip(x_names, clf.coef_[0])}

    # 7. Update manifest
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
        refit_set["linkage_strategy"] = (
            "nodule-level via laterality-aware per-side match in "
            "pub_workspace.us_nodule_path_outcome_v1 (Phase B.6 v2). "
            "Per-laterality malignancy match within -90/+365d of US exam, with "
            "contralateral-only-malignancy treated as benign for the unaffected "
            "side. Bilateral malignancy applies to all real-side nodules."
        )
        refit_set["source"] = (
            f"This cohort (thyroid-canonical-pub-2026), "
            f"70/30 train/holdout, random_state=42, "
            f"sklearn LogisticRegression(penalty='l2', C=1.0, solver='lbfgs', max_iter=1000). "
            f"Train n={len(y_train)}, test n={len(y_test)}. "
            f"Train AUC={train_auc:.4f}, Test AUC={test_auc:.4f}. "
            f"Linkage v2 (nodule-level laterality-aware). "
            f"Supersedes v1 patient-level (which produced Test AUC=0.6611)."
        )
        with open(MANIFEST_PATH, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"INFO: Updated manifest at {MANIFEST_PATH}")

    # 8. Persist train/test split to BQ (v2)
    split_df = df[["nodule_id", "research_id", "linkage_method", "lat_norm"]].copy()
    split_df["split"] = "test"
    split_df.loc[idx_train, "split"] = "train"
    split_df["malignant_label"] = df["malignant_label"].values
    split_df["refit_run_at"] = datetime.now(timezone.utc).isoformat()

    if args.dry_run:
        print(f"DRY-RUN: Would write {len(split_df)} rows to {TABLE_SPLIT_V2}")
    else:
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = client.load_table_from_dataframe(split_df, TABLE_SPLIT_V2, job_config=job_config)
        job.result()
        print(f"INFO: Wrote {len(split_df)} rows to {TABLE_SPLIT_V2}")

    # 9. Persist label-flip diagnostic
    _persist_label_flip(client, args.dry_run)

    print("\n=== Cohort refit v2 complete ===")
    print(f"  Intercept: {intercept:.4f}")
    for k, v in beta_manifest.items():
        print(f"  β {k:30s}: {v:+.4f}")
    print(f"  Train AUC: {train_auc:.4f}")
    print(f"  Test  AUC: {test_auc:.4f}")
    print(f"  Confidence: {confidence}")

    if confidence == "halt":
        print(
            "\n!!! HALT !!! AUC gate failed (both train < 0.72 AND test < 0.68). "
            "Do NOT proceed to Step 4 cohort-refit re-scoring. "
            "Surface to Logan with: training-set N, per-X mean/std by label, "
            "and recommendation."
        )
        sys.exit(1)

    if confidence == "low":
        print(
            "\nMARGINAL: AUC in marginal band. Stamp confidence='low' in audit trail "
            "and treat cohort_refit set as 'this surgical cohort produces weak "
            "Park-feature discrimination' rather than a primary risk model."
        )


if __name__ == "__main__":
    main()
