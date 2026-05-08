"""
Script 429 — TIRADS 11-system cluster replication analysis
===========================================================
Pre-registered in studies/m085_multisystem_tirads_comparison/06_cluster_replication_analysis.md.

Executes after the disagreement queue (Step 5 of Path A patch) is built, which in turn
depends on Horvath scoring (Step 4) being complete.

Hypotheses tested (H1–H4):
  H1: Hierarchical clustering on 11×11 binary concordance reproduces the 4-system
      Phase B cluster structure (ACR/K-TIRADS vs Kwak/C-TIRADS).
  H2: Between-cluster concordance ≤ 80%, ≥ 10 pp below within-cluster concordance.
  H3: Solid+isoechoic subgroup: Kwak/C-TIRADS assigns ≥ Cat 4A at ≥ 1.5× ACR/K-TIRADS rate.
  H4: Horvath clusters closer to ACR 2017 than to Kwak/C-TIRADS.

Deliverables:
  pub_workspace.qc_tirads_multisystem_cluster_v1   — per-nodule binary suspicion flags
  exports/phase_e_audit_adjudication_20260507/cluster_analysis/
    concordance_matrix.csv   — 11×11 pairwise concordance
    dendrogram.png           — Ward's linkage dendrogram
    cluster_assignments.csv  — system → cluster label
    subgroup_solid_isoechoic.csv — H3 subgroup FNA-recommendation rates
    h1_h4_results.json       — verdict per hypothesis

Usage:
    python scripts/429_tirads_cluster_replication.py [--dry-run]

Prerequisites:
    - pub_canonical.canonical_us_nodule_tirads_multisystem_v1  (all 11 systems populated)
    - pub_workspace.qc_tirads_multisystem_disagreement_v1      (built at Step 5)
    - pub_canonical.canonical_us_nodule_v2                     (for composition/echogenicity filter)
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import bigquery

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

PROJECT = "thyroid-canonical-pub-2026"
DATASET_PUB = "pub_canonical"
DATASET_WS = "pub_workspace"
LOCATION = "us-central1"

TABLE_MULTISYS = f"{PROJECT}.{DATASET_PUB}.canonical_us_nodule_tirads_multisystem_v1"
TABLE_NODULE_V2 = f"{PROJECT}.{DATASET_PUB}.canonical_us_nodule_v2"
TABLE_DISAGQ = f"{PROJECT}.{DATASET_WS}.qc_tirads_multisystem_disagreement_v1"
TABLE_CLUSTER = f"{PROJECT}.{DATASET_WS}.qc_tirads_multisystem_cluster_v1"

OUTPUT_DIR = Path("exports/phase_e_audit_adjudication_20260507/cluster_analysis")
RUN_TS = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

SYSTEMS = ["acr", "kwak", "ktirads", "ctirads", "eu", "ata", "bta", "aace",
           "park2009", "park_cohort", "horvath"]

# Suspicion cutoff (>= 3 on normalized 1-5 scale = suspicious)
SUSP_CUT = 3


def _log(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc).isoformat()}] {msg}", flush=True)


def _run_sql(bq: bigquery.Client, sql: str, label: str) -> None:
    job = bq.query(sql, location=LOCATION)
    job.result()
    _log(f"  ✓ {label}")


# ---------------------------------------------------------------------------
# Step 1 — Build per-nodule binary suspicion table
# ---------------------------------------------------------------------------

BUILD_CLUSTER_SQL = f"""
CREATE OR REPLACE TABLE `{TABLE_CLUSTER}`
CLUSTER BY research_id AS
WITH msys AS (
  SELECT
    m.nodule_id, m.research_id,
    n.composition, n.echogenicity,
    -- Normalized suspicion ordinals (from disagreement queue mapping)
    CASE m.acr2017_category_imputed
      WHEN 'TR1' THEN 1 WHEN 'TR2' THEN 2 WHEN 'TR3' THEN 3
      WHEN 'TR4' THEN 4 WHEN 'TR5' THEN 5 END AS sus_acr,
    CASE m.kwak_category
      WHEN '2' THEN 1 WHEN '3' THEN 2 WHEN '4A' THEN 3
      WHEN '4B' THEN 4 WHEN '4C' THEN 5 WHEN '5' THEN 5 END AS sus_kwak,
    CASE m.ktirads_category
      WHEN '1' THEN 1 WHEN '2' THEN 1 WHEN '3' THEN 2
      WHEN '4' THEN 4 WHEN '5' THEN 5 END AS sus_ktirads,
    CASE m.ctirads_category
      WHEN '2' THEN 1 WHEN '3' THEN 2 WHEN '4A' THEN 3
      WHEN '4B' THEN 4 WHEN '4C' THEN 5 WHEN '5' THEN 5 WHEN '6' THEN 5 END AS sus_ctirads,
    CASE m.eutirads_category
      WHEN 'EU2' THEN 1 WHEN 'EU3' THEN 2 WHEN 'EU4' THEN 3 WHEN 'EU5' THEN 5 END AS sus_eu,
    CASE m.ata_pattern
      WHEN 'benign' THEN 1 WHEN 'very_low' THEN 1 WHEN 'low' THEN 2
      WHEN 'intermediate' THEN 3 WHEN 'high' THEN 5 END AS sus_ata,
    CASE m.bta_category
      WHEN 'U2' THEN 1 WHEN 'U3' THEN 2 WHEN 'U4' THEN 4 WHEN 'U5' THEN 5 END AS sus_bta,
    CASE m.aace_class
      WHEN 1 THEN 1 WHEN 2 THEN 3 WHEN 3 THEN 5 END AS sus_aace,
    CASE m.park2009_category
      WHEN 'P1' THEN 1 WHEN 'P2' THEN 2 WHEN 'P3' THEN 3
      WHEN 'P4' THEN 4 WHEN 'P5' THEN 5 END AS sus_park2009,
    CASE m.park_cohort_category
      WHEN 'P1' THEN 1 WHEN 'P2' THEN 2 WHEN 'P3' THEN 3
      WHEN 'P4' THEN 4 WHEN 'P5' THEN 5 END AS sus_park_cohort,
    CASE m.horvath_category
      WHEN '2' THEN 1 WHEN 'TIRADS_2' THEN 1
      WHEN '3' THEN 2 WHEN 'TIRADS_3' THEN 2
      WHEN '4A' THEN 3 WHEN 'TIRADS_4A' THEN 3
      WHEN '4B' THEN 4 WHEN 'TIRADS_4B' THEN 4
      WHEN '4C' THEN 5 WHEN 'TIRADS_4C' THEN 5
      WHEN '5' THEN 5 WHEN 'TIRADS_5' THEN 5 END AS sus_horvath
  FROM `{TABLE_MULTISYS}` m
  JOIN `{TABLE_NODULE_V2}` n USING (nodule_id)
)
SELECT
  nodule_id, research_id, composition, echogenicity,
  sus_acr, sus_kwak, sus_ktirads, sus_ctirads,
  sus_eu, sus_ata, sus_bta, sus_aace,
  sus_park2009, sus_park_cohort, sus_horvath,
  -- Binary suspicious flags (>= cut = 1)
  CASE WHEN sus_acr       >= {SUSP_CUT} THEN 1 ELSE 0 END AS susp_acr,
  CASE WHEN sus_kwak      >= {SUSP_CUT} THEN 1 ELSE 0 END AS susp_kwak,
  CASE WHEN sus_ktirads   >= {SUSP_CUT} THEN 1 ELSE 0 END AS susp_ktirads,
  CASE WHEN sus_ctirads   >= {SUSP_CUT} THEN 1 ELSE 0 END AS susp_ctirads,
  CASE WHEN sus_eu        >= {SUSP_CUT} THEN 1 ELSE 0 END AS susp_eu,
  CASE WHEN sus_ata       >= {SUSP_CUT} THEN 1 ELSE 0 END AS susp_ata,
  CASE WHEN sus_bta       >= {SUSP_CUT} THEN 1 ELSE 0 END AS susp_bta,
  CASE WHEN sus_aace      >= {SUSP_CUT} THEN 1 ELSE 0 END AS susp_aace,
  CASE WHEN sus_park2009  >= {SUSP_CUT} THEN 1 ELSE 0 END AS susp_park2009,
  CASE WHEN sus_park_cohort >= {SUSP_CUT} THEN 1 ELSE 0 END AS susp_park_cohort,
  CASE WHEN sus_horvath   >= {SUSP_CUT} THEN 1 ELSE 0 END AS susp_horvath,
  -- Solid+isoechoic subgroup flag (H3 analysis)
  CASE WHEN composition = 'solid'
            AND echogenicity IN ('isoechoic', 'hyperechoic') THEN TRUE
       ELSE FALSE END AS solid_isoechoic_flag
FROM msys;
"""


def build_pairwise_concordance(rows: list[dict]) -> dict:
    """Compute 11x11 pairwise binary concordance matrix from per-nodule rows."""
    import itertools

    concordance = {}
    for s1, s2 in itertools.product(SYSTEMS, repeat=2):
        col1, col2 = f"susp_{s1}", f"susp_{s2}"
        n_total = 0
        n_agree = 0
        for r in rows:
            v1 = r.get(col1)
            v2 = r.get(col2)
            if v1 is not None and v2 is not None:
                n_total += 1
                if v1 == v2:
                    n_agree += 1
        concordance[(s1, s2)] = n_agree / max(1, n_total)
    return concordance


def run_hierarchical_clustering(concordance: dict, dry_run: bool) -> dict:
    """Ward's linkage on distance = 1 - concordance."""
    try:
        import numpy as np
        from scipy.cluster.hierarchy import dendrogram, linkage
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        n = len(SYSTEMS)
        mat = np.zeros((n, n))
        for i, s1 in enumerate(SYSTEMS):
            for j, s2 in enumerate(SYSTEMS):
                mat[i, j] = concordance.get((s1, s2), 0.5)

        dist = 1.0 - mat
        # Symmetrize
        dist = (dist + dist.T) / 2
        np.fill_diagonal(dist, 0.0)

        # Ward's linkage on condensed distance matrix
        from scipy.spatial.distance import squareform
        condensed = squareform(dist)
        Z = linkage(condensed, method="ward")

        if not dry_run:
            fig, ax = plt.subplots(figsize=(10, 6))
            dendrogram(Z, labels=SYSTEMS, ax=ax, leaf_rotation=45)
            ax.set_title("11-System TIRADS Cluster Dendrogram (Ward linkage, binary concordance)")
            ax.set_ylabel("Distance (1 - concordance)")
            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            fig.tight_layout()
            fig.savefig(OUTPUT_DIR / "dendrogram.png", dpi=300, bbox_inches="tight")
            plt.close(fig)
            _log(f"  Dendrogram saved to {OUTPUT_DIR}/dendrogram.png")

        # Cut at k=4 clusters
        from scipy.cluster.hierarchy import fcluster
        labels_k2 = fcluster(Z, t=2, criterion="maxclust")
        labels_k4 = fcluster(Z, t=4, criterion="maxclust")

        cluster_map = {s: int(labels_k4[i]) for i, s in enumerate(SYSTEMS)}
        return {"cluster_map_k4": cluster_map, "cluster_map_k2": {s: int(labels_k2[i]) for i, s in enumerate(SYSTEMS)},
                "linkage": Z.tolist()}
    except ImportError as e:
        _log(f"  WARNING: scipy/numpy not available — skipping dendrogram ({e})")
        return {"cluster_map_k4": {}, "cluster_map_k2": {}}


def test_h1_h4(concordance: dict, cluster_map: dict, rows: list[dict]) -> dict:
    """Test pre-specified hypotheses H1–H4."""
    results = {}

    # H1: ACR/K-TIRADS in same cluster, Kwak/C-TIRADS in same cluster
    k4 = cluster_map.get("cluster_map_k4", {})
    acr_cluster = k4.get("acr")
    kt_cluster = k4.get("ktirads")
    kwak_cluster = k4.get("kwak")
    ct_cluster = k4.get("ctirads")
    h1_pass = (acr_cluster is not None and acr_cluster == kt_cluster
               and kwak_cluster is not None and kwak_cluster == ct_cluster
               and acr_cluster != kwak_cluster)
    results["H1"] = {"verdict": "PASS" if h1_pass else "FAIL",
                     "acr_cluster": acr_cluster, "ktirads_cluster": kt_cluster,
                     "kwak_cluster": kwak_cluster, "ctirads_cluster": ct_cluster}

    # H2: Between-cluster concordance <= 80%, at least 10pp below within-cluster
    within_acr_kt = concordance.get(("acr", "ktirads"), 0)
    within_kwak_ct = concordance.get(("kwak", "ctirads"), 0)
    between = concordance.get(("acr", "kwak"), 0)
    h2_pass = (between <= 0.80
               and (within_acr_kt - between) >= 0.10
               and (within_kwak_ct - between) >= 0.10)
    results["H2"] = {"verdict": "PASS" if h2_pass else "FAIL",
                     "within_acr_ktirads": round(within_acr_kt, 4),
                     "within_kwak_ctirads": round(within_kwak_ct, 4),
                     "between_acr_kwak": round(between, 4)}

    # H3: Solid+isoechoic subgroup FNA rate comparison
    solid_iso = [r for r in rows if r.get("solid_isoechoic_flag")]
    if solid_iso:
        n_tot = len(solid_iso)
        acr_kt_rate = sum(1 for r in solid_iso if r.get("susp_acr") == 1 or r.get("susp_ktirads") == 1) / n_tot
        kwak_ct_rate = sum(1 for r in solid_iso if r.get("susp_kwak") == 1 or r.get("susp_ctirads") == 1) / n_tot
        multiplier = kwak_ct_rate / max(0.001, acr_kt_rate)
        h3_pass = multiplier >= 1.5
        results["H3"] = {"verdict": "PASS" if h3_pass else "FAIL",
                         "n_solid_isoechoic": n_tot,
                         "acr_ktirads_rate": round(acr_kt_rate, 4),
                         "kwak_ctirads_rate": round(kwak_ct_rate, 4),
                         "multiplier": round(multiplier, 3)}
    else:
        results["H3"] = {"verdict": "SKIP", "reason": "no solid+isoechoic rows"}

    # H4: Horvath clusters closer to ACR than to Kwak
    c_horvath_acr = concordance.get(("horvath", "acr"), 0)
    c_horvath_kwak = concordance.get(("horvath", "kwak"), 0)
    h4_pass = c_horvath_acr > c_horvath_kwak
    results["H4"] = {"verdict": "PASS" if h4_pass else "FAIL",
                     "horvath_acr_concordance": round(c_horvath_acr, 4),
                     "horvath_kwak_concordance": round(c_horvath_kwak, 4)}

    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="TIRADS 11-system cluster replication")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--project", default=PROJECT)
    args = parser.parse_args()

    bq = bigquery.Client(project=args.project)

    # Check prerequisites
    _log("Step 1: Build per-nodule binary suspicion table")
    if not args.dry_run:
        _run_sql(bq, BUILD_CLUSTER_SQL, f"Build {TABLE_CLUSTER}")
    else:
        _log("  [dry-run] skipping BQ write — using existing table if present")

    n_cluster = int(list(bq.query(
        f"SELECT COUNT(*) FROM `{TABLE_CLUSTER}`", location=LOCATION
    ).result())[0][0])
    _log(f"  Cluster table rows: {n_cluster}")

    _log("Step 2: Pull per-nodule binary flags")
    cols = ["nodule_id", "composition", "echogenicity", "solid_isoechoic_flag"]
    cols += [f"susp_{s}" for s in SYSTEMS]
    rows = [dict(r) for r in bq.query(
        f"SELECT {', '.join(cols)} FROM `{TABLE_CLUSTER}`",
        location=LOCATION,
    ).result()]
    _log(f"  Fetched {len(rows)} rows")

    _log("Step 3: Compute 11×11 pairwise concordance")
    concordance = build_pairwise_concordance(rows)

    # Print 4-system Phase B replication check
    _log("  Phase B 4-system replication:")
    _log(f"    ACR <-> K-TIRADS: {concordance.get(('acr','ktirads'),0):.4f} (expected ~0.969)")
    _log(f"    Kwak <-> C-TIRADS: {concordance.get(('kwak','ctirads'),0):.4f} (expected ~0.995)")
    _log(f"    ACR <-> Kwak: {concordance.get(('acr','kwak'),0):.4f} (expected ~0.77)")

    # Compute unanimous 11-system rate
    n_with_all = sum(1 for r in rows if all(r.get(f"susp_{s}") is not None for s in SYSTEMS))
    n_unanimous = sum(
        1 for r in rows
        if all(r.get(f"susp_{s}") is not None for s in SYSTEMS)
        and len(set(r.get(f"susp_{s}") for s in SYSTEMS)) == 1
    )
    _log(f"  11-system unanimous rate: {n_unanimous}/{n_with_all} = "
         f"{n_unanimous/max(1,n_with_all):.1%}")

    _log("Step 4: Hierarchical clustering")
    cluster_result = run_hierarchical_clustering(concordance, args.dry_run)

    _log("Step 5: Test hypotheses H1–H4")
    h_results = test_h1_h4(concordance, cluster_result, rows)
    for h, res in h_results.items():
        _log(f"  {h}: {res['verdict']} — {res}")

    # Save outputs
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    import csv
    with open(OUTPUT_DIR / "concordance_matrix.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow([""] + SYSTEMS)
        for s1 in SYSTEMS:
            w.writerow([s1] + [f"{concordance.get((s1,s2),0):.4f}" for s2 in SYSTEMS])
    _log(f"  Concordance matrix saved: {OUTPUT_DIR}/concordance_matrix.csv")

    results = {
        "run_ts": RUN_TS, "n_rows": n_cluster, "n_with_all_11": n_with_all,
        "n_unanimous_11": n_unanimous, "unanimous_rate_11": n_unanimous / max(1, n_with_all),
        "cluster_assignments": cluster_result.get("cluster_map_k4", {}),
        "phase_b_replication": {
            "acr_ktirads": round(concordance.get(("acr", "ktirads"), 0), 4),
            "kwak_ctirads": round(concordance.get(("kwak", "ctirads"), 0), 4),
            "acr_kwak_cross": round(concordance.get(("acr", "kwak"), 0), 4),
        },
        "hypotheses": h_results,
    }
    with open(OUTPUT_DIR / "h1_h4_results.json", "w") as f:
        json.dump(results, f, indent=2)
    _log(f"  Results saved: {OUTPUT_DIR}/h1_h4_results.json")

    _log("Step 429 complete.")
    overall = "PASS" if all(v.get("verdict") == "PASS" for v in h_results.values()
                             if v.get("verdict") != "SKIP") else "PARTIAL/FAIL"
    _log(f"  Overall hypothesis result: {overall}")


if __name__ == "__main__":
    main()
