"""Phase B.6 Step 5 — Unified audit.

Computes:
  5a. Per-set category distribution (with fractions).
  5b. Three-way concordance on benign-vs-suspicious binary (P4∪P5).
  5c. AUC vs final pathology per set (using us_nodule_path_outcome_v1 labels).

Also persists results to:
  - pub_workspace.qc_phase_b6_park_distribution_v1
  - pub_workspace.qc_phase_b6_park_concordance_v1
  - pub_workspace.qc_phase_b6_park_auc_v1

These are referenced in the Phase B closure DFL row + Linear THY-30 comment.
"""
from __future__ import annotations
import json
from pathlib import Path
from google.cloud import bigquery

try:
    import pandas as pd
    from sklearn.metrics import roc_auc_score
except ImportError as e:
    raise SystemExit(f"Missing dep: {e}")

PROJECT = "thyroid-canonical-pub-2026"
client = bigquery.Client(project=PROJECT)

OUT = {}


def q(sql: str, label: str) -> list:
    print(f"\n=== {label} ===")
    rows = list(client.query(sql).result())
    for r in rows:
        print("  ", dict(r))
    return rows


# ---------- 5a: Per-set distribution ----------
print("\n##### 5a — Per-set category distribution #####")
dist_rows = q(
    """
    WITH by_set AS (
      SELECT 'park_2009_original' AS set_name, park2009_category AS cat FROM `pub_canonical.canonical_us_nodule_tirads_multisystem_v1`
      WHERE park2009_category IS NOT NULL
      UNION ALL
      SELECT 'park_cosmos_validation', park_cosmos_category FROM `pub_canonical.canonical_us_nodule_tirads_multisystem_v1`
      WHERE park_cosmos_category IS NOT NULL
      UNION ALL
      SELECT 'park_cohort_refit_v2', park_cohort_category FROM `pub_canonical.canonical_us_nodule_tirads_multisystem_v1`
      WHERE park_cohort_category IS NOT NULL
    ),
    totals AS (
      SELECT set_name, COUNT(*) AS total_n FROM by_set GROUP BY 1
    )
    SELECT
      b.set_name, b.cat, COUNT(*) AS n,
      ROUND(COUNT(*) / t.total_n, 4) AS frac
    FROM by_set b
    JOIN totals t USING (set_name)
    GROUP BY 1, 2, t.total_n
    ORDER BY 1, 2
    """,
    "5a: per-set category distribution",
)
OUT["5a_distribution"] = [dict(r) for r in dist_rows]


# Persist 5a
client.query(
    """
    CREATE OR REPLACE TABLE `pub_workspace.qc_phase_b6_park_distribution_v1` AS
    WITH by_set AS (
      SELECT 'park_2009_original' AS set_name, park2009_category AS cat FROM `pub_canonical.canonical_us_nodule_tirads_multisystem_v1`
      WHERE park2009_category IS NOT NULL
      UNION ALL
      SELECT 'park_cosmos_validation', park_cosmos_category FROM `pub_canonical.canonical_us_nodule_tirads_multisystem_v1`
      WHERE park_cosmos_category IS NOT NULL
      UNION ALL
      SELECT 'park_cohort_refit_v2', park_cohort_category FROM `pub_canonical.canonical_us_nodule_tirads_multisystem_v1`
      WHERE park_cohort_category IS NOT NULL
    ),
    totals AS (
      SELECT set_name, COUNT(*) AS total_n FROM by_set GROUP BY 1
    )
    SELECT b.set_name, b.cat, COUNT(*) AS n,
      ROUND(COUNT(*) / t.total_n, 4) AS frac
    FROM by_set b
    JOIN totals t USING (set_name)
    GROUP BY 1, 2, t.total_n
    """
).result()
print("  -> persisted to pub_workspace.qc_phase_b6_park_distribution_v1")


# ---------- 5b: Three-way concordance on suspicious binary ----------
print("\n##### 5b — Three-way concordance on suspicious binary (P4∪P5) #####")
concord_rows = q(
    """
    WITH binarized AS (
      SELECT
        nodule_id,
        park2009_category IN ('P4','P5') AS sus_2009,
        park_cosmos_category IN ('P4','P5') AS sus_cosmos,
        park_cohort_category IN ('P4','P5') AS sus_cohort
      FROM `pub_canonical.canonical_us_nodule_tirads_multisystem_v1`
      WHERE park2009_category IS NOT NULL
        AND park_cosmos_category IS NOT NULL
        AND park_cohort_category IS NOT NULL
    )
    SELECT
      COUNT(*) AS n,
      ROUND(COUNTIF(sus_2009 = sus_cohort) / COUNT(*), 4) AS agreement_2009_vs_cohort,
      ROUND(COUNTIF(sus_2009 = sus_cosmos) / COUNT(*), 4) AS agreement_2009_vs_cosmos,
      ROUND(COUNTIF(sus_cohort = sus_cosmos) / COUNT(*), 4) AS agreement_cohort_vs_cosmos,
      COUNTIF(sus_2009) AS n_2009_suspicious,
      COUNTIF(sus_cosmos) AS n_cosmos_suspicious,
      COUNTIF(sus_cohort) AS n_cohort_suspicious
    FROM binarized
    """,
    "5b: three-way concordance (suspicious = P4∪P5)",
)
OUT["5b_concordance"] = [dict(r) for r in concord_rows]

client.query(
    """
    CREATE OR REPLACE TABLE `pub_workspace.qc_phase_b6_park_concordance_v1` AS
    WITH binarized AS (
      SELECT
        nodule_id,
        park2009_category IN ('P4','P5') AS sus_2009,
        park_cosmos_category IN ('P4','P5') AS sus_cosmos,
        park_cohort_category IN ('P4','P5') AS sus_cohort
      FROM `pub_canonical.canonical_us_nodule_tirads_multisystem_v1`
      WHERE park2009_category IS NOT NULL
        AND park_cosmos_category IS NOT NULL
        AND park_cohort_category IS NOT NULL
    )
    SELECT
      COUNT(*) AS n,
      ROUND(COUNTIF(sus_2009 = sus_cohort) / COUNT(*), 4) AS agreement_2009_vs_cohort,
      ROUND(COUNTIF(sus_2009 = sus_cosmos) / COUNT(*), 4) AS agreement_2009_vs_cosmos,
      ROUND(COUNTIF(sus_cohort = sus_cosmos) / COUNT(*), 4) AS agreement_cohort_vs_cosmos,
      COUNTIF(sus_2009) AS n_2009_suspicious,
      COUNTIF(sus_cosmos) AS n_cosmos_suspicious,
      COUNTIF(sus_cohort) AS n_cohort_suspicious
    FROM binarized
    """
).result()
print("  -> persisted to pub_workspace.qc_phase_b6_park_concordance_v1")


# ---------- 5c: AUC vs final pathology ----------
print("\n##### 5c — AUC vs final pathology per set #####")
df = client.query(
    """
    SELECT
      m.nodule_id,
      m.park2009_probability, m.park_cosmos_probability, m.park_cohort_probability,
      o.nodule_path_malignant
    FROM `pub_canonical.canonical_us_nodule_tirads_multisystem_v1` m
    JOIN `pub_workspace.us_nodule_path_outcome_v1` o USING (nodule_id)
    WHERE o.path_label_present
      AND o.nodule_path_malignant IS NOT NULL
      AND m.park2009_probability IS NOT NULL
      AND m.park_cohort_probability IS NOT NULL
    """
).to_dataframe()
print(f"AUC corpus: {len(df)} nodules with path label AND park probabilities")

aucs = {}
for set_name, col in [
    ("park_2009_original", "park2009_probability"),
    ("park_cosmos_validation", "park_cosmos_probability"),
    ("park_cohort_refit_v2", "park_cohort_probability"),
]:
    auc = roc_auc_score(df["nodule_path_malignant"].astype(int).values, df[col].values)
    aucs[set_name] = round(float(auc), 4)
    print(f"  {set_name:25s}  AUC = {auc:.4f}")

OUT["5c_auc_vs_path"] = {"n": int(len(df)), "auc": aucs}

# Persist 5c via a small temp table
auc_rows = [
    {"set_name": k, "auc_vs_path": v, "n": int(len(df))} for k, v in aucs.items()
]
auc_df = pd.DataFrame(auc_rows)
job = client.load_table_from_dataframe(
    auc_df, f"{PROJECT}.pub_workspace.qc_phase_b6_park_auc_v1",
    job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
)
job.result()
print("  -> persisted to pub_workspace.qc_phase_b6_park_auc_v1")


# Save full results to JSON (for README + DFL)
def _ser(o):
    if isinstance(o, dict):
        return {k: _ser(v) for k, v in o.items()}
    if isinstance(o, list):
        return [_ser(x) for x in o]
    if hasattr(o, "isoformat"):
        return o.isoformat()
    return o


out_path = Path("exports/phase_b_deterministic_scorers_20260507/phase_b6_audit_results.json")
out_path.parent.mkdir(parents=True, exist_ok=True)
out_path.write_text(json.dumps(_ser(OUT), indent=2))
print(f"\nSaved unified audit results to {out_path}")
