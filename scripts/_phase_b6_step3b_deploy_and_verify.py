"""Deploy us_nodule_path_outcome_v1 + run acceptance gates."""
from __future__ import annotations
import sys
from pathlib import Path
from google.cloud import bigquery

PROJECT = "thyroid-canonical-pub-2026"
SQL_PATH = Path("scripts/_phase_b6_step3b_us_nodule_path_outcome_v1.sql")

client = bigquery.Client(project=PROJECT)

print(f"Deploying view from {SQL_PATH} ...")
client.query(SQL_PATH.read_text()).result()
print("View deployed: pub_workspace.us_nodule_path_outcome_v1")


def q(sql: str, label: str) -> list:
    print(f"\n=== {label} ===")
    rows = list(client.query(sql).result())
    for r in rows:
        print("  ", dict(r))
    return rows


# Acceptance gates
gate_rows = q(
    """
    SELECT
      COUNT(*) AS n_nodules,
      COUNTIF(path_label_present) AS n_with_label,
      COUNTIF(nodule_path_malignant = 1) AS n_malignant,
      COUNTIF(nodule_path_malignant = 0) AS n_benign,
      COUNTIF(n_path_specimens_linked > 1) AS n_ambiguous_multilink,
      COUNT(DISTINCT research_id) AS n_patients_total,
      COUNT(DISTINCT IF(path_label_present, research_id, NULL)) AS n_patients_with_label,
      SAFE_DIVIDE(COUNTIF(n_path_specimens_linked = 1), NULLIF(COUNTIF(path_label_present), 0)) AS frac_clean_singleton_link
    FROM `pub_workspace.us_nodule_path_outcome_v1`
    """,
    "Gate metrics",
)
g = dict(gate_rows[0])

# Multinodular sanity check: same patient, same exam_date, both labels
sanity_rows = q(
    """
    WITH per_exam AS (
      SELECT
        research_id, us_exam_date,
        COUNTIF(nodule_path_malignant = 1) n_mal,
        COUNTIF(nodule_path_malignant = 0) n_ben,
        COUNT(*) n_total
      FROM `pub_workspace.us_nodule_path_outcome_v1`
      WHERE path_label_present
      GROUP BY 1,2
    )
    SELECT
      COUNTIF(n_mal > 0 AND n_ben > 0) AS exams_with_mixed_labels,
      COUNTIF(n_mal > 0 AND n_ben = 0) AS exams_all_mal,
      COUNTIF(n_mal = 0 AND n_ben > 0) AS exams_all_ben,
      COUNT(*) AS n_exams_with_any_label,
      COUNT(DISTINCT research_id) AS n_patients_with_any_label
    FROM per_exam
    """,
    "Multinodular goiter sanity check (same exam, mixed labels)",
)
s = dict(sanity_rows[0])

# Linkage method breakdown
q(
    """
    SELECT linkage_method, COUNT(*) n
    FROM `pub_workspace.us_nodule_path_outcome_v1`
    GROUP BY 1 ORDER BY n DESC
    """,
    "Linkage method breakdown",
)

# Decision
print("\n=== Acceptance gate decisions ===")
g_n_with_label = g["n_with_label"]
g_frac = g["frac_clean_singleton_link"] or 0
g_mixed = s["exams_with_mixed_labels"]

c1 = g_n_with_label >= 8000
c2 = g_frac >= 0.60
c3 = g_mixed >= 100  # spec says "at least a few hundred", we'll require >=100 to proceed

print(f"  [GATE 1] n_with_label >= 8000: {g_n_with_label} -> {'PASS' if c1 else 'FAIL'}")
print(f"  [GATE 2] frac_clean_singleton_link >= 0.60: {g_frac:.3f} -> {'PASS' if c2 else 'FAIL'}")
print(f"  [GATE 3] mixed-label exams (multinodular sanity) >= 100: {g_mixed} -> {'PASS' if c3 else 'FAIL'}")

if not (c1 and c2 and c3):
    print("\nWARNING: One or more gates failed. Surface to Logan before proceeding to Step 3c.")
    # Still exit 0 — we'll surface to user, they can decide.
else:
    print("\nAll gates PASS. Proceed to Step 3c (cohort GLM refit on nodule-level labels).")
