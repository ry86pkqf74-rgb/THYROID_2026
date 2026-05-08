"""Phase D.2 — Park 2009 X-variable prevalence audit.

D.2.a: Compute per-X prevalence in this cohort (overall + by path label).
D.2.b: Build comparison-to-Park-2009 table with shift_severity classification.
D.2.c artifacts: produces a JSON summary for use in the markdown document.

Tables written:
  pub_workspace.qc_park_x_prevalence_audit_v1
  pub_workspace.qc_park_x_prevalence_vs_park2009_v1

Also writes exports/phase_d_park_prevalence_audit_20260508/
  - park_x_prevalence_audit.csv
  - park_x_vs_park2009.csv
  - d2_summary.json
"""
from __future__ import annotations
import json
import csv
from pathlib import Path
from datetime import datetime
from google.cloud import bigquery

PROJECT = "thyroid-canonical-pub-2026"
client = bigquery.Client(project=PROJECT)
TODAY = datetime.now().strftime("%Y%m%d")
OUT_DIR = Path(f"exports/phase_d_park_prevalence_audit_{TODAY}")
OUT_DIR.mkdir(parents=True, exist_ok=True)


def run(sql: str, label: str) -> None:
    print(f"\n--- {label} ---")
    job = client.query(sql)
    job.result()
    print("  done")


def qrows(sql: str, label: str) -> list[dict]:
    print(f"\n=== {label} ===")
    rows = [dict(r) for r in client.query(sql).result()]
    for r in rows:
        print(" ", r)
    return rows


# ── D.2.a — Per-X prevalence by path_label ──────────────────────────────────
run(
    """
    CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_workspace.qc_park_x_prevalence_audit_v1` AS
    WITH cohort AS (
      SELECT
        m.park_x1_taller,     m.park_x2_halo,      m.park_x3_well_circumscribed,
        m.park_x4_microlobulation, m.park_x5_infiltrative_margin,
        m.park_x6_marked_hypo, m.park_x7_hypo,    m.park_x8_homogeneous,
        m.park_x9_mainly_cystic, m.park_x10_solid,
        m.park_x11_microcalc, m.park_x12_abnormal_ln,
        CASE
          WHEN p.nodule_path_malignant = 1 THEN 'malignant'
          WHEN p.nodule_path_malignant = 0 THEN 'benign'
          ELSE 'unlabeled'
        END AS path_label
      FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_us_nodule_tirads_multisystem_v1` m
      LEFT JOIN `thyroid-canonical-pub-2026.pub_workspace.us_nodule_path_outcome_v1` p
        USING (nodule_id)
      WHERE m.park2009_category IS NOT NULL
    )
    SELECT
      path_label,
      COUNT(*) AS n,
      ROUND(COUNTIF(park_x1_taller)       / COUNT(*), 4) AS prev_x1_taller,
      ROUND(COUNTIF(park_x2_halo)         / COUNT(*), 4) AS prev_x2_halo,
      ROUND(COUNTIF(park_x3_well_circumscribed) / COUNT(*), 4) AS prev_x3_well_circumscribed,
      ROUND(COUNTIF(park_x4_microlobulation) / COUNT(*), 4) AS prev_x4_microlobulation,
      ROUND(COUNTIF(park_x5_infiltrative_margin) / COUNT(*), 4) AS prev_x5_infiltrative_margin,
      ROUND(COUNTIF(park_x6_marked_hypo)  / COUNT(*), 4) AS prev_x6_marked_hypo,
      ROUND(COUNTIF(park_x7_hypo)         / COUNT(*), 4) AS prev_x7_hypo,
      ROUND(COUNTIF(park_x8_homogeneous)  / COUNT(*), 4) AS prev_x8_homogeneous,
      ROUND(COUNTIF(park_x9_mainly_cystic)/ COUNT(*), 4) AS prev_x9_mainly_cystic,
      ROUND(COUNTIF(park_x10_solid)       / COUNT(*), 4) AS prev_x10_solid,
      ROUND(COUNTIF(park_x11_microcalc)   / COUNT(*), 4) AS prev_x11_microcalc,
      ROUND(COUNTIF(park_x12_abnormal_ln) / COUNT(*), 4) AS prev_x12_abnormal_ln
    FROM cohort
    GROUP BY path_label
    """,
    "D.2.a — qc_park_x_prevalence_audit_v1",
)

prev_rows = qrows(
    "SELECT * FROM `thyroid-canonical-pub-2026.pub_workspace.qc_park_x_prevalence_audit_v1` ORDER BY path_label",
    "D.2.a — cohort prevalences by path_label",
)

# ── D.2.b — Comparison-to-Park-2009 wide manifest ───────────────────────────
# Extract this-cohort overall (unlabeled row = all nodules with park category)
# If 'unlabeled' doesn't cover everything, fall back to total row.
# Use ALL rows (not just labeled) for the overall prevalence.

run(
    """
    CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_workspace.qc_park_x_prevalence_audit_v1_overall` AS
    WITH cohort AS (
      SELECT
        m.park_x1_taller,     m.park_x2_halo,      m.park_x3_well_circumscribed,
        m.park_x4_microlobulation, m.park_x5_infiltrative_margin,
        m.park_x6_marked_hypo, m.park_x7_hypo,    m.park_x8_homogeneous,
        m.park_x9_mainly_cystic, m.park_x10_solid,
        m.park_x11_microcalc, m.park_x12_abnormal_ln
      FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_us_nodule_tirads_multisystem_v1` m
      WHERE m.park2009_category IS NOT NULL
    )
    SELECT
      COUNT(*) AS n,
      ROUND(COUNTIF(park_x1_taller)       / COUNT(*), 4) AS prev_x1_taller,
      ROUND(COUNTIF(park_x2_halo)         / COUNT(*), 4) AS prev_x2_halo,
      ROUND(COUNTIF(park_x3_well_circumscribed) / COUNT(*), 4) AS prev_x3_well_circumscribed,
      ROUND(COUNTIF(park_x4_microlobulation) / COUNT(*), 4) AS prev_x4_microlobulation,
      ROUND(COUNTIF(park_x5_infiltrative_margin) / COUNT(*), 4) AS prev_x5_infiltrative_margin,
      ROUND(COUNTIF(park_x6_marked_hypo)  / COUNT(*), 4) AS prev_x6_marked_hypo,
      ROUND(COUNTIF(park_x7_hypo)         / COUNT(*), 4) AS prev_x7_hypo,
      ROUND(COUNTIF(park_x8_homogeneous)  / COUNT(*), 4) AS prev_x8_homogeneous,
      ROUND(COUNTIF(park_x9_mainly_cystic)/ COUNT(*), 4) AS prev_x9_mainly_cystic,
      ROUND(COUNTIF(park_x10_solid)       / COUNT(*), 4) AS prev_x10_solid,
      ROUND(COUNTIF(park_x11_microcalc)   / COUNT(*), 4) AS prev_x11_microcalc,
      ROUND(COUNTIF(park_x12_abnormal_ln) / COUNT(*), 4) AS prev_x12_abnormal_ln
    FROM cohort
    """,
    "D.2.a (overall, all nodules with park category)",
)

overall_rows = qrows(
    "SELECT * FROM `thyroid-canonical-pub-2026.pub_workspace.qc_park_x_prevalence_audit_v1_overall`",
    "D.2.a — overall cohort prevalences",
)
overall = overall_rows[0] if overall_rows else {}

# Park 2009 published prevalence table (from secondary literature)
PARK_2009 = [
    ("X1_taller",             "Taller-than-wide shape",          0.22, 0.50, 0.12),
    ("X2_halo",               "Perinodular halo",                0.31, 0.14, 0.37),
    ("X3_well_circumscribed", "Well-circumscribed margin",        0.37, 0.14, 0.45),
    ("X4_microlobulation",    "Microlobulated margin",           0.14, 0.30, 0.09),
    ("X5_infiltrative_margin","Infiltrative margin",             0.10, 0.22, 0.06),
    ("X6_marked_hypo",        "Marked hypoechogenicity",         0.09, 0.22, 0.05),
    ("X7_hypo",               "Hypoechogenicity",                0.33, 0.50, 0.28),
    ("X8_homogeneous",        "Homogeneous echotexture",         0.58, 0.62, 0.57),
    ("X9_mainly_cystic",      "Mainly cystic",                   0.09, 0.02, 0.12),
    ("X10_solid",             "Solid composition",               0.73, 0.85, 0.70),
    ("X11_microcalc",         "Microcalcification",              0.12, 0.30, 0.06),
    ("X12_abnormal_ln",       "Abnormal lymph node at exam",     0.03, 0.09, 0.01),
]

X_MAP = {  # x_var → overall column name in audit table
    "X1_taller":             "prev_x1_taller",
    "X2_halo":               "prev_x2_halo",
    "X3_well_circumscribed": "prev_x3_well_circumscribed",
    "X4_microlobulation":    "prev_x4_microlobulation",
    "X5_infiltrative_margin":"prev_x5_infiltrative_margin",
    "X6_marked_hypo":        "prev_x6_marked_hypo",
    "X7_hypo":               "prev_x7_hypo",
    "X8_homogeneous":        "prev_x8_homogeneous",
    "X9_mainly_cystic":      "prev_x9_mainly_cystic",
    "X10_solid":             "prev_x10_solid",
    "X11_microcalc":         "prev_x11_microcalc",
    "X12_abnormal_ln":       "prev_x12_abnormal_ln",
}

# Grab labeled-subset row from prev_rows for malignant/benign prevalences
labeled_rows = {r["path_label"]: r for r in prev_rows}
mal_row = labeled_rows.get("malignant", {})
ben_row = labeled_rows.get("benign", {})

comparison = []
for x_var, description, park_overall, park_mal, park_ben in PARK_2009:
    col = X_MAP[x_var]
    this_overall = float(overall.get(col, 0) or 0)
    this_mal = float(mal_row.get(col, 0) or 0) if mal_row else 0.0
    this_ben = float(ben_row.get(col, 0) or 0) if ben_row else 0.0
    delta = round(this_overall - park_overall, 4)
    abs_delta = abs(delta)
    if abs_delta > 0.20:
        severity = "large_shift"
    elif abs_delta > 0.10:
        severity = "moderate_shift"
    elif abs_delta > 0.05:
        severity = "mild_shift"
    else:
        severity = "comparable"
    comparison.append({
        "x_var": x_var,
        "description": description,
        "park_overall": park_overall,
        "park_malignant": park_mal,
        "park_benign": park_ben,
        "this_cohort_overall": this_overall,
        "this_cohort_malignant": this_mal,
        "this_cohort_benign": this_ben,
        "prevalence_delta_overall": delta,
        "abs_delta_overall": abs_delta,
        "shift_severity": severity,
        "provenance": "park_2009_secondary_literature",
    })

# Sort by abs_delta descending
comparison.sort(key=lambda r: r["abs_delta_overall"], reverse=True)

print("\n=== D.2.b — Comparison table ===")
for r in comparison:
    print(f"  {r['x_var']:<25} park_overall={r['park_overall']:.0%}  "
          f"this_cohort={r['this_cohort_overall']:.0%}  "
          f"delta={r['prevalence_delta_overall']:+.0%}  severity={r['shift_severity']}")

# Materialize comparison table in BQ
values_sql = ",\n    ".join(
    f"STRUCT('{r['x_var']}' AS x_var, '{r['description']}' AS description, "
    f"{r['park_overall']} AS park_overall, {r['park_malignant']} AS park_malignant, "
    f"{r['park_benign']} AS park_benign, "
    f"{r['this_cohort_overall']} AS this_cohort_overall, "
    f"{r['this_cohort_malignant']} AS this_cohort_malignant, "
    f"{r['this_cohort_benign']} AS this_cohort_benign, "
    f"{r['prevalence_delta_overall']} AS prevalence_delta_overall, "
    f"{r['abs_delta_overall']} AS abs_delta_overall, "
    f"'{r['shift_severity']}' AS shift_severity, "
    f"'{r['provenance']}' AS provenance)"
    for r in comparison
)

run(
    f"""
    CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_workspace.qc_park_x_prevalence_vs_park2009_v1` AS
    SELECT * FROM UNNEST([
      {values_sql}
    ])
    ORDER BY abs_delta_overall DESC
    """,
    "D.2.b — qc_park_x_prevalence_vs_park2009_v1",
)

# ── Write export files ───────────────────────────────────────────────────────
# CSV 1: by-path-label prevalences
with open(OUT_DIR / "park_x_prevalence_audit.csv", "w", newline="") as f:
    if prev_rows:
        writer = csv.DictWriter(f, fieldnames=list(prev_rows[0].keys()))
        writer.writeheader()
        writer.writerows(prev_rows)

# CSV 2: comparison table
with open(OUT_DIR / "park_x_vs_park2009.csv", "w", newline="") as f:
    if comparison:
        writer = csv.DictWriter(f, fieldnames=list(comparison[0].keys()))
        writer.writeheader()
        writer.writerows(comparison)

# JSON summary for D.2.c markdown doc
large_shifts = [r for r in comparison if r["shift_severity"] == "large_shift"]
moderate_shifts = [r for r in comparison if r["shift_severity"] == "moderate_shift"]
summary = {
    "generated_at": TODAY,
    "cohort_n": int(overall.get("n", 0)),
    "labeled_n_malignant": int(labeled_rows.get("malignant", {}).get("n", 0)),
    "labeled_n_benign": int(labeled_rows.get("benign", {}).get("n", 0)),
    "labeled_n_unlabeled": int(labeled_rows.get("unlabeled", {}).get("n", 0)),
    "n_large_shift": len(large_shifts),
    "n_moderate_shift": len(moderate_shifts),
    "large_shift_vars": [r["x_var"] for r in large_shifts],
    "moderate_shift_vars": [r["x_var"] for r in moderate_shifts],
    "comparison": comparison,
}
with open(OUT_DIR / "d2_summary.json", "w") as f:
    json.dump(summary, f, indent=2)

print(f"\n\nExports written to {OUT_DIR}/")
print(f"  Large shifts (>20pp):    {len(large_shifts)} — {[r['x_var'] for r in large_shifts]}")
print(f"  Moderate shifts (10-20pp): {len(moderate_shifts)} — {[r['x_var'] for r in moderate_shifts]}")
print("\nD.2.a + D.2.b complete.")
