"""Per-manuscript inclusion/exclusion CONSORT-style flow CSVs.

Mirrors M044's m044_inclusion_flow_qc.csv pattern. For each non-M044 manuscript,
writes <manuscript>_inclusion_flow.csv with step-by-step waterfall.
"""
import os, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
import duckdb, csv

OUT_DIR = Path("/Users/ros/THyroid 2026/snowflake_trial/reports/inclusion_flows")
OUT_DIR.mkdir(parents=True, exist_ok=True)
MD_TOKEN = os.environ.get("MOTHERDUCK_TOKEN") or os.environ.get("motherduck_token")
md = duckdb.connect(f"md:thyroid_canonical_publication_v1_0?motherduck_token={MD_TOKEN}")

def waterfall(manuscript, steps):
    """Each step: (criterion_label, sql_query_returning_count)."""
    rows = []
    prev_n = None
    for i, (label, sql) in enumerate(steps, 1):
        n = md.execute(sql).fetchone()[0]
        excluded = (prev_n - n) if prev_n is not None else 0
        rows.append({"step": i, "criterion": label, "n_remaining": n, "excluded_at_step": excluded})
        prev_n = n
    out = OUT_DIR / f"{manuscript}_inclusion_flow.csv"
    with open(out, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=["step","criterion","n_remaining","excluded_at_step"])
        w.writeheader()
        w.writerows(rows)
    print(f"  ✓ {manuscript}: {len(rows)} steps -> {out.name}")
    return rows

# === M032 — 25-yr Descriptive ===
print("=== M032 25-yr Descriptive ===")
waterfall("M032", [
    ("All publication-eligible patients", "SELECT COUNT(*) FROM main.canonical_patient_master"),
    ("With first surgery date known", "SELECT COUNT(*) FROM main.canonical_patient_master WHERE first_surgery_date IS NOT NULL"),
    ("Surgery 1999-2025", "SELECT COUNT(*) FROM main.canonical_patient_master WHERE first_surgery_date BETWEEN DATE '1999-01-01' AND DATE '2025-12-31'"),
    ("Final M032 cohort", "SELECT COUNT(*) FROM manuscript_workspace.cohort_m032_descriptive_25yr_v1"),
])

# === M037 — LN Predictors ===
print("\n=== M037 LN Predictors ===")
waterfall("M037", [
    ("All publication-eligible patients", "SELECT COUNT(*) FROM main.canonical_patient_master"),
    ("Malignant histology", "SELECT COUNT(*) FROM main.canonical_patient_master WHERE is_malignant"),
    ("LN staging assigned (N0/N1a/N1b/Nx)", "SELECT COUNT(*) FROM main.canonical_patient_master WHERE is_malignant AND ajcc8_n_stage IS NOT NULL"),
    ("Final M037 cohort", "SELECT COUNT(*) FROM manuscript_workspace.cohort_m037_ln_metastasis_v1"),
])

# === M025 — TIRADS Performance ===
print("\n=== M025 TIRADS ===")
waterfall("M025", [
    ("All publication-eligible patients", "SELECT COUNT(*) FROM main.canonical_patient_master"),
    ("Has any US exam", "SELECT COUNT(*) FROM main.canonical_patient_master WHERE us_first_exam_date IS NOT NULL"),
    ("Has TIRADS-categorized exam (post-mig_288)", "SELECT COUNT(*) FROM main.canonical_patient_master WHERE tirads_resolved IS NOT NULL"),
    ("Final M025 cohort", "SELECT COUNT(*) FROM manuscript_workspace.cohort_m025_tirads_performance_v1"),
])

# === M038 — Massive Goiter ===
print("\n=== M038 Goiter ===")
waterfall("M038", [
    ("All publication-eligible patients", "SELECT COUNT(*) FROM main.canonical_patient_master"),
    ("Final M038 cohort (full PUB; weight-known subset analytic)", "SELECT COUNT(*) FROM main.cohort_m038_massive_goiter_v1"),
    ("Weight known (gland_weight_final_g IS NOT NULL)", "SELECT COUNT(*) FROM main.cohort_m038_massive_goiter_v1 WHERE gland_weight_final_g IS NOT NULL"),
    ("Massive (≥200g)", "SELECT COUNT(*) FROM main.cohort_m038_massive_goiter_v1 WHERE gland_weight_final_g >= 200"),
])

# === M004 — Autoimmune+Cancer ===
print("\n=== M004 Autoimmune ===")
waterfall("M004", [
    ("All publication-eligible patients", "SELECT COUNT(*) FROM main.canonical_patient_master"),
    ("Final M004 cohort (full PUB)", "SELECT COUNT(*) FROM manuscript_workspace.cohort_m004_autoimmune_cancer_v1"),
    ("Hashimoto-positive (NLP+syn combined)", "SELECT COUNT(*) FROM manuscript_workspace.cohort_m004_autoimmune_cancer_v1 WHERE has_hashi"),
    ("Graves-positive (NLP+syn combined)", "SELECT COUNT(*) FROM manuscript_workspace.cohort_m004_autoimmune_cancer_v1 WHERE has_graves"),
    ("Either autoimmune-positive", "SELECT COUNT(*) FROM manuscript_workspace.cohort_m004_autoimmune_cancer_v1 WHERE has_hashi OR has_graves"),
])

md.close()
print(f"\n=== ALL inclusion flows saved to {OUT_DIR} ===")
