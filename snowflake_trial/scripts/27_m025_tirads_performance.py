"""M025 TIRADS Performance Table — Sensitivity / Specificity / PPV / NPV by TR category.

Cohort: malignant + benign patients with at least one TIRADS-categorized US exam.
Source: CANONICAL_US_PATIENT_MASTER_VIEW_V2_FLAT.MAX_TIRADS_CATEGORY_EVER joined to
CANONICAL_PATIENT_MASTER_FLAT.IS_MALIGNANT.
"""
import sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import get_cursor

OUT = Path("/Users/ros/THyroid 2026/snowflake_trial/reports/m025_tirads_performance.md")
ctx, cur = get_cursor()

print("=== Pulling M025 TIRADS cohort ===")
cur.execute("""
SELECT
  cpm.RESEARCH_ID,
  cpm.IS_MALIGNANT,
  us.MAX_TIRADS_CATEGORY_EVER AS tirads,
  us.HAS_GLAND_FINDINGS_EVER,
  us.PREOP_US_AVAILABLE_FLAG,
  cpm.HISTOLOGY_FINAL,
  cpm.AGE_AT_SURGERY, cpm.SEX
FROM CANONICAL_PATIENT_MASTER_FLAT cpm
JOIN CANONICAL_US_PATIENT_MASTER_VIEW_V2_FLAT us
  ON cpm.RESEARCH_ID = us.RESEARCH_ID
WHERE us.MAX_TIRADS_CATEGORY_EVER IS NOT NULL
""")
rows = cur.fetchall(); cols = [c[0] for c in cur.description]
ctx.close()

import pandas as pd
df = pd.DataFrame(rows, columns=cols)
print(f"  {len(df):,} patients with TIRADS categorization")
print(f"  IS_MALIGNANT distribution: {df['IS_MALIGNANT'].value_counts().to_dict()}")

# TIRADS categories
TR_ORDER = ['TR1', 'TR2', 'TR3', 'TR4', 'TR5']

# Per-category counts
report = ["# M025 — TIRADS Diagnostic Performance Table\n",
          f"**Generated:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n",
          f"**Cohort:** {len(df):,} patients with at least one TIRADS-categorized US exam (max category across exams)\n",
          f"**Source:** CANONICAL_US_PATIENT_MASTER_VIEW_V2_FLAT (post mig_260) joined to CPM\n",
          f"**Note:** Operative cohort — substantially enriched for malignancy vs published ACR cohort (manuscript footnote candidate per round-5 finding)\n\n",
          "## Per-category breakdown\n\n",
          "| TIRADS | n | malignant | ROM% | ACR-expected ROM% |\n| --- | --- | --- | --- | --- |\n"]

acr_expected = {'TR1': '<2%', 'TR2': '<2%', 'TR3': '<5%', 'TR4': '5-20%', 'TR5': '>20%'}
counts = {}
for tr in TR_ORDER:
    sub = df[df['TIRADS'] == tr]
    n = len(sub)
    n_malig = int(sub['IS_MALIGNANT'].fillna(False).astype(bool).sum())
    rom = 100.0 * n_malig / n if n > 0 else 0
    counts[tr] = (n, n_malig)
    report.append(f"| {tr} | {n:,} | {n_malig:,} | {rom:.1f}% | {acr_expected[tr]} |\n")

# Diagnostic performance: treat TR4-5 as "test positive"
report.append("\n## Diagnostic performance — multiple decision thresholds\n\n")

tp_counts = {}
fp_counts = {}
fn_counts = {}
tn_counts = {}
n_total = len(df)
n_malig_total = int(df['IS_MALIGNANT'].fillna(False).astype(bool).sum())
n_benign_total = n_total - n_malig_total

# Iterate thresholds: TR≥2, TR≥3, TR≥4, TR≥5
THRESHOLDS = {
    'TR ≥ TR2': ['TR2','TR3','TR4','TR5'],
    'TR ≥ TR3': ['TR3','TR4','TR5'],
    'TR ≥ TR4': ['TR4','TR5'],
    'TR ≥ TR5': ['TR5'],
}

report.append("| Threshold (test+) | Sens | Spec | PPV | NPV | Accuracy | LR+ | LR- |\n")
report.append("| --- | --- | --- | --- | --- | --- | --- | --- |\n")
for label, pos_set in THRESHOLDS.items():
    test_pos = df[df['TIRADS'].isin(pos_set)]
    test_neg = df[~df['TIRADS'].isin(pos_set)]
    tp = int(test_pos['IS_MALIGNANT'].fillna(False).astype(bool).sum())
    fp = int(len(test_pos) - tp)
    fn = int(test_neg['IS_MALIGNANT'].fillna(False).astype(bool).sum())
    tn = int(len(test_neg) - fn)
    sens = tp / (tp + fn) if (tp + fn) > 0 else 0
    spec = tn / (tn + fp) if (tn + fp) > 0 else 0
    ppv = tp / (tp + fp) if (tp + fp) > 0 else 0
    npv = tn / (tn + fn) if (tn + fn) > 0 else 0
    acc = (tp + tn) / n_total if n_total > 0 else 0
    lr_pos = (sens / (1 - spec)) if (1 - spec) > 0 else float('inf')
    lr_neg = ((1 - sens) / spec) if spec > 0 else float('inf')
    report.append(f"| {label} | {sens:.3f} | {spec:.3f} | {ppv:.3f} | {npv:.3f} | {acc:.3f} | {lr_pos:.2f} | {lr_neg:.2f} |\n")

report.append(f"\n**Cohort:** {n_total:,} patients (malignant {n_malig_total:,}; benign {n_benign_total:,})\n\n")

# 2x2 table at TR ≥ TR4 (canonical decision threshold)
report.append("## 2×2 table at TR ≥ TR4 (canonical decision threshold)\n\n")
test_pos = df[df['TIRADS'].isin(['TR4','TR5'])]
test_neg = df[~df['TIRADS'].isin(['TR4','TR5'])]
tp = int(test_pos['IS_MALIGNANT'].fillna(False).astype(bool).sum())
fp = int(len(test_pos) - tp)
fn = int(test_neg['IS_MALIGNANT'].fillna(False).astype(bool).sum())
tn = int(len(test_neg) - fn)
report.append("|  | malignant | benign | total |\n| --- | --- | --- | --- |\n")
report.append(f"| **TR ≥ TR4** | {tp} | {fp} | {tp+fp} |\n")
report.append(f"| **TR < TR4** | {fn} | {tn} | {fn+tn} |\n")
report.append(f"| **total** | {tp+fn} | {fp+tn} | {n_total} |\n\n")

# Methods
report.append("## Methods\n\n")
report.append("- **Cohort:** Patients with `MAX_TIRADS_CATEGORY_EVER` populated in `canonical_us_patient_master_VIEW_v2`. Excludes patients with US exams not categorized via TIRADS (e.g., older exams pre-2017 ACR adoption).\n")
report.append("- **Outcome (gold standard):** `IS_MALIGNANT` from `canonical_patient_master` (path-confirmed). NIFTP currently coded `IS_MALIGNANT=TRUE` per pre-2017 convention; mig_264b plans to recategorize.\n")
report.append("- **Test variable:** Patient-level `MAX_TIRADS_CATEGORY_EVER` (highest TR across all that patient's US exams).\n")
report.append("- **Per-category ROM:** `n_malignant / n_total` within each TR category.\n")
report.append("- **Diagnostic performance:** Standard 2×2 with TR ≥ T_threshold as test positive.\n")
report.append("- **LR+:** Sensitivity / (1 − Specificity); LR-: (1 − Sensitivity) / Specificity.\n")
report.append("- **Cohort caveat:** Operative bias inflates ROM at every TR category vs ACR-published expected ranges. See round-5 Prompt 7 finding for details.\n")

OUT.write_text("".join(report))
print(f"\n[saved] {OUT}  ({OUT.stat().st_size:,} bytes)")
