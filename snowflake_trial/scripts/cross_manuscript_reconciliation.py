"""Cross-manuscript reconciliation report (v2 - simplified)."""
import os, sys
from pathlib import Path
from datetime import datetime
sys.path.insert(0, str(Path(__file__).parent))
import duckdb

REPORTS = Path("/Users/ros/THyroid 2026/snowflake_trial/reports")
MD_TOKEN = os.environ.get("MOTHERDUCK_TOKEN") or os.environ.get("motherduck_token")
md = duckdb.connect(f"md:thyroid_canonical_publication_v1_0?motherduck_token={MD_TOKEN}")

def q(sql):
    try:
        return md.execute(sql).fetchone()
    except Exception as e:
        print(f"ERROR: {e}")
        return None

# Pull headline numbers
m044 = q("SELECT COUNT(*), COUNT_IF(any_recurrence_flag), COUNT_IF(histology_final ILIKE '%ptc%' OR histology_final ILIKE '%papillary%'), COUNT_IF(ete_grade_final='gross'), COUNT_IF(ete_grade_final='microscopic') FROM manuscript_workspace.cohort_m044_ajcc_ete_v1")
m037 = q("SELECT COUNT(*), COUNT_IF(ajcc8_n_stage LIKE 'N1%'), COUNT_IF(histology_final ILIKE '%ptc%' OR histology_final ILIKE '%papillary%'), COUNT_IF(ajcc8_n_stage='N1a'), COUNT_IF(ajcc8_n_stage='N1b') FROM manuscript_workspace.cohort_m037_ln_metastasis_v1")
m025 = q("SELECT COUNT(*), COUNT_IF(is_malignant), COUNT_IF(is_malignant AND histology_final ILIKE '%ptc%'), 0, 0 FROM manuscript_workspace.cohort_m025_tirads_performance_v1")
m032 = q("SELECT COUNT(*), COUNT_IF(is_malignant), COUNT_IF(is_malignant AND histology_final ILIKE '%ptc%'), COUNT_IF(pmhx_nlp_smoking_status IS NOT NULL), COUNT_IF(pmhx_nlp_family_hx_thyroid IS NOT NULL) FROM manuscript_workspace.cohort_m032_descriptive_25yr_v1")
m038 = q("SELECT COUNT(*), COUNT_IF(is_malignant), COUNT_IF(is_malignant AND histology_final ILIKE '%ptc%'), COUNT_IF(gland_weight_final_g >= 200), COUNT_IF(rai_received_flag) FROM main.cohort_m038_massive_goiter_v1")
if m038 is None:
    m038 = (0, 0, 0, 0, 0)
m004 = q("SELECT COUNT(*), COUNT_IF(is_malignant), COUNT_IF(is_malignant AND histology_final ILIKE '%ptc%'), COUNT_IF(has_hashi), COUNT_IF(has_graves) FROM manuscript_workspace.cohort_m004_autoimmune_cancer_v1")
pub  = q("SELECT COUNT(*), COUNT_IF(is_malignant), COUNT_IF(is_malignant AND histology_final ILIKE '%ptc%'), COUNT_IF(any_recurrence_flag), COUNT_IF(death_occurred) FROM main.canonical_patient_master")

print("Pulled headline numbers:")
for label, r in [('M044', m044), ('M037', m037), ('M025', m025), ('M032', m032), ('M038', m038), ('M004', m004), ('PUB', pub)]:
    print(f"  {label}: {r}")

md.close()

def fmt(v):
    return f"{v:,}" if v is not None else "?"

lines = []
lines.append("# Cross-Manuscript Reconciliation — pub_v1_1_20260504")
lines.append(f"**Generated:** {datetime.now().isoformat()}")
lines.append("**MD HEAD:** post-mig_300 (release_pub_v1_1_20260504)")
lines.append("")
lines.append("## Headline numbers per manuscript (live MD)")
lines.append("")
lines.append("| Manuscript | Cohort n | Events | PTC count | Sub-stat 1 | Sub-stat 2 |")
lines.append("|---|---:|---:|---:|---|---|")
lines.append(f"| M044 ETE | {fmt(m044[0])} | {fmt(m044[1])} (any recur) | {fmt(m044[2])} | gross={fmt(m044[3])} | micro={fmt(m044[4])} |")
lines.append(f"| M037 LN | {fmt(m037[0])} | {fmt(m037[1])} (N1+) | {fmt(m037[2])} | N1a={fmt(m037[3])} | N1b={fmt(m037[4])} |")
lines.append(f"| M025 TIRADS | {fmt(m025[0])} | {fmt(m025[1])} (malig) | {fmt(m025[2])} | n/a | n/a |")
lines.append(f"| M032 25-yr | {fmt(m032[0])} | {fmt(m032[1])} (malig) | {fmt(m032[2])} | smk={fmt(m032[3])} | fhx={fmt(m032[4])} |")
lines.append(f"| M038 Goiter | {fmt(m038[0])} | {fmt(m038[1])} (malig) | {fmt(m038[2])} | >=200g={fmt(m038[3])} | comp={fmt(m038[4])} |")
lines.append(f"| M004 Autoim | {fmt(m004[0])} | {fmt(m004[1])} (malig) | {fmt(m004[2])} | hashi={fmt(m004[3])} | graves={fmt(m004[4])} |")
lines.append(f"| PUB v1.1 | {fmt(pub[0])} | {fmt(pub[1])} (malig) | {fmt(pub[2])} | recur={fmt(pub[3])} | deceased={fmt(pub[4])} |")
lines.append("")

# Consistency checks
lines.append("## Internal consistency checks")
lines.append("")
def check(label, condition):
    return f"- {label}: {'✓' if condition else '✗ DRIFT'}"
lines.append("### Cohort hierarchy (full-cohort papers must equal PUB; subset papers must be ⊆ PUB)")
lines.append(check(f"PUB={fmt(pub[0])} | M032={fmt(m032[0])} should equal PUB", m032[0]==pub[0]))
lines.append(check(f"PUB={fmt(pub[0])} | M038={fmt(m038[0])} should equal PUB", m038[0]==pub[0]))
lines.append(check(f"PUB={fmt(pub[0])} | M004={fmt(m004[0])} should equal PUB", m004[0]==pub[0]))
lines.append(check(f"M044={fmt(m044[0])} should be subset of PUB malig {fmt(pub[1])}", m044[0] <= pub[1]))
lines.append(check(f"M037={fmt(m037[0])} should be subset of PUB malig {fmt(pub[1])}", m037[0] <= pub[1]))
lines.append(check(f"M025={fmt(m025[0])} should be subset of PUB {fmt(pub[0])}", m025[0] <= pub[0]))
lines.append("")

lines.append("### PTC malignancy")
lines.append(f"- CPM PTC malig (gold standard) = **{fmt(pub[2])}**")
lines.append(f"- M044 cohort PTC count = {fmt(m044[2])} (subset filter)")
lines.append(f"- M037 cohort PTC count = {fmt(m037[2])} (subset filter)")
lines.append(check(f"M044 PTC ≤ CPM PTC", m044[2] <= pub[2]))
lines.append(check(f"M037 PTC ≤ CPM PTC", m037[2] <= pub[2]))
lines.append("")

lines.append("### NLP-augmentation cohort scale (post-mig_281)")
lines.append(f"- Smoking known on M032: **{fmt(m032[3])}** ({100*m032[3]/m032[0]:.1f}% of cohort)")
lines.append(f"- Family-hx known on M032: **{fmt(m032[4])}** ({100*m032[4]/m032[0]:.1f}% of cohort)")
lines.append(f"- Hashimoto combined on M004: **{fmt(m004[3])}**")
lines.append(f"- Graves combined on M004: **{fmt(m004[4])}**")
lines.append("")

lines.append("## Headline invariants for all 7 manuscripts")
lines.append("")
lines.append(f"- **Total cohort:** {fmt(pub[0])}")
lines.append(f"- **Total malignant:** {fmt(pub[1])} (post-mig_277 NIFTP carve-out; rate ~37.0%)")
lines.append(f"- **PTC malignant:** {fmt(pub[2])}")
lines.append(f"- **Recurrence (any):** {fmt(pub[3])}")
lines.append(f"- **Deceased:** {fmt(pub[4])}")
lines.append(f"- **Release tag:** `pub_v1_1_20260504` (post-NLP-augmentation milestone)")
lines.append("")

lines.append("## Pre-submission QC checklist (per manuscript)")
lines.append("")
lines.append("1. Methods §Cohort cites total cohort = 10,871 + release tag pub_v1_1")
lines.append("2. Any cited PTC denominator within: M044 ≤ M037 ≤ CPM PTC = " + fmt(pub[2]))
lines.append("3. Any RR/OR/HR reported with 95% CI")
lines.append("4. Reproducibility SQL package in 08_analysis_code/ runs against current MD")
lines.append("5. No `/Users/loganglosser/` paths leftover (mig_299 cleared 14)")
lines.append("6. M044 only: no v1.0 numbers in body (post-mig_295)")
lines.append("")

OUT = REPORTS / "CROSS_MANUSCRIPT_RECONCILIATION_20260504.md"
OUT.write_text("\n".join(lines))
print(f"\n[saved] {OUT}")
print("DONE")
