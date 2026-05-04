"""Sensitivity panels for 6 manuscripts."""
import os, sys
from pathlib import Path
from datetime import datetime
sys.path.insert(0, str(Path(__file__).parent))
import duckdb

REPORTS = Path("/Users/ros/THyroid 2026/snowflake_trial/reports")
MD_TOKEN = os.environ.get("MOTHERDUCK_TOKEN") or os.environ.get("motherduck_token")
md = duckdb.connect(f"md:thyroid_canonical_publication_v1_0?motherduck_token={MD_TOKEN}")

def n(sql):
    try:
        return md.execute(sql).fetchone()[0]
    except Exception as e:
        return f"ERR"

DATE_2017 = "DATE '2017-01-01'"
DATE_2010 = "DATE '2010-01-01'"
DATE_2020 = "DATE '2020-01-01'"

def row(label, sql_n, sql_ev=None):
    nn = n(sql_n)
    nev = n(sql_ev) if sql_ev else "n/a"
    return f"| {label} | {nn} | {nev} |"

lines = ["# Sensitivity Panels — pub_v1_1_20260504", f"**Generated:** {datetime.now().isoformat()}", ""]

# M044
print("M044...")
lines += ["## M044 ETE", "| Panel | n | n_recur_events |", "|---|---:|---:|"]
m044_v = "manuscript_workspace.cohort_m044_ajcc_ete_v1"
lines.append(row("Primary", f"SELECT COUNT(*) FROM {m044_v}", f"SELECT COUNT_IF(any_recurrence_flag) FROM {m044_v}"))
sql_2017 = f"FROM {m044_v} c JOIN main.canonical_patient_master pm USING (research_id) WHERE pm.first_surgery_date >= {DATE_2017}"
lines.append(row("Post-2017 era", f"SELECT COUNT(*) {sql_2017}", f"SELECT COUNT_IF(c.any_recurrence_flag) {sql_2017}"))
sql_tt = f"FROM {m044_v} c JOIN main.canonical_patient_master pm USING (research_id) WHERE pm.surg_total_thyroidectomy=TRUE"
lines.append(row("Total-thy only", f"SELECT COUNT(*) {sql_tt}", f"SELECT COUNT_IF(c.any_recurrence_flag) {sql_tt}"))
sql_cc = f"FROM {m044_v} c JOIN main.canonical_patient_master pm USING (research_id) WHERE pm.age_at_surgery IS NOT NULL AND pm.tumor_size_cm_max IS NOT NULL"
lines.append(row("Complete-case (age + size)", f"SELECT COUNT(*) {sql_cc}", f"SELECT COUNT_IF(c.any_recurrence_flag) {sql_cc}"))
lines.append("")

# M037
print("M037...")
lines += ["## M037 LN Predictors", "| Panel | n | n_LN_pos |", "|---|---:|---:|"]
m037_v = "manuscript_workspace.cohort_m037_ln_metastasis_v1"
lines.append(row("Primary", f"SELECT COUNT(*) FROM {m037_v}", f"SELECT COUNT_IF(ajcc8_n_stage LIKE 'N1%') FROM {m037_v}"))
sql_2017 = f"FROM {m037_v} c JOIN main.canonical_patient_master pm USING (research_id) WHERE pm.first_surgery_date >= {DATE_2017}"
lines.append(row("Post-2017 era", f"SELECT COUNT(*) {sql_2017}", f"SELECT COUNT_IF(c.ajcc8_n_stage LIKE 'N1%') {sql_2017}"))
sql_both = f"FROM {m037_v} c JOIN main.canonical_patient_master pm USING (research_id) WHERE pm.ln_status_source='both'"
lines.append(row("ln_status_source='both'", f"SELECT COUNT(*) {sql_both}", f"SELECT COUNT_IF(c.ajcc8_n_stage LIKE 'N1%') {sql_both}"))
lines.append("")

# M025
print("M025...")
lines += ["## M025 TIRADS", "| Panel | n | n_malig |", "|---|---:|---:|"]
m025_v = "manuscript_workspace.cohort_m025_tirads_performance_v1"
lines.append(row("Primary", f"SELECT COUNT(*) FROM {m025_v}", f"SELECT COUNT_IF(is_malignant) FROM {m025_v}"))
sql_2017 = f"FROM {m025_v} c JOIN main.canonical_patient_master pm USING (research_id) WHERE pm.first_surgery_date >= {DATE_2017}"
lines.append(row("Post-2017 era", f"SELECT COUNT(*) {sql_2017}", f"SELECT COUNT_IF(c.is_malignant) {sql_2017}"))
sql_tr = f"FROM {m025_v} c JOIN main.canonical_patient_master pm USING (research_id) WHERE pm.tirads_resolved IS NOT NULL"
lines.append(row("tirads_resolved IS NOT NULL", f"SELECT COUNT(*) {sql_tr}", f"SELECT COUNT_IF(c.is_malignant) {sql_tr}"))
lines.append("")

# M032
print("M032...")
lines += ["## M032 25-yr Descriptive (era panels)", "| Era | n | n_malig |", "|---|---:|---:|"]
m032_v = "manuscript_workspace.cohort_m032_descriptive_25yr_v1"
for label, where in [
    ("All eras", "TRUE"),
    ("Pre-2010", f"first_surgery_date < {DATE_2010}"),
    ("2010-2019", f"first_surgery_date >= {DATE_2010} AND first_surgery_date < {DATE_2020}"),
    ("2020-2025", f"first_surgery_date >= {DATE_2020}"),
]:
    lines.append(row(label, f"SELECT COUNT(*) FROM {m032_v} WHERE {where}", f"SELECT COUNT_IF(is_malignant) FROM {m032_v} WHERE {where}"))
lines.append("")

# M038
print("M038...")
lines += ["## M038 Massive Goiter (weight-threshold sensitivity)", "| Panel | n | n_malig |", "|---|---:|---:|"]
m038_v = "main.cohort_m038_massive_goiter_v1"
lines.append(row("Full cohort", f"SELECT COUNT(*) FROM {m038_v}", f"SELECT COUNT_IF(is_malignant) FROM {m038_v}"))
for thresh in [100, 150, 200, 250, 300]:
    lines.append(row(f"≥{thresh}g", f"SELECT COUNT(*) FROM {m038_v} WHERE gland_weight_final_g >= {thresh}", f"SELECT COUNT_IF(is_malignant) FROM {m038_v} WHERE gland_weight_final_g >= {thresh}"))
lines.append("")

# M004
print("M004...")
lines += ["## M004 Autoimmune+Cancer (exposure panels)", "| Panel | n | n_malig |", "|---|---:|---:|"]
m004_v = "manuscript_workspace.cohort_m004_autoimmune_cancer_v1"
lines.append(row("Hashimoto combined (NLP+syn)", f"SELECT COUNT_IF(has_hashi) FROM {m004_v}", f"SELECT COUNT_IF(has_hashi AND is_malignant) FROM {m004_v}"))
lines.append(row("Graves combined", f"SELECT COUNT_IF(has_graves) FROM {m004_v}", f"SELECT COUNT_IF(has_graves AND is_malignant) FROM {m004_v}"))
lines.append(row("Either positive", f"SELECT COUNT_IF(has_hashi OR has_graves) FROM {m004_v}", f"SELECT COUNT_IF((has_hashi OR has_graves) AND is_malignant) FROM {m004_v}"))
lines.append(row("Hashi NLP-only (not syn)", f"SELECT COUNT_IF(has_hashi AND NOT COALESCE(syn_hashimoto, FALSE)) FROM {m004_v}", None))
lines.append(row("Hashi syn-only (not NLP)", f"SELECT COUNT_IF(syn_hashimoto AND NOT COALESCE(has_hashi, FALSE)) FROM {m004_v}", None))
lines.append("")

md.close()
OUT = REPORTS / "sensitivity_panels_20260504.md"
OUT.write_text("\n".join(lines))
print(f"[saved] {OUT}")
