"""
M029 FNA Cytology Concordance Analysis
Bethesda classification performance vs. surgical pathology outcomes.
Cohort: manuscript_workspace.cohort_m029_fna_concordance_v1 (N=2,401)
"""

import sys
import os
import math
import warnings
from pathlib import Path
from datetime import datetime

import pandas as pd
import numpy as np
import duckdb

warnings.filterwarnings("ignore")

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from motherduck_client import get_token

OUT = Path(__file__).resolve().parent
OUT.mkdir(parents=True, exist_ok=True)

RUN_TS = datetime.now().strftime("%Y%m%d_%H%M%S")

# ── helpers ──────────────────────────────────────────────────────────────────

def wilson_ci(k: int, n: int, z: float = 1.96):
    """Wilson score confidence interval for a proportion. Returns (lo, hi) as %."""
    if n == 0:
        return (float("nan"), float("nan"))
    p = k / n
    denom = 1 + z**2 / n
    centre = p + z**2 / (2 * n)
    margin = z * math.sqrt(p * (1 - p) / n + z**2 / (4 * n**2))
    lo = (centre - margin) / denom * 100
    hi = (centre + margin) / denom * 100
    return (max(0.0, lo), min(100.0, hi))


def diag_metrics(tp, fp, fn, tn):
    """Sensitivity, specificity, PPV, NPV with 95% Wilson CI."""
    sens = tp / (tp + fn) if (tp + fn) > 0 else float("nan")
    spec = tn / (tn + fp) if (tn + fp) > 0 else float("nan")
    ppv  = tp / (tp + fp) if (tp + fp) > 0 else float("nan")
    npv  = tn / (tn + fn) if (tn + fn) > 0 else float("nan")

    def pct_ci(k, n):
        if n == 0:
            return float("nan"), float("nan"), float("nan")
        lo, hi = wilson_ci(k, n)
        return k / n * 100, lo, hi

    rows = []
    for name, k, n in [
        ("Sensitivity", tp, tp + fn),
        ("Specificity", tn, tn + fp),
        ("PPV",         tp, tp + fp),
        ("NPV",         tn, tn + fn),
    ]:
        v, lo, hi = pct_ci(int(k), int(n))
        rows.append({"Metric": name, "Value_pct": round(v, 1),
                     "CI_lo": round(lo, 1), "CI_hi": round(hi, 1),
                     "Numerator": int(k), "Denominator": int(n)})
    return rows


# ── histology rollup ──────────────────────────────────────────────────────────

def rollup_histology(val: str) -> str:
    if val is None:
        return "Unknown"
    v = str(val).lower().strip()
    # PTC group (includes metastatic variants)
    if "ptc" in v or "papillary" in v:
        return "PTC"
    if "follicular carcinoma" in v:
        return "FTC"
    if "mtc" in v or "medullary" in v:
        return "MTC"
    if "niftp" in v:
        return "NIFTP"
    if "ftump" in v or "hurthle" in v or "hürthle" in v or "oncocytic" in v or "atypical hurthle" in v:
        return "FTUMP/HCC"
    if "poorly differentiated" in v or "pdtc" in v:
        return "PDTC"
    if "anaplastic" in v or "atc" in v:
        return "ATC"
    if "differentiated high grade" in v or "high grade" in v or "high-grade" in v:
        return "High-grade DTC"
    if ("follicular adenoma" in v or "adenoma" in v or "multinodular" in v
            or "hyperplasia" in v or "goiter" in v or "benign" in v
            or "thyroiditis" in v):
        return "Benign"
    # Rare malignant
    if ("carcinoma" in v or "malignant" in v or "cancer" in v
            or "sarcoma" in v or "lymphoma" in v or "nut" in v
            or "neuroendocrine" in v or "squamous" in v or "thymic" in v):
        return "Other malignant"
    return "Other/Unknown"


# ── connection ────────────────────────────────────────────────────────────────

tok = get_token()
print(f"Token: SET (len={len(tok) if tok else 0})")
conn = duckdb.connect(f"md:thyroid_canonical_publication_v1_0?motherduck_token={tok}")

# ── load cohort + CPM molecular ──────────────────────────────────────────────

print("Loading cohort…")
df = conn.execute("""
    SELECT
        c.research_id,
        c.age_at_surgery,
        c.sex,
        c.bethesda_final,
        c.bethesda_final_name,
        c.histology_final,
        c.is_malignant,
        c.tumor_size_cm,
        c.surg_first_date,
        c.fna_path_concordance_category,
        c.fna_path_concordant,
        c.fna_path_outcome,
        -- molecular from CPM
        COALESCE(m.molecular_tested_confirmed, FALSE) AS molecular_tested_confirmed,
        COALESCE(m.braf_positive_final, FALSE)        AS braf_positive_final,
        m.mol_platform,
        m.mol_n_tests
    FROM manuscript_workspace.cohort_m029_fna_concordance_v1 c
    LEFT JOIN main.canonical_patient_master m
        ON c.research_id = m.research_id
""").df()

print(f"  Loaded {len(df)} rows")

# Derive columns
df["histology_group"] = df["histology_final"].apply(rollup_histology)
df["surgery_year"] = pd.to_datetime(df["surg_first_date"], errors="coerce").dt.year

# year period
def year_period(y):
    if pd.isna(y): return "Unknown"
    if y < 2015:   return "Pre-2015"
    if y < 2020:   return "2015–2019"
    return "2020+"
df["year_period"] = df["surgery_year"].apply(year_period)

# age group
def age_group(a):
    if pd.isna(a): return "Unknown"
    if a < 45:  return "<45"
    if a <= 65: return "45–65"
    return ">65"
df["age_group"] = df["age_at_surgery"].apply(age_group)

# tumor size group
def size_group(s):
    if pd.isna(s) or s <= 0: return "Unknown"
    if s < 1:  return "<1 cm"
    if s < 2:  return "1–2 cm"
    if s < 4:  return "2–4 cm"
    return ">4 cm"
df["size_group"] = df["tumor_size_cm"].apply(size_group)

# Safe booleans
df["is_malignant"] = df["is_malignant"].astype(object).apply(lambda v: v is True)

BCAT = {1: "I (Non-diagnostic)", 2: "II (Benign)", 3: "III (AUS/FLUS)",
        4: "IV (FN/SFN)", 5: "V (Suspicious)", 6: "VI (Malignant)"}
df["bethesda_cat"] = df["bethesda_final"].map(BCAT)

# NIFTP flag (treat as borderline benign)
df["is_niftp"] = df["histology_group"] == "NIFTP"
# malignant excluding NIFTP
df["is_malignant_excl_niftp"] = df["is_malignant"] & (~df["is_niftp"])

print(f"  Malignant: {df['is_malignant'].sum()}")
print(f"  Malignant excl NIFTP: {df['is_malignant_excl_niftp'].sum()}")
print(f"  Histology groups: {df['histology_group'].value_counts().to_dict()}")

# ══════════════════════════════════════════════════════════════════════════════
# 1. Bethesda × Histology crosstab
# ══════════════════════════════════════════════════════════════════════════════

print("\n[1] Bethesda × Histology crosstab…")

HISTO_ORDER = ["PTC", "FTC", "MTC", "NIFTP", "FTUMP/HCC",
               "PDTC", "ATC", "High-grade DTC", "Other malignant", "Benign", "Other/Unknown"]

ct = pd.crosstab(
    df["bethesda_final"].map(BCAT),
    df["histology_group"],
    margins=True
)
ct = ct.reindex(
    index=[BCAT[i] for i in range(1, 7)] + ["All"],
    columns=[c for c in HISTO_ORDER if c in ct.columns] + ["All"],
    fill_value=0
)
ct.index.name = "Bethesda"
ct.to_csv(OUT / "bethesda_histology_crosstab.csv")
print(f"  Saved bethesda_histology_crosstab.csv ({ct.shape})")

# ══════════════════════════════════════════════════════════════════════════════
# 2. ROM by Bethesda with Wilson CI
# ══════════════════════════════════════════════════════════════════════════════

print("\n[2] ROM by Bethesda…")

# Published benchmarks (2017 Bethesda 3rd Ed, surgical cohort-adjusted)
BENCHMARKS_INCL = {1: (5, 10), 2: (0, 3), 3: (6, 18), 4: (10, 40), 5: (45, 60), 6: (94, 96)}
BENCHMARKS_EXCL = {1: (5, 10), 2: (0, 3), 3: (4, 16), 4: (6, 30), 5: (40, 55), 6: (94, 96)}

rom_rows = []
for bcat in range(1, 7):
    sub = df[df["bethesda_final"] == bcat]
    n = len(sub)
    if n == 0:
        continue
    n_mal = sub["is_malignant"].sum()
    n_mal_excl = sub["is_malignant_excl_niftp"].sum()
    n_niftp = sub["is_niftp"].sum()
    n_benign = n - n_mal

    rom = n_mal / n * 100
    lo, hi = wilson_ci(int(n_mal), n)
    rom_excl = n_mal_excl / n * 100
    lo_ex, hi_ex = wilson_ci(int(n_mal_excl), n)

    bench_lo, bench_hi = BENCHMARKS_INCL[bcat]
    bench_lo_ex, bench_hi_ex = BENCHMARKS_EXCL[bcat]

    rom_rows.append({
        "Bethesda_num": bcat,
        "Bethesda": BCAT[bcat],
        "Total": n,
        "Malignant": int(n_mal),
        "Benign_other": int(n_benign),
        "NIFTP": int(n_niftp),
        "ROM_pct": round(rom, 1),
        "ROM_CI_lo": round(lo, 1),
        "ROM_CI_hi": round(hi, 1),
        "ROM_excl_NIFTP_pct": round(rom_excl, 1),
        "ROM_excl_NIFTP_CI_lo": round(lo_ex, 1),
        "ROM_excl_NIFTP_CI_hi": round(hi_ex, 1),
        "Published_ROM_lo_pct": bench_lo,
        "Published_ROM_hi_pct": bench_hi,
        "Published_ROM_excl_NIFTP_lo_pct": bench_lo_ex,
        "Published_ROM_excl_NIFTP_hi_pct": bench_hi_ex,
        "Above_published_range": rom > bench_hi,
    })

rom_df = pd.DataFrame(rom_rows)
rom_df.to_csv(OUT / "rom_by_bethesda.csv", index=False)
print(rom_df[["Bethesda", "Total", "Malignant", "ROM_pct", "ROM_CI_lo", "ROM_CI_hi",
              "Published_ROM_lo_pct", "Published_ROM_hi_pct"]].to_string(index=False))

# ══════════════════════════════════════════════════════════════════════════════
# 3. Diagnostic Performance Metrics
# ══════════════════════════════════════════════════════════════════════════════

print("\n[3] Diagnostic performance…")

perf_rows = []
for threshold_name, pos_cats in [
    ("V–VI (suspicious/malignant)", [5, 6]),
    ("IV–VI (FN/SFN through malignant)", [4, 5, 6]),
    ("VI only (malignant)", [6]),
]:
    for excl_label, mal_col in [
        ("Including NIFTP", "is_malignant"),
        ("Excluding NIFTP", "is_malignant_excl_niftp"),
    ]:
        pos_test = df["bethesda_final"].isin(pos_cats)
        neg_test = ~pos_test
        mal = df[mal_col]

        tp = int((pos_test & mal).sum())
        fp = int((pos_test & ~mal).sum())
        fn = int((neg_test & mal).sum())
        tn = int((neg_test & ~mal).sum())

        for row in diag_metrics(tp, fp, fn, tn):
            row["Threshold"] = threshold_name
            row["NIFTP_handling"] = excl_label
            row["TP"] = tp
            row["FP"] = fp
            row["FN"] = fn
            row["TN"] = tn
            perf_rows.append(row)

perf_df = pd.DataFrame(perf_rows)
perf_df.to_csv(OUT / "diagnostic_performance.csv", index=False)
print(perf_df[["Threshold", "NIFTP_handling", "Metric", "Value_pct",
               "CI_lo", "CI_hi"]].to_string(index=False))

# ══════════════════════════════════════════════════════════════════════════════
# 4. Subgroup ROM analyses
# ══════════════════════════════════════════════════════════════════════════════

print("\n[4] Subgroup ROM analyses…")

subgroup_rows = []

def subgroup_rom(sub_df, stratifier, strat_value, bcat=None):
    rows = []
    target = sub_df if bcat is None else sub_df[sub_df["bethesda_final"] == bcat]
    n = len(target)
    if n < 5:
        return rows
    n_mal = target["is_malignant"].sum()
    n_mal_ex = target["is_malignant_excl_niftp"].sum()
    rom = n_mal / n * 100 if n > 0 else float("nan")
    lo, hi = wilson_ci(int(n_mal), n)
    rom_ex = n_mal_ex / n * 100 if n > 0 else float("nan")
    lo_ex, hi_ex = wilson_ci(int(n_mal_ex), n)
    row = {
        "Stratifier": stratifier,
        "Stratum": strat_value,
        "Bethesda": BCAT.get(bcat, "All") if bcat else "All",
        "Bethesda_num": bcat if bcat else "All",
        "N": n,
        "Malignant": int(n_mal),
        "ROM_pct": round(rom, 1),
        "ROM_CI_lo": round(lo, 1),
        "ROM_CI_hi": round(hi, 1),
        "ROM_excl_NIFTP_pct": round(rom_ex, 1),
        "ROM_excl_NIFTP_CI_lo": round(lo_ex, 1),
        "ROM_excl_NIFTP_CI_hi": round(hi_ex, 1),
    }
    rows.append(row)
    return rows


for strat_col, strat_label in [
    ("year_period", "Year period"),
    ("age_group",   "Age group"),
    ("size_group",  "Tumor size"),
]:
    for strat_val in df[strat_col].unique():
        sub = df[df[strat_col] == strat_val]
        subgroup_rows.extend(subgroup_rom(sub, strat_label, strat_val))
        for bcat in range(1, 7):
            subgroup_rows.extend(subgroup_rom(sub, strat_label, strat_val, bcat=bcat))

# Molecular testing subgroup
for mol_val, mol_label in [(True, "Molecular tested"), (False, "Not molecular tested")]:
    sub = df[df["molecular_tested_confirmed"] == mol_val]
    subgroup_rows.extend(subgroup_rom(sub, "Molecular testing", mol_label))
    for bcat in range(1, 7):
        subgroup_rows.extend(subgroup_rom(sub, "Molecular testing", mol_label, bcat=bcat))

sg_df = pd.DataFrame(subgroup_rows)
sg_df.to_csv(OUT / "rom_subgroup_analyses.csv", index=False)
print(f"  Saved {len(sg_df)} subgroup rows to rom_subgroup_analyses.csv")

# ══════════════════════════════════════════════════════════════════════════════
# 5. FNA × Molecular concordance (Bethesda III/IV)
# ══════════════════════════════════════════════════════════════════════════════

print("\n[5] FNA × Molecular (Bethesda III/IV)…")

indet = df[df["bethesda_final"].isin([3, 4])].copy()
n_indet = len(indet)
n_mol_tested = indet["molecular_tested_confirmed"].sum()

mol_rows = []
for bcat in [3, 4]:
    sub = indet[indet["bethesda_final"] == bcat]
    n = len(sub)
    n_mol = sub["molecular_tested_confirmed"].sum()
    n_mol_mal = sub[sub["molecular_tested_confirmed"] == True]["is_malignant"].sum()
    n_no_mol_mal = sub[sub["molecular_tested_confirmed"] == False]["is_malignant"].sum()
    n_mol_total = sub["molecular_tested_confirmed"].sum()
    n_no_mol_total = n - n_mol_total

    mol_rows.append({
        "Bethesda": BCAT[bcat],
        "Bethesda_num": bcat,
        "Total_in_category": n,
        "N_molecular_tested": int(n_mol),
        "Pct_molecular_tested": round(n_mol / n * 100, 1) if n > 0 else float("nan"),
        "ROM_with_molecular_pct": round(n_mol_mal / n_mol_total * 100, 1) if n_mol_total > 0 else float("nan"),
        "ROM_without_molecular_pct": round(n_no_mol_mal / n_no_mol_total * 100, 1) if n_no_mol_total > 0 else float("nan"),
        "N_mal_with_mol": int(n_mol_mal),
        "N_total_with_mol": int(n_mol_total),
        "N_mal_without_mol": int(n_no_mol_mal),
        "N_total_without_mol": int(n_no_mol_total),
    })

# BRAF in indeterminate
braf_in_indet = int(indet["braf_positive_final"].sum())
mol_summary = {
    "Bethesda_III_IV_total": n_indet,
    "Molecular_tested": int(n_mol_tested),
    "Pct_molecular_tested": round(n_mol_tested / n_indet * 100, 1) if n_indet > 0 else float("nan"),
    "BRAF_positive_in_III_IV": braf_in_indet,
}
print(f"  Indeterminate (III/IV): N={n_indet}, molecular tested={n_mol_tested} "
      f"({mol_summary['Pct_molecular_tested']}%)")

mol_df = pd.DataFrame(mol_rows)
mol_df.to_csv(OUT / "fna_molecular_concordance.csv", index=False)
print(mol_df.to_string(index=False))

# ══════════════════════════════════════════════════════════════════════════════
# 6. Patient-level table for MotherDuck upload
# ══════════════════════════════════════════════════════════════════════════════

print("\n[6] Building patient-level analysis table…")

keep_cols = [c for c in [
    "research_id", "bethesda_final", "bethesda_cat", "histology_final",
    "histology_group", "is_malignant", "is_niftp", "is_malignant_excl_niftp",
    "age_at_surgery", "age_group", "sex", "tumor_size_cm", "size_group",
    "surgery_year", "year_period", "surg_procedure_type",
    "molecular_tested_confirmed", "braf_positive_final",
    "fna_path_concordance_category", "fna_path_concordant", "fna_path_outcome",
] if c in df.columns]
upload_df = df[keep_cols].copy()
upload_df["analysis_run_ts"] = RUN_TS
upload_df["bethesda_pos_v56"] = df["bethesda_final"].isin([5, 6])
upload_df["bethesda_pos_v456"] = df["bethesda_final"].isin([4, 5, 6])
upload_df["bethesda_pos_v6only"] = df["bethesda_final"] == 6

# ══════════════════════════════════════════════════════════════════════════════
# 7. LaTeX tables
# ══════════════════════════════════════════════════════════════════════════════

print("\n[7] Generating LaTeX tables…")

def latex_table(df, caption, label, col_format=None):
    rows = []
    rows.append(r"\begin{table}[htbp]")
    rows.append(r"\centering")
    rows.append(r"\small")
    rows.append(rf"\caption{{{caption}}}")
    rows.append(rf"\label{{{label}}}")
    ncols = len(df.columns)
    fmt = col_format or "l" + "r" * (ncols - 1)
    rows.append(rf"\begin{{tabular}}{{{fmt}}}")
    rows.append(r"\toprule")
    # Header
    rows.append(" & ".join(str(c).replace("_", " ").replace("%", r"\%") for c in df.columns) + r" \\")
    rows.append(r"\midrule")
    for _, row in df.iterrows():
        cells = []
        for v in row:
            if isinstance(v, float):
                cells.append(f"{v:.1f}" if not math.isnan(v) else "--")
            else:
                cells.append(str(v).replace("%", r"\%").replace("&", r"\&"))
        rows.append(" & ".join(cells) + r" \\")
    rows.append(r"\bottomrule")
    rows.append(r"\end{tabular}")
    rows.append(r"\end{table}")
    return "\n".join(rows)


tex_parts = []
tex_parts.append(r"% M029 FNA Concordance Analysis — LaTeX Tables")
tex_parts.append(r"% Generated: " + RUN_TS)
tex_parts.append(r"% \usepackage{booktabs} required")
tex_parts.append("")

# Table 1: ROM by Bethesda
rom_tex = rom_df[[
    "Bethesda", "Total", "Malignant", "ROM_pct", "ROM_CI_lo", "ROM_CI_hi",
    "ROM_excl_NIFTP_pct", "Published_ROM_lo_pct", "Published_ROM_hi_pct"
]].copy()
rom_tex.columns = [
    "Bethesda Category", "N", "Malignant",
    "ROM \\%", "95\\% CI lo", "95\\% CI hi",
    "ROM (excl. NIFTP) \\%",
    "Published lo \\%", "Published hi \\%"
]
tex_parts.append(latex_table(
    rom_tex,
    "Risk of malignancy (ROM) by Bethesda category with 95\\% Wilson confidence intervals. "
    "Published benchmarks from the 2017 Bethesda System (3rd Ed).",
    "tab:m029_rom_bethesda",
    "lrrrrrrrrr"
))
tex_parts.append("")

# Table 2: Diagnostic performance (V-VI threshold, incl NIFTP)
perf_sub = perf_df[
    (perf_df["Threshold"] == "V–VI (suspicious/malignant)") &
    (perf_df["NIFTP_handling"] == "Including NIFTP")
][["Metric", "Value_pct", "CI_lo", "CI_hi", "Numerator", "Denominator"]].copy()
perf_sub.columns = ["Metric", "Value (\\%)", "95\\% CI lo", "95\\% CI hi", "Numerator", "Denominator"]
tex_parts.append(latex_table(
    perf_sub,
    "Diagnostic performance of FNA cytology using Bethesda V--VI as positive threshold "
    "(including NIFTP as benign outcome).",
    "tab:m029_diag_perf_v56"
))
tex_parts.append("")

# Table 3: Bethesda × Histology (condensed for manuscript)
ct_tex = ct.copy()
ct_tex.index.name = "Bethesda \\ Histology"
ct_save = ct_tex.reset_index()
tex_parts.append(latex_table(
    ct_save,
    "Cross-tabulation of Bethesda cytology category and final surgical histology group.",
    "tab:m029_bethesda_histology"
))
tex_parts.append("")

# Table 4: Indeterminate molecular
mol_tex = mol_df[[
    "Bethesda", "Total_in_category", "N_molecular_tested", "Pct_molecular_tested",
    "ROM_with_molecular_pct", "ROM_without_molecular_pct"
]].copy()
mol_tex.columns = [
    "Bethesda", "N", "Molecular tested", "\\% tested",
    "ROM w/ molecular (\\%)", "ROM w/o molecular (\\%)"
]
tex_parts.append(latex_table(
    mol_tex,
    "Molecular testing rates and ROM impact for indeterminate FNA categories (Bethesda III--IV).",
    "tab:m029_indet_molecular"
))

tex_out = "\n".join(tex_parts)
(OUT / "fna_concordance_summary.tex").write_text(tex_out)
print("  Saved fna_concordance_summary.tex")

# ══════════════════════════════════════════════════════════════════════════════
# 8. Upload to MotherDuck
# ══════════════════════════════════════════════════════════════════════════════

print("\n[8] Uploading m029_fna_analysis_v1 to MotherDuck…")

# Write via parquet temp
import tempfile
with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tf:
    tmp_path = tf.name

upload_df.to_parquet(tmp_path, index=False)

conn.execute("DROP TABLE IF EXISTS manuscript_workspace.m029_fna_analysis_v1")
conn.execute(f"""
    CREATE TABLE manuscript_workspace.m029_fna_analysis_v1 AS
    SELECT * FROM read_parquet('{tmp_path}')
""")
n_uploaded = conn.execute("SELECT COUNT(*) FROM manuscript_workspace.m029_fna_analysis_v1").fetchone()[0]
print(f"  Uploaded {n_uploaded} rows to manuscript_workspace.m029_fna_analysis_v1")

os.unlink(tmp_path)
conn.close()

print("\n✓ All outputs saved to:", OUT)
print("  - bethesda_histology_crosstab.csv")
print("  - rom_by_bethesda.csv")
print("  - diagnostic_performance.csv")
print("  - rom_subgroup_analyses.csv")
print("  - fna_molecular_concordance.csv")
print("  - fna_concordance_summary.tex")
print("  - MotherDuck: manuscript_workspace.m029_fna_analysis_v1")
