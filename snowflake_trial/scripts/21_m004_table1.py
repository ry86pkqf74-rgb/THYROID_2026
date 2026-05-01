"""M004 Autoimmune Thyroid Disease + Carcinoma — Table 1: Graves vs Hashimoto vs Neither."""
import sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import get_cursor

OUT = Path("/Users/ros/THyroid 2026/snowflake_trial/reports/m004_table1.md")
ctx, cur = get_cursor()

print("=== Pulling M004 cohort ===")
cur.execute("""
SELECT
  RESEARCH_ID, AGE_AT_SURGERY, SEX, RACE, BMI_COMBINED,
  HISTOLOGY_FINAL, IS_MALIGNANT, FIRST_SURGERY_DATE,
  AJCC8_STAGE_GROUP, AJCC8_T_STAGE, AJCC8_N_STAGE, AJCC8_M_STAGE,
  TUMOR_SIZE_CM_MAX, ETE_GRADE,
  LN_TOTAL_EXAMINED, LN_TOTAL_POSITIVE, LN_POSITIVE_FLAG,
  SURG_PROCEDURE_TYPE, RAI_RECEIVED_FLAG,
  ANY_RECURRENCE_FLAG, OVERALL_SURVIVAL_YEARS, FOLLOWUP_YEARS,
  MOLECULAR_TESTED_CONFIRMED, BRAF_POSITIVE_FINAL, RAS_POSITIVE_FINAL,
  HAS_GRAVES, HAS_HASHIMOTO, AUTOIMMUNE_TYPE
FROM COHORT_M004_AUTOIMMUNE_CARCINOMA
""")
rows = cur.fetchall(); cols = [c[0] for c in cur.description]
ctx.close()

import pandas as pd
df = pd.DataFrame(rows, columns=cols)
print(f"  {len(df):,} rows")

# 3 strata: Graves, Hashimoto, Neither
graves_df = df[df['AUTOIMMUNE_TYPE'] == 'Graves']
hashi_df = df[df['AUTOIMMUNE_TYPE'] == 'Hashimoto']
none_df = df[df['AUTOIMMUNE_TYPE'] == 'Neither']
n_g, n_h, n_n = len(graves_df), len(hashi_df), len(none_df)
print(f"  Graves={n_g}  Hashimoto={n_h}  Neither={n_n}")

try:
    from scipy.stats import kruskal, chi2_contingency, fisher_exact
    HAVE_STATS = True
except ImportError:
    HAVE_STATS = False


def fmt_cont(s):
    s = pd.to_numeric(s, errors='coerce').dropna()
    if len(s) == 0: return "—"
    return f"{s.mean():.1f}±{s.std():.1f}; {s.median():.1f} [{s.quantile(0.25):.1f}-{s.quantile(0.75):.1f}]"


def fmt_pct(n, d):
    if d == 0: return "—"
    return f"{n} ({100.0*n/d:.1f}%)"


def cont_row(label, col):
    a = pd.to_numeric(graves_df[col], errors='coerce').dropna()
    b = pd.to_numeric(hashi_df[col], errors='coerce').dropna()
    c = pd.to_numeric(none_df[col], errors='coerce').dropna()
    p = ""
    if HAVE_STATS and len(a) > 1 and len(b) > 1 and len(c) > 1:
        try:
            _, p_val = kruskal(a, b, c)
            p = f"{p_val:.4f}" if p_val >= 0.0001 else "<0.0001"
        except Exception:
            p = "—"
    return [label, fmt_cont(graves_df[col]), fmt_cont(hashi_df[col]), fmt_cont(none_df[col]), p]


def cat_rows(label, col, top=None):
    out = [[f"**{label}**", "", "", "", ""]]
    counts = df[col].value_counts(dropna=False)
    if top: counts = counts.head(top)
    p_overall = ""
    if HAVE_STATS:
        try:
            ct = pd.crosstab(df[col].fillna("(missing)"), df['AUTOIMMUNE_TYPE'])
            _, p_val, _, _ = chi2_contingency(ct)
            p_overall = f"{p_val:.4f}" if p_val >= 0.0001 else "<0.0001"
        except Exception:
            p_overall = "—"
    for v in counts.index:
        v_str = str(v) if v is not None else "(missing)"
        n_a = ((graves_df[col].fillna("(missing)") == v) | (graves_df[col].isna() & (v_str == "(missing)"))).sum()
        n_b = ((hashi_df[col].fillna("(missing)") == v) | (hashi_df[col].isna() & (v_str == "(missing)"))).sum()
        n_c = ((none_df[col].fillna("(missing)") == v) | (none_df[col].isna() & (v_str == "(missing)"))).sum()
        out.append([f"  {v_str}", fmt_pct(int(n_a), n_g), fmt_pct(int(n_b), n_h), fmt_pct(int(n_c), n_n), ""])
    if out: out[0][4] = p_overall
    return out


t1 = []
t1.append(["Total, N", f"{n_g}", f"{n_h}", f"{n_n}", ""])
t1.append(cont_row("Age (years)", "AGE_AT_SURGERY"))
t1.append(cont_row("BMI", "BMI_COMBINED"))
t1.append(cont_row("Tumor size (cm)", "TUMOR_SIZE_CM_MAX"))
t1.append(cont_row("LN examined", "LN_TOTAL_EXAMINED"))
t1.append(cont_row("LN positive", "LN_TOTAL_POSITIVE"))
t1.append(cont_row("Followup (years)", "FOLLOWUP_YEARS"))
t1.append(["", "", "", "", ""])
for blk in [
    cat_rows("Sex", "SEX"),
    cat_rows("Race", "RACE", top=8),
    cat_rows("Histology", "HISTOLOGY_FINAL", top=10),
    cat_rows("AJCC 8 T", "AJCC8_T_STAGE"),
    cat_rows("AJCC 8 N", "AJCC8_N_STAGE"),
    cat_rows("AJCC 8 stage", "AJCC8_STAGE_GROUP"),
    cat_rows("ETE grade", "ETE_GRADE"),
    cat_rows("Surgery type", "SURG_PROCEDURE_TYPE", top=6),
    cat_rows("RAI received", "RAI_RECEIVED_FLAG"),
    cat_rows("Molecular tested", "MOLECULAR_TESTED_CONFIRMED"),
    cat_rows("BRAF positive", "BRAF_POSITIVE_FINAL"),
    cat_rows("Any recurrence", "ANY_RECURRENCE_FLAG"),
]:
    t1.extend(blk)
    t1.append(["", "", "", "", ""])

md = ["# Table 1 — Manuscript M004: Autoimmune Thyroid Disease + Carcinoma\n",
      f"**Generated:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n",
      f"**Cohort:** Malignant patients with autoimmune type assigned (N={len(df):,})\n",
      f"**Strata:** Graves={n_g} | Hashimoto={n_h} | Neither={n_n}\n",
      f"**P-values:** Kruskal-Wallis (3-group continuous), chi-square (categorical)\n",
      f"**Note:** Graves/Hashimoto signals are synoptic-derived (path-report findings)\n",
      f"per memory `project_medications_parathyroid_families_complete_2026-04-29.md`.\n\n"]
md.append("| Variable | Graves | Hashimoto | Neither | p |\n")
md.append("| --- | --- | --- | --- | --- |\n")
for row in t1:
    if row == ["", "", "", "", ""]: continue
    md.append("| " + " | ".join(str(c) for c in row) + " |\n")

OUT.write_text("".join(md))
print(f"\n[saved] {OUT}  ({OUT.stat().st_size:,} bytes)")
