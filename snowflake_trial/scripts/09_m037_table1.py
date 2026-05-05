"""Table 1 for M037 cohort (LN predictors).

Groups by LN_POSITIVE. Continuous: mean (SD), median [IQR]. Categorical: n (%).
P-values approximated via Mann-Whitney for continuous, chi-square for categorical.
"""
import sys, time, math
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import get_cursor

REPO_ROOT = Path(__file__).resolve().parents[2]
OUT = REPO_ROOT / "snowflake_trial" / "reports" / "m037_table1.md"
OUT.parent.mkdir(parents=True, exist_ok=True)

ctx, cur = get_cursor()

# Pull cohort into pandas for stats
print("=== Pulling M037 cohort ===")
cur.execute("""
SELECT RESEARCH_ID, AGE_AT_SURGERY, SEX, RACE, HISTOLOGY_GROUP,
       AJCC8_T_STAGE, AJCC8_N_STAGE, AJCC8_M_STAGE, AJCC8_STAGE_GROUP,
       TUMOR_SIZE_CM_MAX, ETE_GRADE,
       LN_TOTAL_EXAMINED, LN_TOTAL_POSITIVE, LN_POSITIVE,
       SURG_PROCEDURE_TYPE, MOLECULAR_TESTED_CONFIRMED, BRAF_POSITIVE_FINAL,
       OVERALL_SURVIVAL_YEARS, ANY_RECURRENCE_FLAG
FROM COHORT_M037_LN_PREDICTORS
""")
rows = cur.fetchall()
cols = [c[0] for c in cur.description]
ctx.close()

import pandas as pd
df = pd.DataFrame(rows, columns=cols)
print(f"  {len(df):,} rows, {len(df.columns)} cols")
print(f"  LN+ = {df['LN_POSITIVE'].sum():,}  LN- = {(~df['LN_POSITIVE'].astype(bool)).sum():,}")

ln_pos = df[df["LN_POSITIVE"] == True]
ln_neg = df[df["LN_POSITIVE"] == False]
n_pos, n_neg = len(ln_pos), len(ln_neg)

try:
    from scipy.stats import mannwhitneyu, chi2_contingency, fisher_exact
    HAVE_STATS = True
except ImportError:
    HAVE_STATS = False
    print("  (scipy not present — p-values omitted)")

def fmt_cont(s):
    s = pd.to_numeric(s, errors="coerce").dropna()
    if len(s) == 0:
        return "—"
    mean = s.mean()
    sd = s.std()
    median = s.median()
    q1, q3 = s.quantile(0.25), s.quantile(0.75)
    return f"{mean:.1f} ± {sd:.1f}; {median:.1f} [{q1:.1f}–{q3:.1f}]"

def fmt_pct(n, d):
    if d == 0:
        return "—"
    return f"{n} ({100.0*n/d:.1f}%)"

def cont_row(label, col):
    a = pd.to_numeric(ln_pos[col], errors="coerce").dropna()
    b = pd.to_numeric(ln_neg[col], errors="coerce").dropna()
    p = ""
    if HAVE_STATS and len(a) > 1 and len(b) > 1:
        try:
            _, p_val = mannwhitneyu(a, b, alternative="two-sided")
            p = f"{p_val:.4f}" if p_val >= 0.0001 else "<0.0001"
        except Exception:
            p = "—"
    return [label, fmt_cont(ln_pos[col]), fmt_cont(ln_neg[col]), p]

def cat_rows(label, col, top=None):
    rows_out = [[f"**{label}**", "", "", ""]]
    counts = df[col].value_counts(dropna=False)
    if top:
        counts = counts.head(top)
    if HAVE_STATS:
        try:
            ct = pd.crosstab(df[col].fillna("(missing)"), df["LN_POSITIVE"])
            chi2, p_val, _, _ = chi2_contingency(ct)
            p_overall = f"{p_val:.4f}" if p_val >= 0.0001 else "<0.0001"
        except Exception:
            p_overall = "—"
    else:
        p_overall = ""
    for v in counts.index:
        v_str = str(v) if v is not None else "(missing)"
        n_a = ((ln_pos[col].fillna("(missing)") == v) | (ln_pos[col].isna() & (v_str == "(missing)"))).sum()
        n_b = ((ln_neg[col].fillna("(missing)") == v) | (ln_neg[col].isna() & (v_str == "(missing)"))).sum()
        rows_out.append([f"  {v_str}", fmt_pct(int(n_a), n_pos), fmt_pct(int(n_b), n_neg), ""])
    if rows_out:
        rows_out[0][3] = p_overall
    return rows_out

# Build Table 1
t1 = []
t1.append(["Total, N", f"{n_pos:,}", f"{n_neg:,}", ""])
t1.append(cont_row("Age (years), mean ± SD; median [IQR]", "AGE_AT_SURGERY"))
t1.append(cont_row("Tumor size max (cm), mean ± SD; median [IQR]", "TUMOR_SIZE_CM_MAX"))
t1.append(cont_row("LN examined, mean ± SD; median [IQR]", "LN_TOTAL_EXAMINED"))
t1.append(cont_row("LN positive, mean ± SD; median [IQR]", "LN_TOTAL_POSITIVE"))
t1.append(cont_row("Overall survival (years)", "OVERALL_SURVIVAL_YEARS"))
t1.append(["", "", "", ""])

for blk in [
    cat_rows("Sex", "SEX"),
    cat_rows("Race", "RACE", top=8),
    cat_rows("Histology group", "HISTOLOGY_GROUP"),
    cat_rows("AJCC 8 T stage", "AJCC8_T_STAGE"),
    cat_rows("AJCC 8 N stage", "AJCC8_N_STAGE"),
    cat_rows("AJCC 8 M stage", "AJCC8_M_STAGE"),
    cat_rows("AJCC 8 stage group", "AJCC8_STAGE_GROUP"),
    cat_rows("ETE grade", "ETE_GRADE"),
    cat_rows("Surgery type", "SURG_PROCEDURE_TYPE", top=6),
    cat_rows("Molecular tested", "MOLECULAR_TESTED_CONFIRMED"),
    cat_rows("BRAF positive", "BRAF_POSITIVE_FINAL"),
    cat_rows("Any recurrence", "ANY_RECURRENCE_FLAG"),
]:
    t1.extend(blk)
    t1.append(["", "", "", ""])

# Render
md = ["# Table 1 — Manuscript M037: Predictors of Lymph-Node Positivity\n",
      f"**Generated:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n",
      f"**Cohort:** COHORT_M037_LN_PREDICTORS (n={len(df):,} malignant)\n",
      f"**Grouping:** LN_POSITIVE (n+ = {n_pos:,}; n– = {n_neg:,})\n",
      f"**P-values:** Mann-Whitney U (continuous), chi-square (categorical){' [scipy unavailable — p-values omitted]' if not HAVE_STATS else ''}\n\n"]
md.append("| Variable | LN positive | LN negative | p |\n")
md.append("| --- | --- | --- | --- |\n")
for row in t1:
    if row == ["", "", "", ""]:
        continue
    md.append("| " + " | ".join(str(c) for c in row) + " |\n")
md.append("\n---\n\n")
md.append(f"_Generated from THYROID_VALIDATION.PUBLIC.COHORT_M037_LN_PREDICTORS via Snowflake Cortex trial pipeline._\n")

OUT.write_text("".join(md))
print(f"\n[saved] {OUT}  ({OUT.stat().st_size:,} bytes)")
