"""M032 25-year descriptive — Table 1: cohort characteristics × surgical era."""
import sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import get_cursor

OUT = Path("/Users/ros/THyroid 2026/snowflake_trial/reports/m032_table1.md")
ctx, cur = get_cursor()

print("=== Pulling M032 cohort ===")
cur.execute("""
SELECT
  RESEARCH_ID, AGE_AT_SURGERY, SEX, RACE,
  HISTOLOGY_FINAL, IS_MALIGNANT, FIRST_SURGERY_DATE,
  SURGERY_YEAR, ERA,
  TUMOR_SIZE_CM_MAX, AJCC8_STAGE_GROUP, ETE_GRADE,
  SURG_PROCEDURE_TYPE, RAI_RECEIVED_FLAG,
  MOLECULAR_TESTED_CONFIRMED, BRAF_POSITIVE_FINAL,
  ANY_RECURRENCE_FLAG, OVERALL_SURVIVAL_YEARS, FOLLOWUP_YEARS
FROM COHORT_M032_25YR_DESCRIPTIVE
WHERE ERA != 'Unknown'
""")
rows = cur.fetchall(); cols = [c[0] for c in cur.description]
ctx.close()

import pandas as pd
df = pd.DataFrame(rows, columns=cols)
print(f"  {len(df):,} rows")
era_order = ['Early (1999-2005)', 'Middle (2006-2013)', 'Modern (2014-2019)', 'Contemporary (2020+)']
era_dfs = {e: df[df['ERA'] == e] for e in era_order}
for e in era_order:
    print(f"  {e}: {len(era_dfs[e]):,}")

try:
    from scipy.stats import kruskal, chi2_contingency
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
    series = [pd.to_numeric(era_dfs[e][col], errors='coerce').dropna() for e in era_order]
    p = ""
    if HAVE_STATS and all(len(s) > 1 for s in series):
        try:
            _, p_val = kruskal(*series)
            p = f"{p_val:.4f}" if p_val >= 0.0001 else "<0.0001"
        except Exception:
            p = "—"
    return [label] + [fmt_cont(era_dfs[e][col]) for e in era_order] + [p]


def cat_rows(label, col, top=None):
    out = [[f"**{label}**", "", "", "", "", ""]]
    counts = df[col].value_counts(dropna=False)
    if top: counts = counts.head(top)
    p_overall = ""
    if HAVE_STATS:
        try:
            ct = pd.crosstab(df[col].fillna("(missing)"), df['ERA'])
            _, p_val, _, _ = chi2_contingency(ct)
            p_overall = f"{p_val:.4f}" if p_val >= 0.0001 else "<0.0001"
        except Exception:
            p_overall = "—"
    for v in counts.index:
        v_str = str(v) if v is not None else "(missing)"
        cells = [f"  {v_str}"]
        for e in era_order:
            n_e = ((era_dfs[e][col].fillna("(missing)") == v) | (era_dfs[e][col].isna() & (v_str == "(missing)"))).sum()
            cells.append(fmt_pct(int(n_e), len(era_dfs[e])))
        cells.append("")
        out.append(cells)
    if out: out[0][5] = p_overall
    return out


t1 = []
t1.append(["Total, N"] + [f"{len(era_dfs[e]):,}" for e in era_order] + [""])
t1.append(cont_row("Age (years)", "AGE_AT_SURGERY"))
t1.append(cont_row("Tumor size max (cm)", "TUMOR_SIZE_CM_MAX"))
t1.append(cont_row("Followup (years)", "FOLLOWUP_YEARS"))
t1.append(["", "", "", "", "", ""])
for blk in [
    cat_rows("Sex", "SEX"),
    cat_rows("Race", "RACE", top=8),
    cat_rows("Malignant", "IS_MALIGNANT"),
    cat_rows("AJCC 8 stage", "AJCC8_STAGE_GROUP"),
    cat_rows("Surgery type", "SURG_PROCEDURE_TYPE", top=6),
    cat_rows("RAI received", "RAI_RECEIVED_FLAG"),
    cat_rows("Molecular tested", "MOLECULAR_TESTED_CONFIRMED"),
    cat_rows("BRAF positive", "BRAF_POSITIVE_FINAL"),
    cat_rows("Any recurrence", "ANY_RECURRENCE_FLAG"),
]:
    t1.extend(blk)
    t1.append(["", "", "", "", "", ""])

md = ["# Table 1 — Manuscript M032: 25-Year Descriptive Cohort by Surgical Era\n",
      f"**Generated:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n",
      f"**Cohort:** 10,871 patients (1999-2025) excluding ERA='Unknown' (N={len(df):,})\n",
      f"**Strata:** 4-bucket era (Early/Middle/Modern/Contemporary)\n",
      f"**P-values:** Kruskal-Wallis (continuous), chi-square (categorical)\n\n",
      "Generated from THYROID_VALIDATION.PUBLIC.COHORT_M032_25YR_DESCRIPTIVE.\n\n"]
md.append("| Variable | Early (1999-2005) | Middle (2006-2013) | Modern (2014-2019) | Contemporary (2020+) | p |\n")
md.append("| --- | --- | --- | --- | --- | --- |\n")
for row in t1:
    if row == ["", "", "", "", "", ""]: continue
    md.append("| " + " | ".join(str(c) for c in row) + " |\n")

OUT.write_text("".join(md))
print(f"\n[saved] {OUT}  ({OUT.stat().st_size:,} bytes)")
