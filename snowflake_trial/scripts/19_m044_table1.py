"""M044 ETE — Table 1: baseline characteristics by ETE group.

Stratifies the malignant cohort by ETE grade (none/microscopic/gross).
Continuous: age, tumor size, LN counts, followup years.
Categorical: sex, race, histology, T/N/M, surgery type, BRAF, RAI, recurrence.
P-values via Kruskal-Wallis (3-group continuous) + chi-square (categorical).
"""
import sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import deploy_histology_lookup_ssot, get_cursor

OUT = Path("/Users/ros/THyroid 2026/snowflake_trial/reports/m044_table1.md")
ctx, cur = get_cursor()
deploy_histology_lookup_ssot(cur)

# Pull cohort: malignant patients with non-NULL ETE grade
print("=== Pulling M044 cohort ===")
cur.execute("""
SELECT
  cp.RESEARCH_ID, cp.AGE_AT_SURGERY, cp.SEX, cp.RACE, cp.BMI_COMBINED,
  cp.HISTOLOGY_FINAL,
  COALESCE(lu.HISTOLOGY_GROUP, 'Other') AS HISTOLOGY_GROUP,
  cp.AJCC8_T_STAGE, cp.AJCC8_N_STAGE, cp.AJCC8_M_STAGE, cp.AJCC8_STAGE_GROUP,
  cp.TUMOR_SIZE_CM_MAX, cp.ETE_GRADE,
  cp.LN_TOTAL_EXAMINED, cp.LN_TOTAL_POSITIVE, cp.LN_POSITIVE_FLAG,
  cp.SURG_PROCEDURE_TYPE, cp.RAI_RECEIVED_FLAG,
  cp.MOLECULAR_TESTED_CONFIRMED, cp.BRAF_POSITIVE_FINAL, cp.RAS_POSITIVE_FINAL,
  cp.ANY_RECURRENCE_FLAG, cp.TIME_TO_RECURRENCE_DAYS,
  cp.OVERALL_SURVIVAL_YEARS, cp.FOLLOWUP_YEARS,
  cp.FIRST_SURGERY_DATE
FROM CANONICAL_PATIENT_MASTER_FLAT cp
LEFT JOIN CANONICAL_HISTOLOGY_LOOKUP_V1 lu
  ON cp.HISTOLOGY_FINAL = lu.HISTOLOGY_FINAL_RAW
WHERE cp.IS_MALIGNANT = TRUE AND cp.ETE_GRADE IS NOT NULL
  AND cp.ETE_GRADE IN ('none','microscopic','gross')
""")
rows = cur.fetchall()
cols = [c[0] for c in cur.description]
ctx.close()

import pandas as pd
df = pd.DataFrame(rows, columns=cols)
print(f"  {len(df):,} rows, {len(df.columns)} cols")

# 3-group ETE strata
none_df = df[df["ETE_GRADE"] == "none"]
micro_df = df[df["ETE_GRADE"] == "microscopic"]
gross_df = df[df["ETE_GRADE"] == "gross"]
n_none, n_micro, n_gross = len(none_df), len(micro_df), len(gross_df)
print(f"  ETE none={n_none}  microscopic={n_micro}  gross={n_gross}")

try:
    from scipy.stats import kruskal, chi2_contingency
    HAVE_STATS = True
except ImportError:
    HAVE_STATS = False
    print("  scipy unavailable — p-values omitted")


def fmt_cont(s):
    s = pd.to_numeric(s, errors="coerce").dropna()
    if len(s) == 0:
        return "—"
    return f"{s.mean():.1f} ± {s.std():.1f}; {s.median():.1f} [{s.quantile(0.25):.1f}–{s.quantile(0.75):.1f}]"


def fmt_pct(n, d):
    if d == 0:
        return "—"
    return f"{n} ({100.0*n/d:.1f}%)"


def cont_row(label, col):
    a = pd.to_numeric(none_df[col], errors="coerce").dropna()
    b = pd.to_numeric(micro_df[col], errors="coerce").dropna()
    c = pd.to_numeric(gross_df[col], errors="coerce").dropna()
    p = ""
    if HAVE_STATS and len(a) > 1 and len(b) > 1 and len(c) > 1:
        try:
            _, p_val = kruskal(a, b, c)
            p = f"{p_val:.4f}" if p_val >= 0.0001 else "<0.0001"
        except Exception:
            p = "—"
    return [label, fmt_cont(none_df[col]), fmt_cont(micro_df[col]), fmt_cont(gross_df[col]), p]


def cat_rows(label, col, top=None):
    rows_out = [[f"**{label}**", "", "", "", ""]]
    counts = df[col].value_counts(dropna=False)
    if top:
        counts = counts.head(top)
    p_overall = ""
    if HAVE_STATS:
        try:
            ct = pd.crosstab(df[col].fillna("(missing)"), df["ETE_GRADE"])
            chi2, p_val, _, _ = chi2_contingency(ct)
            p_overall = f"{p_val:.4f}" if p_val >= 0.0001 else "<0.0001"
        except Exception:
            p_overall = "—"
    for v in counts.index:
        v_str = str(v) if v is not None else "(missing)"
        n_a = ((none_df[col].fillna("(missing)") == v) | (none_df[col].isna() & (v_str == "(missing)"))).sum()
        n_b = ((micro_df[col].fillna("(missing)") == v) | (micro_df[col].isna() & (v_str == "(missing)"))).sum()
        n_c = ((gross_df[col].fillna("(missing)") == v) | (gross_df[col].isna() & (v_str == "(missing)"))).sum()
        rows_out.append([f"  {v_str}",
                         fmt_pct(int(n_a), n_none),
                         fmt_pct(int(n_b), n_micro),
                         fmt_pct(int(n_c), n_gross), ""])
    if rows_out:
        rows_out[0][4] = p_overall
    return rows_out


t1 = []
t1.append(["Total, N", f"{n_none:,}", f"{n_micro:,}", f"{n_gross:,}", ""])
t1.append(cont_row("Age (years), mean ± SD; median [IQR]", "AGE_AT_SURGERY"))
t1.append(cont_row("BMI, mean ± SD; median [IQR]", "BMI_COMBINED"))
t1.append(cont_row("Tumor size max (cm), mean ± SD; median [IQR]", "TUMOR_SIZE_CM_MAX"))
t1.append(cont_row("LN examined, mean ± SD; median [IQR]", "LN_TOTAL_EXAMINED"))
t1.append(cont_row("LN positive, mean ± SD; median [IQR]", "LN_TOTAL_POSITIVE"))
t1.append(cont_row("Followup (years)", "FOLLOWUP_YEARS"))
t1.append(["", "", "", "", ""])
for blk in [
    cat_rows("Sex", "SEX"),
    cat_rows("Race", "RACE", top=8),
    cat_rows("Histology group", "HISTOLOGY_GROUP"),
    cat_rows("AJCC 8 T stage", "AJCC8_T_STAGE"),
    cat_rows("AJCC 8 N stage", "AJCC8_N_STAGE"),
    cat_rows("AJCC 8 M stage", "AJCC8_M_STAGE"),
    cat_rows("AJCC 8 stage group", "AJCC8_STAGE_GROUP"),
    cat_rows("Surgery type", "SURG_PROCEDURE_TYPE", top=6),
    cat_rows("RAI received", "RAI_RECEIVED_FLAG"),
    cat_rows("Molecular tested", "MOLECULAR_TESTED_CONFIRMED"),
    cat_rows("BRAF positive", "BRAF_POSITIVE_FINAL"),
    cat_rows("RAS positive", "RAS_POSITIVE_FINAL"),
    cat_rows("Any recurrence", "ANY_RECURRENCE_FLAG"),
]:
    t1.extend(blk)
    t1.append(["", "", "", "", ""])

md = ["# Table 1 — Manuscript M044: Extrathyroidal Extension and Outcomes\n",
      f"**Generated:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n",
      f"**Cohort:** Malignant + ETE_GRADE in (none, microscopic, gross) (N={len(df):,})\n",
      f"**Strata:** ETE none={n_none:,} | microscopic={n_micro:,} | gross={n_gross:,}\n",
      f"**P-values:** Kruskal-Wallis (3-group continuous), chi-square (categorical){' [scipy unavailable]' if not HAVE_STATS else ''}\n\n",
      "Generated from THYROID_VALIDATION.PUBLIC.CANONICAL_PATIENT_MASTER_FLAT via Snowflake Cortex trial.\n\n"]
md.append("| Variable | ETE none | ETE microscopic | ETE gross | p |\n")
md.append("| --- | --- | --- | --- | --- |\n")
for row in t1:
    if row == ["", "", "", "", ""]:
        continue
    md.append("| " + " | ".join(str(c) for c in row) + " |\n")
md.append("\n")

OUT.write_text("".join(md))
print(f"\n[saved] {OUT}  ({OUT.stat().st_size:,} bytes)")
