"""M038 Massive Goiter — cohort scaffold + Table 1 (≥200g vs <200g).

Builds COHORT_M038_MASSIVE_GOITER view in Snowflake and renders a Table 1
stratified by gland weight bucket: ≥200g, 50-199g, <50g, NULL.
"""
import sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import deploy_histology_lookup_ssot, get_cursor

OUT = Path("/Users/ros/THyroid 2026/snowflake_trial/reports/m038_table1_massive_goiter.md")
ctx, cur = get_cursor()
deploy_histology_lookup_ssot(cur)

# Build cohort view
print("=== Building COHORT_M038_MASSIVE_GOITER ===")
cur.execute("""
CREATE OR REPLACE VIEW THYROID_VALIDATION.PUBLIC.COHORT_M038_MASSIVE_GOITER AS
SELECT
  cp.RESEARCH_ID, cp.AGE_AT_SURGERY, cp.SEX, cp.RACE,
  cp.HISTOLOGY_FINAL,
  COALESCE(lu.HISTOLOGY_GROUP, 'Other') AS HISTOLOGY_GROUP,
  cp.IS_MALIGNANT, cp.FIRST_SURGERY_DATE,
  cp.AJCC8_T_STAGE, cp.AJCC8_N_STAGE, cp.AJCC8_M_STAGE, cp.AJCC8_STAGE_GROUP,
  cp.TUMOR_SIZE_CM_MAX, cp.ETE_GRADE,
  cp.GLAND_WEIGHT_FINAL_G, cp.GLAND_WEIGHT_SOURCE, cp.MULTIFOCAL_FLAG_PATH,
  cp.SYN_MULTINODULAR_GOITER, cp.CT_GOITER_PRESENT_ANY,
  cp.SURG_PROCEDURE_TYPE, cp.RAI_RECEIVED_FLAG,
  cp.ANY_RECURRENCE_FLAG, cp.OVERALL_SURVIVAL_YEARS, cp.FOLLOWUP_YEARS,
  CASE
    WHEN cp.GLAND_WEIGHT_FINAL_G IS NULL THEN 'unknown'
    WHEN cp.GLAND_WEIGHT_FINAL_G >= 200 THEN 'massive_200g_plus'
    WHEN cp.GLAND_WEIGHT_FINAL_G >= 50  THEN 'moderate_50_to_199g'
    ELSE 'small_under_50g'
  END AS WEIGHT_BUCKET,
  CASE WHEN cp.GLAND_WEIGHT_FINAL_G >= 200 THEN TRUE ELSE FALSE END AS IS_MASSIVE_GOITER
FROM THYROID_VALIDATION.PUBLIC.CANONICAL_PATIENT_MASTER_FLAT cp
LEFT JOIN THYROID_VALIDATION.PUBLIC.CANONICAL_HISTOLOGY_LOOKUP_V1 lu
  ON cp.HISTOLOGY_FINAL = lu.HISTOLOGY_FINAL_RAW
""")
cur.execute("SELECT WEIGHT_BUCKET, COUNT(*) FROM COHORT_M038_MASSIVE_GOITER GROUP BY 1 ORDER BY 2 DESC")
for r in cur.fetchall(): print(f"  {r[0]}: {r[1]:,}")

# Pull for Table 1
cur.execute("SELECT * FROM COHORT_M038_MASSIVE_GOITER WHERE WEIGHT_BUCKET != 'unknown'")
rows = cur.fetchall(); cols = [c[0] for c in cur.description]
ctx.close()

import pandas as pd
df = pd.DataFrame(rows, columns=cols)
df['AGE_AT_SURGERY'] = pd.to_numeric(df['AGE_AT_SURGERY'], errors='coerce')
df['TUMOR_SIZE_CM_MAX'] = pd.to_numeric(df['TUMOR_SIZE_CM_MAX'], errors='coerce')
df['GLAND_WEIGHT_FINAL_G'] = pd.to_numeric(df['GLAND_WEIGHT_FINAL_G'], errors='coerce')
df['FOLLOWUP_YEARS'] = pd.to_numeric(df['FOLLOWUP_YEARS'], errors='coerce')

bucket_order = ['massive_200g_plus', 'moderate_50_to_199g', 'small_under_50g']
bucket_dfs = {b: df[df['WEIGHT_BUCKET'] == b] for b in bucket_order}
print(f"\n  {bucket_order[0]}: {len(bucket_dfs[bucket_order[0]]):,}")
print(f"  {bucket_order[1]}: {len(bucket_dfs[bucket_order[1]]):,}")
print(f"  {bucket_order[2]}: {len(bucket_dfs[bucket_order[2]]):,}")

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
    series = [pd.to_numeric(bucket_dfs[b][col], errors='coerce').dropna() for b in bucket_order]
    p = ""
    if HAVE_STATS and all(len(s) > 1 for s in series):
        try:
            _, p_val = kruskal(*series)
            p = f"{p_val:.4f}" if p_val >= 0.0001 else "<0.0001"
        except Exception: p = "—"
    return [label] + [fmt_cont(bucket_dfs[b][col]) for b in bucket_order] + [p]


def cat_rows(label, col, top=None):
    out = [[f"**{label}**", "", "", "", ""]]
    counts = df[col].value_counts(dropna=False)
    if top: counts = counts.head(top)
    p_overall = ""
    if HAVE_STATS:
        try:
            ct = pd.crosstab(df[col].fillna("(missing)"), df['WEIGHT_BUCKET'])
            _, p_val, _, _ = chi2_contingency(ct)
            p_overall = f"{p_val:.4f}" if p_val >= 0.0001 else "<0.0001"
        except Exception: p_overall = "—"
    for v in counts.index:
        v_str = str(v) if v is not None else "(missing)"
        cells = [f"  {v_str}"]
        for b in bucket_order:
            n_b = ((bucket_dfs[b][col].fillna("(missing)") == v) | (bucket_dfs[b][col].isna() & (v_str == "(missing)"))).sum()
            cells.append(fmt_pct(int(n_b), len(bucket_dfs[b])))
        cells.append("")
        out.append(cells)
    if out: out[0][4] = p_overall
    return out


t1 = []
t1.append(["Total, N"] + [f"{len(bucket_dfs[b]):,}" for b in bucket_order] + [""])
t1.append(cont_row("Gland weight (g)", "GLAND_WEIGHT_FINAL_G"))
t1.append(cont_row("Age (years)", "AGE_AT_SURGERY"))
t1.append(cont_row("Tumor size max (cm)", "TUMOR_SIZE_CM_MAX"))
t1.append(cont_row("Followup (years)", "FOLLOWUP_YEARS"))
t1.append(["", "", "", "", ""])
for blk in [
    cat_rows("Sex", "SEX"),
    cat_rows("Race", "RACE", top=8),
    cat_rows("Histology group (SSOT)", "HISTOLOGY_GROUP"),
    cat_rows("Malignant", "IS_MALIGNANT"),
    cat_rows("Multifocal (path)", "MULTIFOCAL_FLAG_PATH"),
    cat_rows("Multinodular (synoptic)", "SYN_MULTINODULAR_GOITER"),
    cat_rows("CT goiter present", "CT_GOITER_PRESENT_ANY"),
    cat_rows("AJCC 8 stage", "AJCC8_STAGE_GROUP"),
    cat_rows("Surgery type", "SURG_PROCEDURE_TYPE", top=6),
    cat_rows("RAI received", "RAI_RECEIVED_FLAG"),
    cat_rows("Any recurrence", "ANY_RECURRENCE_FLAG"),
]:
    t1.extend(blk)
    t1.append(["", "", "", "", ""])

md = ["# Table 1 — Manuscript M038: Massive Goiter (Definition Paper)\n",
      f"**Generated:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n",
      f"**Cohort:** {len(df):,} patients with non-NULL gland weight\n",
      f"**Strata:** ≥200g (massive)={len(bucket_dfs['massive_200g_plus']):,} | 50-199g (moderate)={len(bucket_dfs['moderate_50_to_199g']):,} | <50g (small)={len(bucket_dfs['small_under_50g']):,}\n",
      "**P-values:** Kruskal-Wallis (continuous), chi-square (categorical)\n",
      "**Source:** THYROID_VALIDATION.PUBLIC.COHORT_M038_MASSIVE_GOITER (post-mig_262 LN flag rebuild)\n\n"]
md.append("| Variable | ≥200g (massive) | 50-199g (moderate) | <50g (small) | p |\n")
md.append("| --- | --- | --- | --- | --- |\n")
for row in t1:
    if row == ["", "", "", "", ""]: continue
    md.append("| " + " | ".join(str(c) for c in row) + " |\n")
md.append("\n")
md.append("## Notes\n\n")
md.append("- Excludes 1,741 patients with NULL gland weight (would need separate CF audit; mig_252 also identified this).\n")
md.append("- ≥200g threshold per Logan-ratified mig_252 audit + literature: 'massive goiter' definition typically ≥150-200g, with ≥200g as the conservative cutoff.\n")
md.append("- Complications by weight strata require mig_252 to land first (the strict 'present + def/probable' definition); current rates may overcount via the broken `comp_*_confirmed` rollup logic.\n")

OUT.write_text("".join(md))
print(f"\n[saved] {OUT}  ({OUT.stat().st_size:,} bytes)")
