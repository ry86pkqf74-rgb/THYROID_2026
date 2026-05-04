"""M032 + M037 post-mig_281+285+286 NLP augment renders. v2 — independent steps."""
import os, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import get_cursor
import duckdb
import pandas as pd

REPORTS = Path("/Users/ros/THyroid 2026/snowflake_trial/reports")
MD_TOKEN = os.environ.get("MOTHERDUCK_TOKEN") or os.environ.get("motherduck_token")

# ============================================================
# M032 — 25-yr Descriptive (SF flat view; works)
# ============================================================
print("=== M032 Table 1 ===")
ctx, cur = get_cursor()
cur.execute("USE DATABASE THYROID_VALIDATION")
cur.execute("USE SCHEMA PUBLIC")
cur.execute("""
SELECT
  COUNT(*) AS n_total, COUNT_IF(IS_MALIGNANT) AS n_malig,
  COUNT_IF(PMHX_NLP_SMOKING_STATUS IS NOT NULL) AS n_smk_known,
  COUNT_IF(PMHX_NLP_SMOKING_STATUS = 'current') AS n_smk_current,
  COUNT_IF(PMHX_NLP_SMOKING_STATUS = 'former') AS n_smk_former,
  COUNT_IF(PMHX_NLP_SMOKING_STATUS = 'never') AS n_smk_never,
  COUNT_IF(PMHX_NLP_FAMILY_HX_THYROID = TRUE) AS n_fhx_thy_present,
  COUNT_IF(PMHX_NLP_FAMILY_HX_THYROID = FALSE) AS n_fhx_thy_absent
FROM CANONICAL_PATIENT_MASTER_FLAT
""")
n_total, n_malig, n_smk_known, n_smk_cur, n_smk_for, n_smk_nev, n_fhx_p, n_fhx_a = cur.fetchone()

# Smoking-by-malignancy cross-tab
cur.execute("""
SELECT
  CASE WHEN IS_MALIGNANT THEN 'malig' ELSE 'benign' END AS arm,
  COUNT_IF(PMHX_NLP_SMOKING_STATUS = 'current') AS cur_,
  COUNT_IF(PMHX_NLP_SMOKING_STATUS = 'former') AS for_,
  COUNT_IF(PMHX_NLP_SMOKING_STATUS = 'never') AS nev_,
  COUNT_IF(PMHX_NLP_SMOKING_STATUS IS NOT NULL) AS known_
FROM CANONICAL_PATIENT_MASTER_FLAT GROUP BY 1
""")
smk_arms = cur.fetchall()

# Era stratification
cur.execute("""
SELECT
  CASE
    WHEN YEAR(FIRST_SURGERY_DATE) < 2005 THEN 'A_1999_2004'
    WHEN YEAR(FIRST_SURGERY_DATE) < 2010 THEN 'B_2005_2009'
    WHEN YEAR(FIRST_SURGERY_DATE) < 2015 THEN 'C_2010_2014'
    WHEN YEAR(FIRST_SURGERY_DATE) < 2020 THEN 'D_2015_2019'
    WHEN YEAR(FIRST_SURGERY_DATE) >= 2020 THEN 'E_2020_2025'
    ELSE 'F_unknown'
  END AS era,
  COUNT(*) AS n,
  COUNT_IF(IS_MALIGNANT) AS n_malig,
  ROUND(100.0 * COUNT_IF(IS_MALIGNANT) / COUNT(*), 1) AS pct_malig,
  ROUND(100.0 * COUNT_IF(PMHX_NLP_SMOKING_STATUS = 'current') / NULLIF(COUNT_IF(PMHX_NLP_SMOKING_STATUS IS NOT NULL), 0), 1) AS pct_current_smk,
  ROUND(100.0 * COUNT_IF(PMHX_NLP_FAMILY_HX_THYROID = TRUE) / NULLIF(COUNT_IF(PMHX_NLP_FAMILY_HX_THYROID IS NOT NULL), 0), 1) AS pct_fhx_present
FROM CANONICAL_PATIENT_MASTER_FLAT GROUP BY era ORDER BY era
""")
era_rows = cur.fetchall()
ctx.close()

m032 = [
    "# M032 — 25-yr Descriptive (post-mig_281/285 NLP augment)",
    "**Generated:** 2026-05-04",
    f"**Cohort:** n={n_total:,} / malig={n_malig:,} ({100*n_malig/n_total:.1f}%)",
    "",
    "## Smoking — cohort-wide",
    "| Status | n | % of known | % of cohort |",
    "|---|---:|---:|---:|",
    f"| Current | {n_smk_cur:,} | {100*n_smk_cur/n_smk_known:.1f}% | {100*n_smk_cur/n_total:.2f}% |",
    f"| Former  | {n_smk_for:,} | {100*n_smk_for/n_smk_known:.1f}% | {100*n_smk_for/n_total:.2f}% |",
    f"| Never   | {n_smk_nev:,} | {100*n_smk_nev/n_smk_known:.1f}% | {100*n_smk_nev/n_total:.2f}% |",
    f"| **Known** | **{n_smk_known:,}** | | **{100*n_smk_known/n_total:.1f}%** |",
    "",
    "## Smoking by malignancy",
    "| Arm | Current | Former | Never | Known |",
    "|---|---:|---:|---:|---:|",
    *[f"| {a[0]} | {a[1]:,} | {a[2]:,} | {a[3]:,} | {a[4]:,} |" for a in smk_arms],
    "",
    "## Family hx of thyroid cancer",
    "| Status | n |",
    "|---|---:|",
    f"| Present | {n_fhx_p:,} ({100*n_fhx_p/(n_fhx_p+n_fhx_a):.1f}% of known) |",
    f"| Absent  | {n_fhx_a:,} |",
    f"| **Known** | **{n_fhx_p + n_fhx_a:,}** ({100*(n_fhx_p+n_fhx_a)/n_total:.1f}% of cohort) |",
    "",
    "## Era stratification",
    "| Era | n | n_malig | %_malig | %_current_smoker | %_fhx_thy_present |",
    "|---|---:|---:|---:|---:|---:|",
    *[f"| {e[0]} | {e[1]:,} | {e[2]:,} | {e[3]} | {e[4]} | {e[5]} |" for e in era_rows],
]
(REPORTS / "M032_table1_with_nlp_20260504.md").write_text("\n".join(m032))
print(f"  M032 -> {REPORTS}/M032_table1_with_nlp_20260504.md")

# ============================================================
# M037 — LN Predictors logreg with family-hx (direct MD query)
# ============================================================
print("\n=== M037 logreg ===")
md = duckdb.connect(f"md:thyroid_canonical_publication_v1_0?motherduck_token={MD_TOKEN}")

# Pull M037 cohort from MD (post-mig_286 augmented view)
df = md.execute("""
SELECT * FROM manuscript_workspace.cohort_m037_ln_metastasis_v1 LIMIT 5000
""").fetch_df()
md.close()
print(f"  M037 cohort cols: {len(df.columns)} / rows: {len(df)}")
print(f"  Family-hx cols available: {[c for c in df.columns if 'family_hx' in c.lower() or 'syndrome' in c.lower()]}")

# Find LN outcome col
ln_candidates = [c for c in df.columns if c.lower() in ('ln_positive','has_ln_metastasis','ln_pos_flag','n_positive_ln_flag','any_ln_pos')]
print(f"  LN candidates: {ln_candidates}")
if not ln_candidates:
    # Fall back to AJCC N stage = N1
    if 'ajcc8_n_stage' in [c.lower() for c in df.columns]:
        ln_col_actual = next(c for c in df.columns if c.lower()=='ajcc8_n_stage')
        df['ln_pos'] = df[ln_col_actual].astype(str).str.contains('N1', na=False).astype(int)
    else:
        df['ln_pos'] = 0
else:
    df['ln_pos'] = df[ln_candidates[0]].astype(bool).astype(int)

# fhx_thy
fhx_col = next((c for c in df.columns if 'family_hx_thyroid' in c.lower()), None)
print(f"  fhx col: {fhx_col}")
if fhx_col:
    df['fhx_thy'] = (df[fhx_col].astype(str).str.lower().isin(['true','t','1','yes'])).astype(int)
else:
    df['fhx_thy'] = 0

# Try logreg
try:
    import statsmodels.api as sm
    import statsmodels.formula.api as smf

    age_col = next((c for c in df.columns if c.lower() == 'age_at_surgery'), None)
    size_col = next((c for c in df.columns if 'tumor_size' in c.lower() and 'cm' in c.lower()), None)
    sex_col = next((c for c in df.columns if c.lower() == 'sex'), None)

    cols = ['ln_pos', 'fhx_thy']
    if age_col: cols.append(age_col)
    if size_col: cols.append(size_col)
    if sex_col: cols.append(sex_col)
    df2 = df[cols].dropna()

    print(f"  complete-case n: {len(df2)} (was {len(df)})")
    print(f"  fhx_thy=1: {df2['fhx_thy'].sum()}, ln_pos=1: {df2['ln_pos'].sum()}")

    formula = "ln_pos ~ fhx_thy"
    if age_col: formula += f" + {age_col}"
    if size_col: formula += f" + {size_col}"
    if sex_col: formula += f" + C({sex_col})"

    if len(df2) > 100 and df2['fhx_thy'].sum() > 5:
        m = smf.glm(formula, data=df2, family=sm.families.Binomial()).fit()
        coefs = m.summary2().tables[1].copy()
        coefs['OR'] = coefs['Coef.'].apply(lambda x: round(2.71828**x, 3))
        coefs['OR_CI_low'] = coefs.apply(lambda r: round(2.71828**(r['Coef.'] - 1.96*r['Std.Err.']), 3), axis=1)
        coefs['OR_CI_high'] = coefs.apply(lambda r: round(2.71828**(r['Coef.'] + 1.96*r['Std.Err.']), 3), axis=1)
        coefs_md = coefs[['OR','OR_CI_low','OR_CI_high','P>|z|']].to_markdown(floatfmt=".4f")
        m037 = [
            "# M037 — LN Predictors logreg (post-mig_286 family-hx augment)",
            "**Generated:** 2026-05-04",
            f"**Cohort:** n={len(df2)} (complete-case)",
            f"**Outcome:** LN_pos (N1 vs N0)",
            f"**Predictor of interest:** family_hx_thyroid (n_present={df2['fhx_thy'].sum()})",
            "",
            "## Logreg results (OR + 95% CI)",
            "",
            coefs_md,
            "",
            f"Pseudo-R² (McFadden) = {1 - m.llf/m.llnull:.4f}",
            f"LR vs null χ² = {2*(m.llf-m.llnull):.2f}, df = {m.df_model}",
        ]
    else:
        m037 = [f"# M037 logreg — UNDERPOWERED", "", f"complete-case n={len(df2)}, fhx_thy=1: {df2['fhx_thy'].sum()}"]
except Exception as e:
    m037 = [f"# M037 logreg — error: {e}", "", f"cohort cols: {list(df.columns)}"]

(REPORTS / "M037_logreg_family_hx_20260504.md").write_text("\n".join(m037))
print(f"  M037 -> {REPORTS}/M037_logreg_family_hx_20260504.md")
print("=== DONE ===")
