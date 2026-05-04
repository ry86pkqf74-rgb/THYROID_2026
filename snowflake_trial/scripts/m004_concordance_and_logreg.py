"""M004 verification:
1. NLP vs syn_* concordance — agreement rate where both available
2. Combined coverage (NLP OR syn) — total patient counts per autoimmune category
3. M004 logreg with NLP-augmented covariates — outcome: is_malignant
"""
import os, sys, json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import get_cursor
import duckdb
import pandas as pd

REPORTS = Path("/Users/ros/THyroid 2026/snowflake_trial/reports")
MD_TOKEN = os.environ.get("MOTHERDUCK_TOKEN") or os.environ.get("motherduck_token")

# === 1. Pull NLP results from SF + sync to MD-side patient flags ===
print("=== 1. Pull SF NLP results + roll up to per-patient ===")
ctx, cur = get_cursor()
cur.execute("USE DATABASE THYROID_VALIDATION")
cur.execute("USE SCHEMA PUBLIC")

# Collapse note-level results to per-patient flag (TRUE if any note flagged present)
cur.execute("""
CREATE OR REPLACE VIEW _M004_NLP_HASHIMOTO_PT AS
SELECT RESEARCH_ID,
       MAX(CASE WHEN HASHIMOTO_STATUS = 'hashimoto_present' THEN TRUE ELSE FALSE END) AS nlp_hashimoto
FROM NLP_HASHIMOTO_FULL_RESULTS_v1 GROUP BY RESEARCH_ID
""")
cur.execute("""
CREATE OR REPLACE VIEW _M004_NLP_GRAVES_PT AS
SELECT RESEARCH_ID,
       MAX(CASE WHEN GRAVES_STATUS = 'graves_present' THEN TRUE ELSE FALSE END) AS nlp_graves
FROM NLP_GRAVES_FULL_RESULTS_v1 GROUP BY RESEARCH_ID
""")

cur.execute("SELECT COUNT_IF(nlp_hashimoto), COUNT(*) FROM _M004_NLP_HASHIMOTO_PT")
hashi_present, hashi_total = cur.fetchone()
cur.execute("SELECT COUNT_IF(nlp_graves), COUNT(*) FROM _M004_NLP_GRAVES_PT")
graves_present, graves_total = cur.fetchone()
print(f"  Hashimoto: NLP-present {hashi_present}/{hashi_total} (in keyword corpus)")
print(f"  Graves:    NLP-present {graves_present}/{graves_total} (in keyword corpus)")

ctx.close()

# === 2. Pull NLP rollups from SF as pandas, push to MD as TEMP ===
print("\n=== 2. Concordance vs syn_* + combined coverage (SF→pandas→MD) ===")
ctx, cur = get_cursor()
cur.execute("USE DATABASE THYROID_VALIDATION")
cur.execute("USE SCHEMA PUBLIC")
df_hashi = cur.execute("SELECT * FROM _M004_NLP_HASHIMOTO_PT").fetch_pandas_all()
df_graves = cur.execute("SELECT * FROM _M004_NLP_GRAVES_PT").fetch_pandas_all()
ctx.close()
print(f"  pulled hashi rollup: {len(df_hashi):,} rows / graves rollup: {len(df_graves):,} rows")

md = duckdb.connect(f"md:thyroid_canonical_publication_v1_0?motherduck_token={MD_TOKEN}")
# Lowercase col names for MD compatibility
df_hashi.columns = [c.lower() for c in df_hashi.columns]
df_graves.columns = [c.lower() for c in df_graves.columns]
md.register("_nlp_hashi", df_hashi)
md.register("_nlp_graves", df_graves)

# Concordance — Hashimoto
hashi_concord = md.execute("""
SELECT
  COUNT(*) AS n_pts_total,
  COUNT_IF(syn_hashimoto OR nlp_hashimoto) AS n_either,
  COUNT_IF(syn_hashimoto AND nlp_hashimoto) AS n_both,
  COUNT_IF(syn_hashimoto AND NOT COALESCE(nlp_hashimoto, FALSE)) AS n_syn_only,
  COUNT_IF(NOT COALESCE(syn_hashimoto, FALSE) AND nlp_hashimoto) AS n_nlp_only
FROM main.canonical_patient_master pm
LEFT JOIN _nlp_hashi h USING (research_id)
""").fetch_df()
print(f"\nHashimoto concordance:")
print(hashi_concord.to_string(index=False))

# Concordance — Graves
graves_concord = md.execute("""
SELECT
  COUNT(*) AS n_pts_total,
  COUNT_IF(syn_graves OR nlp_graves) AS n_either,
  COUNT_IF(syn_graves AND nlp_graves) AS n_both,
  COUNT_IF(syn_graves AND NOT COALESCE(nlp_graves, FALSE)) AS n_syn_only,
  COUNT_IF(NOT COALESCE(syn_graves, FALSE) AND nlp_graves) AS n_nlp_only
FROM main.canonical_patient_master pm
LEFT JOIN _nlp_graves g USING (research_id)
""").fetch_df()
print(f"\nGraves concordance:")
print(graves_concord.to_string(index=False))

# Combined autoimmune category (combined NLP+syn) × malignancy
combined = md.execute("""
WITH joined AS (
  SELECT pm.research_id, pm.is_malignant,
         COALESCE(pm.syn_hashimoto, FALSE) OR COALESCE(h.nlp_hashimoto, FALSE) AS has_hashi,
         COALESCE(pm.syn_graves, FALSE)    OR COALESCE(g.nlp_graves, FALSE)    AS has_graves
  FROM main.canonical_patient_master pm
  LEFT JOIN _nlp_hashi h USING (research_id)
  LEFT JOIN _nlp_graves g USING (research_id)
)
SELECT
  CASE WHEN has_hashi AND has_graves THEN 'A_both'
       WHEN has_hashi THEN 'B_hashimoto_only'
       WHEN has_graves THEN 'C_graves_only'
       ELSE 'D_neither' END AS autoimmune_combined,
  COUNT(*) AS n,
  COUNT_IF(is_malignant) AS n_malig,
  ROUND(100.0 * COUNT_IF(is_malignant) / COUNT(*), 1) AS pct_malig
FROM joined GROUP BY 1 ORDER BY 1
""").fetch_df()
print(f"\nCombined NLP+syn autoimmune × malignancy:")
print(combined.to_string(index=False))

# === 3. M004 logreg with NLP-augmented covariates ===
print("\n=== 3. M004 logreg with NLP-augmented covariates ===")
df = md.execute("""
SELECT pm.research_id, pm.is_malignant, pm.age_at_surgery, pm.sex,
       COALESCE(pm.syn_hashimoto, FALSE) OR COALESCE(h.nlp_hashimoto, FALSE) AS has_hashi,
       COALESCE(pm.syn_graves, FALSE)    OR COALESCE(g.nlp_graves, FALSE)    AS has_graves
FROM main.canonical_patient_master pm
LEFT JOIN _nlp_hashi h USING (research_id)
LEFT JOIN _nlp_graves g USING (research_id)
""").fetch_df()
md.close()

import statsmodels.api as sm
import statsmodels.formula.api as smf
import math

df['has_hashi'] = df['has_hashi'].astype(int)
df['has_graves'] = df['has_graves'].astype(int)
df['malig'] = df['is_malignant'].astype(int)
df = df.dropna(subset=['age_at_surgery','sex'])

m = smf.glm("malig ~ has_hashi + has_graves + age_at_surgery + C(sex)",
            data=df, family=sm.families.Binomial()).fit(disp=0)
coefs = m.summary2().tables[1].copy()
coefs['OR'] = coefs['Coef.'].apply(lambda x: round(math.exp(x), 3))
coefs['OR_CI_low'] = coefs.apply(lambda r: round(math.exp(r['Coef.'] - 1.96*r['Std.Err.']), 3), axis=1)
coefs['OR_CI_high'] = coefs.apply(lambda r: round(math.exp(r['Coef.'] + 1.96*r['Std.Err.']), 3), axis=1)
coefs_md = coefs[['OR','OR_CI_low','OR_CI_high','P>|z|']].to_markdown(floatfmt=".4f")
pseudo_r2 = 1 - m.llf/m.llnull

print("\nLogreg results:")
print(coefs[['OR','OR_CI_low','OR_CI_high','P>|z|']].to_string())
print(f"Pseudo-R²: {pseudo_r2:.4f}; LR χ²: {2*(m.llf-m.llnull):.2f}")

# Write report
report = [
    "# M004 — Autoimmune × Malignancy (NLP-augmented Option 2)",
    "**Generated:** 2026-05-04",
    f"**Cohort:** n={len(df):,} (complete-case)",
    "",
    "## Concordance (NLP vs syn_*)",
    "",
    "### Hashimoto",
    "| metric | n |",
    "|---|---:|",
    *[f"| {col} | {int(hashi_concord.iloc[0][col])} |" for col in hashi_concord.columns],
    "",
    "### Graves",
    "| metric | n |",
    "|---|---:|",
    *[f"| {col} | {int(graves_concord.iloc[0][col])} |" for col in graves_concord.columns],
    "",
    "## Combined autoimmune × malignancy (NLP+syn)",
    "",
    "| category | n | n_malig | %_malig |",
    "|---|---:|---:|---:|",
    *[f"| {r['autoimmune_combined']} | {r['n']:,} | {r['n_malig']:,} | {r['pct_malig']}% |" for _, r in combined.iterrows()],
    "",
    "## Logreg — predictors of malignancy",
    "",
    coefs_md,
    "",
    f"Pseudo-R² (McFadden): **{pseudo_r2:.4f}**; LR vs null χ²: **{2*(m.llf-m.llnull):.2f}** (df={m.df_model})",
    "",
    "## Headline",
    "",
    f"- **Hashimoto:** has_hashi aOR {coefs.loc['has_hashi','OR']:.2f} (95% CI {coefs.loc['has_hashi','OR_CI_low']:.2f}–{coefs.loc['has_hashi','OR_CI_high']:.2f}), p={coefs.loc['has_hashi','P>|z|']:.4g}",
    f"- **Graves:** has_graves aOR {coefs.loc['has_graves','OR']:.2f} (95% CI {coefs.loc['has_graves','OR_CI_low']:.2f}–{coefs.loc['has_graves','OR_CI_high']:.2f}), p={coefs.loc['has_graves','P>|z|']:.4g}",
    "",
    "Note: Graves remains paradoxically protective in NLP-augmented analysis — likely confounding by surgical indication (Graves operated for thyrotoxicosis vs nodule workup).",
]
(REPORTS / "M004_logreg_nlp_augmented_20260504.md").write_text("\n".join(report))
print(f"\n[saved] {REPORTS}/M004_logreg_nlp_augmented_20260504.md")
