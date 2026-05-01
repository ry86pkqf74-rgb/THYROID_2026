"""M044 ETE × race sub-analysis. Extends M037 Black/AA finding to ETE manuscript."""
import sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import get_cursor

OUT = Path("/Users/ros/THyroid 2026/snowflake_trial/reports/m044_race_disparity.md")
ctx, cur = get_cursor()

print("=== M044 race × ETE ===")
cur.execute("""
SELECT
  CASE WHEN RACE = 'White' THEN 'White'
       WHEN RACE = 'Black or African American' THEN 'Black/AA'
       WHEN RACE = 'Asian' THEN 'Asian'
       WHEN RACE IS NULL OR RACE IN ('Unknown or Not Reported') THEN 'Unknown'
       ELSE 'Other' END AS race_grp,
  ETE_GRADE,
  COUNT(*) AS n,
  COUNT_IF(ANY_RECURRENCE_FLAG) AS n_recur,
  ROUND(AVG(TUMOR_SIZE_CM_MAX), 2) AS mean_tumor_cm,
  ROUND(AVG(AGE_AT_SURGERY), 1) AS mean_age,
  COUNT_IF(LN_TOTAL_POSITIVE > 0) AS n_ln_pos
FROM CANONICAL_PATIENT_MASTER_FLAT
WHERE IS_MALIGNANT = TRUE AND ETE_GRADE IN ('none','microscopic','gross')
GROUP BY 1, 2 ORDER BY 1, 2
""")
rows_re = cur.fetchall(); cols_re = [c[0] for c in cur.description]

# Era + ETE
print("=== M044 ETE × era ===")
cur.execute("""
SELECT
  CASE WHEN RACE = 'White' THEN 'White'
       WHEN RACE = 'Black or African American' THEN 'Black/AA'
       WHEN RACE = 'Asian' THEN 'Asian'
       ELSE 'Other/Unknown' END AS race_grp,
  CASE WHEN EXTRACT(YEAR FROM FIRST_SURGERY_DATE) BETWEEN 1999 AND 2013 THEN 'Pre-2014'
       WHEN EXTRACT(YEAR FROM FIRST_SURGERY_DATE) BETWEEN 2014 AND 2019 THEN 'Modern'
       ELSE 'Contemporary' END AS era,
  COUNT(*) AS n_total,
  COUNT_IF(ETE_GRADE IN ('microscopic','gross')) AS n_any_ete,
  ROUND(100.0 * COUNT_IF(ETE_GRADE IN ('microscopic','gross')) / COUNT(*), 1) AS pct_any_ete,
  ROUND(100.0 * COUNT_IF(ETE_GRADE = 'gross') / COUNT(*), 1) AS pct_gross_ete
FROM CANONICAL_PATIENT_MASTER_FLAT
WHERE IS_MALIGNANT = TRUE AND ETE_GRADE IS NOT NULL AND FIRST_SURGERY_DATE IS NOT NULL
GROUP BY 1, 2 ORDER BY 1, 2
""")
rows_era = cur.fetchall(); cols_era = [c[0] for c in cur.description]

ctx.close()

import pandas as pd
df_re = pd.DataFrame(rows_re, columns=cols_re)
df_era = pd.DataFrame(rows_era, columns=cols_era)

md = ["# M044 Race × ETE Sub-analysis\n",
      f"**Generated:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n",
      f"**Cohort:** {df_re['N'].sum():,} malignant patients with ETE in (none/micro/gross)\n",
      "**Background:** M037 Table 1 surfaced Black/AA 13.1% LN+ vs White 28% (n=1,126 LN+ / 4,137 malignant). This extends to M044's ETE strata.\n\n",
      "## Race × ETE distribution (counts)\n\n",
      "| Race | ETE | n | n_recur | mean_tumor_cm | mean_age | n_LN+ |\n",
      "| --- | --- | --- | --- | --- | --- | --- |\n"]
for _, r in df_re.iterrows():
    md.append(f"| {r['RACE_GRP']} | {r['ETE_GRADE']} | {int(r['N']):,} | {int(r['N_RECUR']):,} | {r['MEAN_TUMOR_CM']} | {r['MEAN_AGE']} | {int(r['N_LN_POS']):,} |\n")

# Race totals + ETE prevalence per race
md.append("\n## ETE prevalence by race\n\n")
md.append("| Race | n_total | n any ETE | %any ETE | n gross ETE | %gross ETE |\n| --- | --- | --- | --- | --- | --- |\n")
for race in df_re['RACE_GRP'].unique():
    sub = df_re[df_re['RACE_GRP'] == race]
    total = sub['N'].sum()
    any_ete = sub[sub['ETE_GRADE'].isin(['microscopic','gross'])]['N'].sum()
    gross = sub[sub['ETE_GRADE']=='gross']['N'].sum()
    md.append(f"| {race} | {total:,} | {any_ete:,} | {100.0*any_ete/total:.1f}% | {gross:,} | {100.0*gross/total:.1f}% |\n")

md.append("\n## Race × era × ETE trend\n\n")
md.append("| Race | Era | n | %any ETE | %gross ETE |\n| --- | --- | --- | --- | --- |\n")
for _, r in df_era.iterrows():
    md.append(f"| {r['RACE_GRP']} | {r['ERA']} | {int(r['N_TOTAL']):,} | {r['PCT_ANY_ETE']}% | {r['PCT_GROSS_ETE']}% |\n")

# Quick interpretation
md.append("\n## Interpretation\n\n")
md.append("- ETE prevalence by race in operative cohort. If Black/AA shows similar or higher gross ETE rate but lower LN+ rate, that's biologically inconsistent — points to LN-counting differences (linkage / extraction) rather than biology.\n")
md.append("- Era trend may show whether the Black/AA gap closes in modern era (2014+) — if so, points to historical access-to-care or referral pattern; if persistent, points to biology or persistent disparity.\n\n")
md.append("**Manuscript M044 footnote candidate:** if race × ETE shows persistent gap, methods section should note operative-cohort referral patterns as a caveat to ETE-as-prognostic-marker generalizability.\n")

OUT.write_text("".join(md))
print(f"\n[saved] {OUT}  ({OUT.stat().st_size:,} bytes)")
