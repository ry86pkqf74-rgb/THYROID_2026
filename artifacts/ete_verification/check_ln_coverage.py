"""Check LN coverage in tumor_pathology for the 3278-patient analytic cohort."""
import pandas as pd, duckdb, os, pathlib, json

token = os.environ.get('motherduck_token') or os.environ.get('MOTHERDUCK_TOKEN')
if not token:
    for p in [pathlib.Path.home()/'.motherduck'/'token', pathlib.Path.home()/'.config'/'motherduck'/'token']:
        if p.exists():
            token = p.read_text().strip()
            break

con = duckdb.connect(f'md:?motherduck_token={token}' if token else 'md:')
con.sql('USE "Thyroid 2026"')

coh = pd.read_csv('artifacts/ete_verification/ete_final_cohort_N3278.csv')
coh['research_id'] = coh['research_id'].astype(str)
con.register('cohort', coh[['research_id','ln_ratio','ln_examined','ln_positive']])

q = '''
WITH tp AS (
  SELECT CAST(research_id AS VARCHAR) AS research_id,
         MAX(primary_ln_ln_total_examined) AS ln_examined_tp,
         MAX(primary_ln_ln_total_positive) AS ln_positive_tp,
         MAX(primary_ln_ln_ratio) AS ln_ratio_tp,
         MAX(histology_1_ln_examined) AS ln_examined_h1,
         MAX(histology_1_ln_positive) AS ln_positive_h1,
         MAX(histology_1_ln_ratio) AS ln_ratio_h1
  FROM tumor_pathology
  GROUP BY research_id
)
SELECT
  COUNT(*) AS cohort_n,
  SUM(CASE WHEN ln_examined_tp IS NOT NULL THEN 1 ELSE 0 END) AS n_with_primary_ln_examined,
  SUM(CASE WHEN ln_positive_tp IS NOT NULL THEN 1 ELSE 0 END) AS n_with_primary_ln_positive,
  SUM(CASE WHEN ln_ratio_tp IS NOT NULL THEN 1 ELSE 0 END) AS n_with_primary_ln_ratio,
  SUM(CASE WHEN ln_examined_h1 IS NOT NULL THEN 1 ELSE 0 END) AS n_with_hist1_ln_examined,
  SUM(CASE WHEN c.ln_ratio IS NOT NULL THEN 1 ELSE 0 END) AS frozen_ln_ratio_n,
  SUM(CASE WHEN ln_examined_tp IS NOT NULL AND ln_examined_tp > 1 THEN 1 ELSE 0 END) AS ln_examined_gt1,
  SUM(CASE WHEN ln_examined_tp = 0 THEN 1 ELSE 0 END) AS ln_examined_is_0,
  SUM(CASE WHEN ln_examined_tp = 1 THEN 1 ELSE 0 END) AS ln_examined_is_1
FROM cohort c
LEFT JOIN tp ON tp.research_id = c.research_id
'''
result = con.sql(q).df()
print("=== LN COVERAGE CHECK: 3278 cohort vs tumor_pathology ===")
for col in result.columns:
    print(f"  {col:<32}: {result[col].iloc[0]}")
result.to_csv('artifacts/ete_verification/ln_coverage_check.csv', index=False)

# Now pull the actual joined data for the cohort
q2 = '''
SELECT
  c.research_id,
  c.ln_ratio AS frozen_ln_ratio,
  c.ln_examined AS frozen_ln_examined,
  c.ln_positive AS frozen_ln_positive,
  tp.ln_examined_tp AS md_ln_examined,
  tp.ln_positive_tp AS md_ln_positive,
  tp.ln_ratio_tp AS md_ln_ratio,
  tp.ln_examined_h1 AS md_h1_ln_examined,
  tp.ln_positive_h1 AS md_h1_ln_positive,
  tp.ln_ratio_h1 AS md_h1_ln_ratio
FROM cohort c
LEFT JOIN (
  SELECT CAST(research_id AS VARCHAR) AS research_id,
         MAX(primary_ln_ln_total_examined) AS ln_examined_tp,
         MAX(primary_ln_ln_total_positive) AS ln_positive_tp,
         MAX(primary_ln_ln_ratio) AS ln_ratio_tp,
         MAX(histology_1_ln_examined) AS ln_examined_h1,
         MAX(histology_1_ln_positive) AS ln_positive_h1,
         MAX(histology_1_ln_ratio) AS ln_ratio_h1
  FROM tumor_pathology
  GROUP BY research_id
) tp ON tp.research_id = c.research_id
'''
joined = con.sql(q2).df()
joined.to_csv('artifacts/ete_verification/cohort_ln_recovery.csv', index=False)

# Distribution analysis
print("\n=== Recovery distribution ===")
joined['frozen_has'] = joined['frozen_ln_ratio'].notna()
joined['md_has_primary'] = joined['md_ln_ratio'].notna()
joined['md_has_h1'] = joined['md_h1_ln_ratio'].notna()
xtab = pd.crosstab(joined['frozen_has'], joined['md_has_primary'],
                    rownames=['frozen_has_ln_ratio'], colnames=['md_has_primary_ln_ratio'])
print(xtab)

# What about hist1?
xtab2 = pd.crosstab(joined['frozen_has'], joined['md_has_h1'],
                     rownames=['frozen_has_ln_ratio'], colnames=['md_has_h1_ln_ratio'])
print("\n")
print(xtab2)

# Distribution of examined among those with MD data
avail = joined[joined['md_ln_examined'].notna()].copy()
print(f"\nAmong {len(avail)} with md_ln_examined:")
print(f"  ln_examined distribution:")
print(avail['md_ln_examined'].describe())
print(f"  ln_examined == 0: {(avail['md_ln_examined']==0).sum()}")
print(f"  ln_examined == 1: {(avail['md_ln_examined']==1).sum()}")
print(f"  ln_examined >= 2: {(avail['md_ln_examined']>=2).sum()}")

# Compare to frozen ln_ratio where both exist
both = joined.dropna(subset=['frozen_ln_ratio','md_ln_ratio'])
print(f"\nBoth frozen and MD primary: N={len(both)}")
if len(both):
    print(f"  Mean abs diff: {(both['frozen_ln_ratio'] - both['md_ln_ratio']).abs().mean():.4f}")

summary = {
  'cohort_n': int(len(joined)),
  'frozen_has_ln_ratio': int(joined['frozen_has'].sum()),
  'md_primary_has_ln_ratio': int(joined['md_has_primary'].sum()),
  'md_hist1_has_ln_ratio': int(joined['md_has_h1'].sum()),
  'md_has_ln_examined_count': int(joined['md_ln_examined'].notna().sum()),
  'md_has_ln_examined_ge1': int((joined['md_ln_examined']>=1).sum()),
  'md_has_ln_examined_ge2': int((joined['md_ln_examined']>=2).sum()),
  'md_has_ln_examined_eq0': int((joined['md_ln_examined']==0).sum()),
  'frozen_missing_ln_ratio_count': int(joined['frozen_ln_ratio'].isna().sum()),
  'recoverable_from_md_primary': int(joined[joined['frozen_ln_ratio'].isna() & joined['md_ln_ratio'].notna()].shape[0]),
  'recoverable_from_md_h1': int(joined[joined['frozen_ln_ratio'].isna() & joined['md_h1_ln_ratio'].notna()].shape[0]),
}
with open('artifacts/ete_verification/ln_recovery_summary.json','w') as f:
    json.dump(summary, f, indent=2)
print("\n=== SUMMARY ===")
print(json.dumps(summary, indent=2))
