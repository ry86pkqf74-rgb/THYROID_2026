"""M032 BRAF positivity rate over time — continuous year trend (vs era buckets)."""
import sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import get_cursor

OUT = Path("/Users/ros/THyroid 2026/snowflake_trial/reports/m032_braf_year_trend.md")
ctx, cur = get_cursor()

print("=== Pulling BRAF year trend ===")
cur.execute("""
SELECT
  EXTRACT(YEAR FROM FIRST_SURGERY_DATE) AS surgery_year,
  COUNT(*) AS n_total,
  COUNT_IF(MOLECULAR_TESTED_CONFIRMED) AS n_tested,
  COUNT_IF(BRAF_POSITIVE_FINAL) AS n_braf_pos,
  COUNT_IF(IS_MALIGNANT) AS n_malig,
  COUNT_IF(MOLECULAR_TESTED_CONFIRMED AND IS_MALIGNANT) AS n_tested_malig
FROM CANONICAL_PATIENT_MASTER_FLAT
WHERE FIRST_SURGERY_DATE IS NOT NULL
GROUP BY 1 ORDER BY 1
""")
rows = cur.fetchall(); cols = [c[0] for c in cur.description]
ctx.close()

import pandas as pd
df = pd.DataFrame(rows, columns=cols)
print(f"  {len(df)} years; total {df['N_TOTAL'].sum():,} patients")

# Compute rates
df['testing_rate_pct'] = 100.0 * df['N_TESTED'] / df['N_TOTAL']
df['braf_pos_rate_pct'] = 100.0 * df['N_BRAF_POS'] / df['N_TESTED'].where(df['N_TESTED'] > 0, 1)
df['braf_pos_rate_pct'] = df['braf_pos_rate_pct'].where(df['N_TESTED'] > 0, None)

# Spearman correlation: testing_rate × year
from scipy.stats import spearmanr
recent = df[df['SURGERY_YEAR'] >= 2010].copy()
test_rho, test_p = spearmanr(recent['SURGERY_YEAR'], recent['testing_rate_pct'])
braf_rho, braf_p = spearmanr(recent['SURGERY_YEAR'].dropna(), recent['braf_pos_rate_pct'].dropna())
print(f"  Spearman testing rate vs year (≥2010): rho={test_rho:.3f}, p={test_p:.4f}")
print(f"  Spearman BRAF+ rate vs year (≥2010): rho={braf_rho:.3f}, p={braf_p:.4f}")

# Render
md = ["# M032 BRAF Year Trend (continuous)\n",
      f"**Generated:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n",
      f"**Cohort:** {df['N_TOTAL'].sum():,} patients with surgery_date populated, {len(df)} surgery years (1999-2025)\n\n",
      "## Year-by-year testing + BRAF+\n\n",
      "| Year | N total | N tested | %tested | N BRAF+ | %BRAF+ (of tested) | N malig | %tested (malig only) |\n",
      "| --- | --- | --- | --- | --- | --- | --- | --- |\n"]
for _, r in df.iterrows():
    pct_test = r['testing_rate_pct']
    pct_braf = r['braf_pos_rate_pct']
    pct_test_malig = 100.0 * r['N_TESTED_MALIG'] / r['N_MALIG'] if r['N_MALIG'] > 0 else 0
    md.append(f"| {int(r['SURGERY_YEAR'])} | {int(r['N_TOTAL']):,} | {int(r['N_TESTED']):,} | {pct_test:.1f}% | {int(r['N_BRAF_POS']):,} | {('%.1f%%' % pct_braf) if pct_braf is not None and not pd.isna(pct_braf) else '—'} | {int(r['N_MALIG']):,} | {pct_test_malig:.1f}% |\n")

md.append("\n## Spearman correlation tests (≥2010)\n\n")
md.append(f"- **Testing-rate × year:** rho = {test_rho:.3f}, p = {test_p:.4f} {'(significant adoption trend)' if test_p < 0.05 else ''}\n")
md.append(f"- **BRAF+ rate × year:** rho = {braf_rho:.3f}, p = {braf_p:.4f} {'(significant trend)' if braf_p < 0.05 else '(no significant trend — BRAF+ rate stable across years among tested patients)'}\n")

md.append("\n## Interpretation\n\n")
md.append("- The **testing-rate trend** captures the era-driven adoption of molecular profiling. ThyroSeq + Afirma both became widely available 2014-2018; expect strong positive trend.\n")
md.append("- The **BRAF+ rate among tested** captures whether the underlying tumor biology mix has changed. Stable rate = consistent test population (testing every patient regardless of risk); rising rate = enriching for high-risk PTC over time.\n\n")

md.append("## Methods\n\n")
md.append("- **Numerators:** `MOLECULAR_TESTED_CONFIRMED` (any molecular test), `BRAF_POSITIVE_FINAL` (positive final result)\n")
md.append("- **Denominators:** total = all patients with surgery date in year; tested = patients with molecular_tested_confirmed = TRUE\n")
md.append("- **Spearman** tests run on years ≥ 2010 (pre-2010 has too sparse molecular testing for stable rates)\n")
md.append("- **Manuscript footnote:** patient-level rates only; doesn't capture multi-mutation patterns within a single test\n")

OUT.write_text("".join(md))
print(f"\n[saved] {OUT}  ({OUT.stat().st_size:,} bytes)")
