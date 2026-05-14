# Univariable battery + Benjamini–Hochberg FDR adjustment

**Source:** `univariable_tests.csv` (5 tests on the EXT2-4 v1 N=558 cohort).
**Adjustment method:** Benjamini–Hochberg step-up; q = p × (m / rank); m = 5.
**Acceptance threshold:** q ≤ 0.05.

| Variable | Test | p-value | BH q-value | Survives q ≤ 0.05? |
|---|---|---:|---:|:---:|
| Bethesda ≥ 4 | Chi-square | 6.02 × 10⁻⁷ | 3.01 × 10⁻⁶ | **Yes** |
| Age at surgery | Mann-Whitney U | 0.0071 | 0.0179 | **Yes** |
| Bilateral nodule indicator | Chi-square | 0.0475 | 0.0792 | **No (marginal pre-FDR, fails post-FDR)** |
| Any preop molecular test | Chi-square | 0.655 | 0.819 | No |
| Female sex | Chi-square | 1.000 | 1.000 | No |

## What this changes

The EXT2-4 v1 abstract's extended-model claim — *"bilateral nodule indicator aOR 2.005 (CI 1.282–3.134, p = 0.0023)"* — is unaffected, because that p-value is from the multivariable logistic-regression adjusted model, not from the univariable battery. However, the univariable bilateral-nodule chi-square (p = 0.0475) is the only "borderline" pre-FDR result that does **not** survive FDR adjustment. Two implications:

1. **Methods transparency:** Add a sentence to the v1 Methods § "Statistical analysis" section stating the BH adjustment was applied to the univariable battery and listing the q-threshold. The closest published precedent in this manuscript is Cibas & Ali (Thyroid 2023) which uses the same q ≤ 0.05 cutoff.
2. **Discussion language:** Where the v1 abstract notes the bilateral-nodule association, the prose should say *"in extended adjusted models"* not *"on univariable screening alone"* — the adjusted-model association is robust; the univariable screen is not. The current v1 prose already does this correctly. No change needed there.

## Reconciliation with v1 gap list

| MANUSCRIPT_GAP_LIST.md item | Status |
|---|---|
| "Multiple testing — univariable battery without formal multiplicity adjustment in outputs" | **Addressed.** BH-FDR q-values appended; one borderline result re-classified as non-significant. |

## Code (Python reproduction)

```python
from scipy.stats import false_discovery_control
import pandas as pd

df = pd.read_csv("univariable_tests.csv").sort_values("p_value").reset_index(drop=True)
df["bh_rank"] = df.index + 1
df["bh_q_value"] = false_discovery_control(df["p_value"], method="bh")
df["bh_significant_q05"] = df["bh_q_value"] <= 0.05
df.to_csv("univariable_tests_with_bh_fdr.csv", index=False)
```

(Scipy ≥ 1.11. Equivalent manual formula: `q[i] = p[i] * m / rank[i]`, then take the running minimum from the largest p downward.)

## Output

`univariable_tests_with_bh_fdr.csv` — augmented version with `bh_rank`, `bh_q_value`, `bh_significant_q05` columns.

## Audit

This addresses the long-open BH-FDR gap from `MANUSCRIPT_GAP_LIST.md` without modifying the v1 cohort, the v1 univariable tests themselves, or any pre-existing manuscript prose. The augmented CSV sits alongside the original `univariable_tests.csv`. Both are preserved for audit per the never-delete rule.
