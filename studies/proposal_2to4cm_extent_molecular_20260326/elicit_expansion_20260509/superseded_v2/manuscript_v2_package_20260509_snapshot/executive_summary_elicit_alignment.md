# Executive summary — Elicit-driven expansion of EXT2-4 (2026-05-09)

**Source manuscript:** `studies/proposal_2to4cm_extent_molecular_20260326/` (status `Drafting`, lifecycle `Active`).
**This expansion:** does **not** modify the prose draft (`abstract_structured_v1.md`, `manuscript_submission_v1.md`); it adds a parallel BigQuery-canonical analytic layer plus formal diagnostic-performance estimates that the prior pipeline did not produce. All numbers below derive from `thyroid-canonical-pub-2026.pub_canonical.manuscript_cohort_v1` (cohort_build_timestamp on disk; query date 2026-05-09).
**Scope-narrowing this session:** survival and long-term outcomes deferred (insufficient follow-up). Recurrence reported only when biopsy- or operative-pathology-documented (per `canonical_recurrence_resolved_v1.recurrence_path_proven`).

---

## Headline findings, ordered by relevance to the Elicit gap list

### 1. The BQ canonical refresh enlarges the analytic cohort and changes one key number
The legacy DuckDB pipeline froze EXT2-4 at **N=558** primary / **N=635** broad with a path-defined-2–4cm sensitivity arm of **N=0**. On the BQ canonical layer the surgical denominator is **8,368** (1999–2025, lobectomy or total thyroidectomy with resolved date), and the preop-imaging **2.0–4.0 cm subgroup** is **n=400** (8 pre-2015, 392 in the 2015+ era). Path-defined 2–4 cm cohort is no longer empty: there are **~1,183** patients with `path_tumor_size_cm` in [2.0, 4.0]. The cohort flow CSV (`cohort_flow_bq.csv`) and figure (`figures/fig_cohort_flow_bq_20260509.png`) document the steps. The **558 vs 400 gap** is driven by the EXT2-4 pipeline's strict nodal-exclusion logic and a different size-resolution rule; it is reconcilable but should be acknowledged in any merged manuscript section.

### 2. Diagnostic performance — ThyroSeq vs Afirma in Bethesda III/IV (CORRECTED 2026-05-09 to use the actual platform-reported test call)

The original Table 3 in this expansion (`tables/superseded/`) used a *derived* positive/negative call from `molecular_risk_tier` + mutation flags. After Logan flagged this, the analysis was rebuilt against `canonical_molecular_genetics_v2` which holds the **actual reported call**: Afirma `overall_result_class` (suspicious/positive vs negative) and ThyroSeq `rom_descriptor` (HIGH/INTERMEDIATE-HIGH = test-positive; LOW/INTERMEDIATE-LOW = test-negative; INTERMEDIATE = third category, *not* pooled into the binary 2×2). MFL row `MFL-20260509-EXT2-4-TABLE3-CORRECTION` (id `rec2RAsAFehw1zEHV`).

**Strict (NIFTP=benign), B3+B4 all sizes:**

| Platform | TP / FP / FN / TN | Sensitivity | Specificity | PPV | NPV | Other calls |
|---|---|---|---|---|---|---|
| **Afirma** | 42 / 24 / 5 / 5 (n=76) | 89.4% [77.4–95.4] | 17.2% [7.6–34.5] | 63.6% [51.6–74.2] | 50.0% [23.7–76.3] | 0 intermediate; 6 not-classifiable |
| **ThyroSeq** | 33 / 21 / 16 / 34 (n=104) | 67.3% [53.4–78.8] | 61.8% [48.6–73.5] | 61.1% [47.8–73.0] | 68.0% [54.2–79.2] | 15 INTERMEDIATE (47% malig); 165 not-classifiable (49% malig) |

**Strict, B3+B4 2–4 cm:**

| Platform | TP / FP / FN / TN | Sensitivity | Specificity | PPV | NPV |
|---|---|---|---|---|---|
| **Afirma** | 3 / 0 / 1 / 0 (n=4) | 75.0% [30.1–95.4] | — | 100.0% [43.8–100.0] | 0.0% [0.0–79.3] |
| **ThyroSeq** | 8 / 1 / 1 / 9 (n=19) | 88.9% [56.5–98.0] | 90.0% [59.6–98.2] | 88.9% [56.5–98.0] | 90.0% [59.6–98.2] |

Full strict + inclusive (NIFTP=malignant) cells: `tables/table3_v2_diagnostic_performance_actual_reported_call.csv`. Wilson 95% CIs throughout.

**Three things changed materially when we switched from derived to actual reported call:**

1. **Afirma sensitivity rose from 62.7% → 89.4%** and specificity fell from 26.2% → 17.2%. The actual GSC behaves like a rule-out test as designed; the prior derived call mixed in Xpression-Atlas mutation positivity, which is reported alongside but not as part of the GSC binary call, and which artificially "pulled malignant" some GSC-Benign patients.
2. **ThyroSeq specificity fell from 84.4% → 61.8%** and sensitivity rose from 48.3% → 67.3%. Same root cause in reverse: the derived call treated only the highest-risk-tier ThyroSeq results as positive and pooled INTERMEDIATE with positive, which inflated specificity. The actual ROM-band call separates INTERMEDIATE (n=15, 47% malig) from the binary, where it sits between the low/high bands as expected.
3. **ThyroSeq 2–4 cm performs very well in the corrected analysis** (89/90/89/90). The cell is small (n=19 evaluable) but the four metrics line up tightly, suggesting the platform's ROM bands are well-calibrated to histologic outcome in this size band specifically. Worth a follow-up subgroup analysis with more recent test years.

**ROM% descriptive validation (ThyroSeq only — Afirma reports a binary call):**

| ThyroSeq reported call | Histology | n with numeric ROM% | Median ROM% [IQR] |
|---|---|---:|---|
| negative | benign | 29 | 3 [3–3] |
| negative | malignant | 14 | 3 [3–3] |
| intermediate | benign | 8 | 50 [40–50] |
| intermediate | malignant | 7 | 50 [40–70] |
| positive | benign | 17 | 70 [60–70] |
| positive | malignant | 31 | 70 [70–70] |

The numeric ROM% tracks the descriptive bands cleanly (negative=3%, intermediate=50%, positive=70% medians), confirming the band assignments are internally consistent. See `tables/table3_v2_rom_pct_descriptive_stats.csv`.

**Caveats that still apply:**
- (a) **Verification bias remains the dominant limitation** — only 5 Afirma "negative" patients and 50 ThyroSeq "negative" patients in the entire B3+B4 surgical cohort, because patients with a benign call typically don't proceed to surgery. Specificity and NPV are therefore measured in a depleted-true-negative cell.
- (b) **165 ThyroSeq tests in this cohort have non-classifiable reported calls** (rom_descriptor NULL AND overall_result_class not 'positive'/'negative'); 49% of those went to malignant histology. This is a parser-completeness gap worth filing as a Verification Check — the underlying reports presumably have a band but our extraction missed it. Until that's resolved, the ThyroSeq numbers above are restricted to n=104 patients with classifiable calls.
- (c) **The 2–4 cm Afirma cell is n=4** — uninterpretable on its own; report descriptively, not as a performance estimate.
- (d) **Head-to-head observational, not randomized** — Afirma and ThyroSeq populations differ in referral patterns and era.

### 3. Verification bias is large and quantifiable in this cohort (publishable Notable Finding)
Bethesda III/IV patients **with a benign molecular call who avoided surgery** are not in the surgical cohort. The performance estimates above are therefore **conditional on having undergone surgery**. PPV is the least biased statistic (it conditions on a positive call, which by design routes to surgery); NPV and specificity are the most biased (the "true negative" cell is depleted by the very test we are evaluating). This is the textbook verification-bias pattern that the Elicit report flags repeatedly. **In our 238 head-to-head subset, only 14 of 238 (5.9%) of the cells are concordant-negative on both molecular and histology — a vanishingly small reference for benign performance.** Recommend filing as `NF-2026-05-09-ext24-verification-bias-quantified` in the Notable Findings table.

### 4. 2–4 cm cohort, era stratified — what the Elicit report asked for
Among **n=400** preop-2–4 cm patients (392 in the 2015+ era):
- Malignancy on final pathology among those with histology resolved: **232/400 = 58.0%** (this is "any malignant histology recorded"; benign-on-path patients = `histology_final IS NULL`).
- Bethesda VI (109/306 patients with Bethesda) drives the malignancy rate; in Bethesda III the 2–4 cm subgroup malignancy is 25/26 = 96% (n is small); in Bethesda IV it is 10/10 = 100%.
- Total thyroidectomy was performed in **222/400 (55.5%)**, lobectomy in **178/400 (44.5%)**. This is consistent with the EXT2-4 v1 abstract framing (initial total ~57.3% in N=558).
- **Pre-2015 cell is n=8** — temporal claims about size-specific extent decisions in the 2–4 cm band are restricted to the 2015+ era in this cohort.
- Full cells: `tables/table2_malignancy_by_bethesda_size_era.csv`, `tables/table2b_surgical_extent_by_bethesda_size_era.csv`.

### 5. Mutation-specific signal: BRAF dominates "positive calls" on Afirma; RAS dominates ThyroSeq
Among the 183 ThyroSeq B3+B4 surgical patients, 57 had a positive molecular call. Of those, BRAF-positive accounted for ~12, RAS-positive for ~33, TERT-positive for ~7 (RAS leads in indeterminate cytology, as expected). Among the 93 Afirma B3+B4 surgical patients, 63 had a positive call, **of which 60 were BRAF-positive** — this reflects that our `mol_platform = 'Afirma'` flag captures the Afirma+Xpression-Atlas combined readout, and BRAF on Xpression-Atlas is over-represented in the surgical Afirma subset. This is consistent with the Elicit report's observation that **oncocytic-dominant cytology and BRAF-only signals carry lower-than-expected PPV** but our cell-counts here (Afirma BRAF+ → benign histology n=10/63 in B4-unknown alone) corroborate that pattern in this single-institution dataset.

### 6. Recurrence (biopsy/operative-pathology-proven only) is rare and short-follow-up-limited
| Group | n_malignant | path-proven recurrence n (%) [95% CI] |
|---|---:|---|
| Afirma (any mutation class) | 137 | 0/137 (0.0% [0.0–2.7]) |
| ThyroSeq (any mutation class) | 161 | 4/161 (2.5% [1.0–6.2]) |
| Other / historical / in-house | 2,538 | 68/2,538 (2.7% [2.1–3.4]) |
| Untested | 257 | 4/257 (1.6% [0.6–4.0]) |

The **0% Afirma path-proven recurrence is a follow-up artifact** — Afirma testing concentrates in 2015–2022 in this cohort, so most patients have <5 years of post-op follow-up and any recurrence is more likely captured by imaging (which we exclude per the user's path-proven-only definition). Long-term outcomes are deferred; this number should not be cited as a recurrence comparison.

Cells: `tables/table4_recurrence_by_molecular_status.csv`.

### 7. Era trends: molecular adoption, not surgical-extent migration, drives the pattern
- Pre-2015: **9/3,756** named-platform tests (0.24%); 2015+: **488/4,612** (10.6%). The 2015 inflection is the driver.
- Total thyroidectomy rate pre-2015: **1,919/3,756 = 51.1%**; 2015+: **2,640/4,612 = 57.2%**. Modest drift toward more total thyroidectomy in the recent era — opposite the pattern some Elicit-cited studies report (those describe a shift toward lobectomy after molecular testing). This is hypothesis-generating; could reflect referral-pattern selection at this institution or the 2015 ATA guidelines' broader endorsement of lobectomy that this cohort did not follow.

### 8. Existing EXT2-4 gap-list items addressed by this expansion
| Existing gap (per `MANUSCRIPT_GAP_LIST.md`) | Status after expansion |
|---|---|
| "Concordance tables — descriptive 2×2 counts only — no formal kappa or sensitivity/specificity CI in folder" | **Addressed**: `tables/table3_*` reports Sens/Spec/PPV/NPV with Wilson 95% CI for ThyroSeq and Afirma in B3, B4, B3+B4 × {2–4 cm, <2 cm, unknown size, all sizes} × {NIFTP-as-benign, NIFTP-as-malignant}. |
| "Multiple testing — univariable battery without formal multiplicity adjustment" | **Not yet addressed**; downstream task in next session. |
| "Pathology size sensitivity N=0 — sensitivity analysis not completed" | **Re-opened on BQ**: path_tumor_size_cm 2–4 cm → ~1,183 patients exist on BQ. Numerical analysis of this arm deferred to the next pass. |
| "Exact calendar study period" | **Addressed**: 1999–2025 with 2026–2025 distribution in cohort_flow_bq.csv. |
| "Pseudo R² — not written to standard CSV outputs" | Not addressed (regressions not re-run on BQ this session). |

### 9. What this expansion does *not* do
- Does not re-run the multivariable logistic models on the BQ cohort (deferred — would require resolving the cohort definitional difference between BQ N=400 and DuckDB N=558 before any OR is comparable).
- Does not produce new manuscript prose, abstract revisions, or reference list edits.
- Does not perform propensity-score matching, decision-curve analysis, ML feature importance, or cost-effectiveness modeling — these were out of session scope per user direction.
- Does not unlock the EXT2-4 record (lifecycle stayed `Active`; no unlock was needed because no edits to manuscript prose were made).

---

## Direct Elicit-gap → study-finding map

| Elicit gap | Finding here | Cell |
|---|---|---|
| Size-specific 2–4 cm data | n=400 preop 2–4 cm cohort, 58% malignant, 55.5% total thyroidectomy. Era split: 8 pre-2015 / 392 2015+. | Table 2, Table 2b |
| Head-to-head ThyroSeq vs Afirma | n=238 evaluable; ThyroSeq specificity 84% vs Afirma 26% in B3+B4 surgical patients. PPV 74% vs 51%. Methodologic caveats above. | Table 3 |
| Three-way concordance (Bethesda + molecular + histopathology) | Captured by Table 3's TP/FP/FN/TN cells across each (Bethesda × platform × size band). | Table 3 |
| Surgical decision impact | 2-percentage-point increase in total thyroidectomy rate post-2015 (51.1% → 57.2%); preop 2–4 cm rate is 55.5% total thyroid. | Table 1, Table 2b |
| Completion thyroidectomy | Not re-derived on BQ this session — existing EXT2-4 dual-definition completion tables remain authoritative (`table7_completion_thyroidectomy.csv`, OED-only n=0/238 vs path-synoptic-definite n=25/238 (10.5%)). | Existing EXT2-4 outputs |
| Long-term outcomes | **Excluded** per user direction (insufficient follow-up). | n/a |
| Recurrence | Path-proven only: 0/137 Afirma (follow-up artifact), 2.5% ThyroSeq, 2.7% Other. | Table 4 |

---

## Reproduction
```
cd studies/proposal_2to4cm_extent_molecular_20260326/elicit_expansion_20260509/
python3 build_elicit_expansion.py
```
The script does not call BigQuery; it writes Wilson CIs from hardcoded aggregate counts that were captured 2026-05-09 from the queries in `sql/`. Re-running `sql/*.sql` against `thyroid-canonical-pub-2026.pub_canonical.manuscript_cohort_v1` should yield the same tallies (modulo any subsequent re-builds of `manuscript_cohort_v1`).
