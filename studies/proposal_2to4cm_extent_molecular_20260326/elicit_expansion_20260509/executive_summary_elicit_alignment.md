# Executive summary — Elicit-driven expansion of EXT2-4 (2026-05-09)

**Source manuscript:** `studies/proposal_2to4cm_extent_molecular_20260326/` (status `Drafting`, lifecycle `Active`).
**This expansion:** does **not** modify the prose draft (`abstract_structured_v1.md`, `manuscript_submission_v1.md`); it adds a parallel BigQuery-canonical analytic layer plus formal diagnostic-performance estimates that the prior pipeline did not produce. All numbers below derive from `thyroid-canonical-pub-2026.pub_canonical.manuscript_cohort_v1` (cohort_build_timestamp on disk; query date 2026-05-09).
**Scope-narrowing this session:** survival and long-term outcomes deferred (insufficient follow-up). Recurrence reported only when biopsy- or operative-pathology-documented (per `canonical_recurrence_resolved_v1.recurrence_path_proven`).

---

## Headline findings, ordered by relevance to the Elicit gap list

### 1. The BQ canonical refresh enlarges the analytic cohort and changes one key number
The legacy DuckDB pipeline froze EXT2-4 at **N=558** primary / **N=635** broad with a path-defined-2–4cm sensitivity arm of **N=0**. On the BQ canonical layer the surgical denominator is **8,368** (1999–2025, lobectomy or total thyroidectomy with resolved date), and the preop-imaging **2.0–4.0 cm subgroup** is **n=400** (8 pre-2015, 392 in the 2015+ era). Path-defined 2–4 cm cohort is no longer empty: there are **~1,183** patients with `path_tumor_size_cm` in [2.0, 4.0]. The cohort flow CSV (`cohort_flow_bq.csv`) and figure (`figures/fig_cohort_flow_bq_20260509.png`) document the steps. The **558 vs 400 gap** is driven by the EXT2-4 pipeline's strict nodal-exclusion logic and a different size-resolution rule; it is reconcilable but should be acknowledged in any merged manuscript section.

### 2. Diagnostic performance — ThyroSeq vs Afirma in Bethesda III/IV (v3, 2026-05-13: post-platform-reclassification + Afirma rescue)

This section has been updated twice since the original v1 analysis:
1. **v1 → v2 correction (2026-05-09)**: switched from a *derived* positive/negative call (`molecular_risk_tier` + mutation flags) to the **actual platform-reported call** in `canonical_molecular_genetics_v2` (Afirma `overall_result_class`; ThyroSeq `rom_descriptor`). The original v1 derived-call table is preserved at `tables/superseded/`. MFL row `MFL-20260509-EXT2-4-TABLE3-CORRECTION` (`rec2RAsAFehw1zEHV`).
2. **v2 → v3 correction (2026-05-13)**: applied **mig_323 platform reclassification** after Cowork diagnosis revealed that ~170 rows had `platform = 'ThyroSeq'` but their canonical source (`thyroseq_molecular_enrichment.gep_norm`) clearly identified the test as Afirma or Quest Diagnostics. mig_323 applied 191 platform changes (ThyroSeq→Afirma 158; ThyroSeq→Other 18; NGS_unspecified→named 15) and 148 Afirma call extractions from `molecular_testing.result`. Pre-merge snapshot at `pub_archive.canonical_molecular_genetics_v2_pre_platform_reclass_20260513`. MFL row `MFL-20260509-EXT2-4-PLATFORM-RECLASS-REFRESH` (`reccwUWinX4G12uDe`). The v2 derived-call and v2-actual-call tables are preserved at `superseded_v2/` and `tables/superseded/`.

**v3 — Strict (NIFTP=benign), B3+B4 all sizes:**

| Platform | n_2×2 | Sensitivity | Specificity | PPV | NPV |
|---|---:|---|---|---|---|
| **Afirma** | **91** | **90.4% [79.4–95.8]** | **20.5% [10.8–35.5]** | **60.3% [49.2–70.4]** | **61.5% [35.5–82.3]** |
| **ThyroSeq** | **226** | **69.7% [60.5–77.6]** | **63.2% [54.2–71.4]** | **63.9% [54.9–71.9]** | **69.2% [59.9–77.1]** |

**v3 — Strict, B3+B4 2–4 cm:**

| Platform | n_2×2 | Sensitivity | Specificity | PPV | NPV |
|---|---:|---|---|---|---|
| **Afirma** | **5** | 75.0% [30.1–95.4] | 0.0% [0.0–79.3] | 75.0% [30.1–95.4] | 0.0% [0.0–79.3] |
| **ThyroSeq** | **31** | **86.7% [62.1–96.3]** | **75.0% [50.5–89.8]** | **76.5% [52.7–90.4]** | **85.7% [60.1–96.0]** |

ThyroSeq INTERMEDIATE-band patients reported separately (third category, not pooled). ThyroSeq not-classifiable rows dropped from **165 (v2) → 17 (v3)**, a 90% reduction in the manuscript-relevant subset. Full strict + inclusive (NIFTP=malignant) cells: `tables/table3_v2_diagnostic_performance_actual_reported_call.csv`. Wilson 95% CIs throughout.

**v2 → v3 number movement (what platform reclassification did to the headlines):**

| Metric | v2 (actual reported call, pre-reclass) | v3 (post-mig_323) | Direction |
|---|---|---|---|
| Afirma B3+B4 n | 76 | **91** | +20% (15 ThyroSeq-mislabeled rows correctly reclassified) |
| Afirma sensitivity | 89.4% | **90.4%** | ~stable |
| Afirma specificity | 17.2% | **20.5%** | +3 pp |
| Afirma NPV | 50.0% | **61.5%** | +12 pp (cleaner benign denominator) |
| ThyroSeq B3+B4 n | 104 | **226** | +117% (dual-platform patients now correctly counted in the ThyroSeq arm) |
| ThyroSeq sensitivity | 67.3% | **69.7%** | ~stable |
| ThyroSeq specificity | 61.8% | **63.2%** | ~stable |
| ThyroSeq 2–4 cm n | 19 | **31** | +63% |
| ThyroSeq 2–4 cm sens / spec | 88.9 / 90.0 | **86.7 / 75.0** | down (more variability now that n is realistic) |
| ThyroSeq not-classifiable | 165 | **17** | −90% |

**The two key takeaways from the v3 refresh:**

1. **Afirma's GSC remains a rule-out test** — sensitivity in the 90% range, specificity in the 20% range. The reclassification narrowed but did not eliminate the rule-out signature.
2. **ThyroSeq's evaluable cohort more than doubled** (104 → 226) because patients who had both an Afirma and a ThyroSeq test were being routed to only one platform's arm under the v2 platform-by-canonical-flag logic. With the source-of-truth waterfall, those patients now correctly contribute to the platform whose call we are actually evaluating. The 2–4 cm subgroup more than doubled too (19 → 31), so the previously-too-tight 89/90/89/90 cluster has spread to 87/75/77/86 — more realistic but still favorable.

**v3 ROM% descriptive validation (ThyroSeq only — Afirma reports a binary call):**

| ThyroSeq reported call | Histology | n with numeric ROM% | Median ROM% [IQR] |
|---|---|---:|---|
| negative | benign | 58 | 3 [3–3] |
| negative | malignant | 21 | 3 [3–3] |
| intermediate | benign | 11 | 50 [40–50] |
| intermediate | malignant | 13 | 50 [50–50] |
| positive | benign | 35 | 70 [70–70] |
| positive | malignant | 72 | 70 [70–70] |

The numeric ROM% still tracks the descriptive bands cleanly (negative ≈ 3%, intermediate ≈ 50%, positive ≈ 70% medians). See `tables/table3_v2_rom_pct_descriptive_stats.csv`.

**Caveats that still apply:**
- (a) **Verification bias remains the dominant limitation** — even with n=91 Afirma and n=226 ThyroSeq in the binary 2×2, only a small fraction are "negative" calls because patients with a benign molecular call typically don't proceed to surgery. Specificity and NPV are still measured in a depleted-true-negative cell.
- (b) **ThyroSeq global band coverage is 90.4%, just under the 95% acceptance gate** (`VC-MOL-PARSE-002`, **Verified** as of 2026-05-13 — accepted as source-limited; manuscript-relevant subset is at 94%). Skill bumped to v2.2.0 because the per-platform Afirma gate (98.1%) passed and the ThyroSeq remainder is documented as source-limited (no parseable band text and no numeric ROM in the source).
- (c) **The 2–4 cm Afirma cell is n=5** — still uninterpretable on its own; report descriptively, not as a performance estimate.
- (d) **Head-to-head observational, not randomized** — Afirma and ThyroSeq populations differ in referral patterns and era.
- (e) **16 ThyroSeq-mislabeled rows with pre-existing mig_321 parser-assigned bands** were flagged by the mig_323 reported_text guard and **not** auto-reclassified; they need manual platform confirmation before they can move arms. See `scripts/output/mig_323_diff_report_20260513.md` and `guard_rows_16_decision_matrix.md` (decision matrix shows 15 of 16 are parser hallucinations; 1 is a genuine dual-platform case). These are excluded from the v3 numbers above but may shift them by ±1–2 percentage points once resolved.
- (f) **Date-completeness provenance — refreshed 2026-05-14 post-mig_324b, commit 0e52c62 (FNA-bridge backfill).** `canonical_molecular_genetics_v2.resolved_test_date` has 100% coverage. Post-bridge provenance distribution: **native test dates 481/1,384 (35%)**, **`fna_linkage_via_bridge` 409/1,384 (29.6%) — newly recovered**, **`imported_at_fallback` 494/1,384 (35.7%) — down from 903 pre-bridge**. The FNA-linkage arm was rebuilt by constructing a Path B (research_id + date-proximity) bridge at `pub_workspace.fna_episode_id_bridge_20260514` (363 token→UUID mappings; 342 within 30d + 21 within 90d) to resolve the structural key-type mismatch between `canonical_molecular_genetics_v2.linked_fna_episode_id` (numeric episode tokens) and `canonical_fna_events_v1.fna_event_id` (32-char hex). Pre-bridge snapshot at `pub_archive.canonical_molecular_genetics_v2_pre_fna_bridge_20260514`. `VC-MOL-DATE-BRIDGE-001` lifecycle **Verified / Resolved**; `NF-2026-05-13-canonical-molecular-date-coverage-with-fna-bridging-gap` evidence_summary updated with new provenance distribution. **Era stratification at the year level (pre-2015 vs 2015+) remains unaffected. Sub-year temporal analyses (FNA-to-molecular-test interval, time-to-molecular-test post-FNA) are now defensible for the ~64.6% of rows carrying `native` or `fna_linkage_via_bridge` provenance; rows on `imported_at_fallback` should still be excluded from sub-year claims.**
- (g) **Orphan recovery deferred (mig_324 Phase 1).** ~9,711 patients have molecular content in source tables but are not in canonical_molecular_genetics_v2. After Cursor's strong-signal classification (`afirma gec|gsc|gene expression|thyroseq v[23]|risk of malignancy`), only **1 patient** carried a real-commercial-test signal. The 9,711 are dominantly LLM-extracted clinical mentions and non-thyroid molecular work. **No manuscript-impact loss from this gap.**
- (h) **16 reported_text guard rows cleaned 2026-05-14 (Cursor mig_325, commit 5bada61, run_id mig_325_20260514_f8efd4ac).** Of the 16 originally-flagged ThyroSeq rows: 13 were marked `platform_reclass_status='superseded_by_afirma_row'` (fabricated LOW/HIGH bands over actual Afirma reports); 1 reclassified to platform='Other' (Quest Diagnostics in-house panel); 2 marked `non_diagnostic_cancelled` (rid 5724 ThyroSeq v2 cancelled); 5 Afirma "other" rows corrected to "negative" per the matrix; 2 rows for rid 9991 marked non_diagnostic (Afirma "no result"); rid 8729 left untouched as a genuine dual-platform case. Snapshot at `pub_archive.canonical_molecular_genetics_v2_pre_guard_cleanup_20260514`. **Manuscript Table 3 v3 impact**: Afirma B3+B4 all sizes n 91→90 (Sens 90.4%, Spec 21.1%, PPV 61.0%, NPV 61.5%); ThyroSeq B3+B4 all sizes n 226→222 (Sens 69.7%, Spec 63.7%, PPV 65.0%, NPV 68.6%); ThyroSeq B3+B4 2–4 cm n 31→30 (Sens 86.7%, Spec 73.3%, PPV 76.5%, NPV 84.6%); Afirma 2–4 cm n=5 unchanged. All numerical shifts are within the Wilson 95% CIs already reported; headline conclusions unchanged. VC-MOL-PLATFORM-001 lifecycle: **Resolved/Verified**. Audit chain extended: `MFL-20260514-EXT2-4-GUARD-CLEANUP` (`recqp9bdMTqNfILhJ`). **Residual closed 2026-05-14 (mig_327, commit 5cbf7d3).** The previously deferred manual Afirma INSERT for rids 8218 and 9154 was executed: two Afirma rows added to `canonical_molecular_genetics_v2` (rid 8218 `overall_result_class=suspicious`, `resolved_test_date=2024-05-01`; rid 9154 `suspicious`, `2020-10-28`, Bethesda 3) with `report_source_table='manual_insert_from_THYROSEQ_AFIRMA_12_5_xlsx_20260514'` and `builder_version='mig_327_manual_afirma_insert_bq.py'`. Pre-insert snapshot at `pub_archive.canonical_molecular_genetics_v2_pre_manual_insert_20260514`. Audit chain: DFL `DFL-MIG327-20260514-AFIRMA-MANUAL-8218-9154` (`rec1bsPes8oknyHXn`) → MFL `MFL-20260514-EXT2-4-AFIRMA-MANUAL-INSERT` (`recrw2pSoKMsG7azt`). **Caveat for Table 3:** both rids carry `fna_bethesda_final = NULL` in `manuscript_cohort_v1`, so the live Afirma B3+B4 JOIN cohort in `04b_table3_v2_actual_reported_call.sql` is unchanged by this INSERT (Afirma B3+B4 n stays at 90). To capture these two patients in Table 3, `manuscript_cohort_v1.fna_bethesda_final` would need to be aligned (separate task, blocked on a cohort-side Bethesda refresh).

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
