# Executive summary — Elicit-driven expansion of EXT2-4 (2026-05-09; v4 cohort refresh 2026-05-14)

> **v3 → v4 supersession (2026-05-14, commit `ce362e5`).** The primary EXT2-4 cohort definition shifted from a patient-grain *index-nodule 2–4 cm* gate (v3, n=400) to a nodule-grain *any preop US nodule with `size_cm_max ∈ [2.0, 4.0] cm` on an exam ≤ surgery day* gate (v4, **n=765**, strict-nodal-exclusion arm **n=654**) per Logan's 2026-05-14 decision in `cohort_reconciliation_v1_vs_v3.md`. The v3 analytic artifacts (manuscript draft, zip, figures, tables) are archived under `superseded_v3/` and the active deliverable set is `manuscript_v4_draft.docx` / `manuscript_v4_package_20260513.zip` / `tables/*_v4_*.csv` / `figures/*_v4.{png,pdf}`. MFL `MFL-20260513-EXT2-4-V4-COHORT-REBUILD` (`recylT6gWb9raAiOr`, `change_type=major_revision`). Skill bumped v2.3.2 → v2.4.0. All v4 cells below derive from `thyroid-canonical-pub-2026.pub_canonical.manuscript_cohort_v1` ∩ `canonical_us_nodule_v2` (verified 2026-05-13).

**Source manuscript:** `studies/proposal_2to4cm_extent_molecular_20260326/` (status `Drafting`, lifecycle `Active`).
**This expansion:** does **not** modify the original v1 prose draft (`abstract_structured_v1.md`, `manuscript_submission_v1.md`); it adds a parallel BigQuery-canonical analytic layer plus formal diagnostic-performance estimates that the prior pipeline did not produce. The v4 manuscript docx (`manuscript_v4_draft.docx`) regenerates the IMRAD prose against the n=765 cohort.
**Scope-narrowing this session:** survival and long-term outcomes deferred (insufficient follow-up). Recurrence reported only when biopsy- or operative-pathology-documented (per `canonical_recurrence_resolved_v1.recurrence_path_proven`).

---

## Headline findings, ordered by relevance to the Elicit gap list

### 1. The BQ canonical refresh enlarges the analytic cohort and changes one key number
The legacy DuckDB pipeline froze EXT2-4 v1 at **N=558** primary / **N=635** broad with a path-defined-2–4cm sensitivity arm of **N=0**. On the BQ canonical layer the surgical denominator is **8,368** (1999–2025, lobectomy or total thyroidectomy with resolved date). Three candidate 2.0–4.0 cm cohort definitions were enumerated and reconciled in `cohort_reconciliation_v1_vs_v3.md`:

| Definition (BQ 2026-05-13) | n | Status |
|---|---:|---|
| Resolved *index* nodule 2.0–4.0 cm (patient-grain `imaging_nodule_size_cm`) | 400 | v3 — **superseded** |
| Largest preop nodule 2.0–4.0 cm (patient-grain `MAX(size_cm_max)`) | 674 | reconciliation reference, not adopted |
| **Any preop US nodule 2.0–4.0 cm on exam ≤ surgery day (nodule-grain `EXISTS`)** | **765** | **v4 primary (adopted 2026-05-14)** |
| v4 ∩ strict nodal exclusion (CT/MRI suspicious LN + Bethesda VI LN-FNA) | **654** | v4 sensitivity arm |

The v4 primary definition reproduces (and slightly expands) the v1 N=635 broad framing while avoiding the selection bias of restricting to a single "index" lesion. **758 of 765 (99.1%) v4 patients fall in the 2015+ era; only 7 pre-2015 — temporal claims about size-specific extent decisions remain restricted to the 2015+ era.** Path-defined 2–4 cm cohort is no longer empty: there are **~1,183** patients with `path_tumor_size_cm` in [2.0, 4.0]; sensitivity analysis on this arm remains a deferred follow-up. The v4 cohort flow figure (`figures/fig1_cohort_flow_v4.png`) and Table 1 (`tables/table1_v4_cohort_characteristics.csv`) document the steps and stratum-level descriptives.

### 2. Diagnostic performance — ThyroSeq vs Afirma in Bethesda III/IV (v4, 2026-05-13: restricted to the n=765 cohort)

This section has been updated three times since the original v1 analysis:
1. **v1 → v2 correction (2026-05-09)**: switched from a *derived* positive/negative call (`molecular_risk_tier` + mutation flags) to the **actual platform-reported call** in `canonical_molecular_genetics_v2` (Afirma `overall_result_class`; ThyroSeq `rom_descriptor`). MFL `MFL-20260509-EXT2-4-TABLE3-CORRECTION` (`rec2RAsAFehw1zEHV`).
2. **v2 → v3 correction (2026-05-13)**: applied **mig_323 platform reclassification** after Cowork diagnosis revealed ~170 rows with `platform = 'ThyroSeq'` whose canonical source (`thyroseq_molecular_enrichment.gep_norm`) clearly identified the test as Afirma or Quest Diagnostics. mig_323 applied 191 platform changes + 148 Afirma call extractions. Snapshot `pub_archive.canonical_molecular_genetics_v2_pre_platform_reclass_20260513`. MFL `reccwUWinX4G12uDe`. Then **mig_325** (2026-05-14, commit `5bada61`) cleaned 16 reported_text guard rows. Then **mig_327** (2026-05-14, commit `5cbf7d3`) inserted the two outstanding rids' Afirma rows. Snapshot chain preserved in `pub_archive.*`.
3. **v3 → v4 cohort restriction (2026-05-13, commit `ce362e5`)**: Table 3 v4 is recomputed **restricted to the 765-patient v4 cohort** (any preop US 2.0–4.0 cm nodule on exam ≤ surgery day). The all-sizes Bethesda III/IV cells therefore shrink dramatically vs v3 because the v3 all-sizes denominator was the full 8,368-patient surgical pool while v4's is the 765-patient subset. The 2–4 cm imaging-index subgroup cells are largely unchanged (those were already cohort-restricted in v3 to imaging-index 2–4 cm).

**v4 — Strict (NIFTP=benign), B3+B4 all sizes (restricted to n=765 cohort):**

| Platform | n_2×2 | Sensitivity | Specificity | PPV | NPV |
|---|---:|---|---|---|---|
| **Afirma** | **13** | **80.0% [49.0–94.3]** | **0.0% [0.0–56.2]** | **72.7% [43.4–90.3]** | **0.0% [0.0–65.8]** |
| **ThyroSeq** | **71** | **88.6% [74.0–95.5]** | **75.0% [58.9–86.2]** | **77.5% [62.5–87.7]** | **87.1% [71.1–94.9]** |

**v4 — Strict, B3+B4 imaging-index 2–4 cm (within n=765 cohort):**

| Platform | n_2×2 | Sensitivity | Specificity | PPV | NPV |
|---|---:|---|---|---|---|
| **Afirma** | **5** | 75.0% [30.1–95.4] | 0.0% [0.0–79.3] | 75.0% [30.1–95.4] | 0.0% [0.0–79.3] |
| **ThyroSeq** | **30** | **86.7% [62.1–96.3]** | **73.3% [48.0–89.1]** | **76.5% [52.7–90.4]** | **84.6% [57.8–95.7]** |

ThyroSeq INTERMEDIATE-band patients reported as a third category (not pooled): 13 patients in the B3+B4 all-sizes v4 cell, 61.5% malignant on histology. Full strict + inclusive (NIFTP=malignant) cells: `tables/table3_v4_diagnostic_performance_actual_reported_call.csv`. Wilson 95% CIs throughout. The v3 versions of these cells (Afirma all-sizes n=90, ThyroSeq all-sizes n=222, computed on the full surgical pool rather than the v4 cohort) are preserved at `superseded_v3/` for forensic comparison.

**v3 → v4 cohort-restriction shift (what restricting Table 3 to the n=765 cohort did to the headlines):**

| Metric | v3 (post-mig_325, full surgical pool) | v4 (n=765 cohort) | Direction |
|---|---|---|---|
| Afirma B3+B4 all-sizes n | 90 | **13** | −86% (v3 was unrestricted; v4 conditions on having a 2–4 cm preop nodule) |
| Afirma all-sizes sensitivity | 90.4% | **80.0%** | down (smaller n, wider CI) |
| Afirma all-sizes specificity | 21.1% | **0.0%** | rule-out signature collapses at this n |
| ThyroSeq B3+B4 all-sizes n | 222 | **71** | −68% (cohort restriction) |
| ThyroSeq all-sizes sensitivity | 69.7% | **88.6%** | +19 pp (the v4 subset is enriched for higher-yield surgical patients) |
| ThyroSeq all-sizes specificity | 63.7% | **75.0%** | +11 pp |
| Afirma 2–4 cm n | 5 | **5** | unchanged (this subgroup was already imaging-index 2–4 cm in v3) |
| ThyroSeq 2–4 cm n | 30 | **30** | unchanged |
| ThyroSeq 2–4 cm sens / spec | 86.7 / 73.3 | **86.7 / 73.3** | identical |

**The two key takeaways from the v4 refresh:**

1. **The 2–4 cm imaging-index cells (the manuscript's primary subgroup of interest) are stable across v3 → v4.** ThyroSeq B3+B4 2–4 cm Sens 87%, Spec 73%, PPV 77%, NPV 85% reproduces faithfully — these are the operating characteristics that should be cited in the abstract and Discussion.
2. **The all-sizes cells shrink dramatically.** When you condition Table 3 on "patient had ≥1 preop nodule in [2.0, 4.0] cm" (the v4 cohort gate), only a fraction of the v3 surgical-pool Bethesda III/IV patients remain (13 Afirma, 71 ThyroSeq). The v4 Afirma "all-sizes" cell is now too small to report a stable specificity (CI floor 0.0%). This is structural, not an error: the v4 manuscript framing is "this is what we see in patients with a clinically relevant 2–4 cm nodule" — not "this is the test's operating characteristics in the general thyroid surgical population."

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
- (b) **ThyroSeq global band coverage is 90.2% post-mig_328 (634/703 active rows); numeric ROM% is 76.4% (537/703)** (`VC-MOL-PARSE-002`, **Verified** as of 2026-05-13 — accepted as source-limited; an additional Cursor parser-tail pass under mig_328 (commit `ce1c101`) added net +2 labels and re-touched 42 ThyroSeq rows). Skill bumped to v2.4.1 because the per-platform Afirma gate (98.1%) passes and the ThyroSeq remainder is documented as source-limited (no parseable band text and no numeric ROM in the source — predominantly older `molecular_testing` free-text and 4 `extracted_braf_recovery_v1` rows by design). Within the v4 manuscript cohort: ThyroSeq label coverage **94.4%**, ROM% **85.3%**.
- (c) **The 2–4 cm Afirma cell is n=5** — still uninterpretable on its own; report descriptively, not as a performance estimate.
- (d) **Head-to-head observational, not randomized** — Afirma and ThyroSeq populations differ in referral patterns and era.
- (e) **16 ThyroSeq-mislabeled rows with pre-existing mig_321 parser-assigned bands** were flagged by the mig_323 reported_text guard and **not** auto-reclassified; they need manual platform confirmation before they can move arms. See `scripts/output/mig_323_diff_report_20260513.md` and `guard_rows_16_decision_matrix.md` (decision matrix shows 15 of 16 are parser hallucinations; 1 is a genuine dual-platform case). These are excluded from the v3 numbers above but may shift them by ±1–2 percentage points once resolved.
- (f) **Date-completeness provenance — refreshed 2026-05-14 post-mig_324b, commit 0e52c62 (FNA-bridge backfill).** `canonical_molecular_genetics_v2.resolved_test_date` has 100% coverage. Post-bridge provenance distribution: **native test dates 481/1,384 (35%)**, **`fna_linkage_via_bridge` 409/1,384 (29.6%) — newly recovered**, **`imported_at_fallback` 494/1,384 (35.7%) — down from 903 pre-bridge**. The FNA-linkage arm was rebuilt by constructing a Path B (research_id + date-proximity) bridge at `pub_workspace.fna_episode_id_bridge_20260514` (363 token→UUID mappings; 342 within 30d + 21 within 90d) to resolve the structural key-type mismatch between `canonical_molecular_genetics_v2.linked_fna_episode_id` (numeric episode tokens) and `canonical_fna_events_v1.fna_event_id` (32-char hex). Pre-bridge snapshot at `pub_archive.canonical_molecular_genetics_v2_pre_fna_bridge_20260514`. `VC-MOL-DATE-BRIDGE-001` lifecycle **Verified / Resolved**; `NF-2026-05-13-canonical-molecular-date-coverage-with-fna-bridging-gap` evidence_summary updated with new provenance distribution. **Era stratification at the year level (pre-2015 vs 2015+) remains unaffected. Sub-year temporal analyses (FNA-to-molecular-test interval, time-to-molecular-test post-FNA) are now defensible for the ~64.6% of rows carrying `native` or `fna_linkage_via_bridge` provenance; rows on `imported_at_fallback` should still be excluded from sub-year claims.**
- (g) **Orphan recovery deferred (mig_324 Phase 1).** ~9,711 patients have molecular content in source tables but are not in canonical_molecular_genetics_v2. After Cursor's strong-signal classification (`afirma gec|gsc|gene expression|thyroseq v[23]|risk of malignancy`), only **1 patient** carried a real-commercial-test signal. The 9,711 are dominantly LLM-extracted clinical mentions and non-thyroid molecular work. **No manuscript-impact loss from this gap.**
- (h) **16 reported_text guard rows cleaned 2026-05-14 (Cursor mig_325, commit 5bada61, run_id mig_325_20260514_f8efd4ac).** Of the 16 originally-flagged ThyroSeq rows: 13 were marked `platform_reclass_status='superseded_by_afirma_row'` (fabricated LOW/HIGH bands over actual Afirma reports); 1 reclassified to platform='Other' (Quest Diagnostics in-house panel); 2 marked `non_diagnostic_cancelled` (rid 5724 ThyroSeq v2 cancelled); 5 Afirma "other" rows corrected to "negative" per the matrix; 2 rows for rid 9991 marked non_diagnostic (Afirma "no result"); rid 8729 left untouched as a genuine dual-platform case. Snapshot at `pub_archive.canonical_molecular_genetics_v2_pre_guard_cleanup_20260514`. **Manuscript Table 3 v3 impact**: Afirma B3+B4 all sizes n 91→90 (Sens 90.4%, Spec 21.1%, PPV 61.0%, NPV 61.5%); ThyroSeq B3+B4 all sizes n 226→222 (Sens 69.7%, Spec 63.7%, PPV 65.0%, NPV 68.6%); ThyroSeq B3+B4 2–4 cm n 31→30 (Sens 86.7%, Spec 73.3%, PPV 76.5%, NPV 84.6%); Afirma 2–4 cm n=5 unchanged. All numerical shifts are within the Wilson 95% CIs already reported; headline conclusions unchanged. VC-MOL-PLATFORM-001 lifecycle: **Resolved/Verified**. Audit chain extended: `MFL-20260514-EXT2-4-GUARD-CLEANUP` (`recqp9bdMTqNfILhJ`). **Residual closed 2026-05-14 (mig_327, commit 5cbf7d3).** The previously deferred manual Afirma INSERT for rids 8218 and 9154 was executed: two Afirma rows added to `canonical_molecular_genetics_v2` (rid 8218 `overall_result_class=suspicious`, `resolved_test_date=2024-05-01`; rid 9154 `suspicious`, `2020-10-28`, Bethesda 3) with `report_source_table='manual_insert_from_THYROSEQ_AFIRMA_12_5_xlsx_20260514'` and `builder_version='mig_327_manual_afirma_insert_bq.py'`. Pre-insert snapshot at `pub_archive.canonical_molecular_genetics_v2_pre_manual_insert_20260514`. Audit chain: DFL `DFL-MIG327-20260514-AFIRMA-MANUAL-8218-9154` (`rec1bsPes8oknyHXn`) → MFL `MFL-20260514-EXT2-4-AFIRMA-MANUAL-INSERT` (`recrw2pSoKMsG7azt`). **Caveat for Table 3:** both rids carry `fna_bethesda_final = NULL` in `manuscript_cohort_v1`, so the live Afirma B3+B4 JOIN cohort in `04b_table3_v2_actual_reported_call.sql` is unchanged by this INSERT (Afirma B3+B4 n stays at 90). To capture these two patients in Table 3, `manuscript_cohort_v1.fna_bethesda_final` would need to be aligned (separate task, blocked on a cohort-side Bethesda refresh).
- (i) **Afirma platform-contamination cleanup completed 2026-05-14 (Cursor mig_328, commit `ce1c101`, skill v2.4.0 → v2.4.1).** Logan flagged that Afirma rows shouldn't carry ThyroSeq-inferred ROM% or ThyroSeq band labels. Cowork audit confirmed: of 98 Afirma rows with `rom_descriptor` set, **94** had `platform_raw = 'ThyroSeq'` (mig_323 reclassification residual where ThyroSeq parser–computed bands were left in place). Of 146 Afirma rows with numeric ROM%, 138 had platform_raw='ThyroSeq'. Concrete examples: rid 2130 had `rom_percent_point=599` (OCR error, impossible value); rid 9539 had `rom_percent_point=35` inferred from "30–40% probability of Hurthle cell carcinoma" while the actual Afirma report said "benign with 4% ROM". mig_328 Phase A nulled `rom_descriptor` on all 144 Afirma rows that still carried a ThyroSeq band (including 4 rows where `platform_raw` did not contain "thyroseq" but had stale LOW/INT/HIGH), nulled `rom_percent_*` on the 4 `numeric_rom_inferred` rows + the OCR-garbage row, and removed ThyroSeq-style `rom_description` text. Phase B added 42 ThyroSeq parser-tail row touches (net +2 labels). Phase C found 0 additional Afirma `overall_result_class` recoveries (3 candidates failed text fallback). Snapshot at `pub_archive.canonical_molecular_genetics_v2_pre_mig328_20260514`. **Manuscript impact**: Table 3 v4 binary cells (Afirma n=13, ThyroSeq n=71) are byte-identical pre and post-merge — `overall_result_class` was preserved by design, and Table 3 doesn't use `rom_descriptor` for Afirma. The §2 ROM% descriptive panel (`table3_v4_rom_pct_descriptive_stats.csv`) now correctly reports Afirma cells as "n/a — Afirma reports binary call only" rather than mixing contaminated values into the ThyroSeq ROM% distribution. **VC-MOL-PLATFORM-002** opened and **Resolved/Verified** post-merge; **VC-MOL-PARSE-002** fix_action extended with the mig_328 parser-tail recovery details (does not re-open). Audit chain: DFL `recXE6935BK4T1omU` → MFL `recAjPQiNbHljhtx3`.

### 3. Verification bias is large and quantifiable in this cohort (publishable Notable Finding)
Bethesda III/IV patients **with a benign molecular call who avoided surgery** are not in the surgical cohort. The performance estimates above are therefore **conditional on having undergone surgery**. PPV is the least biased statistic (it conditions on a positive call, which by design routes to surgery); NPV and specificity are the most biased (the "true negative" cell is depleted by the very test we are evaluating). This is the textbook verification-bias pattern that the Elicit report flags repeatedly. **In the v4 cohort's B3+B4 head-to-head subset (Afirma n=13 + ThyroSeq n=71 = 84 evaluable patients), only 27 of 84 (32.1%) are concordant-negative on both molecular and histology — the benign-reference cell remains the limiting denominator, especially for Afirma where TN=0/13.** Recommend filing as `NF-2026-05-09-ext24-verification-bias-quantified` in the Notable Findings table.

### 4. v4 cohort breakdown — what the Elicit report asked for, on the n=765 denominator
Among **n=765** any-preop-2–4cm patients (**758 in the 2015+ era**, 7 pre-2015):
- Female 600/765 (78.4%); age median 55 [IQR 43–66]. Index preop nodule size median 1.97 cm — note that *index* size is patient-grain and frequently <2.0 cm; the v4 inclusion gate is satisfied by *any* preop nodule in [2.0, 4.0], not just the index lesion. The "index size 2–4 cm" stratum within v4 is n=355; the "index size <2 cm but has a 2–4 cm secondary" stratum is n=370.
- Malignancy on final pathology among those with resolved histology: **403/765 with resolved histology, 212/403 (52.7%) malignant** under the strict NIFTP-as-benign rule (Table 1 column "Malign strict histology pct"). Bethesda VI drives the malignancy rate: B6 n=184 (99.4% malig), B5 n=37 (94%), B4 n=64 (97%), B3 n=91 (98%), B2 n=170 (90%), B1 n=25 (100% of the 4 resolved). The high B2/B3/B4 malignancy rates reflect that this is a *surgical* cohort — patients with benign molecular calls who avoided surgery are not represented (verification bias, see §3).
- Initial surgical extent: **total thyroidectomy 448/765 (58.6%)**, **lobectomy 317/765 (41.4%)**. This is consistent with the EXT2-4 v1 abstract framing (initial total ~57.3% in v1's N=558) and very close to the v3's n=400 cohort's 55.5%/44.5% split.
- **Pre-2015 cell is n=7** — temporal claims about size-specific extent decisions remain restricted to the 2015+ era.
- **Strict-nodal-exclusion sensitivity arm (n=654):** 521/654 female (79.6%), age median 56 [43–66], total thyroid 55.5%, lobectomy 44.5%, strict malignancy 322/654 with resolved histology (49.2%). Reproduces v1's N=558 strict cohort to within ~17% (the v4 expansion captures the multinodular goiter patients v1 excluded by the index-nodule gate).
- Full cells: `tables/table1_v4_cohort_characteristics.csv`, `tables/table2_v4_malignancy_by_bethesda_era.csv`, `tables/table2b_v4_surgical_extent_by_bethesda_era.csv`.

### 5. Mutation-specific signal: BRAF dominates "positive calls" on Afirma; RAS dominates ThyroSeq
*Numbers below derive from the full surgical pool (v3-era denominator) rather than the v4 cohort, and have not yet been re-derived on the n=765 v4 subset — recommended next-pass task.* Among the 183 ThyroSeq B3+B4 surgical patients, 57 had a positive molecular call. Of those, BRAF-positive accounted for ~12, RAS-positive for ~33, TERT-positive for ~7 (RAS leads in indeterminate cytology, as expected). Among the 93 Afirma B3+B4 surgical patients, 63 had a positive call, **of which 60 were BRAF-positive** — this reflects that our `mol_platform = 'Afirma'` flag captures the Afirma+Xpression-Atlas combined readout, and BRAF on Xpression-Atlas is over-represented in the surgical Afirma subset. This is consistent with the Elicit report's observation that **oncocytic-dominant cytology and BRAF-only signals carry lower-than-expected PPV** but our cell-counts here (Afirma BRAF+ → benign histology n=10/63 in B4-unknown alone) corroborate that pattern in this single-institution dataset.

### 6. Recurrence (biopsy/operative-pathology-proven only) is rare and short-follow-up-limited
Restricted to the v4 cohort (n=765 any-preop-2–4 cm):

| Group | n_malignant | path-proven recurrence n (%) [95% CI] |
|---|---:|---|
| Afirma (any mutation class) | 24 | 0/24 (0.0% [0.0–13.8]) |
| ThyroSeq (any mutation class) | 57 | 1/57 (1.8% [0.3–9.3]) |
| Other / historical / in-house | 317 | 6/317 (1.9% [0.9–4.1]) |
| Untested | 5 | 0/5 (0.0% [0.0–43.4]) |

The **0% Afirma path-proven recurrence is a follow-up artifact** — Afirma testing concentrates in 2015–2022 in this cohort, so most patients have <5 years of post-op follow-up and any recurrence is more likely captured by imaging (which we exclude per the user's path-proven-only definition). Long-term outcomes are deferred; this number should not be cited as a recurrence comparison. Note that the "any recurrence" / "structural" columns in `table4_v4_recurrence_by_molecular_status.csv` show 50–90% of malignant patients have *some* recurrence signal (imaging or labs), so the path-proven gate is the binding constraint, not the underlying recurrence rate.

Cells: `tables/table4_v4_recurrence_by_molecular_status.csv`.

### 7. Era trends: molecular adoption, not surgical-extent migration, drives the pattern
On the full surgical denominator (8,368), pre-2015: **9/3,756** named-platform tests (0.24%); 2015+: **488/4,612** (10.6%). The 2015 inflection is the driver. Total thyroidectomy rate pre-2015: **1,919/3,756 = 51.1%**; 2015+: **2,640/4,612 = 57.2%**. Modest drift toward more total thyroidectomy in the recent era — opposite the pattern some Elicit-cited studies report (those describe a shift toward lobectomy after molecular testing). This is hypothesis-generating; could reflect referral-pattern selection at this institution or the 2015 ATA guidelines' broader endorsement of lobectomy that this cohort did not follow.

Within the v4 cohort (n=765), the era split is **7 pre-2015 / 758 2015+ (99.1% 2015+)**, so a v4-internal pre/post-2015 comparison is not powered. The 2–4 cm question is effectively a 2015+ question in this institution's data.

### 8. Existing EXT2-4 gap-list items addressed by this expansion
| Existing gap (per `MANUSCRIPT_GAP_LIST.md`) | Status after expansion |
|---|---|
| "Concordance tables — descriptive 2×2 counts only — no formal kappa or sensitivity/specificity CI in folder" | **Addressed (v4)**: `tables/table3_v4_*` reports Sens/Spec/PPV/NPV with Wilson 95% CI for ThyroSeq and Afirma in B3, B4, B3+B4 × {2–4 cm, <2 cm, unknown size, all sizes} × {NIFTP-as-benign, NIFTP-as-malignant} on the n=765 v4 cohort. |
| "Multiple testing — univariable battery without formal multiplicity adjustment" | **Partially addressed**: Cowork-side BH-FDR univariable battery completed (Task #21); v4-cohort re-run still pending. |
| "Pathology size sensitivity N=0 — sensitivity analysis not completed" | **Re-opened on BQ**: path_tumor_size_cm 2–4 cm → ~1,183 patients exist on BQ. Numerical analysis of this arm deferred to the next pass. |
| "Exact calendar study period" | **Addressed**: 1999–2025 with year distribution in `figures/fig1_cohort_flow_v4.png` and `tables/table1_v4_cohort_characteristics.csv`. |
| "v1 N=558 vs v3 N=400 cohort gap" | **Resolved by v4** (n=765 any-preop / n=654 strict); reconciliation memo in `cohort_reconciliation_v1_vs_v3.md`. |
| "Pseudo R² — not written to standard CSV outputs" | Not addressed (regressions not re-run on BQ this session). |

### 9. What this expansion does *not* do
- Does not re-run the multivariable logistic models on the v4 cohort (deferred — Cowork follow-up after v4 prose review).
- Does not refresh §5 mutation-class counts onto the v4 cohort (also deferred follow-up).
- Does not perform propensity-score matching, decision-curve analysis, ML feature importance, or cost-effectiveness modeling — these were out of session scope per user direction.
- Does not include senior-author boilerplate (IRB number, institution, funding, COI, contributor list) — pending Logan.
- Does not unlock the EXT2-4 record (lifecycle stayed `Active`; v4 rebuild is a `change_type=major_revision` MFL entry, not a lifecycle change).

---

## Direct Elicit-gap → study-finding map (v4)

| Elicit gap | Finding here | Cell |
|---|---|---|
| Size-specific 2–4 cm data | n=765 any-preop-2–4 cm cohort, 52.7% strict malignancy, 58.6% total thyroidectomy. Era split: 7 pre-2015 / 758 2015+. | Table 1 v4, Table 2 v4, Table 2b v4 |
| Head-to-head ThyroSeq vs Afirma | n=84 evaluable in v4 B3+B4 (ThyroSeq 71 + Afirma 13); ThyroSeq Sens 88.6%/Spec 75.0%/PPV 77.5%/NPV 87.1% vs Afirma Sens 80.0%/Spec 0.0%/PPV 72.7%/NPV 0.0%. 2–4 cm subgroup ThyroSeq n=30: Sens 86.7%/Spec 73.3%/PPV 76.5%/NPV 84.6%. Methodologic caveats §2 (a)–(h). | Table 3 v4 |
| Three-way concordance (Bethesda + molecular + histopathology) | Captured by Table 3 v4's TP/FP/FN/TN cells across each (Bethesda × platform × size band). | Table 3 v4 |
| Surgical decision impact | Within v4 cohort: 58.6% total thyroidectomy. Across full surgical denominator: 2-pp rise post-2015 (51.1% → 57.2%). | Table 1 v4, Table 2b v4 |
| Completion thyroidectomy | Not re-derived on v4 — existing EXT2-4 dual-definition completion tables remain authoritative (`table7_completion_thyroidectomy.csv`). | Existing EXT2-4 outputs |
| Long-term outcomes | **Excluded** per user direction (insufficient follow-up). | n/a |
| Recurrence | Path-proven only on v4: 0/24 Afirma (follow-up artifact), 1/57 ThyroSeq (1.8%), 6/317 Other (1.9%). | Table 4 v4 |

---

## Reproduction
```
cd studies/proposal_2to4cm_extent_molecular_20260326/elicit_expansion_20260509/
python3 build_elicit_expansion_v4.py
python3 build_table3_v4_actual_call.py
python3 build_figures_v4.py
node build_manuscript_docx_v4.js   # regenerates manuscript_v4_draft.docx
```
Active builders write tables/figures from BigQuery aggregates. SQL is in `sql/` (notably `sql/04b_table3_v4_actual_reported_call.sql`). The v3-era builders are preserved as `build_elicit_expansion_v3_archived.py`, `build_table3_v2_actual_call_v3_archived.py`, `build_figures_v2_v3_archived.py` and should not be re-run. Pre-v4 deliverables live under `superseded_v3/`.
