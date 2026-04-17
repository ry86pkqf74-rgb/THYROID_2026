# THYROID_2026 — Deferred Audit Items (v1_1 cleanup, 2026-04-16)

**Branch:** `cleanup/canonical-finalization-20260416`
**Authoritative DB:** `thyroid_canonical_publication_v1_0`
**Cohort invariant (re-verified):** `canonical_patient_master` = 10,871 patients × 1,500 columns; distinct `research_id` = 10,871; 0 NULL `research_id`; 0 NULL `fna_path_outcome`.

This memo lists the audit findings explicitly **DEFERRED** out of the v1_1 cleanup pass (Scripts 248-251). They are intentionally not patched here because each requires clinical-judgment redesign that exceeds the mechanical-fix scope of v1_1. Each item lists the live count, why it is deferred, and the proposed v1_2 (or later) scope to address it.

The next-session goal is to take this memo into a clinical-judgment-focused Opus 4.6 session and resolve them one at a time, each behind a separate audit-feed review table and a clinician sign-off gate.

---

## D-1. M-stage over-call (M1 = 1,818, including 1,678 patients without PET)

**Live count (post-Phase-2):**
```sql
SELECT COUNT(*) FROM canonical_patient_master WHERE ajcc8_m_stage = 'M1';
-- 1,818
SELECT COUNT(*) FROM canonical_patient_master
WHERE ajcc8_m_stage = 'M1' AND pet_has_data IS NOT TRUE;
-- 1,678
```

**Why deferred:** The current M-stage derivation appears to flag M1 too liberally — 1,678 of 1,818 (92.3%) M1 patients have no PET imaging, and many lack any documented distant-mets evidence (biopsy, bone scan, CT chest with mets-positive findings). Mechanical fix (e.g., "set M1 = M0 if no PET and no distant_mets_proxy") risks downgrading patients with legitimate distant disease documented elsewhere (e.g., palliative-RAI for known mets, scintigraphy outside our PET cohort). The redesign needs:

1. A staged gating logic combining PET, distant_mets_proxy, biopsy-proven mets entities, palliative-RAI history, and structured M-stage from path_synoptics where available.
2. Reviewer adjudication for the residual ambiguous cases.

**Proposed v1_2 scope:** New script `scripts/252_m_stage_redesign.py` that builds `manuscript_workspace.m_stage_adjudication_review_v1_1` with one row per current M1 patient containing all gating evidence in stacked columns. CPM remains unchanged until clinician sign-off; then a follow-up script promotes the adjudicated values to a new column `ajcc8_m_stage_adjudicated_v1_1` (do NOT overwrite existing `ajcc8_m_stage` column; mark it via the AGENTS.md deprecated-column convention).

---

## D-2. PMH inflation for hypothyroidism, hyperthyroidism, and breast cancer

**Live count (post-Phase-2):**
```sql
SELECT
  SUM(CASE WHEN pmhx_nlp_hypothyroidism THEN 1 ELSE 0 END)        AS pmhx_hypo,
  SUM(CASE WHEN pmhx_nlp_hyperthyroidism THEN 1 ELSE 0 END)       AS pmhx_hyper,
  SUM(CASE WHEN pmhx_nlp_prior_cancer_hx THEN 1 ELSE 0 END)       AS pmhx_prior_cancer
FROM canonical_patient_master;
-- pmhx_hypo: ~5,400 (likely overcounted — hypothyroid status follows from RAI/total-thy, not PMH)
-- pmhx_hyper: ~600 (some legitimate Graves; some over-extracted from "?"/historical references)
-- pmhx_prior_cancer: ~3,200 (possible double-counting current thyroid cancer + history)
```

**Why deferred:** Two intertwined NLP issues:
1. The current `pmhx_nlp_*` extractor treats clinical-note mentions of thyroid disease as "past medical history" even when the mention is the patient's *current* thyroid disease being managed (post-thyroidectomy hypothyroidism, hyperthyroidism on methimazole pre-op, etc.).
2. "History of breast cancer" mentions get conflated with current/prior diagnosis without temporal anchoring; needs a `note_date < first_surgery_date - 30d` gate to filter.

Touches `scripts/216_data_gap_resolution.py` and the LLM extraction template for `note_entities_llm_past_medical_hx`. Strengthening the regex requires negation handling for "s/p", "history of", and intent-vs-history distinction; this is a clinical-judgment redesign, not a mechanical fix.

**Proposed v1_2 scope:** New extractor pass with strict temporality gating + manual review of 100 random `pmhx_nlp_hypothyroidism = TRUE` patients to validate the corrected definition before re-running.

---

## D-3. Imaging↔pathology size concordance r ≈ -0.04

**Live finding:** Pearson correlation between `dominant_nodule_size_cm` (from preop ultrasound) and `path_tumor_size_cm` (from synoptics) is essentially zero across the cohort.

**Why deferred:** The negative correlation is NOT a column-mapping bug; it reflects the absence of a deterministic link between the *specific* nodule biopsied/imaged and the *specific* tumor on path. For multifocal cancers, the dominant US nodule may not be the same focus as the largest path tumor, and the rollup logic loses this nodule-to-histology mapping at aggregation time. Mechanical fixes (e.g., max-vs-max comparison) don't help because they ignore the per-tumor identity.

Tables involved: `imaging_nodule_master_v1`, `synoptic_tumor_long_v1`, `specimen_tumor_focus_v1`, `canonical_tumor_characteristics_v1`. The right fix is a per-(US exam, surgery, tumor focus) linkage layer that resolves which preop nodule corresponds to which resected focus by laterality + size + position metadata.

**Proposed v1_2 scope:** New script that builds `imaging_to_path_focus_linkage_v1` using laterality + within-±0.5cm size threshold + within-90-day temporal window, then recomputes per-focus concordance. This requires medical-judgment review of edge cases (multifocal, complete-thyroidectomy where laterality is ambiguous, etc.).

---

## D-4. Smoking status 13.1% combined coverage

**Live count:**
```sql
SELECT COUNT(*) FROM canonical_patient_master
WHERE smoking_status_extracted IS NOT NULL OR smoking_status_structured IS NOT NULL;
-- ~1,425 of 10,871 (~13.1%)
```

**Why deferred:** This is a **structural EHR limitation**, not a pipeline bug. The institutional data extract simply does not include a structured smoking-status field for 86.9% of patients, and clinical-note NLP extraction recovers smoking status only when it's explicitly mentioned in a relevant note (typically pre-op H&P) — which itself isn't always available. There is no pipeline change that lifts this rate; the gap is in the upstream EHR extract.

**Proposed v1_2 scope:** Document this as a known limitation in `MANUSCRIPT_READY_CHECKLIST.md` under "Data caveats" with the exact denominator. Remove smoking from any model that requires near-complete coverage (e.g., do not include in propensity-score covariates). This memo flags it for explicit checklist update.

---

## D-5. LVI granular grade 96.6% `present_ungraded`

**Live count:**
```sql
SELECT lvi_ordinal_worst, COUNT(*) FROM canonical_patient_master
WHERE lvi_ordinal_worst IS NOT NULL
GROUP BY 1 ORDER BY 1;
```
The `lvi_ordinal_worst` column shows ~96.6% in the `present_ungraded` ordinal level (vs `focal`, `extensive`).

**Why deferred:** This is a **pathology-template limitation** — most synoptic templates report LVI as a binary present/absent without further granularity, so the "extensive vs focal vs present-ungraded" stratification is recoverable for only ~3.4% of LVI-positive patients. The binary `lvi_any_present_path` (also already in CPM) IS the clinically actionable answer for most analyses. The granular ordinal is preserved for the minority of cases where the template DID grade it.

**Proposed v1_2 scope:** No pipeline change needed. Update `MANUSCRIPT_READY_CHECKLIST.md` to direct manuscript analyses to use `lvi_any_present_path` (BOOLEAN) as the primary signal, with `lvi_ordinal_worst` reserved as a sub-cohort when granularity is required.

---

## D-6. Broader sentinel-string drift (Check 14b WARN, 119 release-mode)

**Live count (after Phase 2 renames):**
```
12 columns carry 'NaT'/'None'/'' sentinels (17,328 cells total):
  cnln_earliest_date            : 833
  cnln_img_first_date           : 192
  cnln_img_last_date            : 160
  cnln_latest_date              : 587
  cnln_surg_first_date          : 757
  cnln_surg_last_date           : 520
  ene_path_levels               :   3
  lateral_levels_v10            :  21
  DEPRECATED__margin_r_class    : 6,908   (deprecated; safe to ignore in v1_1)
  nsqip_calcium_vitd_category   : 175
  ... + 2 smaller cols
```

**Why deferred:** Phase 1 mandate (Script 248) targeted only the literal-`'nan'` strings (the 87% pollution on syn_architecture / syn_margin_distance_mm). The broader 'NaT'/'None'/'' sentinels are a separate class with their own upstream loaders (cnln_* from `clinical_note_ln_extracted_v1`, nsqip_* from `nsqip_enrichment`, etc.). Each one needs its own loader patch + UPDATE pass — out of scope for Phase 1's literal-'nan' eradication.

**Proposed v1_2 scope:** New script `scripts/253_sentinel_drift_repair.py` that applies the same NULLIF-based repair pattern to the 12 columns above, per their respective upstream loaders. The 119 Check 14b WARN drops to PASS once complete.

---

## D-7. Pointer-view mapping rate 84.73% (target had been ≥90%)

**Live count (post-Phase-3):**
```
manuscript_workspace.canonical_detail_pointer_v1
  total view rows:        1,608
  distinct master_columns:1,500
  mapped (>=1 detail tbl):1,271 (84.73%)
  multi-mapped (>=2 dt):     88
  unmapped (allowlist):     229
  unmapped (real):            0   (well below 105 budget)
```

**Why deferred:** Achieving the original 90% target requires reassigning ~80 currently-allowlisted columns to a detail table. Many of those (PRM_*, AJCC_*, ATA_*, MACIS_*, AMES_*, GM_*, PET_*, dateline aggregates) are legitimately CPM-computed and have no upstream registered table. To move them to "mapped" we'd either need to:
1. Discover unrecognized source tables (estimated lift: 88-92% if 1-2 are found).
2. Add a synthetic `computed_in_cpm_v1` registry pseudo-row mapped to all of them — 100% mapping but no real drill-down value.

**Proposed v1_2 scope:** Iteratively review the 229 allowlisted columns to discover unrecognized source tables. Specifically check whether a `prm_*_v1`, `gm_*_v1`, or `pet_extraction_v1` registry entry is missing. Bump the assertion threshold to ≥90% only after a pass that lifts ~80 columns into mapped status.

---

## Sign-off log

This memo was authored by `Script 250+251` outputs as part of the v1_1 finalization commit history on 2026-04-16. None of the items above is a v1_1 publication blocker — every cohort invariant on `canonical_patient_master` still holds (10,871 × 1,500). Each item is recommended for a focused next-session resolution with appropriate clinical-judgment review.

| Item | Owner | Status | Earliest target |
|---|---|---|---|
| D-1 M-stage over-call | clinical reviewer + script 252 | OPEN | v1_2 |
| D-2 PMH inflation | NLP extractor revision (Script 216 successor) | OPEN | v1_2 |
| D-3 Imaging↔path concordance | per-focus linkage script | OPEN | v1_2 |
| D-4 Smoking 13.1% coverage | documented limitation in MANUSCRIPT_READY_CHECKLIST.md | OPEN | doc-only |
| D-5 LVI 96.6% present_ungraded | documented; primary signal is binary `lvi_any_present_path` | OPEN | doc-only |
| D-6 Sentinel-string drift (12 cols) | Script 253 + loader patches | OPEN | v1_2 |
| D-7 Pointer mapping 84.73% | iterate allowlist; v1_2 lift to ≥90% | OPEN | v1_2 |

Generated by THYROID_2026 v1_1 cleanup on 2026-04-16.
