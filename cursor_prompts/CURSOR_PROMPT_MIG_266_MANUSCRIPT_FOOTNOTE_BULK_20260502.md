# Cursor Composer Dispatch — mig_266: Bulk manuscript footnote update across M032/M037/M044/M025/M004

**Generated:** 2026-05-02 by Cowork (post mig_260/261/262/263/264/265).
**Lane:** mig_266 — All round-6 migs introduced data-quality conventions that must be declared in manuscript methods sections. Single bulk edit across the 5 manuscripts in flight to add the footnotes / filter declarations consistently.
**Recommended agent:** **Cursor Composer** — text-only, mechanical multi-file edit.
**Estimated runtime:** 30–45 min
**Triggered by:** All round-6 migs (mig_258/259/261/262/263/264/265).
**Severity:** MED. Reviewer-defensibility for any submitted manuscript; reduces re-review risk.
**Closes carry-forwards:** CF-mig258-MANUSCRIPT-FILTER-UPDATE, CF-mig264-MANUSCRIPT-FOOTNOTE, CF-mig265-(carry from mig_265 footnote work).

---

## §0 — First message to paste into Cursor Composer

> mig_266 dispatch. Read `cursor_prompts/CURSOR_PROMPT_MIG_266_MANUSCRIPT_FOOTNOTE_BULK_20260502.md` end-to-end. Edit the methods sections of M032, M037, M044, M025, M004 manuscript drafts to declare the round-6 conventions (ln_status_source filter, AJCC IVA/IVC collapse, ETE label normalization, LN-flag rebuild, Bethesda enrichment, NLP coverage limitations). Each footnote is ~3-5 sentences with specific CF references.

---

## §1 — Convention catalog (drop into methods of each affected manuscript)

### F1. LN status source (mig_258 / file 259)
> Lymph-node positivity in this analysis is sourced from a triadic provenance column `ln_status_source ∈ {'staging','count','both',NULL}`, where `staging` indicates AJCC 8 N-stage assertion only, `count` indicates a structured `ln_total_positive > 0` count, and `both` indicates concordance. For analyses requiring numeric LN positivity (e.g. univariable logistic regression on LN burden), we restricted to `ln_status_source = 'both'` (n=1,126 of 4,137 malignant patients) to avoid mixing patients with N-staging assertion only and those with documented per-node counts. (Reference: CF-mig258-MANUSCRIPT-FILTER-UPDATE.)

**Apply to:** M037, M044

### F2. AJCC stage convention (mig_263 Option B)
> The canonical_patient_master `ajcc8_stage_group` column collapses the AJCC 8th edition published labels {IVA, IVB, IVC} into a single `IVB` value, reflecting the patient-level rollup design (mig_266b overlay). All distant-disease (M1) thyroid carcinomas (regardless of histologic subtype: ATC, MTC, PDTC) are coded as `IVB` rather than `IVC`. The full-granularity AJCC labels (IVA/IVB/IVC) remain available in the new `ajcc8_stage_group_resolved` column for analyses requiring textbook-published stage notation.

**Apply to:** M037, M044, M032

### F3. ETE label normalization (mig_261)
> Extrathyroidal extension (ETE) labels were normalized via case- and whitespace- insensitive mapping (mig_261). Six typo variants in `tumor_1_lymphatic_invasion` ("preesent", "indeeterminate", etc.) and two in `tumor_1_extrathyroidal_extension` ("extesive") were corrected. `path_synoptics.surg_date` was retyped from TIMESTAMP to DATE per Logan's clinical-dates calendar-only rule.

**Apply to:** M044 (ETE-primary manuscript)

### F4. LN suspicious flag rebuild (mig_262)
> The `any_suspicious_us_ln_ever` flag was rebuilt from the per-nodule `canonical_us_thyroid_gland_v2` table to capture suspicious LN findings. The original threshold definition fired for only 8 of 4,077 patients with US LN findings; the rebuilt flag fires for 1,733 patients (42% of the eligible cohort, in line with operative cohort biology).

**Apply to:** M037, M044, M076 (LN surveillance)

### F5. Bethesda 2 cohort enrichment (mig_264 + mig_264b)
> Bethesda-2 risk-of-malignancy (ROM) in this operative cohort is 18.9% (385 of 2,033 patients with `bethesda_final = 2`), substantially above the 0–3% range reported for screening cohorts. After mig_264b reclassification (24 NIFTP + follicular adenoma cases recategorized as non-malignant; 19 patients with postoperative-FNA mismapping repointed to the preoperative FNA bethesda value), the residual count is ~342 patients (16.8% ROM). The remaining elevation reflects (1) operative cohort enrichment intrinsic to a tertiary surgical referral cohort, and (2) cytology limitations in distinguishing follicular adenoma from follicular carcinoma. Subgroup analyses stratified by Bethesda category should be interpreted in this context.

**Apply to:** M025, M027 (cytology performance), M037

### F6. NLP coverage limitations (mig_265 + CF-mig260b/c/d / mig_261c/d/e)
> Several patient-level NLP-derived signals are documented as under-extracted in the current canonical pipeline:
> - Vascular invasion mentions (`nlp_path_vasc_inv_mentioned`) miss 749 patients flagged as vasc-positive in `canonical_invasion_events_v1` (mig_177/179 LVI rebuild caught CAP-template patterns the NLP didn't see).
> - LN positivity flags disagree with `canonical_path_malignant_events_v1.ln_involved` in 1,105 patients.
> - Smoking status documented for 27 of 10,871 patients (0.25% vs ~70% expected for any clinical cohort).
> - Family history of thyroid cancer (30 patients) and family history of any cancer (16 patients) are extracted at <1% prevalence vs 5–15% expected for an oncology referral cohort.
>
> Subgroup analyses depending on smoking, family history, or vascular invasion as a primary exposure should be interpreted with these coverage gaps in mind. Cross-validation against `canonical_invasion_events_v1` (vasc) or `canonical_path_malignant_events_v1.ln_involved` (LN) is recommended for primary outcomes.

**Apply to:** M032, M037, M044

---

## §2 — File-edit map

```
M044_ETE_manuscript_draft.md                        → F1, F2, F3, F4, F6
M037_*_manuscript_draft.md (find via grep)          → F1, F2, F4, F5, F6
manuscript_outputs/v1_0_20260501/M032_25yr_descriptive_analysis_DRAFT_v1.md
                                                     → F2, F6
M025_*_manuscript_draft.md (if exists; create if not)→ F5, F6
M004_*_manuscript_draft.md (if exists)              → F2 only (autoimmune, low impact)
M027_*_manuscript_draft.md (FNA performance)        → F5 only
```

## §3 — Post-edit verification

```bash
# Confirm each manuscript references mig_266 in commit message
git log --oneline | grep mig_266 | head -1

# Grep each manuscript for the convention strings
grep -l "ln_status_source" M*manuscript*.md
grep -l "ajcc8_stage_group_resolved" M*manuscript*.md
grep -l "1,733" M*manuscript*.md      # LN flag rebuild number
grep -l "18.9%" M*manuscript*.md      # Bethesda 2 ROM
```

## §4 — Carry-forwards
- CF-mig258-MANUSCRIPT-FILTER-UPDATE → CLOSED
- CF-mig264-MANUSCRIPT-FOOTNOTE → CLOSED
- CF-mig265-NLP-SOCHX-FAMHX-REFRESH-SCOPE → remains open (still future work)

## §5 — Surgical git add
```
M032_*manuscript*.md
M037_*manuscript*.md
M044_*manuscript*.md
M025_*manuscript*.md  (if created/updated)
M027_*manuscript*.md  (if created/updated)
```
