# Cursor run order — M025 nodule-level pivot (queue these in order)

**Generated:** 2026-05-04 by Cowork (Claude). Sequence is intentional — each unblocks the next.

## 1. mig_260 — TIRADS drift repoint (Composer, ~60 min)

**File:** `cursor_prompts/CURSOR_PROMPT_MIG_260_CPM_TIRADS_DRIFT_REPOINT_20260501.md`

**First message to paste:**

> mig_260 dispatch. Read `cursor_prompts/CURSOR_PROMPT_MIG_260_CPM_TIRADS_DRIFT_REPOINT_20260501.md` end-to-end. The fix is NOT a CPM update — the live CPM is correct (post-mig_265 cleanup). The fix is to re-point downstream code: M037 cohort view, M025 cohort definition, snowflake_trial Prompt 7 + M037 Table 1 scripts, and any `manuscript_workspace.cohort_*` views that reference removed TIRADS columns. Source-of-truth substitute is `main.canonical_us_patient_master_VIEW_v2.max_tirads_category_ever`. Cowork-built `manuscript_workspace.cohort_m025_nodule_level_v1` (mig_306) ALSO needs the per-nodule equivalent — verify it doesn't reference any removed CPM TIRADS columns. After landing, INSERT signoff_migration row.

## 2. mig_264 — Bethesda-2 false-negative audit (Chat → Composer, ~2-3 hrs)

**File:** `cursor_prompts/CURSOR_PROMPT_MIG_264_BETHESDA2_FALSE_NEG_AUDIT_20260501.md`

**First message to paste (Chat for the decision pass):**

> mig_264 decision pass. 385 patients with `BETHESDA_FINAL = 2` (benign cytology) on `canonical_patient_master` ended up with `IS_MALIGNANT = TRUE` (mig_264b reduced this to 360 — the audit step is still pending for those 360). Run §2 probes to disambiguate three causes: (a) true false-negative cytology (~3% expected per literature); (b) FNA sampled non-index nodule and the malignancy was elsewhere; (c) `BETHESDA_INDEX_NODULE_LINKAGE_SOURCE` is mismapping which nodule the Bethesda value belongs to. Surface count of each cause + per-pattern disposition. No UPDATEs in this pass — bring me the disposition table. **NEW:** the Cowork-built per-nodule view `manuscript_workspace.cohort_m025_nodule_level_v1` provides per-nodule Bethesda-to-laterality alignment that may help adjudicate cause (c) — query it to see how many of the 360 have a same-side malignant tumor in a *different* nodule than the FNA-linked one. M025 manuscript is being pivoted to nodule-level; this audit's outcome materially affects per-nodule TR sens/spec.

## 3. mig_307 — M025 nodule-level submission package scaffold (Composer, ~45 min)

**No prompt file yet — paste this directly into Cursor Composer:**

> mig_307 dispatch. M025 has been pivoted from patient-level to nodule-level per Cowork session 2026-05-04 (see `memory/skill_snowflake_cortex_2026_05_04.md`, `cursor_prompts/CURSOR_PROMPT_MIG_306_NODULE_LEVEL_SPINE_20260504.md`, and the headline finding in §5 of that mig_306 prompt). Build `M025_submission_package_v2_0/` mirroring the v1_0 structure but driven by `manuscript_workspace.cohort_m025_nodule_level_v1` (37,438 rows / 6,523 patients / 3,687 strict-ACR analytic-eligible). Adapt `08_analysis_code/build_m025_tables.py`, `build_m025_figures.py`, and `build_m025_manuscript_md.py` to: (a) compute per-nodule TR sens/spec/PPV/NPV at TR≥TR3, TR4, TR5; (b) compute the patient-vs-nodule-level ROM comparison table from §5 of mig_306; (c) compute Bethesda × TIRADS cross-strat at nodule grain; (d) compute ROC curve per-nodule. Use the new title "Patient-level vs nodule-level TI-RADS calibration in a 25-year operative thyroid cohort" (see `M025_submission_package_v2_0/00_README.md` Cowork drafted alongside this). Keep the v1_0 package frozen as the patient-level companion (will be cited in v2_0's discussion as a sister analysis). After landing, INSERT signoff_migration row + write CLOSEOUT_NOTES.md.

## 4. mig_303 already signed — no action needed.

## 5. After all three above land, ping Cowork. I'll re-pull every number, regenerate the drift report, and tell you exactly which paragraphs in the new v2_0 manuscript need editing.

## Optional parallel work for the cursor agent

- `cursor_prompts/CURSOR_PROMPT_MIG_270_HISTOLOGY_SSOT_REPOINT_20260502.md` — ~30 min, Composer, mechanical histology lookup repoint. Doesn't change M025 numbers but worth shipping for consistency before submission.
- `cursor_prompts/CURSOR_PROMPT_MIG_278_252_BACKFILL_SIGNOFF_20260503.md` and `CURSOR_PROMPT_MIG_279_REGISTRY_AUDIT_253_256_258_259_20260503.md` — registry hygiene; 5 min and 30 min respectively.
