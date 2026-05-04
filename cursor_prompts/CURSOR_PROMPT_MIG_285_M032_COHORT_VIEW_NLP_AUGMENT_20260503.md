# Cursor Composer Dispatch — mig_285: M032 cohort view NLP augmentation (smoking + family-hx)

**Generated:** 2026-05-03 by Cowork at HEAD `1284973`.
**Lane:** mig_285 — Wire NLP-augmented `pmhx_nlp_smoking_status` + `pmhx_nlp_family_hx_thyroid` covariates (post-mig_281 SF→MD promotion) into `manuscript_workspace.cohort_m032_descriptive_25yr_v1`. M032 25-yr Descriptive paper Table 1 currently can't report smoking prevalence at <12% coverage; post-mig_281 coverage will be ~70-80%, unblocking the row.
**Recommended agent:** **Cursor Composer** — view-only modification; reads mig_281 outputs.
**Estimated runtime:** 30 min.
**Triggered by:** mig_281 SF→MD NLP promotion landing.
**Severity:** MED. Unblocks M032 Table 1 + downstream M032 logreg with smoking covariate.
**Precondition:** mig_281 must land first (provides `pmhx_nlp_smoking_status` + `pmhx_nlp_family_hx_thyroid` rebuild).
**Closes:** CF-M032-SMOKING-PREVALENCE-BLOCKER.

---

## §0 — First message to paste into Cursor Composer

> mig_285 dispatch. Read `cursor_prompts/CURSOR_PROMPT_MIG_285_M032_COHORT_VIEW_NLP_AUGMENT_20260503.md`. PRECONDITION: mig_281 must have landed (check signoff_migration). If not landed, STOP and surface to Logan. MotherDuck DB is `thyroid_canonical_publication_v1_0`.

---

## §1 — Pre-task probes

```sql
-- 1.1 mig_281 landed?
SELECT mig_id, signed_off_at FROM main.signoff_migration WHERE mig_id = 'mig_281';
-- If 0 rows: STOP, surface to Logan.

-- 1.2 New CPM coverage post-mig_281 (sanity check before adding to view)
SELECT
  COUNT(*) AS n_total,
  COUNT_IF(is_malignant) AS n_malig,
  COUNT_IF(is_malignant AND pmhx_nlp_smoking_status IS NOT NULL) AS n_malig_smk_known,
  COUNT_IF(is_malignant AND pmhx_nlp_family_hx_thyroid IS NOT NULL) AS n_malig_fhx_known
FROM main.canonical_patient_master;
-- Expected: n_malig ≥ 4,018; smk_known ≥ 2,800 (target 70%); fhx_known ≥ 2,400 (target 60%)
```

If coverage < 50% on either, surface to Logan — may indicate mig_281 issue.

---

## §2 — Inspect current cohort view

```sql
SELECT view_definition FROM information_schema.views
WHERE table_schema='manuscript_workspace' AND table_name='cohort_m032_descriptive_25yr_v1';
```

Identify SELECT clause; we'll add 4 new cols.

---

## §3 — Apply

### §3a — Pre-snapshot view definition

```sql
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.view_def_cohort_m032_pre_mig285_20260503 AS
SELECT view_name, view_definition, CURRENT_TIMESTAMP AS snapshot_at
FROM information_schema.views
WHERE table_schema='manuscript_workspace' AND table_name='cohort_m032_descriptive_25yr_v1';
```

### §3b — CREATE OR REPLACE view with NLP-augmented covariates

```sql
CREATE OR REPLACE VIEW manuscript_workspace.cohort_m032_descriptive_25yr_v1 AS
SELECT
  cm.*,  -- preserve all existing cols
  -- NLP smoking augmentation (post-mig_281)
  pm.pmhx_nlp_smoking_status,
  COALESCE(pm.pmhx_nlp_smoking_status, CASE WHEN pm.nsqip_smoker IS NOT NULL THEN 'nsqip_'||pm.nsqip_smoker ELSE NULL END) AS smoking_status_combined,
  -- NLP family hx augmentation
  pm.pmhx_nlp_family_hx_thyroid,
  pm.pmhx_nlp_family_hx_cancer
FROM <existing_FROM_clause> cm  -- replicate per probe in §2
LEFT JOIN main.canonical_patient_master pm USING (research_id);
```

(Specifics: replicate the existing FROM/WHERE/GROUP BY exactly; only add the SELECT cols + LEFT JOIN.)

### §3c — Verify view resolves + new cols populate

```sql
SELECT
  COUNT(*) AS n_cohort,
  COUNT_IF(pmhx_nlp_smoking_status IS NOT NULL) AS n_smk_known,
  COUNT_IF(pmhx_nlp_family_hx_thyroid IS NOT NULL) AS n_fhx_known
FROM manuscript_workspace.cohort_m032_descriptive_25yr_v1;
-- Expected: n_cohort matches pre-mig_285 count; smk + fhx known counts non-zero.
```

### §3d — Registry signoff

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_285', CURRENT_TIMESTAMP, 'cursor_composer_mig285',
 'mig_285: CREATE OR REPLACE VIEW manuscript_workspace.cohort_m032_descriptive_25yr_v1 with pmhx_nlp_smoking_status + smoking_status_combined + pmhx_nlp_family_hx_thyroid + pmhx_nlp_family_hx_cancer cols (LEFT JOIN canonical_patient_master). Cohort n unchanged at <X>; new cols coverage smk=NN fhx=NN. Closes CF-M032-SMOKING-PREVALENCE-BLOCKER. Cowork to re-render M032 Table 1 next round.');
```

---

## §4 — Carry-forwards

| ID | Status | Notes |
|---|---|---|
| CF-M032-SMOKING-PREVALENCE-BLOCKER | **CLOSED on apply** | M032 Table 1 smoking row now populates |
| CF-M032-LOGREG-SMOKING-COVARIATE | **OPEN** | If M032 expands to multivariable analysis, smoking should enter as covariate (Cowork follow-up) |

---

## §5 — Surgical git add

```
qc_framework_v1/migrations/285_m032_cohort_view_nlp_augment_20260503.sql
scripts/output/mig_285_apply_log.txt
cursor_prompts/CURSOR_PROMPT_MIG_285_M032_COHORT_VIEW_NLP_AUGMENT_20260503.md
```

Commit message:
```
feat(md): mig_285 M032 cohort view NLP augmentation (smoking + family-hx)

- CREATE OR REPLACE manuscript_workspace.cohort_m032_descriptive_25yr_v1
- Added LEFT JOIN canonical_patient_master for pmhx_nlp_smoking_status + family_hx
- Closes CF-M032-SMOKING-PREVALENCE-BLOCKER
```

---

**End of mig_285 dispatch.**
