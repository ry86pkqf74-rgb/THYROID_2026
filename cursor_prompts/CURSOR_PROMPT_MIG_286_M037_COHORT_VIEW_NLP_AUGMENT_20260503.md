# Cursor Composer Dispatch — mig_286: M037 cohort view NLP augmentation (family-hx covariate)

**Generated:** 2026-05-03 by Cowork at HEAD `1284973`.
**Lane:** mig_286 — Wire NLP-augmented `pmhx_nlp_family_hx_thyroid` (post-mig_281) into `manuscript_workspace.cohort_m037_ln_metastasis_v1`. M037 LN Predictors logreg currently drops family-hx as a covariate due to <5% coverage; post-mig_281 coverage ~60% on malig unblocks it as a known LN-met confounder (familial syndromes: FMTC / MEN2 / Carney).
**Recommended agent:** **Cursor Composer** — view-only modification; mirror of mig_285 pattern.
**Estimated runtime:** 20 min.
**Triggered by:** mig_281 SF→MD NLP promotion landing.
**Severity:** MED. Strengthens M037 multivariable model.
**Precondition:** mig_281 must land first.
**Closes:** CF-M037-FAMILY-HX-COVARIATE-DROP.

---

## §0 — First message to paste into Cursor Composer

> mig_286 dispatch. Mirror of mig_285 but for M037 cohort view. PRECONDITION: mig_281 + (preferably) mig_285 already landed. MotherDuck DB is `thyroid_canonical_publication_v1_0`.

---

## §1 — Pre-task probes

```sql
-- 1.1 Preconditions
SELECT mig_id FROM main.signoff_migration WHERE mig_id IN ('mig_281','mig_285');
-- If mig_281 missing: STOP, surface.

-- 1.2 cohort_m037 baseline (post mig_280 view fix)
SELECT COUNT(*) AS n FROM manuscript_workspace.cohort_m037_ln_metastasis_v1;
-- Expected: 2,233 (per Cowork probe 2026-05-03)
```

---

## §2 — Inspect current view definition

```sql
SELECT view_definition FROM information_schema.views
WHERE table_schema='manuscript_workspace' AND table_name='cohort_m037_ln_metastasis_v1';
```

Confirm presence (or absence) of family-hx cols already.

---

## §3 — Apply

### §3a — Pre-snapshot

```sql
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.view_def_cohort_m037_pre_mig286_20260503 AS
SELECT view_name, view_definition, CURRENT_TIMESTAMP AS snapshot_at
FROM information_schema.views
WHERE table_schema='manuscript_workspace' AND table_name='cohort_m037_ln_metastasis_v1';
```

### §3b — CREATE OR REPLACE view with family-hx covariate

```sql
CREATE OR REPLACE VIEW manuscript_workspace.cohort_m037_ln_metastasis_v1 AS
SELECT
  cm.*,
  pm.pmhx_nlp_family_hx_thyroid,
  pm.pmhx_nlp_family_hx_cancer,
  -- syndromic-flag composite (FMTC / MEN2 / Carney) — text scan over PMH
  CASE
    WHEN pm.pmhx_nlp_family_hx_thyroid = TRUE THEN TRUE
    WHEN pm.histology_final ILIKE '%mtc%' AND pm.pmhx_nlp_family_hx_cancer = TRUE THEN TRUE
    ELSE FALSE
  END AS family_syndrome_flag
FROM <existing_FROM_clause> cm
LEFT JOIN main.canonical_patient_master pm USING (research_id);
```

### §3c — Verify

```sql
SELECT
  COUNT(*) AS n_cohort,
  COUNT_IF(pmhx_nlp_family_hx_thyroid IS NOT NULL) AS n_fhx_known,
  COUNT_IF(family_syndrome_flag) AS n_syndrome
FROM manuscript_workspace.cohort_m037_ln_metastasis_v1;
-- Expected: n_cohort = 2,233; fhx_known ≥ 1,300 (post-mig_281); syndrome ~50-100
```

### §3d — Registry signoff

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_286', CURRENT_TIMESTAMP, 'cursor_composer_mig286',
 'mig_286: CREATE OR REPLACE VIEW manuscript_workspace.cohort_m037_ln_metastasis_v1 with pmhx_nlp_family_hx_thyroid + pmhx_nlp_family_hx_cancer + family_syndrome_flag covariates (LEFT JOIN canonical_patient_master). Cohort n unchanged at 2233; fhx_known=NN, syndrome_n=NN. Closes CF-M037-FAMILY-HX-COVARIATE-DROP. Cowork to re-render M037 Table 2 logreg with family-hx as covariate next round.');
```

---

## §4 — Carry-forwards

| ID | Status | Notes |
|---|---|---|
| CF-M037-FAMILY-HX-COVARIATE-DROP | **CLOSED on apply** | logreg can include family-hx |
| CF-M037-SYNDROME-COMPOSITE | **OPEN** | The composite flag in §3b is a heuristic; future mig may refine using note_entities_llm_pmhx category='syndrome' |

---

## §5 — Surgical git add

```
qc_framework_v1/migrations/286_m037_cohort_view_nlp_augment_20260503.sql
scripts/output/mig_286_apply_log.txt
cursor_prompts/CURSOR_PROMPT_MIG_286_M037_COHORT_VIEW_NLP_AUGMENT_20260503.md
```

Commit message:
```
feat(md): mig_286 M037 cohort view NLP augmentation (family-hx + syndrome flag)

- CREATE OR REPLACE manuscript_workspace.cohort_m037_ln_metastasis_v1
- Added LEFT JOIN canonical_patient_master for pmhx_nlp_family_hx_thyroid + cancer
- Added family_syndrome_flag composite (FMTC/MEN2 heuristic)
- Closes CF-M037-FAMILY-HX-COVARIATE-DROP
```

---

**End of mig_286 dispatch.**
