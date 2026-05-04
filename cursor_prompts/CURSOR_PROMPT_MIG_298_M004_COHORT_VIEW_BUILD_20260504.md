# Cursor Composer Dispatch — mig_298: M004 Autoimmune+Cancer cohort view (Option 1 — descriptive logistic)

**Generated:** 2026-05-04 by Cowork at HEAD `e590e40`.
**Lane:** mig_298 — Build `manuscript_workspace.cohort_m004_autoimmune_cancer_v1` per M004 scope brief Option 1 (descriptive logistic, not survival). Logan-decision pending; this prompt assumes Option 1 default. If Logan picks Option 2 (NLP-augmented), defer until N4/N5 SF AI_CLASSIFY pilots land.
**Recommended agent:** **Cursor Composer**.
**Estimated runtime:** 30 min.
**Severity:** MED. Unblocks M004 logreg + ready-for-writing pathway.
**Triggered by:** Pending Logan decision on M004 scope; Cowork defaulting to Option 1.

---

## §0 — First message

> mig_298 dispatch. Build M004 cohort view for autoimmune-thyroiditis × cancer descriptive analysis. Option 1 scope (existing syn_hashimoto/syn_graves coverage; ~800 patients). MotherDuck DB is `thyroid_canonical_publication_v1_0`. CONFIRM with me whether Option 1 or 2 is desired before applying.

## §1 — Decision gate (do not skip)

Before applying, confirm with Logan whether he wants:
- **Option 1 (current syn_*):** ~800 autoimmune patients (240 Hashimoto / 566 Graves / 8 both)
- **Option 2 (NLP-augmented):** Awaits N4/N5 SF pilots to expand to ~2,000 patients

Option 1 is shippable now; Option 2 takes ~3 weeks. Default to Option 1 if Logan unavailable.

## §2 — Cohort view (Option 1)

```sql
CREATE OR REPLACE VIEW manuscript_workspace.cohort_m004_autoimmune_cancer_v1 AS
SELECT
  pm.research_id,
  pm.age_at_surgery, pm.sex, pm.race,
  pm.is_malignant,
  pm.histology_final,
  pm.first_surgery_date,
  pm.followup_years,
  pm.death_occurred,
  pm.overall_survival_days,
  pm.any_recurrence_flag,
  -- Autoimmune exposures (Option 1 — synoptic-only)
  pm.syn_hashimoto, pm.syn_graves,
  CASE
    WHEN pm.syn_hashimoto AND pm.syn_graves THEN 'both'
    WHEN pm.syn_hashimoto THEN 'hashimoto_only'
    WHEN pm.syn_graves THEN 'graves_only'
    ELSE 'neither'
  END AS autoimmune_category,
  -- Future Option 2 placeholder (NULL until N4/N5 NLP land)
  NULL::BOOLEAN AS pmhx_nlp_hashimoto,
  NULL::BOOLEAN AS pmhx_nlp_graves,
  -- Smoking + family-hx covariates from mig_281
  pm.pmhx_nlp_smoking_status,
  pm.pmhx_nlp_family_hx_thyroid
FROM main.canonical_patient_master pm;
```

Row count: 10,871 (whole cohort; Option 1 logreg filters in-script).

## §3 — Verify

```sql
SELECT
  autoimmune_category,
  COUNT(*) AS n,
  COUNT_IF(is_malignant) AS n_malig,
  ROUND(100.0 * COUNT_IF(is_malignant) / COUNT(*), 1) AS pct_malig
FROM manuscript_workspace.cohort_m004_autoimmune_cancer_v1
GROUP BY 1 ORDER BY 1;
-- Expected (per M004 scope brief):
-- both           8 / 1   / 12.5%
-- graves_only  566 / 53  / 9.4%
-- hashimoto_only 240 / 90 / 37.5%
-- neither     10057 / 3874 / 38.5%
```

## §4 — Registry signoff

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_298', CURRENT_TIMESTAMP, 'cursor_composer_mig298',
 'mig_298: Built manuscript_workspace.cohort_m004_autoimmune_cancer_v1 (Option 1 descriptive scope). 10,871 rows. autoimmune_category enum (both/hashimoto_only/graves_only/neither). Smoking + family-hx covariates included from mig_281. Option 2 (NLP-augmented) deferred to N4/N5 pilot completion. Unblocks M004 ready-for-writing path.');
```

## §5 — Followup

After mig_298 lands, Cowork will:
1. Add cohort_m004 view to SF refresh (extend mig_289 list)
2. Build M004 logreg + Table 1 + ready-for-writing brief

## §6 — Surgical git add

```
qc_framework_v1/migrations/298_m004_cohort_view_20260504.sql
scripts/output/mig_298_apply_log.txt
cursor_prompts/CURSOR_PROMPT_MIG_298_M004_COHORT_VIEW_BUILD_20260504.md
```

---

**End of mig_298 dispatch.**
