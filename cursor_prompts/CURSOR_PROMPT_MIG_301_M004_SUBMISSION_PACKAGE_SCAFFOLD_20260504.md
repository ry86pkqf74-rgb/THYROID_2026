# Cursor Composer Dispatch — mig_301: M004 Autoimmune+Cancer submission package scaffold

**Generated:** 2026-05-04 by Cowork at HEAD `17baa2b` (post-pub_v1_1).
**Lane:** mig_301 — Build `M004_submission_package_v1_0/` mirroring M044/M038/M032/M037/M025 pattern. M004 cohort + logreg numbers locked (post-mig_298 NLP-augmented + Cowork's `M004_logreg_nlp_augmented_20260504.md`). Headline: Hashimoto aOR 1.37, p=0.002 ↑; Graves aOR 0.87, p=0.017 ↓.
**Recommended agent:** **Cursor Chat (Sonnet 4 / GPT-5) → Composer**.
**Estimated runtime:** 2-3 hours.
**Severity:** MED. Closes the M004 ready-for-writing pathway — last manuscript without a submission package.

---

## §0 — First message

> mig_301 dispatch. Build `M004_submission_package_v1_0/` mirroring `M044_submission_package_v1_0/` structure. Source: `manuscript_workspace.cohort_m004_autoimmune_cancer_v1` (mig_298 Option 2 NLP-augmented). MotherDuck DB is `thyroid_canonical_publication_v1_0`. Walk through table structure before generating .docx files.

## §1 — Pre-task: existing inputs

- [`manuscript_outputs/v1_0_20260501/M004_READY_FOR_WRITING_BRIEF.md`](computer:///Users/ros/THyroid 2026/manuscript_outputs/v1_0_20260501/M004_READY_FOR_WRITING_BRIEF.md) — locked numbers + headline findings
- [`snowflake_trial/reports/M004_logreg_nlp_augmented_20260504.md`](computer:///Users/ros/THyroid 2026/snowflake_trial/reports/M004_logreg_nlp_augmented_20260504.md) — full logreg output + concordance
- M044/M038/M032/M037/M025 submission packages — pattern reference

## §2 — Locked numbers

- Cohort: 10,871 / 4,019 malig
- Hashimoto combined (NLP+syn): **400** patients (348 only-Hashimoto + 52 both)
- Graves combined: **1,656** (1,604 only-Graves + 52 both)
- aOR Hashimoto: **1.37 (1.12–1.68), p=0.002**
- aOR Graves: **0.87 (0.78–0.98), p=0.017**

## §3 — Tables (proposed)

| # | Title |
|---|---|
| 1 | Cohort demographics by autoimmune category (Hashimoto / Graves / Both / Neither) |
| 2 | Logreg predictors of malignancy with 95% CI |
| 3 | Concordance (NLP vs synoptic) for each exposure |
| 4 | Sub-analysis: surgical indication on Graves cohort (thyrotoxicosis vs nodule workup) |
| Supp S1 | NLP-only / syn-only / combined sensitivity panel |
| Supp S2 | Histology distribution by autoimmune category |

## §4 — Figures (proposed)

| # | Title |
|---|---|
| 1 | Cohort flow diagram |
| 2 | Forest plot of multivariable predictors |
| 3 | Bar chart of malignancy rate by autoimmune category |
| 4 | Concordance Venn (NLP / synoptic / both) |

## §5 — Apply

Standard scaffold pattern. Mirror mig_290/291/292/300 structure with M004-specific cohort + logreg.

### Registry signoff

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_301', CURRENT_TIMESTAMP, 'cursor_composer_mig301',
 'mig_301: M004 Autoimmune+Cancer submission package v1.0 built. Mirrors M044/M038 structure. Tables 1-4 + Supp S1-S2 + 4 figures. SQL reproducibility (M004_autoimmune_analysis.sql) + 3 build scripts. Headline: Hashimoto aOR 1.37 (p=0.002) ↑ malignancy; Graves aOR 0.87 (p=0.017) ↓. Closes M004 ready-for-writing gate.');
```

## §6 — Surgical git add

```
M004_submission_package_v1_0/
qc_framework_v1/migrations/301_m004_submission_package_20260504.sql
scripts/output/mig_301_apply_log.txt
cursor_prompts/CURSOR_PROMPT_MIG_301_M004_SUBMISSION_PACKAGE_SCAFFOLD_20260504.md
```

---

**End of mig_301 dispatch.**
