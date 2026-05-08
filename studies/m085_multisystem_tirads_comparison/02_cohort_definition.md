# M085 — Cohort Definition

## Unit of analysis

**Nodule** (not patient). Each nodule is an independent unit of analysis for diagnostic performance calculations. Multiple nodules from the same patient are included independently; sensitivity analyses will use clustered standard errors at the patient level.

## Source table

`pub_canonical.canonical_us_nodule_tirads_multisystem_v1` (BQ, 37,579 rows as of 2026-05-07)

## Primary denominator: all scored nodules (N=37,579)

Used for cross-system concordance analyses (Cochran's Q, pairwise kappa) and for coverage comparison (what fraction of nodules each system can score).

## Analysis-eligible denominator: labeled nodules (N≈8,000+)

**Inclusion:**
- Nodule in `canonical_us_nodule_tirads_multisystem_v1` with at least one of the 11 systems scored (score is non-NULL and non-imputation-placeholder)
- Per-nodule pathology outcome label in `pub_workspace.us_nodule_path_outcome_v1` (malignant/benign/indeterminate)
- Path label derived from laterality-aware FNA-to-pathology match (Phase B.6 method)

**Exclusions:**
- No pathology label (label = NULL or label_confidence < threshold)
- No feature data (all 12 Park features are NULL; no composition/echogenicity/shape/margin/calcification data for ACR)
- Prior ipsilateral thyroidectomy on same side as the exam (nodule represents post-surgical surveillance, not de-novo evaluation)
- Exam explicitly flagged as post-ablation surveillance (RAI follow-up US)

**Per-system denominators:** Each system is evaluated on the subset of labeled nodules for which that specific system produced a non-NULL score. Per-system N will be reported alongside AUC.

## Path outcome label definition

- **Malignant:** Final surgical pathology = any thyroid carcinoma (PTC, FTC, HCC, MTC, ATC, PDTC) on the ipsilateral surgical specimen, linked to the nodule by laterality-aware match.
- **Benign:** Ipsilateral surgical specimen with no carcinoma; or FNA Bethesda II (benign) with ≥2 years follow-up and no surgical intervention.
- **Indeterminate:** Bethesda III/IV without surgery; or surgical specimen with NIFTP without carcinoma on the same side (sensitivity analysis: treat NIFTP as benign vs malignant).

## Multinodular exams

For the diagnostic performance analysis, each nodule is treated independently. For the cross-exam concordance analysis (agreement between systems on the most suspicious nodule per exam), the index nodule is the highest-scored nodule per exam per system.

## Sanity check

Post Phase B.6 nodule-level linkage: 613 exams now show mixed benign+malignant labels (was 0 with patient-level rollup) — expected finding confirming nodule-level linkage is working correctly.
