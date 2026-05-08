# M085 — Multi-system TIRADS Comparison

**Full title:** Comparative performance of eleven thyroid imaging reporting and data systems in a 25-year American surgical thyroid cancer cohort

**Status:** Idea  
**Airtable record:** `recotdCiIuU8UQbLs` (THYROID_MANUSCRIPT base `appJYOnUb7KrHKwpV`, Manuscripts table `tblLsp8ls3rU1eEc9`)  
**BQ manuscript_id:** 85 (`thyroid-canonical-pub-2026.pub_workspace.manuscript_feasibility_v1`)  
**Candidate cohort N:** 37,579 nodules (~10,871 patients)  
**Lifecycle:** Active  
**Owner:** Logan Glosser  
**Scaffolded:** 2026-05-08

---

## Quick summary

This manuscript performs a head-to-head diagnostic performance comparison of eleven thyroid ultrasound risk-stratification systems on a single large American surgical thyroid cancer cohort. The primary infrastructure (per-nodule canonical scoring, Phase B, commit 778a61b) and the per-nodule pathology linkage (Phase B.6) are complete as of 2026-05-07.

The Park 2009 Korean-cohort coefficients show null discrimination (AUC 0.5365) on this cohort — a key primary result and Notable Finding NF-2026-05-07-park2009-noncalibration.

---

## Key data sources

| Resource | Location | Notes |
|---|---|---|
| Multi-system scoring | `pub_canonical.canonical_us_nodule_tirads_multisystem_v1` | 37,579 rows, scored 2026-05-07 |
| Per-nodule path labels | `pub_workspace.us_nodule_path_outcome_v1` | 8,000+ labeled nodules |
| Park cohort refit | `pub_workspace.park_cohort_refit_split_v2` | 70/30 split, AUC 0.6914 holdout |
| Phase B closure | `exports/phase_b_deterministic_scorers_20260507/README.md` | Full audit numbers |
| Coefficient manifest | `scripts/manifests/park_coefs_v1.json` | Park 2009 coefficients |

---

## Sub-files

- `01_aims_and_hypotheses.md` — Primary and secondary aims, pre-specified hypotheses
- `02_cohort_definition.md` — Inclusion / exclusion, denominators
- `03_analysis_plan.md` — Statistical methods, McNemar tests, Cochran's Q
- `04_park_prevalence_audit.md` — Park 2009 null-discrimination finding
- `05_horvath_subgroup_findings.md` — Horvath pattern subgroup analysis
- `06_cluster_replication_analysis.md` — **Pre-registered** 11-system cluster replication analysis (H1–H4; 76.06% unanimous Phase B baseline; ACR/K-TIRADS vs Kwak/C-TIRADS cluster structure)

---

## Related manuscripts

- **M025** — ACR TI-RADS Performance Study (single-system baseline)
- **M048** — TNM staging in multifocal thyroid carcinoma (nodule-level analysis overlap)
- **M075** — Multiple TI-RADS scored nodules: FNA selection (nodule-level design)

## Notable Finding cross-links

- `NF-2026-05-07-park2009-noncalibration` — Park 2009 null discrimination (AUC 0.54)
- `NF-2026-05-07-tirads-pipeline-version-state-mismatch` — Step 7 of Path A patch; evidence_summary includes the 76.06% Phase B unanimous rate and the ACR/K-TIRADS vs Kwak/C-TIRADS cluster pattern as a publishable observation

## Cluster replication analysis (pre-registered 2026-05-08)

The 76.06% 4-system unanimous-agreement rate and cluster structure from Phase A.3
(commit 3c727e6) are formally pre-registered as M085 Results § "Cross-system
concordance cluster structure" via `06_cluster_replication_analysis.md`.
Execution script: `scripts/429_tirads_cluster_replication.py` (runs post-Step 5).

Phase A.3 coverage context: aggregate-denominator gate (scorer-success-rate ≥ 98%
on feasible rows) substituted for the infeasible absolute ≥ 30,000 threshold
because echogenic-foci coverage was 24.8%, shape 58%, margins 60%.
