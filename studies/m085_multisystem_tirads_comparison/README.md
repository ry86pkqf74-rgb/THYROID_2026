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

---

## Related manuscripts

- **M025** — ACR TI-RADS Performance Study (single-system baseline)
- **M048** — TNM staging in multifocal thyroid carcinoma (nodule-level analysis overlap)
- **M075** — Multiple TI-RADS scored nodules: FNA selection (nodule-level design)

## Notable Finding cross-links

- `NF-2026-05-07-park2009-noncalibration` — Park 2009 null discrimination (AUC 0.54)
