# Manuscript Workspace Handoff — v1.0

**Database:** `thyroid_canonical_publication_v1_0`
**Schema:** `manuscript_workspace`
**Source table:** `main.canonical_patient_master` (N=10,871)
**Date:** 2026-04-16
**Built by:** Claude (Sprint A + Sprint B)

---

## Schema Inventory

| Object type | Count | Naming convention |
|---|---|---|
| Consolidated full-cohort view | 1 | `cohort_descriptive_full_cohort_v1` |
| Per-manuscript thin wrappers (full-cohort) | 24 | `cohort_m0XX_<slug>_v1` |
| Dedicated specific-cohort views | 39 | `cohort_m0XX_<slug>_v1` |
| Lookup table | 1 | `manuscript_dive_map_v1` |
| Feasibility table | 1 | `manuscript_feasibility_v1` |
| **Total objects** | **66** | |

## View Architecture

### Consolidated full-cohort view

`cohort_descriptive_full_cohort_v1` selects ~130 columns from `canonical_patient_master` covering demographics, surgery, pathology, staging, LN, ETE/margins/invasion, synoptic pathology, gland size, FNA/Bethesda, TIRADS, molecular, parathyroid, RAI, Tg kinetics, labs, recurrence, survival, complications, PMH/hereditary, frozen section NLP, operative detail, and scoring flags. All 10,871 patients. No WHERE clause.

24 thin wrapper views (M48-M66, M68-M71, M76) SELECT manuscript-specific column subsets from this consolidated view with no additional filtering. This avoids 24 copies of the same base query.

### Dedicated specific-cohort views

39 views (19 from Sprint A, 20 from Sprint B) apply WHERE clauses to `canonical_patient_master` and select only manuscript-relevant columns. Examples:

- `cohort_m001_indeterminate_genetics_v1` — WHERE bethesda_final IN (3, 4)
- `cohort_m019_rai_outcomes_v1` — WHERE rai_received_flag = true
- `cohort_m067_tsh_tg_tumorigenesis_v1` — WHERE tg_n_measurements > 0

## Manuscript Dive Map

`manuscript_dive_map_v1` maps every manuscript to its cohort view and Dive:

| Column | Type | Description |
|---|---|---|
| manuscript_id | INT | Manuscript number (1-82) |
| manuscript_title | VARCHAR | Short title from feasibility table |
| cohort_view_name | VARCHAR | View name in manuscript_workspace |
| dive_id | UUID | MotherDuck Dive ID |
| dive_title | VARCHAR | Dive display title |
| dive_type | VARCHAR | 'dedicated' or 'thematic' |
| canonical_version | VARCHAR | 'v1_0' (default) |
| notes | VARCHAR | Sprint/theme label |

Coverage: 63 manuscripts, 31 Dives (19 dedicated, 12 thematic).

## Dive Inventory

### Sprint A — 19 Dedicated Dives (M25-M47)

Each manuscript has its own Dive with manuscript-specific panels. Named `M0XX v1_0 — <Title>`.

### Sprint B — 12 Thematic Dives (44 manuscripts)

| Dive | ID prefix | Manuscripts | Theme |
|---|---|---|---|
| T1 | 89588c45 | M48-M54, M58-M60 | Whole-Cohort Pathology Descriptives |
| T2 | ec2fed70 | M62-M65 | Frozen Section Series |
| T3 | f82a9a72 | M4, M16, M61, M69, M78 | Graves/Hashimoto/Thyroiditis |
| T4 | 8f7459cf | M6, M18, M23, M68, M72, M80 | Molecular Testing Applications |
| T5 | 031f5d51 | M67, M73, M76 | Post-op Surveillance & Tg Kinetics |
| T6 | 3b81b143 | M19, M55, M81 | RAI Treatment Outcomes |
| T7 | 5b2ff9b1 | M9, M17, M66, M79, M82 | Parathyroid Intraop & Pathology |
| T8 | 61e0a279 | M11, M75 | TIRADS Decision Support |
| T9 | 16743a51 | M7, M57 | Risk Stratification & Reclassification |
| T10 | 61ae43c0 | M56 | Age & Epidemiology |
| T11 | 59fb81f3 | M1 | Indeterminate Nodule Outcomes |
| T12 | c0404775 | M70, M71 | Hereditary & Immunologic |

## v1.1 Upgrade Flags

- T5 (M67, M73, M76): Improved Tg date/measurement coverage expected
- T7 (M17, M79): PTH/Calcium longitudinal data upgrade expected

## Guardrails

- All views read from `thyroid_canonical_publication_v1_0.main.canonical_patient_master` only
- No cross-database references
- PHI safety: `research_id` is the only patient identifier exposed
- All objects use `_v1` suffix for version tracking
- Sprint A artifacts (19 dedicated Dives + 19 cohort views) were preserved untouched during Sprint B
