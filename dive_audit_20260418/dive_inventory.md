# THYROID_2026 Dive Inventory — 2026-04-17

**Audit scope**: All Dives owned by `logan_glosser_eras` that belong to the THYROID_2026 publication project.
**Canonical database**: `thyroid_canonical_publication_v1_0`
**Total Dives**: 31 (19 M-series, 12 T-series)
**Inventory method**: `mcp__motherduck__list_dives(limit=500)`; JSX content read per Dive.

---

## Access mode

Two distinct access patterns are in use:

- **M-series (19 Dives)**: `REQUIRED_DATABASES = [{ type: 'database', path: 'md:thyroid_canonical_publication_v1_0', alias: 'thyroid_canonical_publication_v1_0' }]` — read direct from the canonical DB.
- **T-series (12 Dives)**: `REQUIRED_DATABASES = [{ type: 'share', path: 'md:_share/thyroid_publication_v1_0_readonly/2ac4370a-87cd-4dfe-b68e-140de43a7ad1', alias: 'thyroid_canonical_publication_v1_0' }]` — read through the `thyroid_publication_v1_0_readonly` share (aliased to the same name so fully-qualified SQL works unchanged).

Both patterns resolve to the canonical DB. **No Dive references `Thyroid 2026 UPdated`, `Thyroid 2026`, or any archive schema directly.**

---

## M-series (manuscript-specific cohort apps)

| Dive | ID | Bound view (`manuscript_workspace.*`) | Updated |
|---|---|---|---|
| M025 — ACR TI-RADS Performance | `ef4e5075-dacb-4eee-9d4b-d3d118158d72` | `cohort_m025_tirads_performance_v1` | 2026-04-16 11:49 UTC |
| M028 — Bethesda III/IV Analysis | `8bb7fd9a-ed33-45cd-b791-bb07fe7eb47a` | `cohort_m028_bethesda_iii_iv_v1` | 2026-04-16 11:49 UTC |
| M029 — FNA Concordance | `68fd3fe0-c310-4c86-99fd-8af2074164ae` | `cohort_m029_fna_concordance_v1` | 2026-04-16 11:49 UTC |
| M030 — Genetic Predictive Modeling | `6483ff38-f28d-4322-8f38-23c00ba1f89d` | `cohort_m030_genetic_predictive_v1` | 2026-04-16 11:49 UTC |
| M031 — Nuclear Medicine Scans | `f1ef6c26-218a-4905-bbee-17a8ae8a095c` | `cohort_m031_nuclear_medicine_v1` | 2026-04-16 11:50 UTC |
| M032 — 25-Year Descriptive | `937983bf-398e-45c4-87a1-8adc7ff6021f` | `cohort_m032_descriptive_25yr_v1` | 2026-04-16 11:50 UTC |
| M033 — Afirma/ThyroSeq Outcomes | `8b9a6682-8b7c-4650-9e29-742935b06132` | `cohort_m033_afirma_thyroseq_v1` | 2026-04-16 11:50 UTC |
| M035 — Bethesda V Analysis | `568b6b74-2304-4fcf-91ef-4c7fce9612d4` | `cohort_m035_bethesda_v_v1` | 2026-04-16 11:50 UTC |
| M036 — ATA Risk Stratification | `53ed4daa-0ebc-41bd-a7c3-381b12de6c40` | `cohort_m036_ata_risk_comparison_v1` | 2026-04-16 11:51 UTC |
| M037 — LN Metastasis Patterns | `7b89a2c4-5601-4eab-9844-cdcf8d13a490` | `cohort_m037_ln_metastasis_v1` | 2026-04-16 11:51 UTC |
| M038 — Massive Goiter Surgery | `f4d25858-e13c-4197-a2ce-08e6cb444314` | `cohort_m038_massive_goiter_v1` | 2026-04-16 11:51 UTC |
| M039 — PTH/Calcium Protocol | `7ae99f58-ac5e-4932-8ca5-d87cf9b6e0f3` | `cohort_m039_pth_calcium_v1` | 2026-04-16 11:51 UTC |
| M040 — Reoperative Thyroid Surgery | `f8f69c3f-4b4f-49f8-90b7-02316c064c26` | `cohort_m040_reoperative_v1` | 2026-04-16 11:52 UTC |
| M042 — Incidental Parathyroid | `e5cb8a45-462f-4dec-b9c4-f152d1e07bb7` | `cohort_m042_incidental_parathyroid_v1` | 2026-04-16 11:52 UTC |
| M043 — LN Metastasis Predictors | `a27b6a9a-68e8-4716-bdaa-abfb9efbbe61` | `cohort_m043_ln_predictors_v1` | 2026-04-16 11:52 UTC |
| M044 — AJCC Staging ETE Impact | `58d5ff4d-d3df-4dec-849f-b01874b4aefe` | `cohort_m044_ajcc_ete_v1` | 2026-04-16 11:52 UTC |
| M045 — Multimodal Risk Stratification | `af1677e3-29d1-49da-a43e-17288f048490` | `cohort_m045_multimodal_risk_v1` | 2026-04-16 11:53 UTC |
| M046 — NIFTP-Era Bethesda ROM | `291874fa-fb75-47fe-aa2a-8e082f2fef1b` | `cohort_m046_niftp_era_bethesda_v1` | 2026-04-16 11:53 UTC |
| M047 — Frozen Section Concordance | `3c4c4ee7-add1-4010-87fd-1849b4e8cba5` | `cohort_m047_frozen_section_v1` | 2026-04-16 11:53 UTC |

## T-series (theme-level dashboards over full cohort)

| Dive | ID | Bound view (`manuscript_workspace.*`) | Updated |
|---|---|---|---|
| T1 — Whole-Cohort Pathology Descriptives | `89588c45-03d9-4c0c-b67a-a75fe7b5ed61` | `cohort_descriptive_full_cohort_v1` | 2026-04-16 12:20 UTC |
| T2 — Frozen Section Series | `ec2fed70-7e65-469b-ae9a-c3f3c9b08abb` | `cohort_descriptive_full_cohort_v1` | 2026-04-16 12:20 UTC |
| T3 — Graves / Hashimoto / Thyroiditis | `f82a9a72-e922-41c9-99d2-35ae5d287cf7` | `cohort_descriptive_full_cohort_v1` | 2026-04-16 12:21 UTC |
| T4 — Molecular Testing Applications | `8f7459cf-ab2e-4216-8ae3-803ce0107b72` | `cohort_descriptive_full_cohort_v1` | 2026-04-16 12:21 UTC |
| T5 — Post-op Surveillance & Tg Kinetics | `031f5d51-a269-43b5-ae66-42b179adf121` | `cohort_descriptive_full_cohort_v1` | 2026-04-16 12:22 UTC |
| T6 — RAI Treatment Outcomes | `3b81b143-d529-44d9-b424-f1da8774c8ef` | `cohort_descriptive_full_cohort_v1` | 2026-04-16 12:22 UTC |
| T7 — Parathyroid Intraop & Pathology | `5b2ff9b1-6629-4b10-8c56-4b805c0e4f99` | `cohort_descriptive_full_cohort_v1` | 2026-04-16 12:23 UTC |
| T8 — TIRADS Decision Support | `61e0a279-82f2-4ae8-9653-cb650efc7dfc` | `cohort_descriptive_full_cohort_v1` | 2026-04-16 12:23 UTC |
| T9 — Risk Stratification & Reclassification | `16743a51-81b4-4988-b624-d9998d20b709` | `cohort_descriptive_full_cohort_v1` | 2026-04-16 12:23 UTC |
| T10 — Age & Epidemiology | `61ae43c0-e5ce-43b2-b8c9-cc943f6b96fe` | `cohort_descriptive_full_cohort_v1` | 2026-04-16 12:24 UTC |
| T11 — Indeterminate Nodule Outcomes | `59fb81f3-9ac2-413d-8975-a056d7d8d900` | `cohort_m001_indeterminate_genetics_v1` | 2026-04-16 12:24 UTC |
| T12 — Hereditary & Immunologic | `c0404775-ebd8-44bf-89fd-3b814986cb41` | `cohort_descriptive_full_cohort_v1` | 2026-04-16 12:24 UTC |

---

## Observations

1. Every Dive is v1 (current_version=1) — the full fleet is fresh and hasn't been hand-edited since creation.
2. All 12 T-series Dives were created in a single batch (12:20–12:25 UTC on 2026-04-16) and all share the `thyroid_publication_v1_0_readonly` share alias.
3. All 19 M-series Dives were created in a single batch (11:49–11:54 UTC on 2026-04-16) and all use the direct DB alias.
4. **Description ↔ SQL mismatch on 8 of 12 T-Dives** (T4, T5, T6, T7, T8, T9, T10, T12): descriptions claim they source from dedicated `cohort_m0NN_*` views but the SQL actually queries `cohort_descriptive_full_cohort_v1`. See `dive_triage_report.md` under "Documentation Debt" for details. SQL is healthy; descriptions are misleading.
