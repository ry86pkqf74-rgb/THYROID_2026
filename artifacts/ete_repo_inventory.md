# ETE Repo Inventory — Phase 1 (read-only)

**Git SHA at inventory:** `4795d42df5f7b63b1c94a69395a4db8300657a9d` (branch `ete-remediation-20260413`)

## 1. AJCC7 derivation — three implementations, two stale, one correct

| File | Function | Line | T3b maps to | Status |
|---|---|---|---|---|
| `studies/proposal2_ete_staging/proposal2_ete_analysis.py` | `derive_ajcc7_t_stage` | 90 (T3b rule @117) | **T4a** | **STALE (executable)** |
| `studies/proposal2_ete_staging/proposal2_expanded_cohort.py` | `derive_ajcc7` | 137 (T3b rule @149) | **T4a** | **STALE (executable)** |
| `studies/proposal2_ete_staging/audit_reproduce.py` | `derive_ajcc7_corrected` | 182 (T3b rule @205) | T3 | CANONICAL — CORRECT |
| `studies/proposal2_ete_staging/audit_reproduce.py` | `derive_ajcc7_original` | 260 (T3b rule @273) | T4a | REFERENCE ONLY (audit comparison) |

Audit report (`studies/proposal2_ete_staging/audit_report.md:14`) states: *"T3b→T4a mapping affected 346 patients. Corrected to T3b→T3: 346 T-stage and 46 overall-stage reclassifications."*

Manuscript packet (`manuscripts/ete_ajcc8_202603/MANUSCRIPT_REVISION_PACKET_20260326.md:109`) already asserts the corrected mapping: *"T3b mapped to T3 under AJCC 7th (not T4a)—346 patient-level T-stage revisions."* → the manuscript's declared numbers presuppose the corrected mapping, so unifying all executable paths to T3b→T3 is a **correctness fix, not a policy change**.

## 2. ETE export readers

All three CSVs read from `exports/*.csv` by:

- `studies/proposal2_ete_staging/proposal2_ete_analysis.py:53-54` (ptc, rec)
- `studies/proposal2_ete_staging/proposal2_expanded_cohort.py:71-73` (rec, img, ptc)
- `studies/proposal2_ete_staging/proposal2_endpoint_psm_strata.py:56-58` (rec, img, ptc)
- `studies/proposal2_ete_staging/proposal2_recommendations.py:57-58` (ptc, rec)
- `studies/proposal2_ete_staging/audit_reproduce.py:109-111` (rec, img, ptc)
- `manuscripts/ete_ajcc8_202603/revision_rerun_20260326/run_psm_reproduction.py:22-24`
- Zenodo archive copies under `exports/zenodo_archive_2026.03.10/…` (immutable archive, do not modify).

## 3. PSM implementation

`studies/proposal2_ete_staging/proposal2_endpoint_psm_strata.py::propensity_match` (L150-195).

**Design:**
- Cohort: `ete_group in {"No ETE", "Microscopic ETE"}`
- Treatment: `ete_micro` (0/1)
- Covariates: age_at_surgery, female, largest_tumor_cm, n_positive_flag
- PS estimator: `LogisticRegression(max_iter=1000, random_state=SEED)`
- Caliper: 0.05, greedy 1:1 nearest neighbor without replacement
- OR estimator: 2×2 cross-tab with Haldane-Anscombe 0.5 continuity correction
- Inference: Fisher exact on the matched crosstab
- SEED = 42 at module top, `np.random.seed(SEED)` applied once

**Identified rerun-instability sources:**

1. **Input order dependence** — `df` is built from CSV joins with no canonical sort before `dropna`; row-removal order can affect downstream iteration.
2. **Tie-breaking in `sort_values("propensity")`** — no secondary stable key; ties produce non-deterministic order across pandas/numpy versions.
3. **`dist.idxmin()`** — returns the first index matching the minimum; depends on control frame order, which depends on upstream join/sort.
4. **`available_controls` as a list** — `.remove(cidx)` is deterministic given the input, but the input is not stably sorted.
5. **Package-version drift** — `random_state=SEED` fixes LR solver randomness, but differences across sklearn versions (liblinear vs lbfgs default behavior, n_jobs) have been known to shift PS by ~1e-6, which compounds with greedy matching near the caliper boundary.
6. **dfs_years NaN-drop interaction** — `dropna(subset=covars + ["structural_recurrence", "dfs_years"])` silently drops different rows depending on which follow-up-date columns are populated in upstream exports.

**Observed PSM counts across the repo (conflict ledger per `generate_final_numeric_audit.py` L306-312):**

| Source | PSM matched pairs |
|---|---|
| Frozen structural PSM effect table | (from `audit_tables/table6_propensity_matching_effect.csv`) |
| Blind PSM reproduction rerun | (from `revision_rerun_20260326`) |
| Forensics JSON `cohort_sizes.psm_matched` | 1006 |
| `MANUSCRIPT_REVISION_PACKET_20260326.md` | 711 |
| Main submission DOCX | 711 |
| Supplementary DOCX (sensitivity) | 712 |

The manuscript's declared anchor is **711** — and the 711 vs 712 main vs supp is a ±1-pair rerun-noise artifact, exactly the instability we need to control.

## 4. Manuscript packet

- **Drafting base:** `manuscripts/ete_ajcc8_202603/MANUSCRIPT_REVISION_PACKET_20260326.md` (located, will use in Phase 6).
- Do **not** source numerics from `manuscript_v1.md` without a numeric-alignment proof.

## 5. MotherDuck canonical objects relevant to ETE

Discovered via `list_tables` on the RO share `thyroid_research_ro_v2` (376 objects). Likely-canonical sources if a refreshed rerun is authorized:

- `main.manuscript_cohort_v1` — highest-priority candidate for the frozen cohort.
- `main.analysis_cancer_cohort_v1` — PTC analytical cohort.
- `main.analysis_recurrence_subset_v1` — recurrence cohort.
- `main.extracted_ete_subgraded_v1` — ETE sub-grading (mETE vs gross vs absent).
- `main.imaging_patient_summary_v1`, `main.imaging_fna_linkage_mm_v1` — imaging correlation inputs.
- `main.patient_refined_master_clinical_v12` — clinical master (demographics, dates, surgery).

Further drill-down deferred to Phase 3, gated by export-source decision.

## 6. Governance script locations (Phase 7 targets)

- `scripts/90_manuscript_freeze_rebuild.py`
- `scripts/94_map_dedup_validator.py`
- `scripts/95_environment_promotion.py`
- `scripts/96_release_manifest.py`

All present and unmodified from base SHA.
