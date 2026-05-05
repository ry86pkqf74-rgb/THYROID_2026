# M025 v2.0 Methods — draft prose for the manuscript

**Author:** Cowork (Claude). Source-of-truth statements; the cursor agent should adapt voice/length to journal.

## Cohort assembly

Patients in the institutional 25-year operative thyroid cohort (n=10,871 unique research IDs in `canonical_patient_master`) were screened for any record in the canonical ultrasound nodule table (`canonical_us_nodule_v2`), yielding **6,523 patients** with at least one US-detected thyroid nodule (37,438 individual nodule rows after excluding gland-aggregate rows). For per-nodule TI-RADS performance, nodules were further restricted to those with a strict ACR TI-RADS 2017 categorization derived from complete five-feature ACR 2017 scoring (composition, echogenicity, shape, margins, calcifications/echogenic foci), with known laterality, with no size-outlier quarantine flag, and with no unresolved multi-nodule attribution flag. This yielded the **publication-grade analytic cohort of 3,687 nodules**.

## Per-nodule TI-RADS predictor

The primary predictor was `acr2017_tirads_category` (TR1–TR5), computed from `acr2017_tirads_points` per the Tessler 2017 ACR TI-RADS algorithm: TR1=0 points, TR2=2, TR3=3, TR4=4–6, TR5≥7. An institutional updated TI-RADS tier (`updated_tirads_category`) was retained for sensitivity analysis (concordance flag carried in the analytic spine). Ambiguous-band rows (`acr2017_band_ambiguous=TRUE`) are flagged but retained in the primary cohort because manual chart review confirmed their assignments.

## Per-nodule reference standard (gold)

The outcome `nodule_path_proven_malignant` was assigned TRUE if a same-side malignant tumor existed in `canonical_path_malignant_events_v1` with surgery date in the interval [exam_date, exam_date + 365 days]. Same-side matching used normalized laterality (`left`, `right`, `isthmus`); bilateral path tumors were associated with any unilateral US nodule on either side, which is conservative (sensitivity arm D excludes bilateral matches; see Supplementary Table S1). Patients with no surgery within 365 days post-US were treated as outcome-negative for that nodule, which is consistent with conventional operative-cohort TI-RADS validation methodology.

## FNA Bethesda linkage

Per-nodule FNA Bethesda 2023 was attached via the legacy nodule-FNA linkage table `imaging_fna_linkage_v3` (Script 237). Because that table predates the canonical_us_nodule_v2 keying scheme, the bridge was reconstructed at the v2 level using (research_id, normalized laterality, |US date − FNA date| ≤ 30 days). Best link per nodule was selected by smallest day_gap then highest legacy `linkage_score`. The bridged FNA episode was joined to `canonical_fna_events_v1` via the per-patient `fna_index` (the linkage's `fna_episode_id` is a per-patient integer, not the global MD5 `fna_event_id`). Of the 3,687 strict-ACR analytic-eligible nodules, 2,216 (60%) had a bridged Bethesda value. Open carry-forward CF-FNA-SIZE-CM-NULL: per-nodule FNA size is NULL by design in linkage v1.0; size-aware concordance scoring is deferred to v1.1 NLP extraction (`note_entities_llm_us_nodule_dynamics` / `note_entities_llm_tirads_granular`).

## Statistical analysis

Per-TR risk-of-malignancy (ROM) was computed as the proportion of nodules with `nodule_path_proven_malignant = TRUE` within each ACR 2017 category, with 95% Wilson confidence intervals. Diagnostic performance was evaluated at three pre-specified thresholds (TR≥TR3, TR≥TR4 [primary clinical action threshold per ACR 2017], TR≥TR5) with 2×2 derived sensitivity, specificity, PPV, NPV, and likelihood ratios with Wilson 95% CIs. Discrimination was summarized by the area under the ROC curve (AUC) computed as the rank-based Mann–Whitney equivalent over comparable malignant–benign nodule pairs (1,928,336 pairs in the primary cohort).

## Patient-level vs nodule-level comparison

For direct comparison to the predecessor patient-level analysis (M025 v1.0, n=3,375 patients), per-TR ROM was also computed at patient grain using `tirads_resolved` (post-mig_288 canonical column derived from the cohort view's worst-category COALESCE preop-category) as the test variable and `canonical_patient_master.is_malignant` as the outcome. Inflation in percentage points was computed as `patient_rom − nodule_rom` per TR. Both grain levels share the same source data and operative cohort.

## Subgroup / sensitivity analyses (Supplementary Table S1)

Four pre-specified sensitivity arms:
- **Arm A (relaxed):** drops the ACR feature-points-complete requirement, increasing n to 15,309 nodules but introducing nodules where ACR features were partially absent.
- **Arm B (first-US-only):** restricted to the chronologically earliest US per nodule_master_id. ROM identical to primary because nodule_master_id already deduplicates across exams.
- **Arm C (single-nodule patients):** restricted to patients with only one nodule. Per-TR ROM is *higher* than primary at TR4/TR5, consistent with selection effects for single-nodule patients reaching surgery.
- **Arm D (unilateral-path-only):** counts only matched unilateral path tumors as malignant. Per-TR ROM is *lower* than primary, likely an underestimate because true bilateral malignancy is excluded from numerator.

## Software / reproducibility

All analytic SQL ran against MotherDuck database `thyroid_canonical_publication_v1_0` (publication-frozen at `pub_v1_1` tag, 2026-05-04). The per-nodule view was built by mig_306 (`qc_framework_v1/migrations/306_nodule_level_spine_20260504.sql`). Exact reproduction queries are in `08_analysis_code/M025_v2_tirads_analysis.sql`. Wilson CIs were computed in DuckDB SQL (no external library dependency); AUC used the closed-form rank Mann–Whitney equivalent. Cross-validation against the Snowflake mirror (`THYROID_VALIDATION.PUBLIC.COHORT_M025_TIRADS_PERFORMANCE_V1_FLAT` for patient-level; nodule-level mirror to be created per the mig_289 pattern) was performed via `CALL VALIDATE_ALL_COHORTS()` on the Snowflake side and confirmed identical patient-level numbers (17/17 PASS as of 2026-05-04).

## IRB / data use

All analyses performed on de-identified data under [TBD IRB protocol number]. Research IDs are pseudonymized; no PHI columns appear in any analytic spine or output.
