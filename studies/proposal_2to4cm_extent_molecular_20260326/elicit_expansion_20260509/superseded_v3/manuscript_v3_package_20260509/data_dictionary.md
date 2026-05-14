# Data dictionary — EXT2-4 Elicit-expansion (2026-05-09)

All BigQuery references use the canonical layer at `thyroid-canonical-pub-2026.pub_canonical.*`.
The primary analytic table is `manuscript_cohort_v1` (n=10,871 patients).

## Cohort definition (used throughout this expansion)
Inclusion: `surg_first_date IS NOT NULL` AND `EXTRACT(YEAR FROM surg_first_date) BETWEEN 1999 AND 2025` AND `surg_procedure_type IN ('total_thyroidectomy','hemithyroidectomy')`. → n = 8,368.

Note this is **broader** than the EXT2-4 v1 primary cohort (N=558), which additionally applied strict nodal exclusion, preop imaging size 2.0–4.0 cm only, and a different size-resolution rule from a legacy DuckDB pipeline. The two definitions are reconcilable; this expansion intentionally uses the broader BQ-canonical surgical denominator so era and Bethesda-stratified rates are comparable across the full cohort.

## Field-by-field provenance

| Local variable in this expansion | BQ source column | BQ source table | Type | Notes |
|---|---|---|---|---|
| `research_id` | `research_id` | `manuscript_cohort_v1` | INT64 | De-identified per HIPAA Safe Harbor. The only patient identifier exported anywhere in this expansion. |
| `age_at_surgery` | `age_at_surgery` | `manuscript_cohort_v1` | INT64 | Years at first qualifying surgery date. |
| `sex` | `sex` | `manuscript_cohort_v1` | STRING | "female" / "male"; NULL handled. |
| `bethesda` | `fna_bethesda_final` | `manuscript_cohort_v1` | INT64 | 1–6, derived per `bethesda_derivation_method` provenance in `canonical_fna_events_v1`. NULL if not resolvable from FNA text. |
| `mol_platform` | `mol_platform` | `manuscript_cohort_v1` | STRING | "Afirma" / "ThyroSeq" / "Other" / NULL. "Other" includes Quest Diagnostics in-house BRAF, MD Anderson panel, FoundationOne, etc. |
| `molecular_risk_tier` | `molecular_risk_tier` | `manuscript_cohort_v1` | STRING | "wild_type" / "low_intermediate" / "intermediate" / "high" / NULL. |
| `braf_positive_final` | `braf_positive_final` | `manuscript_cohort_v1` | BOOL | Final-call BRAF V600E positivity, integrated from path/molecular/note sources. |
| `ras_positive_final` | `ras_positive_final` | `manuscript_cohort_v1` | BOOL | NRAS, KRAS, HRAS pooled. |
| `tert_positive_final` | `tert_positive_final` | `manuscript_cohort_v1` | BOOL | TERT promoter mutation. |
| `preop_size_cm` | `imaging_nodule_size_cm` | `manuscript_cohort_v1` | FLOAT64 | Preoperative imaging-derived index nodule largest dimension (cm). |
| `path_tumor_size_cm` | `path_tumor_size_cm` | `manuscript_cohort_v1` | FLOAT64 | Final pathology tumor size (cm). |
| `imaging_tirads_best/worst` | same | `manuscript_cohort_v1` | INT64 | Best/worst ACR-TIRADS-derived category among preop nodule records. |
| `surg_procedure_type` | same | `manuscript_cohort_v1` | STRING | "total_thyroidectomy" / "hemithyroidectomy" / "other" / "unknown". |
| `surg_first_date` | same | `manuscript_cohort_v1` | TIMESTAMP | First qualifying surgery (lobe or total). |
| `histology_final` | same | `manuscript_cohort_v1` | STRING | Free-text final-pathology category. **Populated only for malignant histologies**; NULL implies benign-on-final-path among surgical patients. Normalized via case-insensitive LIKE rules in the Table 2 / Table 3 SQL. |
| `any_recurrence_flag` | same | `manuscript_cohort_v1` | BOOL | Any-evidence recurrence flag (path/imaging/biochemical). |
| `structural_recurrence_flag` | same | `manuscript_cohort_v1` | BOOL | Structural recurrence (imaging or path). |
| `recurrence_path_proven` | `recurrence_path_proven` | `canonical_recurrence_resolved_v1` | BOOL | **Used for Table 4** per user definition: biopsy- or operative-pathology-confirmed recurrence only. |
| era | derived | — | STRING | "pre_2015" if `EXTRACT(YEAR FROM surg_first_date) < 2015`, else "2015_plus". 2015 chosen as cutoff to align with widespread Afirma/ThyroSeq adoption + 2015 ATA guideline release. |
| size band | derived | — | STRING | "lt2cm" / "2to4cm" / "gt4cm" / "unknown" based on `preop_size_cm`. |

## Histology classification rules (Tables 2 and 3)
Applied to `LOWER(TRIM(histology_final))`:
- **malignant**: contains any of `ptc`, `papillary`, `mtc`, `medullary`, `follicular carcinoma`, `anaplastic`, `poorly differentiated` (incl. typos `pooly`, `differentied`), `differentiated high grade`, `differentiated thyroid carcinoma`, `nut carcinoma`, `adenoid cystic`, `angiosarcoma`, `high grade carcinoma`, `infiltrating carcinoma`, `metastatic <thyroid|follicular|anaplastic|mtc|carcinoma>`.
- **niftp**: contains `niftp`, `nifcp`, `nifp`, or `nifpt`.
- **borderline**: contains `ftump`, `hyalinizing trabecular tumor`.
- **benign_adenoma**: contains `adenoma` (excluding `adenoid`).
- **benign**: NULL `histology_final` among surgical patients (no malignant histology found at OR).

The strict-vs-inclusive distinction in Table 3 is whether NIFTP and borderline count as malignant outcomes. Strict (NIFTP=benign) is the post-2016 default; inclusive is provided for cross-study comparison.

## Molecular call rule (Table 3 — CORRECTED 2026-05-09)

The original Table 3 used a **derived** call from `manuscript_cohort_v1` (molecular_risk_tier + mutation flags). That table is preserved at `tables/superseded/table3_diagnostic_performance_thyroseq_vs_afirma_DERIVED_CALL.csv`. The corrected Table 3 (`tables/table3_v2_diagnostic_performance_actual_reported_call.csv`) uses the **actual platform-reported call** from `canonical_molecular_genetics_v2`:

| Platform | test-positive | test-negative | INTERMEDIATE (third category) | Not classifiable |
|---|---|---|---|---|
| Afirma | `overall_result_class IN ('suspicious','positive')` | `overall_result_class = 'negative'` | n/a (Afirma reports binary) | `overall_result_class IS NULL` or `'other'` or `'non_diagnostic'` |
| ThyroSeq | `rom_descriptor IN ('HIGH','INTERMEDIATE-HIGH','INTERMEDIATEHIGH')` OR `overall_result_class = 'positive'` | `rom_descriptor IN ('LOW','INTERMEDIATE-LOW')` OR `overall_result_class = 'negative'` | `rom_descriptor = 'INTERMEDIATE'` (excluded from 2×2; reported separately per Logan's direction) | otherwise |

When a patient has multiple molecular tests, the latest preoperative test is used (`resolved_test_date <= DATE(surg_first_date)`), falling back to the most recent test if no preop test exists. SQL: `sql/04b_table3_v2_actual_reported_call.sql`.

ROM% numeric values (`rom_percent_point`, `rom_percent_low`, `rom_percent_high`) are reported in `tables/table3_v2_rom_pct_descriptive_stats.csv`. Afirma rows show "n/a — Afirma reports binary call only" because the Afirma GSC does not emit a numeric ROM% on commercial reports in this dataset.

## What this expansion does NOT pull
- Operative procedure codes (`canonical_operative_events_v1` / `canonical_operative_procedure_codes_v1`) beyond the rolled-up `surg_procedure_type`. Completion thyroidectomy ascertainment is **not** re-derived; the existing EXT2-4 dual-definition (`table7_completion_thyroidectomy.csv`) remains authoritative.
- Complications (`canonical_complications_*`). Out of session scope.
- Survival (`canonical_survival_followup_v1`). Excluded per user direction.
- Imaging-only suspicious recurrence (`recurrence_imaging_suspicious`) — excluded per user direction (path-proven only).

## Verification audit
The aggregate counts hardcoded in `build_elicit_expansion.py` were captured by direct query of `manuscript_cohort_v1` on 2026-05-09. Re-running each query in `sql/` against the same table at a later date may yield slightly different counts if `manuscript_cohort_v1` is rebuilt. The corresponding row counts at capture time:

- `manuscript_cohort_v1` total rows: 10,871
- After surgical-extent + date filter: 8,368
- Bethesda III + Afirma|ThyroSeq with histology: 96 + 142 = 238
- Patients with malignant histology: 4,137 (cohort-wide); 3,093 within the surgical denominator

Any discrepancy >5% on rerun should trigger a Verification Check in Airtable (`tblQB1tvxYqELbNvZ` Verification Checks table) before this expansion's numbers are quoted in any new manuscript text.
