# Cursor Prompt: Molecular Platform Audit & Classification for M028/M033

**Agent:** Claude Opus 4.7 (Composer 2.0) — requires deep domain knowledge of molecular thyroid pathology and nuanced interpretation of test result semantics; Opus excels at this type of domain-expert reasoning  
**Estimated time:** 2–3 hours  
**Date:** 2026-05-04

## Context

We have a MotherDuck database `thyroid_canonical_publication_v1_0` with 10,871 thyroid surgery patients. Two manuscripts depend on accurate molecular testing classification:

- **M028** — Molecular testing utilization trends and outcomes (all 1,286 tested patients)
- **M033** — BRAF V600E impact on recurrence and management (BRAF-tested subset)

### Current Molecular Testing Data (N=1,286 tested)

| Platform | N | BRAF+ | RAS+ | Fusion+ |
|---|---|---|---|---|
| ThyroSeq | 618 | 97 | 167 | 362 |
| unknown | 316 | 95 | 89 | 1 |
| Afirma | 188 | 53 | 2 | 81 |
| ThyroSeq+Afirma | 163 | 131 | 34 | 136 |
| Quest | 1 | 0 | 0 | 0 |

### Molecular Risk Tier Distribution by Platform

| Platform | High | Intermediate | Low/Intermediate | Wild Type | NULL |
|---|---|---|---|---|---|
| ThyroSeq | 41 | 81 | 139 | 190 | 167 |
| Afirma | 1 | 52 | 2 | 76 | 57 |
| ThyroSeq+Afirma | 34 | 98 | 15 | 15 | 1 |
| unknown | 0 | 95 | 77 | 117 | 27 |

### Key Problems to Resolve

1. **316 patients with `unknown` platform** — need to determine if these are ThyroSeq, Afirma, single-gene BRAF PCR, IHC, or other tests based on available evidence
2. **Afirma subtypes not distinguished** — Afirma GSC (Genomic Sequencing Classifier) vs GEC (Gene Expression Classifier) are clinically different tests with different performance characteristics
3. **ThyroSeq versions not distinguished** — ThyroSeq v2 vs v3 have different gene panels and clinical validation
4. **163 patients with "ThyroSeq+Afirma"** — need to determine which was primary diagnostic test vs confirmatory

## Available Columns in `canonical_patient_master`

### Molecular Testing
- `molecular_tested_confirmed` — boolean (1,286 TRUE)
- `mol_platform` — ThyroSeq/Afirma/ThyroSeq+Afirma/unknown/Quest
- `mol_n_tests` — number of molecular tests
- `mol_test_date` — date of testing
- `mol_first_test_date` — earliest test date
- `mol_first_test_days_from_surg` — days from surgery to first test (negative = preop)
- `mol_test_date_source` — provenance
- `mol_genes_list` — comma-separated gene list
- `mol_variant_classes` — variant class annotations
- `mol_n_distinct_genes` — count of genes tested
- `mol_n_variants_total` — total variants found
- `mol_n_snvs` — single nucleotide variants
- `mol_n_fusions` — fusion count
- `mol_has_afirma` — boolean
- `mol_has_thyroseq` — boolean
- `mol_has_fusion` — boolean
- `mol_has_snv` — boolean
- `mol_has_dicer1` — boolean
- `mol_has_pik3ca` — boolean
- `mol_has_tshr` — boolean
- `mol_test_count` — integer
- `molecular_data_confidence` — quality tier
- `molecular_eligible_flag` — boolean
- `molecular_platforms_v7` — older version field
- `molecular_risk_tier` — high/intermediate/low_intermediate/wild_type
- `molecular_risk_calculable_flag` — boolean
- `n_molecular_tests_v7` — older count

### BRAF-Specific
- `braf_positive_final` — boolean (reconciled)
- `braf_positive` — boolean
- `braf_positive_v7` — older version
- `braf_variant` — specific variant (V600E, etc.)
- `braf_variant_raw` — raw variant text
- `braf_source` — provenance
- `braf_status_v7` — older status
- `braf_detection_method` — PCR/NGS/IHC/etc.
- `braf_detection_method_v11` — updated method
- `braf_recovered_status_v11` — recovery flag
- `braf_recovered_variant_v11` — recovered variant
- `ihc_braf_result_v13` — IHC BRAF result
- `ihc_braf_confidence_v13` — IHC confidence
- `ihc_braf_note_type_v13` — IHC note type

### RAS-Specific
- `ras_positive_final` — boolean
- `ras_positive` — boolean
- `ras_subtype` — NRAS/HRAS/KRAS
- `ras_primary_subtype_v11` — resolved subtype
- `ras_protein_change_v11` — specific mutation (Q61R, etc.)
- `ras_allele_freq_v11` — allele frequency
- `nras_positive_v11`, `hras_positive_v11`, `kras_positive_v11` — per-gene flags
- `ras_resolution_confidence_v13` — confidence tier
- `ras_resolution_source_v13` — source

### NSQIP Molecular (linked subset)
- `nsqip_molecular_testing` — NSQIP molecular testing field
- `nsqip_molecular_result` — NSQIP result

## Task

### 1. Platform Resolution for "unknown" (N=316)

Write a Python script that attempts to resolve `mol_platform = 'unknown'` using available evidence:

**Resolution logic (priority order):**
1. `braf_detection_method` / `braf_detection_method_v11` — if "IHC" → classify as "IHC_BRAF_only"; if "PCR" → "PCR_BRAF_only"; if "NGS" → likely ThyroSeq or panel
2. `ihc_braf_result_v13` is not null → "IHC_BRAF"
3. `mol_genes_list` — if contains multiple genes (>5) → likely panel (ThyroSeq/Afirma); if only BRAF → "single_gene_BRAF"
4. `mol_n_distinct_genes` — 1 gene = single-gene test; >7 genes = panel test
5. `mol_test_date` — Afirma GEC was discontinued ~2017, replaced by GSC; ThyroSeq v2 → v3 transition ~2017
6. `nsqip_molecular_testing` / `nsqip_molecular_result` — may have platform info
7. `molecular_platforms_v7` — older annotation may have more specific info

Output: `mol_platform_resolved` with values:
- `ThyroSeq_v2`, `ThyroSeq_v3`, `ThyroSeq_version_unknown`
- `Afirma_GEC`, `Afirma_GSC`, `Afirma_version_unknown`
- `PCR_BRAF_only`, `IHC_BRAF_only`, `single_gene_other`
- `multi_panel_unknown` (panel test but can't determine which)
- `truly_unknown` (insufficient evidence)

### 2. Afirma Subtype Resolution (N=188 + those resolved from unknown)

Distinguish GSC vs GEC:
- `mol_test_date` before 2017-06 → likely GEC
- `mol_test_date` after 2017-06 → likely GSC
- If `mol_genes_list` contains expression classifier genes → GEC; if genomic sequencing genes → GSC
- Cross-check with `mol_n_distinct_genes` (GEC reports fewer individual genes)

### 3. ThyroSeq Version Resolution (N=618 + those resolved from unknown)

Distinguish v2 vs v3:
- `mol_test_date` before 2018 → likely v2
- `mol_test_date` after 2018 → likely v3
- v3 tests more genes (112 vs 60) — use `mol_n_distinct_genes` as signal
- v3 includes RNA fusions; v2 was DNA-only

### 4. Dual-Platform Patients (N=163 "ThyroSeq+Afirma")

For each patient:
- Determine which test was first (preoperative diagnostic) vs second (confirmatory/additional)
- Flag the "primary" molecular result for analysis
- Note if results were concordant or discordant

### 5. BRAF Audit for M033

For all patients with any BRAF data:
- Reconcile `braf_positive_final` vs `braf_positive` vs `braf_positive_v7` vs `ihc_braf_result_v13`
- Flag discordances between detection methods (IHC vs molecular)
- Determine V600E vs other BRAF variants
- Create `braf_audit_tier`: Tier 1 (molecular + IHC concordant), Tier 2 (single source confirmed), Tier 3 (IHC only), Tier 4 (inferred/uncertain)

### 6. Output

Save to `studies/m028_m033_molecular_audit/`:
- `molecular_platform_resolved.csv` — patient-level with original + resolved platform, version, evidence used
- `braf_audit.csv` — BRAF reconciliation with confidence tiers
- `platform_resolution_summary.csv` — aggregate counts before/after resolution
- `dual_platform_analysis.csv` — ThyroSeq+Afirma patients with primary/secondary designation
- `molecular_data_quality_report.md` — narrative summary of findings and remaining gaps

### 7. Upload to MotherDuck

Create these tables in `manuscript_workspace`:
- `molecular_platform_resolved_v1` — full resolution with evidence columns
- `braf_audit_v1` — BRAF reconciliation with tiers
- `cohort_m028_molecular_utilization_v1` — M028-ready cohort with resolved platforms
- `cohort_m033_braf_outcomes_v1` — M033-ready cohort with BRAF audit tiers

## Connection

```python
import duckdb
conn = duckdb.connect('md:thyroid_canonical_publication_v1_0')
```

Use the MotherDuck token from `.env.motherduck` or environment variable `MOTHERDUCK_TOKEN`.

## Important Notes

- `research_id` is VARCHAR in canonical_patient_master
- Boolean columns: use `IS TRUE` / `IS NOT TRUE`, never compare with strings
- The "unknown" platform resolution is the highest-priority deliverable — M028 cannot proceed without it
- When resolution is ambiguous, assign a confidence tier and document the evidence
- Afirma GEC→GSC transition date varies by institution; use 2017-06 as default but flag edge cases (2017-01 to 2017-12)
- ThyroSeq v2→v3 transition was ~2017-2018; again institution-dependent
- `mol_has_fusion = TRUE` with `mol_platform = 'unknown'` is suspicious — fusions are typically only detected on panel tests, which should narrow the platform
- 362 ThyroSeq patients have fusions vs only 81 Afirma — this ratio can help with Bayesian platform inference for unknowns
