# M028/M033 Molecular Platform Audit — Data Quality Report

Generated: 2026-05-04T22:38:51
Source: `thyroid_canonical_publication_v1_0.canonical_patient_master` (10,871 patients) +
`canonical_molecular_genetics_v2` (1,384 episode rows).

## 1. Cohort

- Patients with `molecular_tested_confirmed = TRUE`: **1286**
- Patients with `mol_platform = 'unknown'` going in: **316**
- Patients still classified `truly_unknown` after resolution: **168**
  (148 of 316 resolved, 46.8%)

## 2. Original `mol_platform` distribution

- `ThyroSeq`: 618
- `unknown`: 316
- `Afirma`: 188
- `ThyroSeq+Afirma`: 163
- `Quest`: 1

## 3. Resolved `mol_platform_family` distribution

- `ThyroSeq`: 632
- `Afirma`: 197
- `unknown`: 168
- `ThyroSeq+Afirma`: 167
- `multi_panel_unknown`: 66
- `single_gene`: 55
- `Quest`: 1

## 4. Resolved `mol_platform_resolved` distribution

- `ThyroSeq_v3`: 533
- `truly_unknown`: 168
- `ThyroSeq_v3+Afirma_GSC`: 115
- `ThyroSeq_version_unknown`: 82
- `Afirma_GEC`: 72
- `Afirma_version_unknown`: 72
- `multi_panel_unknown`: 66
- `single_gene_BRAF`: 55
- `Afirma_GSC`: 53
- `ThyroSeq_version_unknown+Afirma_version_unknown`: 18
- `ThyroSeq_v3+Afirma_version_unknown`: 18
- `ThyroSeq_v2`: 17
- `ThyroSeq_version_unknown+Afirma_GSC`: 9
- `ThyroSeq_v2+Afirma_GEC`: 3
- `ThyroSeq_version_unknown+Afirma_GEC`: 2
- `ThyroSeq_v3+Afirma_GEC`: 2
- `Quest_unspecified`: 1

## 5. Confidence distribution

- `low`: 512
- `ambiguous`: 353
- `high`: 284
- `medium`: 137

## 6. Afirma subtype breakdown (N=197)

- `Afirma_GEC`: 72
- `Afirma_version_unknown`: 72
- `Afirma_GSC`: 53

Approximate institutional GEC→GSC transition: **2017-06-01**
(ambiguous window 2017-01-01 – 2017-12-31).
Patients dated inside that window with no text marker are labelled
`Afirma_version_unknown`.

## 7. ThyroSeq version breakdown (N=632)

- `ThyroSeq_v3`: 533
- `ThyroSeq_version_unknown`: 82
- `ThyroSeq_v2`: 17

ThyroSeq v3 launch approximated as **2018-01-01** with
an ambiguous window of 2017-06-01 –
2018-12-31. v3 reports >=80 distinct genes and includes
RNA fusions; v2 is DNA-only.

## 8. Dual-platform (ThyroSeq + Afirma) — N=167

- `rows in dual-platform analysis`: 167
- `discordant_braf`: 99
- `concordant`: 59
- `secondary_episode_missing_in_cmg_v2`: 7
- `discordant_ras`: 2

## 9. BRAF audit summary

- `rows`: 378
- `audit_positive`: 377
- `discordant_records`: 1
- `tier_counts`: {'tier_2_single_source_confirmed': 376, 'tier_3_ihc_only': 1, 'tier_5_negative_only': 1}
- `method_counts`: {'NGS': 312, 'NLP': 64, 'IHC': 2}

Tiers:
- `tier_1_molecular_ihc_concordant` — molecular and IHC both positive (highest confidence)
- `tier_2_single_source_confirmed` — molecular positive, no IHC done
- `tier_3_ihc_only` — IHC positive, no supporting molecular
- `tier_4_discordant` — molecular positive but IHC negative (or vice versa)
- `tier_4_inferred_or_uncertain` — heuristic / NLP-only signal
- `tier_5_negative_only` — IHC negative with no molecular positivity

## 10. Remaining gaps

- 168 patients remain `truly_unknown`. These typically have no
  episode in `canonical_molecular_genetics_v2`, no BRAF detection method, no
  IHC, no gene list, and no test date. Most are NLP-extracted molecular
  mentions without a structured panel record. They should be excluded from
  M028 utilization analyses or treated as a separate sensitivity stratum.
- Afirma_version_unknown / ThyroSeq_version_unknown rows fall in the
  GEC→GSC or v2→v3 transition window with no text markers. Manuscripts
  should report an institutional default (e.g. assume GSC after 2017-12 and
  v3 after 2018-01) and quantify sensitivity against the alternate label.
- 220 of the 316 original `unknown` patients
  have no row in `canonical_molecular_genetics_v2`. For these, resolution
  relies entirely on CPM-internal flags (`mol_has_thyroseq`,
  `mol_has_afirma`, `braf_detection_method_v11`, `mol_first_test_date`).

## 11. M028 / M033 cohort outputs (manuscript_workspace)

- `manuscript_workspace.molecular_platform_resolved_v1`
- `manuscript_workspace.braf_audit_v1`
- `manuscript_workspace.cohort_m028_molecular_utilization_v1`
- `manuscript_workspace.cohort_m033_braf_outcomes_v1`
