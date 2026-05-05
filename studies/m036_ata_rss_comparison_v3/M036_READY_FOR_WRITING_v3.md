# M036 — ATA 2025 Initial Risk Stratification: Ready-for-Writing Brief (v3 / post-mig_313)

**Status:** v3 distribution clean. Manuscript writing can begin.
**Last refreshed:** 2026-05-05 (mig_314, cowork)
**Prior runs:** v1 → v2 (cursor 421e4d3, distribution distorted by M-stage corruption) → **v3 (current, post-mig_313)**

---

## Why v3 supersedes v1/v2

The v1 and v2 runs of M036 were both corrupted by the upstream `m_stage_ajcc8_resolved` defect (45.19% M1 in malignant cohort; PTC 44.23%, FC 57.82%, FA 100% — all clinically impossible). v2 fixed the margin column (R1 ≠ incomplete) but inherited the M-stage corruption, so 1,642 patients were spuriously classified `high:distant_metastasis`.

mig_313 corrected M-stage at the canonical layer (signed off 2026-05-05 02:24:20 by `cursor_composer_mig313`). M036 v3 was re-run against the corrected canonical surface; the `high:distant_metastasis` rule now fires for 86 patients (2.1% of malignant cohort), consistent with published Emory metastatic-presentation rates.

---

## Cohort

- **N malignant analyzable:** 4,019 (matches CPM `is_malignant`)
- **Histologies:** PTC, follicular carcinoma, MTC, anaplastic, PDTC, NIFTP, FTUMP, plus metastatic/recurrent variants
- **Time window:** 1999–2025
- **Source:** `manuscript_workspace.cohort_m036_ata_risk_comparison_v1`
- **Output table:** `manuscript_workspace.m036_ata_2025_rss_v2` (refreshed in place; table name retained for downstream compat)

## Distribution (v3 locked numbers)

| Category | n | % | Rule drivers |
|---|---:|---:|---|
| **High** | 1,445 | 35.9% | gross_ete_or_t4 (1,089), 5+ positive LN (159), distant_metastasis (86), extensive_vascular_invasion (57), high_risk_molecular (33), incomplete_R2 (15), LN deposit >3cm (6) |
| **Intermediate** | 2,120 | 52.7% | microscopic_ete (2,088), limited_nodal_metastases (19), minor_vascular_invasion (11), braf_v600e_alone (2) |
| **Low** | 27 | 0.7% | intrathyroidal_PTC ≤4cm N0 (12), minimally invasive FTC no VI (8), unifocal papillary microcarcinoma (7) |
| **Uncalculable** | 427 | 10.6% | non-DTC histology (414), insufficient anatomic risk data (13) |

## Reclassification vs ATA 2015

Cross-tab in `studies/m036_ata_rss_comparison_v3/reclassification_crosstab.csv`. Headline reclassification metrics, KM curves, and outcome validation are in companion CSV/TEX outputs.

## Headline interpretation

The 2025 ATA RSS reclassifies the Emory operative cohort substantially. The `microscopic_ete` rule alone places 2,088 patients (52.0% of the malignant cohort) in **intermediate** — a major shift away from the 2015 framework's tendency to upstage these patients into **high** via T3 staging. This is the single largest reclassification signal in the data.

Concurrently, the high category contracts from 2,353 (pre-fix) to 1,445 (−39%) — driven primarily by the M-stage correction (1,642 → 86) and the margin definition correction.

The low category remains very small (n=27; 0.7%) because this is a tertiary surgical referral cohort enriched for anatomic risk; this is expected, not a defect.

## Drivers of v3 vs brief prediction

The handoff brief predicted v3 would land at high ≈ 600–900 / low ≈ 200–500. Actual: high=1,445 / low=27. The deviation is explained by:

1. **Gross ETE / T4 prevalence underestimated.** The brief assumed M1 was the dominant high-risk driver. Once M1 corrected, gross_ete_or_t4 (1,089) and ≥5 positive LN (159) emerge as the dominant signals — both are real in this cohort.
2. **Low-risk pathway too narrow.** ATA 2025 low-risk requires intrathyroidal disease without ETE, which excludes 99%+ of operatively-treated tumors at a tertiary center.

Both observations are clinically sound; no further data fixes warranted.

## Outputs

```
studies/m036_ata_rss_comparison_v3/
├── ata_2025_km_curves.png
├── ata_2025_rss_classification.csv
├── ata_2025_rules_audit.csv
├── km_summary.csv
├── model_performance.{csv,tex}
├── outcome_validation.{csv,tex}
└── reclassification_crosstab.{csv,tex}
```

## Recommended manuscript structure

- **Headline finding 1:** ATA 2025 microscopic-ETE intermediate rule reclassifies 2,088 patients (52.0%) downward from 2015's high-risk designation; this is the largest single reclassification signal.
- **Headline finding 2:** The high-risk category contracts by 39% under ATA 2025, primarily by tightening the M-stage and margin criteria (true distant metastasis is a far smaller population than R1 margins or T3 microscopic ETE).
- **Headline finding 3:** Outcome validation (KM curves, recurrence rates by category) — see `outcome_validation.csv` to confirm the 2025 categories preserve discrimination relative to 2015.
- **Limitation:** 2015 vs 2025 reclassification is not a randomized comparison; downstream RAI, surveillance, and surgical decisions reflect 2015 logic at the time of treatment, so observed-outcome stratification is correlative.

## Open data-quality questions for writing

- Stage IV cases where the only driver was M1 are now correctly demoted; cross-check that no patients with documented metastases were demoted *too* (audit `high:distant_metastasis` n=86 vs CPM `ajcc8_m_stage='M1'` n=114 — small gap likely explained by non-DTC histology).
- The 2 `braf_v600e_alone → intermediate` patients merit a chart-review sanity check.

## Provenance & signoff

- Pipeline: `scripts/m036_ata_2025_rss.py` (no code change since v2; behavior change is entirely upstream M-stage correction).
- Migration: **mig_314** (cowork, 2026-05-05). Signoff summary records pre/post counts.
- Closes downstream cascade of `CF-MSTAGE-CORRUPTION`.
