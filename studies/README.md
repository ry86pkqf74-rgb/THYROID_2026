# Studies

Per-proposal analysis folders. Each subfolder contains scripts, figures,
tables, and reports for a specific research question.

## Proposal 2 — ETE & Staging (`proposal2_ete_staging/`)

Evaluates the impact of extrathyroidal extension (ETE) on AJCC 8th edition
staging accuracy and recurrence risk using propensity score matching (PSM).

**Recurrence endpoint:** structural recurrence (imaging-confirmed or
biopsy-proven disease) after initial therapy.

**Key materialized views used:**
- `risk_enriched_mv` — combined risk features with ETE, BRAF, staging
- `survival_cohort_ready_mv` — time-to-event cohort for KM/Cox analysis
- `advanced_features_v3` — full 60+ engineered feature set

**Outputs:** 9 figures (PNG + PDF), 7+ tables (CSV), analysis report,
audit report with reproducibility scripts.

## NSQIP Linkage (`nsqip_linkage/`)

Linkage of NSQIP surgical outcomes data to the thyroid cohort via
`IDN = EUH_MRN`. Validation report and case-level linkage results.

## NSQIP PTH Protocol Manuscript (`nsqip_pth_protocol_manuscript/`)

Manuscript content for the parathyroid hormone protocol study using
linked NSQIP data. Includes statistical methods, limitations, and
linkage methodology documentation.

## QA Cross-Check (`qa_crosscheck/`)

Cross-file validation outputs (laterality mismatches, report matching,
missing demographics) and summary report.
