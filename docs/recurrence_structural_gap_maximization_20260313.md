# Recurrence Structural Gap Maximization Audit — 20260313

## Summary

This document captures the recurrence date-quality distribution and
analytically-usable subset. The primary finding is that 88.8% of
recurrence cases have unresolved dates — a source limitation, not a
pipeline gap. Structural recurrence flags are derived from
`recurrence_risk_features_mv`; biochemical recurrence from Tg trajectory.

## Three Recurrence Concepts

| Concept | Definition | Manuscript Use |
|---------|------------|----------------|
| Structurally flagged | `recurrence_any = TRUE` in extracted_recurrence_refined_v1 | Prevalence reporting |
| Date-bearing | flagged AND `recurrence_date_status != 'unresolved_date'` | Time-to-event eligible |
| Analytically usable | date-bearing AND multi-source corroboration | Primary survival analysis |

## Date Quality Taxonomy

| Tier | Confidence | Description | Analysis Suitability |
|------|------------|-------------|---------------------|
| exact_source_date | 1.0 | Day-level date from structured source | Full time-to-event |
| biochemical_inflection_inferred | 0.5 | Tg inflection point as proxy | Coarse survival with caveat |
| unresolved_date | 0.0 | Flag only, no recoverable date | Flag-only; exclude from TTE |
| not_applicable | N/A | No recurrence flagged | N/A |

## Readiness Table

| Category | N | % | Date Tier | Usable |
|----------|---|---|-----------|--------|
| total_patients | 10,871 | 100.0% |  |  |
| recurrence_any_flagged | 1,986 | 18.3% |  |  |
| recurrence_structural_only | 1,818 | 16.7% |  |  |
| date_unresolved | 1,764 | 88.8% | unresolved_date | NO -- flag-only; cannot be used in time-to-event models |
| source_linked_multi | 1,072 | 54.0% |  | HIGH CONFIDENCE -- multi-source corroboration |
| tg_trajectory_available | 848 | 42.7% |  |  |
| recurrence_biochemical_only | 168 | 1.5% |  |  |
| date_biochem_inferred | 168 | 8.5% | biochemical_inflection_inferred | CONDITIONAL -- usable for coarse survival analysis with caveat |
| date_exact_source | 54 | 2.7% | exact_source_date | YES -- suitable for time-to-event analysis |
| site_identified | 0 | 0.0% |  |  |

## Source Limitations

The 88.8% unresolved-date rate reflects that most recurrence data comes from
`recurrence_risk_features_mv.recurrence_flag` which is a boolean flag without
a day-level date. The underlying `extracted_clinical_events_v3` NLP extraction
tagged events as 'recurrence' but frequently from H&P notes that describe
history rather than incident events. Without a structured recurrence registry
(e.g., cancer registry follow-up data), day-level dates cannot be recovered.

## Validation Tables Created

- `val_recurrence_readiness_v1` — manuscript readiness summary (10 rows)

Generated: 2026-03-13T14:11:41.736148