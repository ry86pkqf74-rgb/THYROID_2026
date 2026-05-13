# SUPERSEDED — derived-call Table 3

This folder contains the original Table 3 from the 2026-05-09 EXT2-4 Elicit
expansion. It was built from a **derived** positive/negative call (`molecular_risk_tier`
plus BRAF/RAS/TERT mutation flags from `manuscript_cohort_v1`) rather than the
**actual platform-reported test call** from `canonical_molecular_genetics_v2`.

Logan flagged this on 2026-05-09. The corrected version using the actual reported
call (`overall_result_class` for Afirma; `rom_descriptor` + `rom_percent_point` for
ThyroSeq, with INTERMEDIATE-only as a third category per Logan's direction) is at:

  ../table3_v2_diagnostic_performance_actual_reported_call.csv
  ../table3_v2_rom_pct_descriptive_stats.csv

**Do NOT cite the file in this folder for diagnostic-performance claims.** It is
preserved here per the project's append-only / never-delete rule. The corresponding
Manuscript Feedback Log row is `MFL-20260509-EXT2-4-TABLE3-CORRECTION`
(`rec2RAsAFehw1zEHV`).

Caveats that drove the supersession:
- Afirma "positive" in the derived call = (BRAF or RAS or TERT positive) OR
  (molecular_risk_tier ∈ {high, intermediate, low_intermediate}). This conflates
  the GSC binary Suspicious vs Benign call with downstream Xpression-Atlas
  mutation findings, which are reported alongside but separately from the GSC
  classifier on commercial Afirma reports.
- ThyroSeq "positive" in the derived call used the same rule, conflating the
  ROM-band classification with mutation positivity, and silently pooled
  INTERMEDIATE with positive.

The corrected Table 3 separates these signals.
