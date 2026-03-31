# Rerun Log

Generated: 2026-03-31T15:15:58

## Decision
- No study analysis script was re-run as part of this evidence-pack build.
- Reason: the final ETE package is already tied to a frozen audit bundle, and re-running the modeling scripts would overwrite manuscript-facing outputs or create fresh drift against the selected submission freeze.
- Existing safe deterministic evidence was reused instead: the frozen 2026-03-10 audit artifacts plus the explicit 2026-03-26 PSM sensitivity rerun already checked into the manuscript folder.

## Candidate deterministic steps reviewed
- studies/proposal2_ete_staging/proposal2_ete_analysis.py
- studies/proposal2_ete_staging/proposal2_expanded_cohort.py
- studies/proposal2_ete_staging/proposal2_endpoint_psm_strata.py
- studies/proposal2_ete_staging/proposal2_cox_regression.py
- studies/proposal2_ete_staging/audit_reproduce.py

## Output fingerprints retained instead of re-running
- studies/proposal2_ete_staging/analysis_metadata.yaml | mtime=2026-03-14T19:59:50 | sha256=36a8dfa2e4e30ad8
- studies/proposal2_ete_staging/audit_report.md | mtime=2026-03-14T19:59:50 | sha256=f2bf3e5e04c17979
- studies/proposal2_ete_staging/audit_tables/table3_ordinal_regression.csv | mtime=2026-03-14T19:59:50 | sha256=785877684baaa17d
- studies/proposal2_ete_staging/audit_tables/table6_propensity_matching_effect.csv | mtime=2026-03-14T19:59:50 | sha256=20146220320206fa
- studies/proposal2_ete_staging/audit_tables/table8_interaction_tests.csv | mtime=2026-03-14T19:59:50 | sha256=730924259085714f
- manuscripts/ete_ajcc8_202603/revision_rerun_20260326/table6_propensity_matching_effect_rerun.csv | mtime=2026-03-26T00:45:47 | sha256=18b54c1250df2663

## Implication
- Any regeneration-required item identified elsewhere in this audit should be treated as a deliberate follow-up task, not silently refreshed during final evidence-pack assembly.
