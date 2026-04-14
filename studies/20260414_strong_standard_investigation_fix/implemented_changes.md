# Implemented Changes — 2026-04-14

## Summary
No code or data changes were implemented. The investigation confirmed that all remaining gaps are source-limited and no deterministic fixes exist. The work product is a comprehensive audit and documentation package.

## Files Created

### Audit CSVs
| File | Description | Rows |
|------|-------------|------|
| us_nodule_coverage_audit_strict.csv | Per-corpus TI-RADS coverage | 3 |
| us_nodule_coverage_audit_policy_aligned.csv | Policy-aligned gaps (all 0) | 3 |
| tirads_completeness_audit_all_corpora.csv | TI-RADS by corpus with source sufficiency | 3 |
| nodule_linkage_audit_expanded.csv | Linkage state × reason code | 7 |
| pathology_linkage_audit.csv | Pathology linkage via FNA state | 1 |
| us_lymph_node_audit_expanded.csv | LN assessment categories | 3 |
| fna_bethesda_audit_expanded.csv | Full episode Bethesda audit | 8,119 |
| unresolved_gaps.csv | All remaining gaps with classification | 6 |
| human_review_packet.csv | Items for clinician review | 23 |

### Documentation
| File | Description |
|------|-------------|
| executive_verdict.md | Final two-standard verdict |
| repo_update_review.md | Recent commit analysis |
| git_and_env_snapshot.md | Environment state |
| fresh_state.md | Script 144 MotherDuck refresh |
| fix_plan.md | Investigation results and plan |
| source_inventory_summary.md | Complete source corpus map |
| db_inventory_summary.md | Database table inventory |
| before_after_metrics.md | Before/after comparison |
| before_after_metrics.json | Machine-readable metrics |
| clinician_review_packet.md | Items for clinical review |
| delta_vs_20260413.md | Changes since prior audit |
| commands_run.log | All commands executed |

## Script Runs (Read-Only)
| Script | Mode | Result |
|--------|------|--------|
| scripts/144_md_repo_current_state_summary.py --md | Read | Fresh state generated |
| scripts/119_md_formalization_validate.py --md --release-mode | Read | 40 PASS / 5 WARN / 0 FAIL |
| scripts/152_fna_episode_bethesda_backfill_from_cytology.py --md --dry-run | Dry-run | 0 candidates |
| scripts/154_fna_cytology_bethesda_from_path_text.py --md --dry-run | Dry-run | 0 candidates |

## Commit
- **SHA:** `3b1bf01024eb4820b348d1a7439f74ed2d38e8be`
- **Files committed:** study artifacts only (no code changes)
