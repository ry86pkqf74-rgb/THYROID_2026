# Repo Truth Synchronization Report

**Date:** 2026-03-13

---

## Version Synchronization

| Location | Version | Status |
|----------|---------|--------|
| dashboard.py `_APP_VERSION` | v3.2.0-2026.03.13 | CURRENT |
| RELEASE_NOTES.md latest | v2026.03.13-post-hardening-cleanup | CURRENT |
| CITATION.cff version | 2026.03.10 → **UPDATED to 2026.03.13** | FIXED |
| CITATION.cff date-released | 2026-03-10 → **UPDATED to 2026-03-13** | FIXED |
| Git tag | v2026.03.10-publication-ready | PRESERVED (historical) |
| README.md | v2026.03.13 | CURRENT |

## Claim Corrections

### Fixed This Session

1. **CITATION.cff**: Updated version from 2026.03.10 to 2026.03.13 and date from
   2026-03-10 to 2026-03-13
2. **License clarity**: CITATION.cff says MIT; README says private research data.
   Added note to CITATION.cff clarifying that the code is MIT-licensed but the
   underlying research data are proprietary and not redistributable.
3. **README lineage reference**: Tier 4 now references `patient_refined_master_clinical_v12`
   (was stale at v9)

### Claims That Are Now Accurate

- "578 MotherDuck tables" — verified via `information_schema.tables`
- "10,871 manuscript cohort" — verified via `manuscript_cohort_v1`
- "4,136 analysis-eligible cancer patients" — verified
- "13-phase extraction pipeline" — verified (v1 through v11 engines)
- "7/7 readiness gates PASS" — verified

### Claims Removed or Softened

- Removed any implication of "fully extracted" note coverage
- Clarified that note coverage is ~50% of patients
- Clarified that operative NLP enrichment fields are at 0% due to pipeline architecture
- Clarified that recurrence dates are mostly unresolved (88.8%)

## Documentation Status

| Document | Status |
|----------|--------|
| README.md | CURRENT (updated CITATION version reference) |
| RELEASE_NOTES.md | CURRENT (8 release entries) |
| MANUSCRIPT_READY_CHECKLIST.md | CURRENT (all gates checked) |
| CITATION.cff | FIXED (version + date + license note) |
| docs/REPO_STATUS.md | CURRENT (honest "not dataset-mature" hedge) |
| docs/statistical_analysis_plan_thyroid_manuscript.md | CURRENT (909 lines) |
| docs/analysis_resolved_layer.md | CURRENT (18 KB architecture doc) |
