# Canonical Lakehouse Finalization — Final Handoff

**Date:** 2026-04-14  
**Branch:** `canonical-lakehouse-finalization-20260414T021944Z`  
**Before SHA:** `ac8642e833c7b24327fcef46c338af8e0b88a9d9`

---

## VERDICT

**single SSOT achieved with documented governance blocker**

---

## Success criteria assessment

| Criterion | Status | Evidence |
|-----------|--------|----------|
| A. One documented canonical live source of truth | **PASS** | `docs/final_source_of_truth_contract.md` + `MANUSCRIPT_DATA_START_HERE.md` |
| B. One documented manuscript-facing dataset contract | **PASS** | `MANUSCRIPT_DATA_START_HERE.md` — exact tables, views, citation rules |
| C. Every in-scope final fact linked to provenance/source lineage | **PASS** | 55,500/55,500 facts have domain, run_id, method, timestamp, fact_id (100%) |
| D. Live counts, verified views, lineage views, release manifest agree | **PASS** | 119 --release-mode: 40 PASS / 5 WARN / 0 FAIL |
| E. Historical artifacts available but clearly labeled | **PASS** | 21 legacy docs bannered as HISTORICAL / SUPERSEDED |
| F. Final handoff package showing manuscript readiness | **PASS** | This document |
| G. Governance human validation: if missing, stated plainly | **PASS (blocker documented)** | See governance section below |

## Live MotherDuck state (verified 2026-04-14)

| Object | Rows |
|--------|-----:|
| canonical_extracted_fact_long_v2 | 55,500 |
| canonical_fact_quarantine_v2 | 199 |
| note_extraction_runs | 3 |
| master_fact_long_verified_v1 | 55,500 |
| master_patient_rollup_verified_v1 | 5,141 |
| master_source_lineage_v1 | 55,500 |
| longitudinal_lab_canonical_v1 | 116,932 |
| longitudinal_lab_deduped_v | 108,680 |
| specimen_master_v1 | 10,139 |
| specimen_tumor_focus_v1 | 11,103 |
| specimen_genomic_assay_v1 | 10,370 |
| fhir_bundle_specimen_export_v1 | 10,139 |

## Parity (Check 11b)

- canonical_facts == master_facts == lineage_rows: **55,500** (PASS)
- patients in rollup == distinct research_id in master: **5,141** (PASS)
- Duplicate fact_ids: **0** (PASS)
- Duplicate natural keys: **542 groups** (WARN — documented as legitimate multi-entity/lab grain)
- release_tag alignment: `20260411` matches latest `qa.release_manifest` (PASS)

## Lineage completeness

- canonical_domain: 55,500/55,500 (100%)
- extraction_run_id: 55,500/55,500 (100%)
- extraction_method: 55,500/55,500 (100%)
- extracted_at: 55,500/55,500 (100%)
- fact_id: 55,500/55,500 (100%)

## Governance blocker

| Metric | Value |
|--------|-------|
| MRQ total rows | 5,622 |
| MRQ pending (NULL status) | 0 |
| MRQ auto_accepted_standard | 3,081 |
| MRQ auto_accepted_critical_sample_ok | 1,646 |
| MRQ auto_accepted_informational | 893 |
| MRQ confirmed_correct | 2 |
| **True human-reviewed rows** | **0** |
| Promotion decisions (batch-level) | 6 (all automation-tier) |

**Blocker:** No named human reviewer has adjudicated any MRQ row. All statuses are automation-tier. Worklist CSVs exist but contain no completed review decisions. See `artifacts/governance_blocker_dossier.md` for full evidence.

**Impact:** Cannot claim "manuscript-grade validated" or "human-reviewed." Can claim "technically validated" (structural, parity, lineage).

## Specimen/FHIR (validated adjunct)

- 10,155 open genomic linkage review items (WARN, not FAIL)
- 1 pending specimen merge review item
- Check 13: PASS (QA diagnostics clean, no broken FHIR refs)

## Deliverables created

1. `MANUSCRIPT_DATA_START_HERE.md` (new — analyst quick-start)
2. `docs/final_source_of_truth_contract.md` (strengthened — scope inventory, claims, definitions)
3. `studies/CURRENT_MOTHERDUCK_REPO_STATE.md` (refreshed from live)
4. `scripts/144_md_repo_current_state_summary.py` (added note_extraction_runs count)
5. `tests/test_canonical_finalization.py` (19 tests, all pass)
6. `exports/full_canonical_release_20260408r4/` (canonical export bundle with checksums)
7. `studies/canonical_lakehouse_finalization_20260414T021944Z/` (working directory with before/after state, SQL probes, LLM audit, governance dossier)
8. Historical/superseded banners on 21 legacy docs
9. Top-level narrative unified (README, REPO_STATUS, truth_sync_summary, RELEASE_NOTES)

## Assumptions made

1. The 542 duplicate natural-key groups are legitimate multi-entity/lab grain (documented in contract)
2. `source_file_id` being NULL for all facts is expected (column exists but was not populated for these extraction batches)
3. The 2 `confirmed_correct` MRQ rows lack reviewer identity and are treated as automation-tier
4. Specimen/FHIR review burden (10,155 open) is WARN-level, not a manuscript blocker
5. `note_extraction_runs.run_id` (UUID) is a different tracking level than `canonical_extracted_fact_long_v2.extraction_run_id` (hex hash per batch)

## Recommended next steps

1. **Close governance blocker:** Assign named reviewer(s) to MRQ rows (or representative sample); import decisions via `126 --decisions-csv`
2. **Refresh checked-in manifest:** Run `scripts/145_export_release_manifest_pointer.py --md` if checked-in JSON should track live
3. **Specimen genomic review:** Triage 10,155 open items via `120 --md` export or Streamlit UI
